//! Ignored network probe: real H3 producer ownership, not Product integration.

use super::super::Endpoint;
use super::*;
use crate::mux::MuxLimits;
use crate::protocol::StreamId;
use crate::transport::encrypted::{TcpClientTlsConfig, TcpServerTlsConfig};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::time::{Instant, sleep_until, timeout};

const BODY_DURATION: Duration = Duration::from_secs(25);
const BODY_STREAM: StreamId = StreamId(37);
const BODY_BYTE: u8 = 0x73;
const ECHO_COUNT: usize = 50;

/// Invoke exactly one role per process. Both arms use default transport limits,
/// the same finite H3 future, and the same one-operation native E/P boundary.
/// Only ownership of that server future changes; no native flags are supplied.
#[tokio::test(flavor = "current_thread")]
#[ignore = "requires the predeclared reflection network and paired processes"]
async fn native_driven_h3_network_probe() {
    let role = std::env::var("MPTUNNEL_SOURCE_PROBE_ROLE").expect("server/client probe role");
    let driven = match std::env::var("MPTUNNEL_SOURCE_PROBE_DRIVEN").as_deref() {
        Ok("0") => false,
        Ok("1") => true,
        _ => panic!("MPTUNNEL_SOURCE_PROBE_DRIVEN must be 0 or 1"),
    };
    timeout(Duration::from_secs(45), async {
        match role.as_str() {
            "server" => run_server(driven).await,
            "client" => run_client(driven).await,
            _ => panic!("MPTUNNEL_SOURCE_PROBE_ROLE must be server or client"),
        }
    })
    .await
    .expect("network probe process lifecycle exceeded 45 seconds");
}

async fn run_server(driven: bool) {
    let limits = CodecLimits::default();
    let mux_limits = MuxLimits::default();
    let chunk_bytes = mux_limits.max_reliable_relay_chunk_bytes;
    let endpoint = Endpoint::bind_server(
        "0.0.0.0:17443".parse().expect("server address"),
        &TcpServerTlsConfig::new(
            vec![
                CertificateDer::from_pem_file(
                    std::env::var("MPTUNNEL_SOURCE_PROBE_CERT").expect("shared certificate path"),
                )
                .expect("shared reflection certificate"),
            ],
            PrivateKeyDer::from_pem_file(
                std::env::var("MPTUNNEL_SOURCE_PROBE_KEY").expect("shared key path"),
            )
            .expect("shared reflection key"),
        )
        .expect("shared server TLS config"),
        super::super::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    eprintln!("native_source_probe_listening role=server driven={driven} port=17443");
    let connection = endpoint.accept().await.expect("accepted connection");
    let (mut send, mut recv) = connection.accept_bi().await.expect("bulk H3 request");
    assert_eq!(
        read_frame(&mut recv, limits)
            .await
            .expect("bulk warmup request"),
        Frame::Ping { nonce: 71 }
    );
    write_frame(&mut send, &Frame::Pong { nonce: 71 }, limits)
        .await
        .expect("bulk warmup response");
    let native = send.connection.clone();
    let observer = send
        .native_progress_observer()
        .expect("bulk native observer");
    let backlog = send.write_backlog.clone();
    let initial_epoch = connection.native_path_epoch();
    let native_identity = native.stable_id();
    let source_native = native.clone();
    let payload = Bytes::from(vec![BODY_BYTE; chunk_bytes]);
    let (body_done_tx, body_done_rx) = tokio::sync::oneshot::channel();
    let source = Box::pin(async move {
        let start = Instant::now();
        let until = start + BODY_DURATION;
        let mut bytes = 0u64;
        let mut operations = 0u64;
        let mut largest_envelope = 0u64;
        let mut max_unsent = 0u64;
        let mut samples = BTreeMap::new();
        while Instant::now() < until {
            let pending = observer.snapshot().expect("live native body stream");
            observer
                .wait_until_packetized(pending.accepted_end)
                .await
                .expect("exact previous-operation packetization");
            if Instant::now() >= until {
                break;
            }
            let before = observer.snapshot().expect("before H3 operation");
            assert_eq!(before.first_unpacketized, before.accepted_end);
            let frame = Frame::StreamData {
                stream_id: BODY_STREAM,
                offset: bytes,
                payload: payload.clone(),
            };
            // This exact future owns the H3 half and all partial acceptance
            // across Pending. There is no second staged body operation.
            write_frame(&mut send, &frame, limits)
                .await
                .expect("real H3 body operation");
            let after = observer.snapshot().expect("accepted H3 envelope");
            let envelope = after.accepted_end - before.accepted_end;
            let unsent = after.accepted_end - after.first_unpacketized;
            assert!(unsent <= envelope);
            largest_envelope = largest_envelope.max(envelope);
            max_unsent = max_unsent.max(unsent);
            operations += 1;
            bytes += payload.len() as u64;
            if let Some(sample) = source_native.congestion_state().latest_bandwidth_sample() {
                samples.insert(
                    sample.revision.get(),
                    (sample.valid, sample.source_round, sample.app_limited),
                );
            }
        }
        let accepted = observer.snapshot().expect("final accepted envelope");
        let final_progress = observer
            .wait_until_packetized(accepted.accepted_end)
            .await
            .expect("finite body fully packetized");
        assert_eq!(
            final_progress.accepted_end,
            final_progress.first_unpacketized
        );
        finish_stream(&mut send).await.expect("finite body H3 FIN");
        let valid_samples = samples.values().filter(|(valid, _, _)| *valid).count();
        let unmarked_samples = samples
            .values()
            .filter(|(valid, _, limited)| *valid && !*limited)
            .count();
        let qualified_rounds: BTreeSet<_> = samples
            .values()
            .filter(|(valid, _, limited)| *valid && !*limited)
            .map(|(_, round, _)| *round)
            .collect();
        body_done_tx.send((bytes, serde_json::json!({
            "driven": driven, "native_connection_identity": native_identity,
            "path_epoch_at_start": initial_epoch,
            "bytes": bytes, "chunk_bytes": chunk_bytes, "operations": operations,
            "body_source_elapsed_s": start.elapsed().as_secs_f64(),
            "largest_native_envelope_bytes": largest_envelope,
            "max_observed_native_unsent_bytes": max_unsent,
            "final_native_accepted_end": final_progress.accepted_end,
            "final_native_first_unpacketized": final_progress.first_unpacketized,
            "sampled_revisions": samples.len(), "sampled_valid_revisions": valid_samples,
            "sampled_valid_non_app_limited_revisions": unmarked_samples,
            "sampled_valid_non_app_limited_rounds": qualified_rounds,
            "sample_scope": "latest native sample at completed H3 operations; not all ACKs or rounds"
        }))).expect("body result receiver");
    });
    let separate = if driven {
        native
            .register_transmit_source(source)
            .expect("driver-owned H3 source");
        None
    } else {
        Some(tokio::spawn(source))
    };
    let control_connection = connection.clone();
    let control_task = tokio::spawn(async move {
        let (mut send, mut recv) = control_connection
            .accept_bi()
            .await
            .expect("echo H3 request");
        for nonce in 0..ECHO_COUNT as u64 {
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("echo request"),
                Frame::Ping { nonce }
            );
            write_frame(&mut send, &Frame::Pong { nonce }, limits)
                .await
                .expect("echo response");
        }
        let Frame::Ping {
            nonce: received_bytes,
        } = read_frame(&mut recv, limits)
            .await
            .expect("client body receipt acknowledgement")
        else {
            panic!("expected explicit body receipt acknowledgement")
        };
        (send, recv, received_bytes)
    });
    let (bytes, body_report) = body_done_rx.await.expect("finite body source result");
    if let Some(task) = separate {
        task.await.expect("separate body source task");
    }
    let (mut control_send, mut control_recv, received_bytes) =
        control_task.await.expect("control task");
    assert_eq!(
        received_bytes, bytes,
        "client must receive all body bytes and H3 FIN"
    );
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
    write_frame(&mut control_send, &Frame::Pong { nonce: bytes }, limits)
        .await
        .expect("confirm exact body receipt");
    assert!(
        matches!(
            read_frame(&mut control_recv, limits).await,
            Err(QuicCarrierError::StreamFinished)
        ),
        "client received confirmation before shutdown"
    );
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
    let metrics = connection.congestion_metrics();
    eprintln!(
        "native_source_probe_server {}",
        serde_json::json!({
            "body": body_report, "receipt_confirmed_bytes": received_bytes,
            "native_connection_identity": native.stable_id(), "path_epoch_at_end": metrics.path_epoch,
            "total_native_acked_bytes": metrics.total_acked_bytes,
            "flight_bytes": metrics.bytes_in_flight, "cwnd_bytes": metrics.congestion_window,
            "capacity_bps": metrics.bandwidth_estimate_bps, "pacing_bps": metrics.pacing_rate_bps,
            "app_limited_at_end": metrics.app_limited, "write_backlog_bytes": metrics.pending_bytes,
            "scope": "one H3 body FIFO plus sibling echoes; no MPP Product scheduler"
        })
    );
    connection.close();
}

async fn run_client(driven: bool) {
    let limits = CodecLimits::default();
    let endpoint = Endpoint::bind_client(
        "0.0.0.0:0".parse().expect("client address"),
        &TcpClientTlsConfig::new(
            "localhost",
            CertificateDer::from_pem_file(
                std::env::var("MPTUNNEL_SOURCE_PROBE_CERT").expect("shared certificate path"),
            )
            .expect("shared reflection certificate"),
        )
        .expect("shared client TLS config"),
        super::super::test_candidate_selector(),
        MuxLimits::default(),
    )
    .await
    .expect("client endpoint");
    let connection = endpoint
        .connect("10.238.47.20:17443".parse().expect("server address"))
        .await
        .expect("client connection");
    let (mut send, mut recv) = connection.open_bi().await.expect("bulk H3 request");
    let native = send.connection.clone();
    write_frame(&mut send, &Frame::Ping { nonce: 71 }, limits)
        .await
        .expect("bulk warmup");
    assert_eq!(
        read_frame(&mut recv, limits)
            .await
            .expect("bulk warmup response"),
        Frame::Pong { nonce: 71 }
    );
    let start = Instant::now();
    let echo_connection = connection.clone();
    let echo_task = tokio::spawn(async move {
        let (mut send, mut recv) = echo_connection.open_bi().await.expect("echo H3 request");
        let mut echoes = Vec::with_capacity(ECHO_COUNT);
        for nonce in 0..ECHO_COUNT as u64 {
            sleep_until(start + Duration::from_millis(nonce * 500)).await;
            let began = Instant::now();
            write_frame(&mut send, &Frame::Ping { nonce }, limits)
                .await
                .expect("echo request");
            assert_eq!(
                read_frame(&mut recv, limits).await.expect("echo response"),
                Frame::Pong { nonce }
            );
            echoes.push(serde_json::json!({
                "index": nonce, "start_s": began.duration_since(start).as_secs_f64(),
                "latency_ms": began.elapsed().as_secs_f64() * 1000.0
            }));
        }
        (send, recv, echoes)
    });
    let mut bytes = 0u64;
    let mut records = 0u64;
    let mut first_body_s = None;
    let mut last_body_at = None;
    let mut max_gap_s = 0.0f64;
    let mut bytes_by_second = BTreeMap::<u64, u64>::new();
    loop {
        match read_frame(&mut recv, limits).await {
            Ok(Frame::StreamData {
                stream_id,
                offset,
                payload,
            }) => {
                let now = Instant::now();
                assert_eq!(stream_id, BODY_STREAM);
                assert_eq!(offset, bytes);
                assert!(!payload.is_empty());
                assert!(payload.iter().all(|byte| *byte == BODY_BYTE));
                first_body_s.get_or_insert(now.duration_since(start).as_secs_f64());
                if let Some(previous) = last_body_at {
                    max_gap_s = max_gap_s.max(now.duration_since(previous).as_secs_f64());
                }
                last_body_at = Some(now);
                *bytes_by_second
                    .entry(now.duration_since(start).as_secs())
                    .or_default() += payload.len() as u64;
                bytes += payload.len() as u64;
                records += 1;
            }
            Err(QuicCarrierError::StreamFinished) => break,
            other => panic!("expected exact body data or H3 FIN, got {other:?}"),
        }
    }
    let body_elapsed_s = start.elapsed().as_secs_f64();
    assert!(bytes > 0);
    let (mut control_send, mut control_recv, echoes) = echo_task.await.expect("echo task");
    write_frame(&mut control_send, &Frame::Ping { nonce: bytes }, limits)
        .await
        .expect("acknowledge complete body receipt and H3 FIN");
    assert_eq!(
        read_frame(&mut control_recv, limits)
            .await
            .expect("server receipt confirmation"),
        Frame::Pong { nonce: bytes }
    );
    eprintln!(
        "native_source_probe_client {}",
        serde_json::json!({
            "driven": driven, "native_connection_identity": native.stable_id(),
            "bytes": bytes, "records": records, "first_body_s": first_body_s,
            "body_elapsed_s": body_elapsed_s, "body_mbps": bytes as f64 * 8.0 / body_elapsed_s / 1e6,
            "max_body_read_gap_s": max_gap_s, "bytes_by_second": bytes_by_second,
            "echo_successes": echoes.len(), "echoes": echoes,
            "h3_fin_received": true, "server_receipt_confirmed": true,
            "scope": "read completions after H3 warmup; read-gap maximum excludes startup and final FIN"
        })
    );
    finish_stream(&mut control_send)
        .await
        .expect("release receipt confirmation stream");
    let _ = native.closed().await;
}

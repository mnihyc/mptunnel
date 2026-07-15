//! Typed TCP capacity calibration and receiver-receipt ownership.
//!
//! The train remains a protocol transaction with an exact receipt. Writer
//! serialization is delegated to `client_writer`; publication stays here.

use super::capacity::{
    RequestTcpCapacityProbeLease, request_tcp_capacity_receipt_metrics,
    tcp_capacity_proof_validity, tcp_capacity_receipt_rate_bps,
};
use super::client_state::{
    ClientTcpCapacityProbeMeasurement, ClientTcpPathConnection, ClientTcpPathSessionRuntime,
    ClientTcpRequestReceipt,
};
use super::io::EncryptedTcpWriter;
use super::metrics::{TcpMetricPublisher, TcpSenderQueueSnapshot};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{TRANSPORT_TIMER_GRANULARITY, TcpCapacityProofCandidate};
use crate::protocol::{Frame, PathId, PathMetricDirection, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{TcpCapacityProbeCommand, TcpCapacityProbeOwner};
use bytes::Bytes;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClientTcpCapacityProbeWriteOutcome {
    NoWire,
    Measured(ClientTcpCapacityProbeMeasurement),
}

pub(super) async fn client_write_tcp_capacity_probe(
    writer: &mut EncryptedTcpWriter,
    metrics: Option<&TcpMetricPublisher>,
    probe: &TcpCapacityProbeCommand,
    max_payload_bytes: usize,
) -> Result<ClientTcpCapacityProbeWriteOutcome, RuntimeError> {
    writer.flush().await?;
    let Some(baseline_expires_at) = probe.baseline_expires_at else {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    };
    #[cfg(feature = "lab-diagnostics")]
    let baseline_wait_started_at = Instant::now();
    let (proof_started_at, _baseline) =
        match wait_for_client_tcp_write_queue_drain(metrics, baseline_expires_at).await {
            Ok(baseline) => baseline,
            Err(_) => return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire),
        };
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "request_tcp_capacity_probe",
        format_args!(
            "phase=write_boundary_ready path_id={} calibration_id={} wait_ms={} native_queue={} unacked_packets={} notsent_bytes={}",
            probe.path_id.0,
            probe.calibration_id,
            baseline_wait_started_at.elapsed().as_millis(),
            _baseline.is_some(),
            _baseline
                .and_then(|snapshot| snapshot.unacked_packets)
                .unwrap_or(0),
            _baseline.map_or(0, |snapshot| snapshot.notsent_bytes),
        ),
    );
    let Some(write_expires_at) = probe.write_expires_at else {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    };
    if Instant::now() >= write_expires_at {
        return Ok(ClientTcpCapacityProbeWriteOutcome::NoWire);
    }
    let writer_wire_baseline = writer.wire_bytes_written();
    // A short cumulative-ACK tail is not delivery-rate authority: delayed or
    // compressed ACKs can make a healthy 100-500 Mbps path look multi-gigabit.
    // The full typed train and its receiver receipt form the conservative seed;
    // same-socket TCP_INFO stays diagnostic until product ACKs replace the seed.
    let write_result =
        tokio::time::timeout_at(tokio::time::Instant::from_std(probe.expires_at), async {
            client_write_tcp_capacity_payload(
                writer,
                probe,
                probe.train_payload_bytes,
                max_payload_bytes,
            )
            .await?;
            let train_wire_bytes = writer
                .wire_bytes_written()
                .checked_sub(writer_wire_baseline)
                .filter(|bytes| *bytes > 0)
                .ok_or(RuntimeError::Protocol(
                    "request TCP capacity wire counter moved backwards",
                ))?;
            writer
                .write_frame(&Frame::PathCapacityFinish {
                    path_id: probe.path_id,
                    calibration_id: probe.calibration_id,
                    payload_bytes: probe.train_payload_bytes,
                })
                .await?;
            writer.flush().await?;
            Ok::<u64, RuntimeError>(train_wire_bytes)
        })
        .await;
    match write_result {
        Ok(Ok(train_wire_bytes)) => Ok(ClientTcpCapacityProbeWriteOutcome::Measured(
            ClientTcpCapacityProbeMeasurement {
                proof_started_at,
                train_wire_bytes,
            },
        )),
        Ok(Err(error)) => {
            if writer.wire_bytes_written() == writer_wire_baseline {
                Ok(ClientTcpCapacityProbeWriteOutcome::NoWire)
            } else {
                Err(error)
            }
        }
        Err(_) => {
            if writer.wire_bytes_written() == writer_wire_baseline {
                Ok(ClientTcpCapacityProbeWriteOutcome::NoWire)
            } else {
                Err(RuntimeError::Protocol(
                    "request TCP capacity probe timed out after a partial train",
                ))
            }
        }
    }
}

async fn client_write_tcp_capacity_payload(
    writer: &mut EncryptedTcpWriter,
    probe: &TcpCapacityProbeCommand,
    payload_bytes: u64,
    max_payload_bytes: usize,
) -> Result<(), RuntimeError> {
    let frame_payload_bytes = max_payload_bytes.max(1) as u64;
    let mut remaining = payload_bytes;
    while remaining > 0 {
        // Dequeue-time validation owns the transaction once the first record
        // can hit the wire. Later logical cancellation suppresses publication,
        // but interrupting a multi-record encrypted epoch would kill the path.
        let payload_bytes = remaining.min(frame_payload_bytes) as usize;
        writer
            .write_frame(&Frame::PathCapacityData {
                path_id: probe.path_id,
                calibration_id: probe.calibration_id,
                payload: Bytes::from(vec![0u8; payload_bytes]),
            })
            .await?;
        remaining = remaining.saturating_sub(payload_bytes as u64);
    }
    Ok(())
}

async fn wait_for_client_tcp_write_queue_drain(
    metrics: Option<&TcpMetricPublisher>,
    expires_at: Instant,
) -> Result<(Instant, Option<TcpSenderQueueSnapshot>), RuntimeError> {
    let Some(metrics) = metrics else {
        // A completed writer flush is a portable, conservative boundary. Any
        // older kernel-queued bytes only lengthen the typed receipt interval.
        return Ok((Instant::now(), None));
    };
    loop {
        // Start before getsockopt so receipt timing cannot omit syscall time.
        let observed_at = Instant::now();
        let Some(snapshot) = metrics.sender_queue_snapshot() else {
            return Ok((observed_at, None));
        };
        if snapshot.is_write_queue_drained() {
            return Ok((observed_at, Some(snapshot)));
        }
        let now = Instant::now();
        if now >= expires_at {
            return Err(RuntimeError::Protocol(
                "request TCP capacity sender write queue did not drain",
            ));
        }
        tokio::time::sleep(
            TRANSPORT_TIMER_GRANULARITY.min(expires_at.saturating_duration_since(now)),
        )
        .await;
    }
}

pub(super) async fn handle_client_tcp_capacity_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    let path_id = PathId(runtime.path_index as u16);
    match frame {
        Frame::PathCapacityData {
            path_id: capacity_path_id,
            calibration_id,
            payload,
        } if capacity_path_id == path_id => connection
            .capacity
            .record_received_data(calibration_id, payload.len())
            .map_err(Into::into),
        Frame::PathCapacityFinish {
            path_id: capacity_path_id,
            calibration_id,
            payload_bytes,
        } if capacity_path_id == path_id => {
            let received_payload_bytes = connection
                .capacity
                .finish_received_data(calibration_id, payload_bytes)?;
            connection
                .carrier
                .writer
                .write_frame(&Frame::PathCapacityReceipt {
                    path_id,
                    calibration_id,
                    received_payload_bytes,
                })
                .await?;
            connection.carrier.writer.flush().await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "tcp_capacity_receipt",
                format_args!(
                    "role=client phase=sent path_id={} calibration_id={} received_payload_bytes={}",
                    path_id.0, calibration_id, received_payload_bytes,
                ),
            );
            Ok(())
        }
        Frame::PathCapacityReceipt {
            path_id: receipt_path_id,
            calibration_id,
            received_payload_bytes,
        } if receipt_path_id == path_id => {
            let pending = match connection
                .capacity
                .take_request_receipt(calibration_id, received_payload_bytes)
            {
                ClientTcpRequestReceipt::Active(pending) => pending,
                ClientTcpRequestReceipt::Discarded => {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "request_tcp_capacity_probe",
                        format_args!(
                            "phase=discarded reason=matched_late_receipt path_index={} calibration_id={} train_bytes={}",
                            runtime.path_index, calibration_id, received_payload_bytes,
                        ),
                    );
                    return Ok(());
                }
                ClientTcpRequestReceipt::Missing => {
                    return Err(RuntimeError::Protocol(
                        "request TCP capacity receipt has no active epoch",
                    ));
                }
            };
            let TcpCapacityProbeOwner::Request {
                stream_id,
                path_instance,
            } = pending.probe.owner
            else {
                return Err(RuntimeError::Protocol(
                    "client TCP capacity receipt has response ownership",
                ));
            };
            if pending.probe.calibration_id != calibration_id
                || pending.probe.train_payload_bytes != received_payload_bytes
                || path_instance.key.underlay != UnderlayProtocol::Tcp
                || path_instance.key.index != runtime.path_index
            {
                return Err(RuntimeError::Protocol(
                    "request TCP capacity receipt does not match active epoch",
                ));
            }
            if Instant::now() >= pending.probe.expires_at
                || !pending
                    .probe
                    .request_lease()
                    .is_some_and(RequestTcpCapacityProbeLease::is_current)
            {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!(
                        "phase=discarded reason=stale_matching_receipt path_index={} calibration_id={}",
                        runtime.path_index, calibration_id,
                    ),
                );
                return Ok(());
            }
            let elapsed = pending.measurement.proof_started_at.elapsed();
            let Some(receipt_rate_bps) =
                tcp_capacity_receipt_rate_bps(received_payload_bytes, elapsed)
            else {
                return Ok(());
            };
            let native_observation =
                connection
                    .carrier
                    .tcp_metrics
                    .as_mut()
                    .and_then(|publisher| {
                        publisher.maybe_observe(path_id, PathMetricDirection::ClientToServer, true)
                    });
            #[cfg(feature = "lab-diagnostics")]
            let kernel_delivery_rate_bps = native_observation
                .and_then(super::metrics::TcpNativeObservation::delivery_rate_bps)
                .unwrap_or(0);
            #[cfg(feature = "lab-diagnostics")]
            let kernel_pacing_rate_bps = native_observation
                .and_then(super::metrics::TcpNativeObservation::pacing_rate_bps)
                .unwrap_or(0);
            // Cold request trains can be smaller than the real path BDP, so
            // native delivery remains diagnostic here. The full typed receipt
            // is the seed; product ACK evidence replaces it after handoff.
            let metrics = request_tcp_capacity_receipt_metrics(
                path_id,
                received_payload_bytes,
                receipt_rate_bps,
                Some(connection.startup_metrics),
                native_observation,
            );
            let rate_bps = metrics.delivery_rate_bps;
            let accepted_at = Instant::now();
            let validity = tcp_capacity_proof_validity(metrics);
            let candidate = TcpCapacityProofCandidate {
                token: calibration_id,
                train_bytes: pending.probe.train_payload_bytes,
                received_bytes: received_payload_bytes,
                rate_sample_bytes: received_payload_bytes,
                proof_elapsed: elapsed,
                receipt_rate_bps,
                rate_bps,
                accepted_at,
                expires_at: accepted_at.checked_add(validity).unwrap_or(accepted_at),
            };
            let accepted = runtime
                .state
                .health()
                .lock()
                .expect("client path health lock")
                .tcp
                .get_mut(runtime.path_index)
                .is_some_and(|record| {
                    record.accept_request_tcp_capacity_proof(
                        stream_id,
                        path_instance,
                        candidate,
                        metrics,
                        native_observation,
                        accepted_at,
                    )
                });
            if !accepted {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "request_tcp_capacity_probe",
                    format_args!(
                        "phase=discarded reason=proof_publication_rejected path_index={} calibration_id={}",
                        runtime.path_index, calibration_id,
                    ),
                );
                return Ok(());
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "request_tcp_capacity_probe",
                format_args!(
                    "phase=confirmed stream_id={} path_index={} instance_id={} calibration_id={} train_bytes={} train_wire_bytes={} receipt_elapsed_ms={} receipt_rate_mbps={:.3} published_rate_mbps={:.3} kernel_delivery_rate_mbps={:.3} kernel_pacing_rate_mbps={:.3} srtt_ms={:.3}",
                    stream_id.0,
                    runtime.path_index,
                    path_instance.id,
                    calibration_id,
                    received_payload_bytes,
                    pending.measurement.train_wire_bytes,
                    elapsed.as_millis(),
                    receipt_rate_bps as f64 / 1_000_000.0,
                    rate_bps as f64 / 1_000_000.0,
                    kernel_delivery_rate_bps as f64 / 1_000_000.0,
                    kernel_pacing_rate_bps as f64 / 1_000_000.0,
                    metrics.srtt_us as f64 / 1_000.0,
                ),
            );
            Ok(())
        }
        _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
    }
}

#[cfg(test)]
#[path = "client_capacity_test.rs"]
mod tests;

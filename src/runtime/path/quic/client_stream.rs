//! Client QUIC reliable-stream receive and control loop.

use super::capacity::udp_path_write_capacity_receipt;
use super::client_writer::drain_client_udp_stream_commands;
use super::io::{
    UdpPathRecvStream, UdpPathSendStream, spawn_quic_path_reader, udp_path_finish_stream,
    udp_path_write_frame,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::reliable_capacity_calibration_session_limit_bytes;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::path_capacity::CapacityReceiveTracker;
use crate::protocol::{Frame, PathId, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, recv_reliable_path_command, reliable_path_receivers_closed,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame};
use crate::runtime::path::state::ClientPathState;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

pub(super) async fn run_client_udp_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_id: StreamId,
    path_index: usize,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    reader_queue_size: usize,
    state: Arc<ClientPathState>,
    mut commands: ReliablePathCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mut carrier_frames = spawn_quic_path_reader(recv, codec_limits, reader_queue_size);
    let mut deferred_capacity_frames = std::collections::VecDeque::<Frame>::new();
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let mut capacity_receive = CapacityReceiveTracker::new(
        reliable_capacity_calibration_session_limit_bytes(mux_limits),
    );
    let path_id = PathId(path_index as u16);
    loop {
        if send.connection.capacity_probe_active() {
            let release_connection = send.connection.clone();
            tokio::select! {
                biased;
                frame = carrier_frames.recv() => {
                    match frame {
                        Some(Ok(Frame::PathCapacityReceipt {
                            path_id: receipt_path_id,
                            calibration_id,
                            received_payload_bytes,
                        })) => {
                            if receipt_path_id != path_id
                                || !send.connection.confirm_capacity_probe_receipt(
                                    calibration_id,
                                    received_payload_bytes,
                                    Instant::now(),
                                )
                            {
                                let _ = frames.send(Err(RuntimeError::Protocol(
                                    "invalid client QUIC capacity receipt",
                                ))).await;
                                return;
                            }
                            #[cfg(feature = "lab-diagnostics")]
                            lab_diagnostic(
                                "request_quic_capacity_receipt",
                                format_args!(
                                    "phase=confirmed path_id={} stream_id={} calibration_id={} received_payload_bytes={}",
                                    path_id.0, stream_id.0, calibration_id, received_payload_bytes,
                                ),
                            );
                        }
                        Some(Ok(Frame::PathCapacityData {
                            path_id: capacity_path_id,
                            calibration_id,
                            payload,
                        })) => {
                            if capacity_path_id != path_id
                                || capacity_receive
                                    .record_data(calibration_id, payload.len())
                                    .is_err()
                            {
                                let _ = frames.send(Err(RuntimeError::Protocol(
                                    "invalid simultaneous client QUIC capacity data",
                                ))).await;
                                return;
                            }
                        }
                        Some(Ok(Frame::PathCapacityFinish {
                            path_id: capacity_path_id,
                            calibration_id,
                            payload_bytes,
                        })) => {
                            if capacity_path_id != path_id {
                                let _ = frames.send(Err(RuntimeError::Protocol(
                                    "simultaneous client QUIC capacity finish path mismatch",
                                ))).await;
                                return;
                            }
                            let received_payload_bytes = match capacity_receive
                                .finish(calibration_id, payload_bytes)
                            {
                                Ok(bytes) => bytes,
                                Err(err) => {
                                    let _ = frames.send(Err(err.into())).await;
                                    return;
                                }
                            };
                            if let Err(err) = udp_path_write_capacity_receipt(
                                &mut send,
                                path_id,
                                calibration_id,
                                received_payload_bytes,
                                codec_limits,
                            ).await {
                                let _ = frames.send(Err(err)).await;
                                return;
                            }
                        }
                        Some(Ok(frame)) => {
                            if deferred_capacity_frames.len() >= reader_queue_size {
                                let _ = frames.send(Err(RuntimeError::Protocol(
                                    "client QUIC capacity receipt defer queue exceeded",
                                ))).await;
                                return;
                            }
                            deferred_capacity_frames.push_back(frame);
                        }
                        Some(Err(err)) => {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                        None => {
                            let _ = frames.send(Err(RuntimeError::ReliablePathSessionClosed)).await;
                            return;
                        }
                    }
                }
                _ = release_connection.wait_for_capacity_probe_release() => {}
            }
            continue;
        }
        let command_may_recv = !reliable_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = udp_path_finish_stream(&mut send);
            return;
        }
        if let Some(command) = try_recv_reliable_path_priority_command(&mut commands) {
            let result = drain_client_udp_stream_commands(
                command,
                &mut commands,
                &mut send,
                stream_id,
                codec_limits,
                mux_limits,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            match result {
                Ok(false) => {}
                Ok(true) => return,
                Err(err) => {
                    let _ = frames.send(Err(err)).await;
                    return;
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            frame = async {
                match deferred_capacity_frames.pop_front() {
                    Some(frame) => Some(Ok::<Frame, RuntimeError>(frame)),
                    None => carrier_frames.recv().await,
                }
            } => {
                match frame {
                    Some(Ok(Frame::Ping { nonce })) => {
                        if let Err(err) = udp_path_write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        if let Err(err) = udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            codec_limits,
                        ).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(record) = state
                                .health()
                                .lock()
                                .expect("client path health lock")
                                .udp
                                .get_mut(path_index)
                        {
                            record.mark_path_proof_success(observation);
                        }
                    }
                    Some(Ok(Frame::PathCapacityData {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload,
                    })) => {
                        if capacity_path_id != path_id
                            || capacity_receive
                                .record_data(calibration_id, payload.len())
                                .is_err()
                        {
                            let _ = frames.send(Err(RuntimeError::Protocol(
                                "invalid QUIC capacity data epoch",
                            ))).await;
                            return;
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "quic_capacity_data_received",
                            format_args!(
                                "role=client path_id={} stream_id={} calibration_id={} payload_bytes={}",
                                path_id.0,
                                stream_id.0,
                                calibration_id,
                                payload.len(),
                            ),
                        );
                    }
                    Some(Ok(Frame::PathCapacityFinish {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload_bytes,
                    })) => {
                        if capacity_path_id != path_id {
                            let _ = frames.send(Err(RuntimeError::Protocol(
                                "QUIC capacity finish path mismatch",
                            ))).await;
                            return;
                        }
                        let received_payload_bytes = match capacity_receive
                            .finish(calibration_id, payload_bytes)
                        {
                            Ok(bytes) => bytes,
                            Err(err) => {
                                let _ = frames.send(Err(err.into())).await;
                                return;
                            }
                        };
                        if let Err(err) = udp_path_write_capacity_receipt(
                            &mut send,
                            path_id,
                            calibration_id,
                            received_payload_bytes,
                            codec_limits,
                        ).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "quic_capacity_receipt",
                            format_args!(
                                "role=client phase=sent path_id={} stream_id={} calibration_id={} received_payload_bytes={}",
                                path_id.0,
                                stream_id.0,
                                calibration_id,
                                received_payload_bytes,
                            ),
                        );
                    }
                    Some(Ok(Frame::PathCapacityReceipt { .. })) => {
                        let _ = frames.send(Err(RuntimeError::Protocol(
                            "client QUIC path received server capacity receipt",
                        ))).await;
                        return;
                    }
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(frame @ Frame::PathStatus { .. })) => {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Frame::SessionClose { reason })) => {
                        let _ = frames.send(Err(RuntimeError::RemoteClosed(reason))).await;
                        return;
                    }
                    Some(Ok(_)) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol("unexpected QUIC UDP path reliable stream frame")))
                            .await;
                        return;
                    }
                    Some(Err(err)) => {
                        let _ = frames.send(Err(err)).await;
                        return;
                    }
                    None => {
                        let _ = frames.send(Err(RuntimeError::ReliablePathSessionClosed)).await;
                        return;
                    }
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands) {
                    let result = drain_client_udp_stream_commands(
                        command,
                        &mut commands,
                        &mut send,
                        stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        )
                    .await;
                    match result {
                        Ok(false) => {}
                        Ok(true) => return,
                        Err(err) => {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_client_udp_stream_commands(
                            command,
                            &mut commands,
                            &mut send,
                            stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
                        match result {
                            Ok(false) => {}
                            Ok(true) => return,
                            Err(err) => {
                                let _ = frames.send(Err(err)).await;
                                return;
                            }
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

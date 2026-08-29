//! Client QUIC reliable-stream receive and control loop.

use super::client_writer::drain_client_udp_stream_commands;
use super::io::{
    UdpPathRecvStream, UdpPathSendStream, spawn_quic_path_reader, udp_path_finish_stream,
    udp_path_input_finished, udp_path_write_frame,
};
use crate::model::path::CarrierPathInstanceId;
use crate::mux::MuxLimits;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, PathId, PathUsage, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, recv_reliable_path_command, reliable_path_receivers_closed,
    try_recv_reliable_path_command, try_recv_reliable_path_priority_command,
};
use crate::runtime::path::proof::{PathProofTracker, path_proof_ack_frame};
use crate::runtime::path::state::ClientPathState;
use std::sync::Arc;
use tokio::sync::mpsc;

#[allow(clippy::too_many_arguments)]
pub(super) async fn run_client_udp_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_id: StreamId,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    reader_queue_size: usize,
    state: Arc<ClientPathState>,
    mut commands: ReliablePathCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mut carrier_frames = spawn_quic_path_reader(recv, codec_limits, reader_queue_size);
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::from_limits(mux_limits);
    let mut deferred_input: Option<Result<Frame, RuntimeError>> = None;
    // A peer FIN closes only the QUIC receive half. Final Data ACK and local
    // FIN work must retain this actor's independent send half.
    let mut carrier_input_open = true;
    let mut product_terminal_received = false;
    let path_id = PathId(path_index as u16);
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = udp_path_finish_stream(&mut send).await;
            return;
        }
        if let Some(input) = deferred_input.take() {
            if input.as_ref().is_err_and(udp_path_input_finished) {
                if !product_terminal_received {
                    let _ = frames
                        .send(Err(RuntimeError::ReliablePathSessionClosed))
                        .await;
                }
                carrier_input_open = false;
                continue;
            }
            product_terminal_received |= client_udp_product_terminal(&input, stream_id);
            if let Err(err) = handle_client_udp_stream_input(
                input,
                &mut send,
                stream_id,
                path_index,
                path_instance_id,
                path_id,
                codec_limits,
                &state,
                &mut path_proofs,
                &frames,
            )
            .await
            {
                let _ = frames.send(Err(err)).await;
                return;
            }
            continue;
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
                &mut carrier_frames,
                &frames,
                &mut deferred_input,
                carrier_input_open,
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
            frame = carrier_frames.recv(), if carrier_input_open => {
                let input = frame.unwrap_or(Err(RuntimeError::ReliablePathSessionClosed));
                if input.as_ref().is_err_and(udp_path_input_finished) {
                    if !product_terminal_received {
                        let _ = frames
                            .send(Err(RuntimeError::ReliablePathSessionClosed))
                            .await;
                    }
                    carrier_input_open = false;
                    continue;
                }
                product_terminal_received |= client_udp_product_terminal(&input, stream_id);
                if let Err(err) = handle_client_udp_stream_input(
                    input,
                    &mut send,
                    stream_id,
                    path_index,
                    path_instance_id,
                    path_id,
                    codec_limits,
                    &state,
                    &mut path_proofs,
                    &frames,
                ).await {
                    let _ = frames.send(Err(err)).await;
                    return;
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
                            &mut carrier_frames,
                            &frames,
                            &mut deferred_input,
                            carrier_input_open,
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
                if let Some(command) = command {
                    let result = drain_client_udp_stream_commands(
                        command,
                        &mut commands,
                        &mut send,
                        stream_id,
                        codec_limits,
                        mux_limits,
                        &mut pending_frames,
                        &mut path_proofs,
                        &mut carrier_frames,
                        &frames,
                        &mut deferred_input,
                        carrier_input_open,
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
            }
        }
    }
}

fn client_udp_product_terminal(input: &Result<Frame, RuntimeError>, stream_id: StreamId) -> bool {
    matches!(
        input,
        Ok(Frame::StreamFin {
            stream_id: terminal_stream_id,
            ..
        } | Frame::StreamReset {
            stream_id: terminal_stream_id,
            ..
        }) if *terminal_stream_id == stream_id
    )
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_udp_stream_input(
    input: Result<Frame, RuntimeError>,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    path_id: PathId,
    codec_limits: CodecLimits,
    state: &ClientPathState,
    path_proofs: &mut PathProofTracker,
    frames: &mpsc::Sender<Result<Frame, RuntimeError>>,
) -> Result<(), RuntimeError> {
    match input? {
        Frame::Ping { nonce } => {
            udp_path_write_frame(send, &Frame::Pong { nonce }, codec_limits).await?;
        }
        Frame::PathProofData {
            path_id: proof_path_id,
            proof_id,
            payload,
        } if proof_path_id == path_id => {
            udp_path_write_frame(
                send,
                &path_proof_ack_frame(path_id, proof_id, payload.len()),
                codec_limits,
            )
            .await?;
        }
        Frame::PathProofAck {
            path_id: proof_path_id,
            proof_id,
            payload_bytes,
        } if proof_path_id == path_id => {
            if let Some(observation) = path_proofs.acknowledge(path_id, proof_id, payload_bytes) {
                let _ = state.mutate_path_model(
                    crate::model::path::RelayPathKey {
                        underlay: crate::protocol::UnderlayProtocol::Udp,
                        index: path_index,
                    },
                    |record| record.mark_path_proof_success(observation),
                );
            }
        }
        Frame::PathCapacityData { .. }
        | Frame::PathCapacityFinish { .. }
        | Frame::PathCapacityReceipt { .. } => {
            return Err(RuntimeError::Protocol(
                "PATH_CAPACITY frames are not valid on QUIC carriers",
            ));
        }
        frame @ (Frame::StreamData {
            stream_id: received_stream_id,
            ..
        }
        | Frame::StreamAck {
            stream_id: received_stream_id,
            ..
        }
        | Frame::StreamMaxData {
            stream_id: received_stream_id,
            ..
        }
        | Frame::StreamFin {
            stream_id: received_stream_id,
            ..
        }
        | Frame::StreamReset {
            stream_id: received_stream_id,
            ..
        }) if received_stream_id == stream_id => {
            frames
                .send(Ok(frame))
                .await
                .map_err(|_| RuntimeError::ReliablePathSessionClosed)?;
        }
        Frame::StreamDetach {
            stream_id: detached_stream_id,
        } if detached_stream_id == stream_id => return Err(RuntimeError::ReliablePathRetired),
        Frame::PathStatus {
            path_id: status_path_id,
            sequence,
            usage,
        } => {
            apply_client_udp_path_status(
                state,
                path_index,
                path_instance_id,
                path_id,
                status_path_id,
                sequence,
                usage,
            )?;
        }
        Frame::SessionClose { reason } => {
            let reason = state.session_lifecycle().retire(reason);
            return Err(RuntimeError::RemoteClosed(reason));
        }
        _ => {
            return Err(RuntimeError::Protocol(
                "unexpected QUIC UDP path reliable stream frame",
            ));
        }
    }
    Ok(())
}

pub(super) fn apply_client_udp_path_status(
    state: &ClientPathState,
    path_index: usize,
    path_instance_id: CarrierPathInstanceId,
    expected_path_id: PathId,
    status_path_id: PathId,
    sequence: u64,
    usage: PathUsage,
) -> Result<bool, RuntimeError> {
    if status_path_id != expected_path_id {
        return Err(RuntimeError::Protocol(
            "QUIC path usage advertisement path mismatch",
        ));
    }
    Ok(state.update_peer_path_usage(
        UnderlayProtocol::Udp,
        path_index,
        path_instance_id,
        sequence,
        usage,
    ))
}

#[cfg(test)]
#[path = "tests_client_stream.rs"]
mod tests;

//! Inbound reliable TCP frame classification and delegation.
//!
//! This owner validates path-level frame roles, then delegates product stream
//! lifecycle and typed capacity receipts to their respective state owners.

use super::capacity::handle_client_tcp_capacity_frame;
use super::datagram::ClientTcpDatagramState;
use super::state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::stream::{
    ClientTcpPathStreamState, expire_client_tcp_pending_opens, handle_client_tcp_stream_frame,
};
use crate::protocol::{Frame, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::proof::path_proof_ack_frame;
use crate::runtime::recent_ids::RecentIdCache;
use std::collections::HashMap;

pub(in crate::runtime::path::tcp) async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    datagrams: &mut ClientTcpDatagramState,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    connection.carrier.refresh_liveness();
    expire_client_tcp_pending_opens(connection, streams, closed_streams).await?;
    let path_id = runtime.path_id();
    match &frame {
        Frame::PathCapacityData { .. }
        | Frame::PathCapacityFinish { .. }
        | Frame::PathCapacityReceipt { .. } => {}
        Frame::PathProofAck {
            path_id: proof_path_id,
            ..
        } if *proof_path_id == path_id => {
            runtime.observe_sender_transport_state(connection, true);
        }
        _ => runtime.observe_sender_transport_state(connection, false),
    }
    match frame {
        frame @ (Frame::StreamMaxData { .. }
        | Frame::StreamReset { .. }
        | Frame::StreamData { .. }
        | Frame::StreamAck { .. }
        | Frame::StreamFin { .. }
        | Frame::StreamDetach { .. }) => {
            handle_client_tcp_stream_frame(frame, connection, streams, closed_streams, runtime)
                .await
        }
        frame @ (Frame::DatagramData { .. }
        | Frame::DatagramFeedback { .. }
        | Frame::DatagramClose { .. }) => datagrams.route_inbound(frame),
        Frame::Ping { nonce } => {
            connection
                .carrier
                .writer
                .write_frame(&Frame::Pong { nonce })
                .await?;
            connection.carrier.writer.flush().await?;
            Ok(())
        }
        Frame::PathProofData {
            path_id: proof_path_id,
            proof_id,
            payload,
        } if proof_path_id == path_id => {
            connection
                .carrier
                .writer
                .write_frame(&path_proof_ack_frame(path_id, proof_id, payload.len()))
                .await?;
            connection.carrier.writer.flush().await?;
            Ok(())
        }
        Frame::PathProofAck {
            path_id: proof_path_id,
            proof_id,
            payload_bytes,
        } if proof_path_id == path_id => {
            if let Some(observation) =
                connection
                    .path_proofs
                    .acknowledge(path_id, proof_id, payload_bytes)
            {
                let _ = runtime.state.mutate_path_eligibility(
                    crate::model::path::RelayPathKey {
                        underlay: crate::protocol::UnderlayProtocol::Tcp,
                        index: runtime.path_index,
                    },
                    |record| record.mark_path_proof_success(observation),
                );
            }
            Ok(())
        }
        frame @ (Frame::PathCapacityData { .. }
        | Frame::PathCapacityFinish { .. }
        | Frame::PathCapacityReceipt { .. }) => {
            handle_client_tcp_capacity_frame(frame, connection, runtime).await
        }
        Frame::Pong { nonce } => connection.carrier.complete_expected_heartbeat(nonce),
        Frame::PathStatus {
            path_id: status_path_id,
            sequence,
            usage,
        } if status_path_id == path_id => {
            if runtime.state.update_peer_path_usage(
                UnderlayProtocol::Tcp,
                runtime.path_index,
                connection.path_instance_id,
                sequence,
                usage,
            ) {
                connection.startup_snapshot.peer_usage = Some(usage);
            }
            Ok(())
        }
        Frame::PathStatus { .. } => Err(RuntimeError::Protocol(
            "TCP path usage advertisement path mismatch",
        )),
        Frame::PeerStatusRequest { request_id } => {
            let response =
                connection
                    .peer_status
                    .response_frame(request_id, runtime.codec_limits, || {
                        runtime.peer_status_snapshot.snapshot()
                    });
            connection.carrier.writer.write_frame(&response).await?;
            connection.carrier.writer.flush().await?;
            Ok(())
        }
        Frame::PeerStatusResponse {
            request_id,
            code,
            paths,
        } => {
            let _ = connection
                .peer_status
                .receive_response(request_id, code, paths);
            Ok(())
        }
        Frame::SessionClose { reason } => Err(RuntimeError::RemoteClosed(reason)),
        Frame::PathDrain { .. } => Err(RuntimeError::Protocol(
            "TCP client received peer path drain request",
        )),
        Frame::PathClose { .. } => Err(RuntimeError::Protocol(
            "TCP path close preceded a client drain request",
        )),
        _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
    }
}

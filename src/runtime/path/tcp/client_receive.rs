//! Inbound reliable TCP frame classification and delegation.
//!
//! This owner validates path-level frame roles, then delegates product stream
//! lifecycle and typed capacity receipts to their respective state owners.

use super::client_capacity::handle_client_tcp_capacity_frame;
use super::client_state::{ClientTcpPathConnection, ClientTcpPathSessionRuntime};
use super::client_stream::{
    ClientTcpPathStreamState, expire_client_tcp_pending_opens, handle_client_tcp_stream_frame,
};
use crate::protocol::{Frame, PathId, PathMetricDirection, StreamId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::proof::path_proof_ack_frame;
use crate::runtime::recent_ids::RecentIdCache;
use std::collections::HashMap;

pub(super) async fn handle_client_tcp_path_frame(
    frame: Frame,
    connection: &mut ClientTcpPathConnection,
    streams: &mut HashMap<StreamId, ClientTcpPathStreamState>,
    closed_streams: &mut RecentIdCache<StreamId>,
    runtime: &ClientTcpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    connection.carrier.refresh_liveness();
    expire_client_tcp_pending_opens(connection, streams, closed_streams).await?;
    let path_id = PathId(runtime.path_index as u16);
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
                let transport_state =
                    connection
                        .carrier
                        .tcp_metrics
                        .as_mut()
                        .and_then(|publisher| {
                            publisher.maybe_observe(
                                path_id,
                                PathMetricDirection::ClientToServer,
                                true,
                            )
                        });
                if let Some(record) = runtime
                    .state
                    .health()
                    .lock()
                    .expect("client path health lock")
                    .tcp
                    .get_mut(runtime.path_index)
                {
                    record.mark_path_proof_success(observation);
                    if let Some(metrics) = transport_state {
                        record.mark_tcp_transport_state(metrics);
                    }
                }
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
        Frame::PathDrain { .. } | Frame::PathClose { .. } => {
            Err(RuntimeError::ReliablePathSessionClosed)
        }
        _ => Err(RuntimeError::Protocol("unexpected TCP path session frame")),
    }
}

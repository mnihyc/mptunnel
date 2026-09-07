//! A repair ordering domain, not another physical carrier or Product output.

use super::io::{
    UdpPathRecvStream, UdpPathSendStream, udp_path_input_finished, udp_path_read_frame,
    udp_path_write_frame,
};
use super::repair_binding::RepairBindings;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, StreamId};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::queue::ReliablePathRepairReceiver;
use std::future::Future;

pub(super) type RepairStreams = (UdpPathSendStream, UdpPathRecvStream);
pub(super) type QuicRepairBindings = RepairBindings<RepairStreams>;

pub(super) async fn run_repair_channel<F, R>(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    mut commands: ReliablePathRepairReceiver,
    stream_id: StreamId,
    limits: CodecLimits,
    route: F,
) -> Result<(), RuntimeError>
where
    F: FnMut(Frame) -> R,
    R: Future<Output = Result<(), RuntimeError>>,
{
    send.set_repair_priority()?;
    let write = async {
        loop {
            let Some(work) = commands.recv().await? else {
                // Parent terminal selection stops new repair work, but the
                // ordinary writer must finish its own terminal transaction.
                // The parent owns and cancels this future when that completes.
                return std::future::pending::<Result<(), RuntimeError>>().await;
            };
            udp_path_write_frame(&mut send, work.frame(), limits).await?;
            // `work` releases its exact queue/writer byte charge here, or on
            // cancellation/error. Native receipt does not settle Product debt.
        }
    };
    let read = read_repair_channel(recv, stream_id, limits, route);
    tokio::try_join!(write, read).map(|_: ((), ())| ())
}

pub(super) async fn read_repair_channel<F, R>(
    mut recv: UdpPathRecvStream,
    stream_id: StreamId,
    limits: CodecLimits,
    mut route: F,
) -> Result<(), RuntimeError>
where
    F: FnMut(Frame) -> R,
    R: Future<Output = Result<(), RuntimeError>>,
{
    loop {
        let frame = match udp_path_read_frame(&mut recv, limits).await {
            Ok(frame) => frame,
            // Native EOF closes only this receive half. The peer's ordinary
            // FIN/ACK may still be in flight on its independent stream, and
            // our repair writer remains owned by the parent.
            Err(error) if udp_path_input_finished(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        match &frame {
            Frame::StreamData {
                stream_id: owner, ..
            }
            | Frame::StreamRequalifyData {
                stream_id: owner, ..
            } if *owner == stream_id => {}
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC repair attachment frame",
                ));
            }
        }
        route(frame).await?;
    }
}

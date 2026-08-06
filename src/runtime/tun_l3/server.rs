//! Neutral server TUN pump. Host routing, firewall, DNS, and NAT stay outside
//! this packet copy boundary.

use super::ServerIpTunnelDevice;
use crate::platform::PacketDevice;
use crate::runtime::error::RuntimeError;
use bytes::BytesMut;
use futures::{SinkExt, StreamExt};
use tun_rs::async_framed::{BytesCodec, DeviceFramed};

pub(in crate::runtime) async fn run_server_tun_l3(
    inbound: String,
    service: ServerIpTunnelDevice,
    device: PacketDevice,
) -> Result<(), RuntimeError> {
    let interface = service
        .interface_name()
        .unwrap_or("host-selected interface")
        .to_string();
    let (device, mut managed) = device.into_parts();
    let framed = DeviceFramed::new(device, BytesCodec::new());
    let (mut tun_sink, mut tun_stream) = framed.split();
    let (service, mut peer_packets) = service.into_parts();
    let mut tun_writer = tokio::task::JoinSet::new();
    tun_writer.spawn(async move {
        while let Some(packet) = peer_packets.recv().await {
            tun_sink.send(BytesMut::from(packet.as_ref())).await?;
        }
        Err::<(), RuntimeError>(RuntimeError::Protocol("TUN-L3 peer packet source closed"))
    });
    if let Some(managed) = managed.as_mut() {
        managed.signal_ready();
    }
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "inbound",
        "ready",
        format_args!("{inbound}: TUN-L3 packet service ready on {interface}"),
    );
    loop {
        tokio::select! {
            packet = tun_stream.next() => {
                let Some(packet) = packet else {
                    return Err(RuntimeError::Protocol("TUN-L3 device packet source closed"));
                };
                let packet = packet?;
                let _ = service.try_send_to_peer(packet.freeze())?;
            }
            result = tun_writer.join_next() => {
                return match result {
                    Some(Ok(result)) => result,
                    Some(Err(error)) => Err(RuntimeError::TaskJoin(error)),
                    None => Err(RuntimeError::Protocol("TUN-L3 device writer stopped")),
                };
            }
        }
    }
}

//! Packet-device construction at the host/runtime boundary.
//!
//! The packet loop consumes one opaque device. Desktop hosts may create that
//! device directly, while mobile hosts inject a device already established by
//! their VPN service.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

/// Portable device-construction values supplied by a TUN ingress.
///
/// Packet processing, DNS, routing, and managed-VPN policy are intentionally
/// absent. A host provider receives only the values needed to open or identify
/// the layer-3 device.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PacketDeviceConfig<'a> {
    pub interface_name: Option<&'a str>,
    pub ipv4: Option<Ipv4Addr>,
    pub ipv4_prefix: u8,
    pub ipv4_gateway: Option<Ipv4Addr>,
    pub ipv6: Option<Ipv6Addr>,
    pub ipv6_prefix: u8,
    pub mtu: u16,
}

/// An asynchronous layer-3 packet device owned by the runtime.
///
/// The concrete backend stays private so packet processing cannot depend on
/// platform-specific device controls.
pub struct PacketDevice {
    inner: tun_rs::AsyncDevice,
    managed: Option<ManagedPacketDeviceGuard>,
}

impl PacketDevice {
    /// Takes ownership of a configured Unix TUN descriptor.
    ///
    /// This is the descriptor-based mobile embedding path: Android should pass
    /// a descriptor detached from `VpnService`. The host must configure
    /// addresses, routes, and MTU before starting the runtime. The descriptor
    /// is closed when this device is dropped.
    ///
    /// Apple's public `NEPacketTunnelFlow` is not an owned TUN descriptor. A
    /// Network Extension integration therefore needs a packet-flow adapter and
    /// must not manufacture a descriptor for this constructor.
    #[cfg(unix)]
    pub fn from_owned_fd(fd: std::os::fd::OwnedFd) -> io::Result<Self> {
        use std::os::fd::IntoRawFd;

        // SAFETY: OwnedFd guarantees that the descriptor is open and transfers
        // its sole ownership to tun-rs. The host remains responsible for
        // supplying a configured layer-3 TUN rather than an arbitrary fd.
        let device = unsafe { tun_rs::AsyncDevice::from_fd(fd.into_raw_fd()) }?;
        Ok(Self {
            inner: device,
            managed: None,
        })
    }

    pub(crate) fn into_parts(self) -> (tun_rs::AsyncDevice, Option<ManagedPacketDeviceGuard>) {
        (self.inner, self.managed)
    }
}

struct ManagedPacketDeviceState {
    live: AtomicBool,
    ready: Mutex<Option<oneshot::Sender<()>>>,
}

pub(crate) struct ManagedPacketDeviceGuard {
    state: Arc<ManagedPacketDeviceState>,
}

impl ManagedPacketDeviceGuard {
    pub(crate) fn signal_ready(&mut self) {
        let sender = self
            .state
            .ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(sender) = sender {
            let _ = sender.send(());
        }
    }
}

impl Drop for ManagedPacketDeviceGuard {
    fn drop(&mut self) {
        self.state.live.store(false, Ordering::Release);
        // Cancel a readiness waiter if the worker died before reaching ready.
        self.state
            .ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

/// One-shot provider for a TUN prepared by the host transaction.
///
/// The provider is intentionally crate-private: callers receive it only from
/// the managed VPN lifecycle and cannot manufacture an untracked activation.
#[cfg(any(target_os = "linux", target_os = "windows"))]
pub(super) struct ManagedPacketDeviceProvider {
    device: Mutex<Option<tun_rs::AsyncDevice>>,
    state: Arc<ManagedPacketDeviceState>,
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
impl ManagedPacketDeviceProvider {
    pub(super) fn new(device: tun_rs::AsyncDevice) -> (Arc<Self>, oneshot::Receiver<()>) {
        let (ready_tx, ready_rx) = oneshot::channel();
        let state = Arc::new(ManagedPacketDeviceState {
            live: AtomicBool::new(false),
            ready: Mutex::new(Some(ready_tx)),
        });
        (
            Arc::new(Self {
                device: Mutex::new(Some(device)),
                state,
            }),
            ready_rx,
        )
    }

    pub(super) fn device_live(&self) -> bool {
        self.state.live.load(Ordering::Acquire)
    }

    pub(super) fn discard_unopened_device(&self) {
        self.device
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        self.state
            .ready
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
impl PacketDeviceProvider for ManagedPacketDeviceProvider {
    fn open(&self, _config: &PacketDeviceConfig<'_>) -> io::Result<PacketDevice> {
        let device = self
            .device
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "managed VPN owns exactly one TUN ingress",
                )
            })?;
        if self.state.live.swap(true, Ordering::AcqRel) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "managed VPN TUN worker is already running",
            ));
        }
        Ok(PacketDevice {
            inner: device,
            managed: Some(ManagedPacketDeviceGuard {
                state: self.state.clone(),
            }),
        })
    }
}

/// Creates packet devices for configured TUN ingresses.
///
/// Providers may retain host state, so one shared `Send + Sync` provider is
/// used by every client service in a combined node.
pub trait PacketDeviceProvider: Send + Sync {
    fn open(&self, config: &PacketDeviceConfig<'_>) -> io::Result<PacketDevice>;
}

/// Creates packet devices through tun-rs on platforms with a native builder.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemPacketDeviceProvider;

impl PacketDeviceProvider for SystemPacketDeviceProvider {
    fn open(&self, config: &PacketDeviceConfig<'_>) -> io::Result<PacketDevice> {
        open_system_packet_device(config)
    }
}

#[cfg(any(
    target_os = "windows",
    all(target_os = "linux", not(target_env = "ohos")),
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
))]
fn open_system_packet_device(config: &PacketDeviceConfig<'_>) -> io::Result<PacketDevice> {
    let mut builder = tun_rs::DeviceBuilder::new().mtu(config.mtu);
    if let Some(interface_name) = &config.interface_name {
        builder = builder.name((*interface_name).to_owned());
    }
    if let Some(ipv4) = config.ipv4 {
        builder = builder.ipv4(ipv4, config.ipv4_prefix, config.ipv4_gateway);
    }
    if let Some(ipv6) = config.ipv6 {
        builder = builder.ipv6(ipv6, config.ipv6_prefix);
    }
    builder.build_async().map(|inner| PacketDevice {
        inner,
        managed: None,
    })
}

#[cfg(not(any(
    target_os = "windows",
    all(target_os = "linux", not(target_env = "ohos")),
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
)))]
fn open_system_packet_device(_config: &PacketDeviceConfig<'_>) -> io::Result<PacketDevice> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this platform requires a host-provided TUN device; catch-all VPN hosts must use runtime::run_with_vpn_host_providers",
    ))
}

#[cfg(test)]
mod managed_tests {
    use super::*;

    #[test]
    fn managed_guard_signals_ready_and_reports_drop() {
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let state = Arc::new(ManagedPacketDeviceState {
            live: AtomicBool::new(true),
            ready: Mutex::new(Some(ready_tx)),
        });
        let mut guard = ManagedPacketDeviceGuard {
            state: state.clone(),
        };

        guard.signal_ready();
        assert_eq!(ready_rx.try_recv(), Ok(()));
        assert!(state.live.load(Ordering::Acquire));
        drop(guard);
        assert!(!state.live.load(Ordering::Acquire));
    }

    #[test]
    fn dropping_unready_guard_cancels_publication_barrier() {
        let (ready_tx, mut ready_rx) = oneshot::channel();
        let state = Arc::new(ManagedPacketDeviceState {
            live: AtomicBool::new(true),
            ready: Mutex::new(Some(ready_tx)),
        });

        drop(ManagedPacketDeviceGuard { state });
        assert!(matches!(
            ready_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }
}

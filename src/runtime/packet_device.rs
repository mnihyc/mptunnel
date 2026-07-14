//! Packet-device construction at the host/runtime boundary.
//!
//! The packet loop consumes one opaque device. Desktop hosts may create that
//! device directly, while mobile hosts inject a device already established by
//! their VPN service.

use crate::ingress::tun::TunL4Config;
use std::io;

/// An asynchronous layer-3 packet device owned by the runtime.
///
/// The concrete backend stays private so packet processing cannot depend on
/// platform-specific device controls.
pub struct PacketDevice(tun_rs::AsyncDevice);

impl PacketDevice {
    /// Takes ownership of a configured Unix TUN descriptor.
    ///
    /// This is the mobile embedding path: Android should pass a descriptor
    /// detached from `VpnService`, and Apple packet-tunnel hosts should pass an
    /// equivalent owned descriptor. The host must configure addresses, routes,
    /// and MTU before starting the runtime. The descriptor is closed when this
    /// device is dropped.
    #[cfg(unix)]
    pub fn from_owned_fd(fd: std::os::fd::OwnedFd) -> io::Result<Self> {
        use std::os::fd::IntoRawFd;

        // SAFETY: OwnedFd guarantees that the descriptor is open and transfers
        // its sole ownership to tun-rs. The host remains responsible for
        // supplying a configured layer-3 TUN rather than an arbitrary fd.
        let device = unsafe { tun_rs::AsyncDevice::from_fd(fd.into_raw_fd()) }?;
        Ok(Self(device))
    }

    pub(super) fn into_inner(self) -> tun_rs::AsyncDevice {
        self.0
    }
}

/// Creates packet devices for configured TUN ingresses.
///
/// Providers may retain host state, so one shared `Send + Sync` provider is
/// used by every client service in a combined node.
pub trait PacketDeviceProvider: Send + Sync {
    fn open(&self, config: &TunL4Config) -> io::Result<PacketDevice>;
}

/// Creates packet devices through tun-rs on platforms with a native builder.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemPacketDeviceProvider;

impl PacketDeviceProvider for SystemPacketDeviceProvider {
    fn open(&self, config: &TunL4Config) -> io::Result<PacketDevice> {
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
fn open_system_packet_device(config: &TunL4Config) -> io::Result<PacketDevice> {
    let mut builder = tun_rs::DeviceBuilder::new().mtu(config.mtu);
    if let Some(name) = &config.name {
        builder = builder.name(name.clone());
    }
    if let Some(ipv4) = config.ipv4 {
        builder = builder.ipv4(ipv4, config.ipv4_prefix, config.ipv4_gateway);
    }
    if let Some(ipv6) = config.ipv6 {
        builder = builder.ipv6(ipv6, config.ipv6_prefix);
    }
    builder.build_async().map(PacketDevice)
}

#[cfg(not(any(
    target_os = "windows",
    all(target_os = "linux", not(target_env = "ohos")),
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
)))]
fn open_system_packet_device(_config: &TunL4Config) -> io::Result<PacketDevice> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "this platform requires a host-provided TUN device; use runtime::run_with_packet_device_provider",
    ))
}

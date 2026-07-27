//! Privileged-process utun packet-device acquisition for macOS.
//!
//! This is useful to a privileged helper, but it is not a substitute for the
//! entitled `NEPacketTunnelProvider` required by a supported consumer VPN.

use crate::platform::ManagedVpnConfig;
use std::ffi::CString;
use std::fmt;
use std::io;
use std::sync::Arc;
use tun_rs::DeviceBuilder;

pub struct PreparedMacosUtun {
    device: Arc<tun_rs::AsyncDevice>,
    interface_name: String,
    interface_index: u32,
}

impl PreparedMacosUtun {
    pub fn device(&self) -> &Arc<tun_rs::AsyncDevice> {
        &self.device
    }

    pub fn into_device(self) -> Arc<tun_rs::AsyncDevice> {
        self.device
    }

    pub fn interface_name(&self) -> &str {
        &self.interface_name
    }

    pub const fn interface_index(&self) -> u32 {
        self.interface_index
    }
}

pub struct MacosUtunDeviceFactory;

impl MacosUtunDeviceFactory {
    /// Allocates a new utun and configures MTU/addresses without adding routes.
    pub fn create(
        requested_name: Option<&str>,
        managed: &ManagedVpnConfig,
    ) -> Result<PreparedMacosUtun, MacosUtunCreateError> {
        if let Some(name) = requested_name {
            validate_utun_name(name)?;
        }
        let mut builder = DeviceBuilder::new()
            .associate_route(false)
            .packet_information(false)
            .mtu(managed.mtu())
            .enable(true);
        if let Some(name) = requested_name {
            builder = builder.name(name);
        }
        let device = builder
            .build_async()
            .map_err(|source| MacosUtunCreateError::Io {
                action: "allocate utun packet device",
                source,
            })?;

        let mut configured = Vec::new();
        for address in managed.addresses() {
            let result = match address {
                ipnet::IpNet::V4(network) => {
                    device.add_address_v4(network.addr(), network.prefix_len())
                }
                ipnet::IpNet::V6(network) => {
                    device.add_address_v6(network.addr(), network.prefix_len())
                }
            };
            if let Err(source) = result {
                let cleanup_failures = rollback_addresses(&device, &configured);
                return Err(MacosUtunCreateError::Address {
                    address: *address,
                    source,
                    cleanup_failures,
                });
            }
            configured.push(address.addr());
        }

        let interface_name = device.name().map_err(|source| MacosUtunCreateError::Io {
            action: "read allocated utun name",
            source,
        })?;
        let name = CString::new(interface_name.as_str())
            .map_err(|_| MacosUtunCreateError::InvalidAllocatedName)?;
        let interface_index = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if interface_index == 0 {
            return Err(MacosUtunCreateError::Io {
                action: "resolve allocated utun interface index",
                source: io::Error::last_os_error(),
            });
        }
        Ok(PreparedMacosUtun {
            device: Arc::new(device),
            interface_name,
            interface_index,
        })
    }
}

fn validate_utun_name(name: &str) -> Result<(), MacosUtunCreateError> {
    let Some(unit) = name.strip_prefix("utun") else {
        return Err(MacosUtunCreateError::InvalidRequestedName(name.to_owned()));
    };
    if unit.is_empty() || !unit.bytes().all(|byte| byte.is_ascii_digit()) || name.len() >= 16 {
        return Err(MacosUtunCreateError::InvalidRequestedName(name.to_owned()));
    }
    Ok(())
}

fn rollback_addresses(device: &tun_rs::AsyncDevice, addresses: &[std::net::IpAddr]) -> usize {
    addresses
        .iter()
        .rev()
        .filter(|address| device.remove_address(**address).is_err())
        .count()
}

#[derive(Debug)]
pub enum MacosUtunCreateError {
    InvalidRequestedName(String),
    InvalidAllocatedName,
    Address {
        address: ipnet::IpNet,
        source: io::Error,
        cleanup_failures: usize,
    },
    Io {
        action: &'static str,
        source: io::Error,
    },
}

impl fmt::Display for MacosUtunCreateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequestedName(name) => {
                write!(
                    formatter,
                    "macOS utun name {name:?} must match utun<number>"
                )
            }
            Self::InvalidAllocatedName => {
                formatter.write_str("macOS returned an invalid NUL-containing utun name")
            }
            Self::Address {
                address,
                source,
                cleanup_failures,
            } => write!(
                formatter,
                "configure utun address {address}: {source}; {cleanup_failures} address reversals failed"
            ),
            Self::Io { action, source } => write!(formatter, "{action}: {source}"),
        }
    }
}

impl std::error::Error for MacosUtunCreateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Address { source, .. } | Self::Io { source, .. } => Some(source),
            Self::InvalidRequestedName(_) | Self::InvalidAllocatedName => None,
        }
    }
}

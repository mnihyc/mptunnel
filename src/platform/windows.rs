//! Safe Wintun packet-device acquisition.
//!
//! The signed `wintun.dll` is intentionally not embedded. Release packaging
//! must provide an explicit architecture-matched path.

use crate::platform::ManagedVpnConfig;
use crate::platform::ProcessVpnEnvironment;
use crate::transport::{HostSocketHandle, HostSocketProtectionRequest, HostSocketProtector};
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tun_rs::DeviceBuilder;
use windows_sys::Win32::NetworkManagement::IpHelper::ConvertInterfaceLuidToGuid;
use windows_sys::Win32::Networking::WinSock::{
    IP_UNICAST_IF, IPPROTO_IP, IPPROTO_IPV6, IPV6_UNICAST_IF, SOCKET_ERROR, WSAGetLastError,
    setsockopt,
};
use windows_sys::core::GUID;

const MIN_RING_CAPACITY: u32 = 0x2_0000;
const MAX_RING_CAPACITY: u32 = 0x400_0000;
const DEFAULT_RING_CAPACITY: u32 = 0x20_0000;
const MAX_WINTUN_TEXT_UNITS: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowsWintunConfig {
    name: String,
    tunnel_type: String,
    guid: u128,
    dll_path: PathBuf,
    ring_capacity: u32,
}

impl WindowsWintunConfig {
    pub fn new(
        name: impl Into<String>,
        tunnel_type: impl Into<String>,
        guid: u128,
        dll_path: impl Into<PathBuf>,
    ) -> Result<Self, WindowsWintunConfigError> {
        let name = name.into();
        let tunnel_type = tunnel_type.into();
        validate_wintun_text("adapter name", &name)?;
        validate_wintun_text("tunnel type", &tunnel_type)?;
        if guid == 0 {
            return Err(WindowsWintunConfigError::ZeroGuid);
        }
        let dll_path = dll_path.into();
        if dll_path.as_os_str().is_empty() {
            return Err(WindowsWintunConfigError::EmptyDllPath);
        }
        if dll_path.to_str().is_none() {
            return Err(WindowsWintunConfigError::NonUtf8DllPath);
        }
        Ok(Self {
            name,
            tunnel_type,
            guid,
            dll_path,
            ring_capacity: DEFAULT_RING_CAPACITY,
        })
    }

    pub fn with_ring_capacity(
        mut self,
        ring_capacity: u32,
    ) -> Result<Self, WindowsWintunConfigError> {
        if !(MIN_RING_CAPACITY..=MAX_RING_CAPACITY).contains(&ring_capacity) {
            return Err(WindowsWintunConfigError::InvalidRingCapacity {
                actual: ring_capacity,
                minimum: MIN_RING_CAPACITY,
                maximum: MAX_RING_CAPACITY,
            });
        }
        self.ring_capacity = ring_capacity;
        Ok(self)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn tunnel_type(&self) -> &str {
        &self.tunnel_type
    }

    pub const fn guid(&self) -> u128 {
        self.guid
    }

    pub fn dll_path(&self) -> &Path {
        &self.dll_path
    }

    pub const fn ring_capacity(&self) -> u32 {
        self.ring_capacity
    }
}

fn validate_wintun_text(field: &'static str, value: &str) -> Result<(), WindowsWintunConfigError> {
    if value.is_empty() {
        return Err(WindowsWintunConfigError::EmptyText { field });
    }
    if value.contains('\0') {
        return Err(WindowsWintunConfigError::NulText { field });
    }
    let units = value.encode_utf16().count() + 1;
    if units > MAX_WINTUN_TEXT_UNITS {
        return Err(WindowsWintunConfigError::TextTooLong {
            field,
            actual: units,
            maximum: MAX_WINTUN_TEXT_UNITS,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowsWintunConfigError {
    EmptyText {
        field: &'static str,
    },
    NulText {
        field: &'static str,
    },
    TextTooLong {
        field: &'static str,
        actual: usize,
        maximum: usize,
    },
    ZeroGuid,
    EmptyDllPath,
    NonUtf8DllPath,
    InvalidRingCapacity {
        actual: u32,
        minimum: u32,
        maximum: u32,
    },
}

impl fmt::Display for WindowsWintunConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyText { field } => write!(formatter, "Wintun {field} must not be empty"),
            Self::NulText { field } => write!(formatter, "Wintun {field} contains NUL"),
            Self::TextTooLong {
                field,
                actual,
                maximum,
            } => write!(
                formatter,
                "Wintun {field} is {actual} UTF-16 units; maximum including terminator is {maximum}"
            ),
            Self::ZeroGuid => formatter.write_str(
                "Wintun generation GUID must be nonzero and unique for this runtime generation",
            ),
            Self::EmptyDllPath => formatter.write_str("Wintun DLL path must not be empty"),
            Self::NonUtf8DllPath => {
                formatter.write_str("tun-rs requires the Wintun DLL path to be valid UTF-8")
            }
            Self::InvalidRingCapacity {
                actual,
                minimum,
                maximum,
            } => write!(
                formatter,
                "Wintun ring capacity {actual:#x} is outside [{minimum:#x}, {maximum:#x}]"
            ),
        }
    }
}

impl std::error::Error for WindowsWintunConfigError {}

pub struct PreparedWindowsWintun {
    device: tun_rs::AsyncDevice,
    interface_index: u32,
}

impl PreparedWindowsWintun {
    pub fn device(&self) -> &tun_rs::AsyncDevice {
        &self.device
    }

    pub fn into_device(self) -> tun_rs::AsyncDevice {
        self.device
    }

    pub const fn interface_index(&self) -> u32 {
        self.interface_index
    }
}

pub struct WindowsWintunDeviceFactory;

impl WindowsWintunDeviceFactory {
    /// Creates a fresh, enabled packet session without capture routes or DNS.
    ///
    /// The caller snapshots native routes first, starts the packet worker with
    /// the returned device, and uses `TransactionalProcessVpnController` to
    /// publish capture routes only after worker readiness.
    pub fn create(
        config: &WindowsWintunConfig,
        managed: &ManagedVpnConfig,
    ) -> Result<PreparedWindowsWintun, WindowsWintunCreateError> {
        if !config.dll_path.is_file() {
            return Err(WindowsWintunCreateError::MissingDll(
                config.dll_path.clone(),
            ));
        }
        let dll_path = config
            .dll_path
            .to_str()
            .ok_or(WindowsWintunCreateError::NonUtf8DllPath)?;
        let device = DeviceBuilder::new()
            .name(config.name.clone())
            .description(config.tunnel_type.clone())
            .device_guid(config.guid)
            .wintun_file(dll_path.to_owned())
            .ring_capacity(config.ring_capacity)
            .mtu(managed.mtu())
            .enable(true)
            .build_async()
            .map_err(|source| WindowsWintunCreateError::Io {
                action: "create Wintun adapter and packet session",
                source,
            })?;

        let luid = device
            .if_luid()
            .map_err(|source| WindowsWintunCreateError::Io {
                action: "read Wintun interface LUID",
                source,
            })?;
        let mut actual_guid: GUID = unsafe { std::mem::zeroed() };
        let status = unsafe { ConvertInterfaceLuidToGuid(&luid, &mut actual_guid) };
        if status != 0 {
            return Err(WindowsWintunCreateError::Io {
                action: "resolve Wintun interface GUID",
                source: io::Error::from_raw_os_error(status as i32),
            });
        }
        let expected_guid = GUID::from_u128(config.guid);
        if !guids_equal(&actual_guid, &expected_guid) {
            return Err(WindowsWintunCreateError::AdapterIdentityCollision {
                name: config.name.clone(),
            });
        }

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
                return Err(WindowsWintunCreateError::Address {
                    address: *address,
                    source,
                    cleanup_failures,
                });
            }
            configured.push(address.addr());
        }
        let interface_index = device
            .if_index()
            .map_err(|source| WindowsWintunCreateError::Io {
                action: "read Wintun interface index",
                source,
            })?;
        Ok(PreparedWindowsWintun {
            device,
            interface_index,
        })
    }
}

fn rollback_addresses(device: &tun_rs::AsyncDevice, addresses: &[std::net::IpAddr]) -> usize {
    addresses
        .iter()
        .rev()
        .filter(|address| device.remove_address(**address).is_err())
        .count()
}

fn guids_equal(left: &GUID, right: &GUID) -> bool {
    left.data1 == right.data1
        && left.data2 == right.data2
        && left.data3 == right.data3
        && left.data4 == right.data4
}

#[derive(Debug)]
pub enum WindowsWintunCreateError {
    MissingDll(PathBuf),
    NonUtf8DllPath,
    AdapterIdentityCollision {
        name: String,
    },
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

impl fmt::Display for WindowsWintunCreateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingDll(path) => write!(
                formatter,
                "signed architecture-matched Wintun DLL is missing at {}",
                path.display()
            ),
            Self::NonUtf8DllPath => {
                formatter.write_str("tun-rs requires the Wintun DLL path to be valid UTF-8")
            }
            Self::AdapterIdentityCollision { name } => write!(
                formatter,
                "Wintun adapter name {name:?} already resolves to a different generation GUID"
            ),
            Self::Address {
                address,
                source,
                cleanup_failures,
            } => write!(
                formatter,
                "configure Wintun address {address}: {source}; {cleanup_failures} address reversals failed"
            ),
            Self::Io { action, source } => write!(formatter, "{action}: {source}"),
        }
    }
}

impl std::error::Error for WindowsWintunCreateError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Address { source, .. } | Self::Io { source, .. } => Some(source),
            Self::MissingDll(_) | Self::NonUtf8DllPath | Self::AdapterIdentityCollision { .. } => {
                None
            }
        }
    }
}

/// Binds every process-created native socket to the pre-VPN Windows egress
/// interface selected by the immutable route snapshot.
///
/// This is a generation-boundary socket option, not a packet-path callback.
/// Exact bootstrap/carrier routes remain part of the host transaction; this
/// binding additionally protects flow-scoped target and proxy sockets whose
/// remote addresses are not known when the generation is prepared.
#[derive(Debug, Clone)]
pub struct WindowsNativeSocketBinder {
    environment: Arc<ProcessVpnEnvironment>,
}

impl WindowsNativeSocketBinder {
    pub fn new(environment: Arc<ProcessVpnEnvironment>) -> Self {
        Self { environment }
    }

    pub fn environment(&self) -> &Arc<ProcessVpnEnvironment> {
        &self.environment
    }

    fn interface_index_for(
        &self,
        remote_address: IpAddr,
        bound_source: Option<IpAddr>,
    ) -> io::Result<u32> {
        if let Some(source) = bound_source.filter(|source| !source.is_unspecified()) {
            return self
                .environment
                .native_networks()
                .iter()
                .filter(|network| network.directly_connected() && network.prefix().contains(&source))
                .max_by_key(|network| network.prefix().prefix_len())
                .map(|network| network.route().interface_index().get())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::AddrNotAvailable,
                        format!(
                            "bound source {source} does not belong to the pre-VPN native route snapshot"
                        ),
                    )
                });
        }
        self.environment
            .native_route_for_address(remote_address)
            .map(|route| route.interface_index().get())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::NetworkUnreachable,
                    format!("pre-VPN route snapshot has no native route for {remote_address}"),
                )
            })
    }
}

impl HostSocketProtector for WindowsNativeSocketBinder {
    fn protect(
        &self,
        socket: HostSocketHandle<'_>,
        request: HostSocketProtectionRequest,
    ) -> io::Result<()> {
        let address = request.remote_addr.ip();
        if address.is_loopback() {
            return Ok(());
        }
        let borrowed = socket.as_socket();
        let bound_source = socket2::SockRef::from(&borrowed)
            .local_addr()
            .ok()
            .and_then(|address| address.as_socket())
            .map(|address| address.ip())
            .filter(|source| source.is_ipv4() == address.is_ipv4());
        let interface_index = self.interface_index_for(address, bound_source)?;
        apply_windows_unicast_interface(socket.as_raw_socket(), address, interface_index)
    }
}

fn apply_windows_unicast_interface(
    socket: std::os::windows::io::RawSocket,
    address: IpAddr,
    interface_index: u32,
) -> io::Result<()> {
    let (level, option, encoded_index) = windows_unicast_interface_option(address, interface_index);
    let socket = usize::try_from(socket)
        .map_err(|_| io::Error::other("Windows socket handle does not fit WinSock SOCKET"))?;
    // Windows requires network byte order for IP_UNICAST_IF and host byte
    // order for IPV6_UNICAST_IF.
    let status = unsafe {
        setsockopt(
            socket,
            level,
            option,
            encoded_index.as_ptr(),
            i32::try_from(encoded_index.len()).expect("interface index has a bounded length"),
        )
    };
    if status == SOCKET_ERROR {
        Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
    } else {
        Ok(())
    }
}

fn windows_unicast_interface_option(
    address: IpAddr,
    interface_index: u32,
) -> (i32, i32, [u8; std::mem::size_of::<u32>()]) {
    match address {
        IpAddr::V4(_) => (IPPROTO_IP, IP_UNICAST_IF, interface_index.to_be_bytes()),
        IpAddr::V6(_) => (IPPROTO_IPV6, IPV6_UNICAST_IF, interface_index.to_ne_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::{AddressFamily, ProcessNativeNetwork, ProcessNativeRoute};

    fn route(family: AddressFamily, interface_index: u32) -> ProcessNativeRoute {
        ProcessNativeRoute::new(family, interface_index, None, 10).expect("native route")
    }

    #[test]
    fn socket_binding_uses_longest_prefix_native_route() {
        let default = route(AddressFamily::Ipv4, 7);
        let specific = route(AddressFamily::Ipv4, 9);
        let network =
            ProcessNativeNetwork::new("198.51.100.0/24".parse().expect("network"), specific, true)
                .expect("native network");
        let environment =
            Arc::new(ProcessVpnEnvironment::new([default], vec![network]).expect("environment"));
        let binder = WindowsNativeSocketBinder::new(environment);

        assert_eq!(
            binder
                .interface_index_for("198.51.100.42".parse().expect("address"), None)
                .expect("specific route"),
            9
        );
        assert_eq!(
            binder
                .interface_index_for("203.0.113.42".parse().expect("address"), None)
                .expect("default route"),
            7
        );
    }

    #[test]
    fn explicit_source_binding_selects_its_native_interface() {
        let default = route(AddressFamily::Ipv4, 7);
        let source_route = route(AddressFamily::Ipv4, 11);
        let source_network =
            ProcessNativeNetwork::new("192.0.2.0/24".parse().expect("network"), source_route, true)
                .expect("source network");
        let environment = Arc::new(
            ProcessVpnEnvironment::new([default], vec![source_network]).expect("environment"),
        );
        let binder = WindowsNativeSocketBinder::new(environment);

        assert_eq!(
            binder
                .interface_index_for(
                    "203.0.113.42".parse().expect("remote"),
                    Some("192.0.2.10".parse().expect("source")),
                )
                .expect("source-selected route"),
            11
        );
    }

    #[test]
    fn interface_socket_options_use_windows_required_byte_order() {
        let index = 0x0102_0304;
        let (level, option, encoded) =
            windows_unicast_interface_option("192.0.2.1".parse().expect("IPv4"), index);
        assert_eq!((level, option), (IPPROTO_IP, IP_UNICAST_IF));
        assert_eq!(encoded, index.to_be_bytes());

        let (level, option, encoded) =
            windows_unicast_interface_option("2001:db8::1".parse().expect("IPv6"), index);
        assert_eq!((level, option), (IPPROTO_IPV6, IPV6_UNICAST_IF));
        assert_eq!(encoded, index.to_ne_bytes());
    }
}

//! Concrete carrier adapters and endpoint-local path configuration.
//!
//! TCP and QUIC share framing and host-network contracts but retain independent
//! connection, congestion, telemetry, and recovery mechanics.

mod carrier_network;
pub mod encrypted;
pub mod framed;
mod native_egress;
pub mod quic;
mod spec;
pub mod tcp;
pub(crate) mod tcp_telemetry;
pub mod udp;

#[cfg(target_os = "linux")]
pub use carrier_network::LinuxMarkedCarrierNetworkProvider;
pub(crate) use carrier_network::interleave_socket_addr_families;
pub(crate) use carrier_network::validate_carrier_resolution_port;
pub use carrier_network::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionFuture, CarrierResolutionRequest,
    CarrierSocket, CarrierSocketRequest, PreparedCarrierNetworkProvider, PreparedCarrierPath,
    ProtectedCarrierNetworkProvider, SystemCarrierNetworkProvider,
};
pub use native_egress::{
    HostSocketHandle, HostSocketProtectionRequest, HostSocketProtector, HostSocketPurpose,
    NativeEgressPurpose, NativeSocketConfigurator, NativeSocketRequest,
    ProtectedNativeSocketConfigurator, SystemNativeSocketConfigurator,
};
#[cfg(target_os = "linux")]
pub use native_egress::{LinuxMarkedNativeSocketConfigurator, LinuxSocketMarker};
pub use spec::{
    CARRIER_PATH_QUERY_KEYS, CarrierEndpoint, CarrierEndpointParseError, CarrierPortSet,
    DEFAULT_CARRIER_PORT_HOP_INTERVAL_MS, DEFAULT_QUIC_LOSS_COMPENSATION_PERCENT,
    DEFAULT_TCP_CARRIER_MAX, Endpoint, EndpointParseError, LossPolicyPercent,
    MIN_CARRIER_PORT_HOP_INTERVAL_MS, PathBinding, PathMetadata, PathPolicy, PathSpec,
    PathSpecParseError, RateHint, TcpCarrierRange, TcpCarrierRangeError,
};

//! Concrete carrier adapters and endpoint-local path configuration.
//!
//! TCP and QUIC share framing and host-network contracts but retain independent
//! connection, congestion, telemetry, and recovery mechanics.

pub(crate) mod aead;
mod carrier_network;
pub mod encrypted;
pub mod framed;
pub mod quic;
mod spec;
pub mod tcp;
pub(crate) mod tcp_telemetry;
pub mod udp;

pub(crate) use carrier_network::interleave_socket_addr_families;
pub use carrier_network::{
    CarrierNetworkProvider, CarrierPathIdentity, CarrierResolutionFuture, CarrierResolutionRequest,
    CarrierSocket, CarrierSocketRequest, SystemCarrierNetworkProvider,
};
pub use spec::{
    Endpoint, EndpointParseError, PathBinding, PathMetadata, PathPolicy, PathSpec,
    PathSpecParseError, RateHint,
};

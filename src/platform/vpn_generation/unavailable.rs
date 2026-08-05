//! Truthful process-host behavior until a target adapter is supplied.
//!
//! Android and macOS enter managed VPN operation through application-owned host
//! adapters. Neither target silently claims route/DNS mutation when that host
//! integration is unavailable.

use super::super::{PacketDeviceProvider, VpnCapability, VpnPlatform, VpnPlatformCapabilities};
use super::VpnGenerationError;
use crate::config::{AppConfig, CommandConfig};
use crate::ingress::IngressConfig;
use crate::transport::{CarrierNetworkProvider, NativeSocketConfigurator};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

fn has_managed_vpn(config: &AppConfig) -> bool {
    let CommandConfig::Node(node) = &config.command;
    node.local_ingresses.iter().any(|ingress| {
        matches!(
            &ingress.config,
            IngressConfig::TunL4(tun) if tun.managed_vpn().is_some()
        )
    })
}

pub(super) fn validate(config: &AppConfig) -> Result<(), VpnGenerationError> {
    if !has_managed_vpn(config) {
        return Ok(());
    }
    let capabilities =
        VpnPlatformCapabilities::current().ok_or(VpnGenerationError::TargetUnsupported)?;
    capabilities
        .require_built_in_on_current_target(VpnCapability::PacketDevice)
        .map_err(VpnGenerationError::Capability)
}

pub(super) async fn prepare(
    config: &AppConfig,
) -> Result<Option<PreparedVpnGeneration>, VpnGenerationError> {
    validate(config)?;
    Ok(None)
}

pub(super) struct PreparedVpnGeneration;

impl PreparedVpnGeneration {
    pub(super) fn packet_device_provider(&self) -> Arc<dyn PacketDeviceProvider> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }

    pub(super) fn carrier_network_provider(&self) -> Arc<dyn CarrierNetworkProvider> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }

    pub(super) fn native_socket_configurator(&self) -> Arc<dyn NativeSocketConfigurator> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }

    pub(super) async fn publish_when_worker_ready(
        &mut self,
        _timeout: Duration,
    ) -> Result<(), VpnGenerationError> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }

    pub(super) async fn unpublish(
        &mut self,
        _attempts: NonZeroUsize,
        _retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }

    pub(super) async fn cleanup_after_worker_stopped(
        &mut self,
        _attempts: NonZeroUsize,
        _retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        unreachable!("an unavailable process adapter cannot prepare a VPN")
    }
}

#[allow(dead_code)]
fn current_platform() -> Result<VpnPlatform, VpnGenerationError> {
    VpnPlatform::current().ok_or(VpnGenerationError::TargetUnsupported)
}

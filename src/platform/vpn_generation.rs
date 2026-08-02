//! Platform-neutral managed-VPN generation boundary.
//!
//! Process composition uses only this module. Concrete Linux, Android,
//! Windows, and macOS ownership remains in target adapters selected below.
//! Every operation runs at generation boundaries; packet and Core loops never
//! call this API.

#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use super::VpnCapabilityError;
use super::{PacketDeviceProvider, VpnPlatform};
use crate::config::AppConfig;
use crate::transport::{CarrierNetworkProvider, NativeSocketConfigurator};
use std::fmt;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
use linux as platform_impl;
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
mod unavailable;
#[cfg(not(any(target_os = "linux", target_os = "windows")))]
use unavailable as platform_impl;
#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
use windows as platform_impl;

pub(crate) fn validate(config: &AppConfig) -> Result<(), VpnGenerationError> {
    platform_impl::validate(config)
}

pub(crate) async fn prepare(
    config: &AppConfig,
) -> Result<Option<PreparedVpnGeneration>, VpnGenerationError> {
    platform_impl::prepare(config)
        .await
        .map(|prepared| prepared.map(PreparedVpnGeneration))
}

pub(crate) struct PreparedVpnGeneration(platform_impl::PreparedVpnGeneration);

impl PreparedVpnGeneration {
    pub(crate) fn packet_device_provider(&self) -> Arc<dyn PacketDeviceProvider> {
        self.0.packet_device_provider()
    }

    pub(crate) fn carrier_network_provider(&self) -> Arc<dyn CarrierNetworkProvider> {
        self.0.carrier_network_provider()
    }

    pub(crate) fn native_socket_configurator(&self) -> Arc<dyn NativeSocketConfigurator> {
        self.0.native_socket_configurator()
    }
}

/// Platform-neutral lifecycle used by process generation composition.
///
/// Unpublication and worker stop are deliberately separate operations. A
/// caller must retain the active packet runtime until `unpublish` succeeds;
/// only then may it stop the worker and invoke cleanup.
pub(crate) trait VpnGenerationLifecycle {
    fn publish_when_worker_ready(
        &mut self,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), VpnGenerationError>> + Send;

    fn unpublish(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> impl Future<Output = Result<(), VpnGenerationError>> + Send;

    fn cleanup_after_worker_stopped(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> impl Future<Output = Result<(), VpnGenerationError>> + Send;
}

impl VpnGenerationLifecycle for PreparedVpnGeneration {
    async fn publish_when_worker_ready(
        &mut self,
        timeout: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0.publish_when_worker_ready(timeout).await
    }

    async fn unpublish(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0.unpublish(attempts, retry_delay).await
    }

    async fn cleanup_after_worker_stopped(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0
            .cleanup_after_worker_stopped(attempts, retry_delay)
            .await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VpnGenerationStage {
    Validate,
    Prepare,
    Publish,
    Unpublish,
    Stop,
    Cleanup,
}

impl fmt::Display for VpnGenerationStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Validate => "validation",
            Self::Prepare => "preparation",
            Self::Publish => "publication",
            Self::Unpublish => "unpublication",
            Self::Stop => "packet-runtime stop",
            Self::Cleanup => "cleanup",
        })
    }
}

#[derive(Debug)]
pub enum VpnGenerationError {
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    Capability(VpnCapabilityError),
    #[cfg(not(any(target_os = "linux", target_os = "windows")))]
    TargetUnsupported,
    Adapter {
        platform: VpnPlatform,
        stage: VpnGenerationStage,
        detail: Box<str>,
    },
}

impl VpnGenerationError {
    #[cfg(any(target_os = "linux", target_os = "windows", test))]
    pub(crate) fn adapter(
        platform: VpnPlatform,
        stage: VpnGenerationStage,
        error: impl fmt::Display,
    ) -> Self {
        Self::Adapter {
            platform,
            stage,
            detail: error.to_string().into_boxed_str(),
        }
    }
}

impl fmt::Display for VpnGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(not(any(target_os = "linux", target_os = "windows")))]
            Self::Capability(error) => fmt::Display::fmt(error, formatter),
            #[cfg(not(any(target_os = "linux", target_os = "windows")))]
            Self::TargetUnsupported => {
                formatter.write_str("managed VPN is unavailable on this build target")
            }
            Self::Adapter {
                platform,
                stage,
                detail,
            } => write!(formatter, "{platform} managed-VPN {stage} failed: {detail}"),
        }
    }
}

impl std::error::Error for VpnGenerationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(not(any(target_os = "linux", target_os = "windows")))]
            Self::Capability(error) => Some(error),
            #[cfg(not(any(target_os = "linux", target_os = "windows")))]
            Self::TargetUnsupported => None,
            Self::Adapter { .. } => None,
        }
    }
}

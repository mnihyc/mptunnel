//! Windows implementation of the platform-neutral VPN generation boundary.

use super::super::windows_vpn::{
    PreparedWindowsVpn, WindowsVpnShutdownError, compile_windows_vpn_prepare_request,
    prepare_windows_vpn,
};
use super::super::{PacketDeviceProvider, VpnPlatform};
use super::{VpnGenerationError, VpnGenerationStage};
use crate::config::AppConfig;
use crate::transport::{CarrierNetworkProvider, NativeSocketConfigurator};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

const PLATFORM: VpnPlatform = VpnPlatform::Windows;

pub(super) fn validate(config: &AppConfig) -> Result<(), VpnGenerationError> {
    compile_windows_vpn_prepare_request(config)
        .map(|_| ())
        .map_err(|error| VpnGenerationError::adapter(PLATFORM, VpnGenerationStage::Validate, error))
}

pub(super) async fn prepare(
    config: &AppConfig,
) -> Result<Option<PreparedVpnGeneration>, VpnGenerationError> {
    let request = compile_windows_vpn_prepare_request(config).map_err(|error| {
        VpnGenerationError::adapter(PLATFORM, VpnGenerationStage::Validate, error)
    })?;
    match request {
        Some(request) => prepare_windows_vpn(request)
            .await
            .map(PreparedVpnGeneration)
            .map(Some)
            .map_err(|error| {
                VpnGenerationError::adapter(PLATFORM, VpnGenerationStage::Prepare, error)
            }),
        None => Ok(None),
    }
}

pub(super) struct PreparedVpnGeneration(PreparedWindowsVpn);

impl PreparedVpnGeneration {
    pub(super) fn packet_device_provider(&self) -> Arc<dyn PacketDeviceProvider> {
        self.0.packet_device_provider()
    }

    pub(super) fn carrier_network_provider(&self) -> Arc<dyn CarrierNetworkProvider> {
        self.0.carrier_network_provider()
    }

    pub(super) fn native_socket_configurator(&self) -> Arc<dyn NativeSocketConfigurator> {
        self.0.native_socket_configurator()
    }

    pub(super) async fn publish_when_worker_ready(
        &mut self,
        timeout: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0
            .publish_when_worker_ready(timeout)
            .await
            .map_err(|error| {
                VpnGenerationError::adapter(PLATFORM, VpnGenerationStage::Publish, error)
            })
    }

    pub(super) async fn unpublish(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0
            .unpublish(attempts, retry_delay)
            .await
            .map_err(|error| {
                VpnGenerationError::adapter(PLATFORM, VpnGenerationStage::Unpublish, error)
            })
    }

    pub(super) async fn cleanup_after_worker_stopped(
        &mut self,
        attempts: NonZeroUsize,
        retry_delay: Duration,
    ) -> Result<(), VpnGenerationError> {
        self.0
            .cleanup_after_worker_stopped(attempts, retry_delay)
            .await
            .map_err(|error| {
                let stage = match &error {
                    WindowsVpnShutdownError::PublicationStillActive { .. }
                    | WindowsVpnShutdownError::Unpublish(_) => VpnGenerationStage::Unpublish,
                    WindowsVpnShutdownError::PacketWorkerStillRunning => VpnGenerationStage::Stop,
                    WindowsVpnShutdownError::Cleanup(_) => VpnGenerationStage::Cleanup,
                };
                VpnGenerationError::adapter(PLATFORM, stage, error)
            })
    }
}

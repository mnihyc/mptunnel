use super::*;
use crate::platform::{ManagedVpnConfig, RouteMode};
use std::convert::Infallible;

fn config() -> ManagedVpnConfig {
    ManagedVpnConfig::new(
        vec!["10.88.0.1/24".parse().expect("address")],
        1500,
        RouteMode::Full,
    )
    .expect("config")
}

#[test]
fn every_platform_profile_is_explicit_and_truthful() {
    let linux = VpnPlatformCapabilities::for_platform(VpnPlatform::Linux);
    assert_eq!(linux.ownership(), VpnOwnership::ProcessManaged);
    assert_eq!(
        linux.activation(),
        VpnActivationModel::TransactionalTwoPhase
    );
    for capability in VpnCapability::ALL {
        assert_eq!(
            linux.availability(capability),
            VpnCapabilityAvailability::BuiltIn
        );
    }

    let android = VpnPlatformCapabilities::for_platform(VpnPlatform::Android);
    assert_eq!(android.ownership(), VpnOwnership::HostOwned);
    assert_eq!(android.activation(), VpnActivationModel::HostEstablished);
    assert_eq!(
        android.availability(VpnCapability::NativeSocketBypass),
        VpnCapabilityAvailability::HostRequired
    );
    assert_eq!(
        android.availability(VpnCapability::TwoPhasePublication),
        VpnCapabilityAvailability::Unsupported
    );

    let windows = VpnPlatformCapabilities::for_platform(VpnPlatform::Windows);
    assert_eq!(windows.ownership(), VpnOwnership::ProcessManaged);
    assert_eq!(
        windows.activation(),
        VpnActivationModel::TransactionalTwoPhase
    );
    for capability in [
        VpnCapability::PacketDevice,
        VpnCapability::AddressConfiguration,
        VpnCapability::RouteConfiguration,
        VpnCapability::DnsConfiguration,
        VpnCapability::NativeSocketBypass,
        VpnCapability::TwoPhasePublication,
        VpnCapability::TransactionalCleanup,
    ] {
        assert_eq!(
            windows.availability(capability),
            VpnCapabilityAvailability::BuiltIn
        );
    }

    let macos = VpnPlatformCapabilities::for_platform(VpnPlatform::Macos);
    assert_eq!(macos.ownership(), VpnOwnership::HostOwned);
    assert_eq!(
        macos.activation(),
        VpnActivationModel::TransactionalTwoPhase
    );
    for capability in VpnCapability::ALL {
        assert_eq!(
            macos.availability(capability),
            VpnCapabilityAvailability::AdapterRequired
        );
    }
}

#[test]
fn missing_integrations_return_precise_capability_errors() {
    let android = VpnPlatformCapabilities::for_platform(VpnPlatform::Android)
        .require_built_in(VpnCapability::NativeSocketBypass)
        .expect_err("Android host owns protect");
    assert!(matches!(
        android,
        VpnCapabilityError::HostIntegrationRequired {
            platform: VpnPlatform::Android,
            capability: VpnCapability::NativeSocketBypass,
        }
    ));
    assert!(android.to_string().contains("VpnService"));

    assert!(
        VpnPlatformCapabilities::for_platform(VpnPlatform::Windows)
            .require_built_in(VpnCapability::PacketDevice)
            .is_ok()
    );

    let macos = VpnPlatformCapabilities::for_platform(VpnPlatform::Macos)
        .require_built_in(VpnCapability::DnsConfiguration)
        .expect_err("macOS adapter is not implemented");
    assert!(macos.to_string().contains("utun"));
}

#[test]
fn activation_model_rejects_false_publication_claims() {
    let linux = VpnPlatformCapabilities::for_platform(VpnPlatform::Linux);
    assert!(
        linux
            .validate_prepared_publication(VpnTrafficPublication::Inert)
            .is_ok()
    );
    assert!(matches!(
        linux.validate_prepared_publication(VpnTrafficPublication::Published),
        Err(VpnLifecycleContractError::UnexpectedPreparedPublication { .. })
    ));

    let android = VpnPlatformCapabilities::for_platform(VpnPlatform::Android);
    assert!(
        android
            .validate_prepared_publication(VpnTrafficPublication::Published)
            .is_ok()
    );
    assert!(
        android
            .validate_prepared_publication(VpnTrafficPublication::Inert)
            .is_err()
    );
}

#[test]
fn lifecycle_request_canonicalizes_and_bounds_bypass_inputs() {
    let address = "203.0.113.7".parse().expect("carrier");
    let request = VpnLifecycleRequest::new(
        config(),
        [address, address],
        ["9.9.9.9".parse().expect("bootstrap")],
    )
    .expect("request");
    assert_eq!(request.carrier_endpoints(), &[address]);

    assert!(matches!(
        VpnLifecycleRequest::new(config(), ["127.0.0.1".parse().expect("loopback")], [],),
        Err(VpnLifecycleRequestError::InvalidBypassAddress {
            kind: VpnBypassAddressKind::CarrierEndpoint,
            ..
        })
    ));

    let excessive = (1..=129)
        .map(|last| IpAddr::from([198, 51, 100, last as u8]))
        .collect::<Vec<_>>();
    assert!(matches!(
        VpnLifecycleRequest::new(config(), excessive, []),
        Err(VpnLifecycleRequestError::TooManyBypassAddresses {
            kind: VpnBypassAddressKind::CarrierEndpoint,
            maximum: 128,
            ..
        })
    ));
}

#[test]
fn current_profile_matches_the_compiled_target() {
    #[cfg(target_os = "linux")]
    assert_eq!(
        VpnPlatformCapabilities::current().map(VpnPlatformCapabilities::platform),
        Some(VpnPlatform::Linux)
    );
    #[cfg(target_os = "android")]
    assert_eq!(
        VpnPlatformCapabilities::current().map(VpnPlatformCapabilities::platform),
        Some(VpnPlatform::Android)
    );
    #[cfg(target_os = "windows")]
    assert_eq!(
        VpnPlatformCapabilities::current().map(VpnPlatformCapabilities::platform),
        Some(VpnPlatform::Windows)
    );
    #[cfg(target_os = "macos")]
    assert_eq!(
        VpnPlatformCapabilities::current().map(VpnPlatformCapabilities::platform),
        Some(VpnPlatform::Macos)
    );
}

struct ContractAdapter {
    capabilities: VpnPlatformCapabilities,
}

impl VpnLifecycleAdapter for ContractAdapter {
    type PacketDevice = &'static str;
    type Error = Infallible;

    fn capabilities(&self) -> VpnPlatformCapabilities {
        self.capabilities
    }

    fn prepare(
        &mut self,
        _request: &VpnLifecycleRequest,
    ) -> Result<PreparedVpn<Self::PacketDevice>, VpnLifecycleError<Self::Error>> {
        Ok(match self.capabilities.activation() {
            VpnActivationModel::TransactionalTwoPhase => PreparedVpn::inert("device"),
            VpnActivationModel::HostEstablished => PreparedVpn::host_published("host device"),
        })
    }

    fn publish(&mut self) -> Result<(), VpnLifecycleError<Self::Error>> {
        self.capabilities
            .require_built_in(VpnCapability::TwoPhasePublication)
            .map_err(Into::into)
    }

    fn unpublish(&mut self) -> Result<(), VpnLifecycleError<Self::Error>> {
        Ok(())
    }

    fn cleanup(&mut self) -> Result<(), VpnLifecycleError<Self::Error>> {
        Ok(())
    }

    fn protect_native_socket(
        &mut self,
        _socket: VpnSocketHandle,
    ) -> Result<(), VpnLifecycleError<Self::Error>> {
        self.capabilities
            .require_built_in(VpnCapability::NativeSocketBypass)
            .map_err(Into::into)
    }
}

#[test]
fn one_adapter_contract_represents_two_phase_and_host_established_lifecycles() {
    let request = VpnLifecycleRequest::new(config(), [], []).expect("request");
    let mut linux = ContractAdapter {
        capabilities: VpnPlatformCapabilities::for_platform(VpnPlatform::Linux),
    };
    let prepared = linux.prepare(&request).expect("Linux prepare");
    linux
        .capabilities()
        .validate_prepared_publication(prepared.publication())
        .expect("inert Linux device");
    linux.publish().expect("Linux built-in publication");

    let mut android = ContractAdapter {
        capabilities: VpnPlatformCapabilities::for_platform(VpnPlatform::Android),
    };
    let prepared = android.prepare(&request).expect("Android establish");
    android
        .capabilities()
        .validate_prepared_publication(prepared.publication())
        .expect("published Android device");
    assert!(matches!(
        android.publish(),
        Err(VpnLifecycleError::Capability(
            VpnCapabilityError::Unsupported {
                platform: VpnPlatform::Android,
                capability: VpnCapability::TwoPhasePublication,
            }
        ))
    ));
}

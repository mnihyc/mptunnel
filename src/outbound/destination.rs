use crate::product::{
    AclError, AuthorizedTarget, DestinationAcl, FlowContext, FlowError, InboundId, Network,
    PreResolutionApproval, PrincipalId, ProtocolTarget, SourceEndpoint,
};
use crate::protocol::TargetAddr;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

const SERVER_INBOUND_ID: &str = "mpp-server";

/// One immutable server destination-authorization generation. It is shared by
/// admission and final connectors; carrier and relay hot paths do not invoke it.
#[derive(Debug, Clone)]
pub struct ServerDestinationPolicy {
    acl: Arc<DestinationAcl>,
    inbound: InboundId,
}

/// One authenticated principal bound to the shared server ACL generation.
#[derive(Debug, Clone)]
pub struct ServerPrincipalDestinationPolicy {
    acl: Arc<DestinationAcl>,
    principal: PrincipalId,
    inbound: InboundId,
}

/// Flow-scoped authorization seam used by every native connector. Implementors
/// create the normalized Product flow, while this shared implementation binds
/// pre-resolution approval, the complete DNS answer, and the literal addresses
/// handed to the connector.
pub trait DestinationAuthorizer: Send + Sync {
    fn destination_acl(&self) -> &DestinationAcl;

    fn flow(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<FlowContext, DestinationAuthorizationError>;

    fn begin(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<DestinationAuthorization, DestinationAuthorizationError> {
        let normalized_target = protocol_target(target)?;
        let flow = self.flow(network, target)?;
        if flow.network() != network || flow.target() != &normalized_target {
            return Err(DestinationAuthorizationError::TargetChanged);
        }
        let approval = self
            .destination_acl()
            .authorize_pre_resolution(flow)
            .map_err(DestinationAuthorizationError::Acl)?;
        Ok(DestinationAuthorization {
            target: normalized_target,
            approval,
        })
    }

    fn authorize_addresses(
        &self,
        authorization: DestinationAuthorization,
        addresses: &[IpAddr],
    ) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
        authorize_addresses(self.destination_acl(), authorization, addresses)
    }
}

impl ServerDestinationPolicy {
    pub fn new(acl: DestinationAcl) -> Self {
        Self::for_inbound(
            acl,
            InboundId::parse(SERVER_INBOUND_ID).expect("static server inbound ID is valid"),
        )
    }

    pub(crate) fn for_inbound(acl: DestinationAcl, inbound: InboundId) -> Self {
        Self {
            acl: Arc::new(acl),
            inbound,
        }
    }

    pub fn generation(&self) -> u64 {
        self.acl.generation()
    }

    /// Cheap pre-resolution admission. Connectors repeat this check so a
    /// caller cannot bypass the post-resolution proof by skipping admission.
    pub fn for_principal(&self, principal: PrincipalId) -> ServerPrincipalDestinationPolicy {
        ServerPrincipalDestinationPolicy {
            acl: self.acl.clone(),
            principal,
            inbound: self.inbound.clone(),
        }
    }

    #[cfg(test)]
    pub(crate) fn test_principal_policy(&self) -> ServerPrincipalDestinationPolicy {
        self.for_principal(PrincipalId::parse("test-peer").expect("static test principal is valid"))
    }

    #[cfg(test)]
    pub(crate) fn allow_restricted_for_test() -> Self {
        use crate::product::{AclEffect, AclRuleSpec, RouteMatchSpec, RuleId};

        let acl = DestinationAcl::compile(
            1,
            vec![AclRuleSpec::new(
                RuleId::parse("test-allow-restricted")
                    .expect("static test destination ACL rule ID is valid"),
                RouteMatchSpec::default(),
                AclEffect::AllowRestricted,
            )],
        )
        .expect("static test destination ACL compiles");
        Self::new(acl)
    }
}

impl ServerPrincipalDestinationPolicy {
    /// Cheap pre-resolution admission. Connectors repeat this check so a
    /// caller cannot bypass the post-resolution proof by skipping admission.
    pub fn authorize_pre(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<(), DestinationAuthorizationError> {
        DestinationAuthorizer::begin(self, network, target).map(drop)
    }
}

impl DestinationAuthorizer for ServerPrincipalDestinationPolicy {
    fn destination_acl(&self) -> &DestinationAcl {
        &self.acl
    }

    fn flow(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<FlowContext, DestinationAuthorizationError> {
        Ok(FlowContext::new(
            network,
            protocol_target(target)?,
            SourceEndpoint::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            self.principal.clone(),
            self.inbound.clone(),
        ))
    }
}

fn authorize_addresses(
    acl: &DestinationAcl,
    authorization: DestinationAuthorization,
    addresses: &[IpAddr],
) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
    let resolution = acl
        .authorize_resolution(authorization.approval, addresses)
        .map_err(DestinationAuthorizationError::Acl)?;
    resolution
        .addresses()
        .iter()
        .copied()
        .map(|address| {
            resolution
                .authorize_connect(&authorization.target, address)
                .map_err(DestinationAuthorizationError::Acl)
        })
        .collect()
}

pub struct DestinationAuthorization {
    target: ProtocolTarget,
    approval: PreResolutionApproval,
}

fn protocol_target(target: &TargetAddr) -> Result<ProtocolTarget, DestinationAuthorizationError> {
    match target {
        TargetAddr::Domain { host, port } => ProtocolTarget::from_host_port(host, *port),
        TargetAddr::Ip(address) => ProtocolTarget::from_ip(address.ip(), address.port()),
    }
    .map_err(DestinationAuthorizationError::Target)
}

#[derive(Debug)]
pub enum DestinationAuthorizationError {
    Target(FlowError),
    Acl(AclError),
    TargetChanged,
}

impl std::fmt::Display for DestinationAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(error) => write!(formatter, "invalid Product destination: {error}"),
            Self::Acl(error) => write!(formatter, "destination denied: {error}"),
            Self::TargetChanged => {
                formatter.write_str("destination authorizer changed the normalized flow")
            }
        }
    }
}

impl std::error::Error for DestinationAuthorizationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Target(error) => Some(error),
            Self::Acl(error) => Some(error),
            Self::TargetChanged => None,
        }
    }
}

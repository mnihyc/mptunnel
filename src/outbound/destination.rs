use crate::product::{
    AclError, AuthorizedDomainTarget, AuthorizedTarget, DestinationAcl, FlowContext, FlowError,
    InboundId, Network, PreResolutionDecision, PrincipalId, ProtocolTarget, SourceEndpoint,
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
/// pre-resolution decision, the complete DNS answer, and the literal addresses
/// handed to the connector.
pub trait DestinationAuthorizer: Send + Sync {
    fn destination_acl(&self) -> &DestinationAcl;

    fn flow(
        &self,
        network: Network,
        target: &ProtocolTarget,
    ) -> Result<Arc<FlowContext>, DestinationAuthorizationError>;

    fn begin(
        &self,
        network: Network,
        target: &TargetAddr,
    ) -> Result<DestinationAuthorization, DestinationAuthorizationError> {
        let normalized_target = protocol_target(target)?;
        self.begin_target(network, &normalized_target)
    }

    fn begin_target(
        &self,
        network: Network,
        target: &ProtocolTarget,
    ) -> Result<DestinationAuthorization, DestinationAuthorizationError> {
        let flow = self.flow(network, target)?;
        if flow.network() != network || flow.target() != target {
            return Err(DestinationAuthorizationError::TargetChanged);
        }
        let decision = self
            .destination_acl()
            .evaluate_pre_resolution_shared(flow)
            .map_err(DestinationAuthorizationError::Acl)?;
        Ok(DestinationAuthorization { decision })
    }

    fn authorize_addresses(
        &self,
        authorization: DestinationAuthorization,
        addresses: &[IpAddr],
    ) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
        authorize_addresses(self.destination_acl(), authorization, addresses)
    }

    fn authorize_domain(
        &self,
        authorization: DestinationAuthorization,
    ) -> Result<AuthorizedDomainTarget, DestinationAuthorizationError> {
        self.destination_acl()
            .authorize_domain(authorization.decision)
            .map_err(DestinationAuthorizationError::Acl)
    }

    fn authorize_domain_addresses(
        &self,
        domain: &AuthorizedDomainTarget,
        addresses: &[IpAddr],
    ) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
        let resolution = self
            .destination_acl()
            .authorize_domain_resolution(domain, addresses)
            .map_err(DestinationAuthorizationError::Acl)?;
        resolution
            .addresses()
            .iter()
            .copied()
            .map(|address| {
                resolution
                    .authorize_connect(domain.flow().target(), address)
                    .map_err(DestinationAuthorizationError::Acl)
            })
            .collect()
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
    /// Cheap pre-resolution policy evaluation. Stable denials fail here;
    /// decisions requiring address evidence continue to the final connector,
    /// which performs the mandatory post-resolution check.
    pub fn evaluate_pre(
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
        target: &ProtocolTarget,
    ) -> Result<Arc<FlowContext>, DestinationAuthorizationError> {
        Ok(Arc::new(FlowContext::new(
            network,
            target.clone(),
            SourceEndpoint::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            self.principal.clone(),
            self.inbound.clone(),
        )))
    }
}

fn authorize_addresses(
    acl: &DestinationAcl,
    authorization: DestinationAuthorization,
    addresses: &[IpAddr],
) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
    let resolution = acl
        .authorize_resolution(authorization.decision, addresses)
        .map_err(DestinationAuthorizationError::Acl)?;
    resolution
        .addresses()
        .iter()
        .copied()
        .map(|address| {
            resolution
                .authorize_connect(resolution.flow().target(), address)
                .map_err(DestinationAuthorizationError::Acl)
        })
        .collect()
}

#[derive(Clone)]
pub struct DestinationAuthorization {
    decision: PreResolutionDecision,
}

impl DestinationAuthorization {
    pub(crate) fn flow(&self) -> &FlowContext {
        self.decision.flow()
    }

    pub(crate) fn target(&self) -> &ProtocolTarget {
        self.decision.flow().target()
    }

    pub(crate) const fn requires_post_resolution(&self) -> bool {
        self.decision.requires_post_resolution()
    }
}

fn protocol_target(target: &TargetAddr) -> Result<ProtocolTarget, DestinationAuthorizationError> {
    match target {
        TargetAddr::Domain { host, port } => ProtocolTarget::from_host_port(host, *port),
        TargetAddr::Ip(address) => ProtocolTarget::from_ip(address.ip(), address.port()),
    }
    .map_err(DestinationAuthorizationError::Target)
}

pub(crate) fn protocol_target_addr(target: &ProtocolTarget) -> TargetAddr {
    match target.ip() {
        Some(address) => TargetAddr::Ip(std::net::SocketAddr::new(address, target.port().get())),
        None => TargetAddr::Domain {
            host: target
                .domain()
                .expect("non-IP Product target has a domain")
                .as_str()
                .to_string(),
            port: target.port().get(),
        },
    }
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
            Self::Target(error) => write!(formatter, "invalid destination: {error}"),
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

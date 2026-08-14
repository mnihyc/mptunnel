use crate::product::{
    AuthorizedDomainTarget, AuthorizedTarget, FlowContext, FlowError, Network,
    PreResolutionDecision, ProductPolicyGeneration, ProtocolTarget, RouteAuthorizationError,
};
use crate::protocol::TargetAddr;
use std::net::IpAddr;
use std::sync::Arc;

/// Flow-scoped authorization seam used by every native connector. Implementors
/// provide one normalized flow and the immutable routing generation that must
/// authorize every address handed to the connector.
pub trait DestinationAuthorizer: Send + Sync {
    fn product_policy(&self) -> &ProductPolicyGeneration;

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
            .product_policy()
            .evaluate_pre_resolution_shared(flow)
            .map_err(DestinationAuthorizationError::Policy)?;
        Ok(DestinationAuthorization { decision })
    }

    fn authorize_addresses(
        &self,
        authorization: DestinationAuthorization,
        addresses: &[IpAddr],
    ) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
        self.product_policy()
            .authorize_resolution(authorization.decision, addresses, |_, _| true)
            .map(|resolution| resolution.into_targets())
            .map_err(DestinationAuthorizationError::Policy)
    }

    fn authorize_domain(
        &self,
        authorization: DestinationAuthorization,
    ) -> Result<AuthorizedDomainTarget, DestinationAuthorizationError> {
        self.product_policy()
            .authorize_domain(authorization.decision)
            .map_err(DestinationAuthorizationError::Policy)
    }

    fn authorize_domain_addresses(
        &self,
        domain: &AuthorizedDomainTarget,
        addresses: &[IpAddr],
    ) -> Result<Vec<AuthorizedTarget>, DestinationAuthorizationError> {
        self.product_policy()
            .authorize_domain_resolution(domain, addresses)
            .map(|resolution| resolution.into_targets())
            .map_err(DestinationAuthorizationError::Policy)
    }
}

#[derive(Clone)]
pub struct DestinationAuthorization {
    pub(crate) decision: PreResolutionDecision,
}

impl DestinationAuthorization {
    #[cfg(test)]
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
    Policy(RouteAuthorizationError),
    TargetChanged,
}

impl std::fmt::Display for DestinationAuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Target(error) => write!(formatter, "invalid destination: {error}"),
            Self::Policy(error) => write!(formatter, "destination denied: {error}"),
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
            Self::Policy(error) => Some(error),
            Self::TargetChanged => None,
        }
    }
}

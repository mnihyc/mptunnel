//! Management composition and endpoint-local path controls.
//!
//! The sampler publishes immutable typed snapshots, while `http` owns browser
//! transport details. Peer diagnostic exchange remains on authenticated MPP
//! carrier control channels and is only initiated here on an explicit request.

mod control;
mod http;
mod projection;
mod schema;
mod snapshot;

#[cfg(test)]
use self::http::{ManagementRequest, management_auth_ok};
use self::schema::ManagementSnapshot;
use self::snapshot::ManagementState;
#[cfg(test)]
use super::*;
use crate::config::ManagementConfig;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::{ClientPathContext, ServerPathContext};
use std::sync::Arc;
use std::time::Duration;

const SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

pub(super) fn spawn_client_management_services(
    config: ManagementConfig,
    context: ClientPathContext,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("client");
    let target = ManagementTarget::Client { context, state };
    spawn_management_services(config, target, services);
}

pub(super) fn spawn_server_management_services(
    config: ManagementConfig,
    context: ServerPathContext,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("server");
    let target = ManagementTarget::Server { context, state };
    spawn_management_services(config, target, services);
}

pub(super) fn spawn_node_management_services(
    config: ManagementConfig,
    clients: Vec<ClientPathContext>,
    servers: Vec<ServerPathContext>,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    let state = ManagementState::new("node");
    let target = ManagementTarget::Node {
        clients,
        servers,
        state,
    };
    spawn_management_services(config, target, services);
}

fn spawn_management_services(
    config: ManagementConfig,
    target: ManagementTarget,
    services: &mut tokio::task::JoinSet<Result<(), RuntimeError>>,
) {
    target.refresh_sample_snapshot();
    services.spawn(run_sampler(target.clone()));
    services.spawn(http::run_listeners(config, target));
}

async fn run_sampler(target: ManagementTarget) -> Result<(), RuntimeError> {
    let start = tokio::time::Instant::now() + SAMPLE_INTERVAL;
    let mut ticker = tokio::time::interval_at(start, SAMPLE_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        target.refresh_sample_snapshot();
    }
}

#[derive(Clone)]
enum ManagementTarget {
    Client {
        context: ClientPathContext,
        state: ManagementState,
    },
    Server {
        context: ServerPathContext,
        state: ManagementState,
    },
    Node {
        clients: Vec<ClientPathContext>,
        servers: Vec<ServerPathContext>,
        state: ManagementState,
    },
}

impl ManagementTarget {
    fn state(&self) -> &ManagementState {
        match self {
            Self::Client { state, .. } | Self::Server { state, .. } | Self::Node { state, .. } => {
                state
            }
        }
    }

    fn refresh_sample_snapshot(&self) {
        self.state().refresh(self, true);
    }

    fn refresh_current_snapshot(&self) {
        self.state().refresh(self, false);
    }

    fn snapshot(&self) -> Arc<ManagementSnapshot> {
        self.state().snapshot()
    }
}

#[cfg(test)]
#[path = "management_test.rs"]
mod tests;

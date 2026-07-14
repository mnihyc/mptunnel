//! Process composition for client, server, and combined-node roles.
//!
//! Node code starts long-lived services. Carrier state and scheduling policy
//! stay in their owning `path`, `stream`, and `sender` modules.

mod client;
mod combined;
pub(super) mod server;

#[cfg(test)]
pub(in crate::runtime) use client::probe_paths;

use crate::config::{AppConfig, CommandConfig};
use crate::runtime::error::RuntimeError;

pub async fn run(config: AppConfig) -> Result<(), RuntimeError> {
    match config.command {
        CommandConfig::Client(client) => {
            client::run(client, config.resources, config.management).await
        }
        CommandConfig::Server(server) => {
            server::run(
                server.bind_paths,
                server.outbound,
                server.outbound_dns,
                server.outbound_connect_timeout,
                server.security,
                server.performance,
                config.resources,
                config.management,
            )
            .await
        }
        CommandConfig::Node(node) => combined::run(node, config.resources, config.management).await,
    }
}

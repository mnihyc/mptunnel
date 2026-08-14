//! Canonical configuration API operations.
//!
//! These operations own desired-state validation and persistence only. A
//! successful change requests a clean runtime generation replacement; it
//! never writes path, scheduler, stream, or DNS actor fields in place.

use super::ManagementTarget;
use super::http::ManagementHttpError;
use crate::config::{AppConfig, CommandConfig, ConfigRevision, ConfigStoreError};
use crate::product::CredentialAuthority;
use serde_json::{Value, json};

#[derive(Debug)]
pub(super) struct ConfigApplyOutcome {
    pub(super) response: Value,
    pub(super) pending_activation: bool,
    pub(super) request_reload: bool,
}

struct DynamicConfigUpdate {
    credential_authorities: Option<Vec<CredentialAuthority>>,
}

impl ManagementTarget {
    pub(super) fn config_status_json(&self) -> Result<Value, ManagementHttpError> {
        let control = self.config_control().ok_or_else(config_unavailable)?;
        let store = control.store();
        Ok(json!({
            "schema": "mptunnel.config.v4",
            "path": store.path().display().to_string(),
            "desired_revision": store.revision().to_string(),
            "active_revision": store.active_revision().to_string(),
            "runtime_revision": control.runtime_revision().to_string(),
            "pending_revision": store.pending_revision().map(|revision| revision.to_string()),
            "mutation": {
                "validate": "POST /api/v4/config/validate",
                "apply": "POST /api/v4/config/apply",
                "precondition": "If-Match"
            }
        }))
    }

    pub(super) fn validate_config_document(
        &self,
        document: &[u8],
    ) -> Result<Value, ManagementHttpError> {
        let control = self.config_control().ok_or_else(config_unavailable)?;
        let candidate = control
            .store()
            .validate_candidate(document)
            .map_err(map_store_error)?;
        crate::observability::validate_store_path(&candidate.config().logging, control.store())
            .map_err(map_logging_error)?;
        Ok(json!({
            "valid": true,
            "revision": candidate.revision().to_string(),
            "activation": "determined-on-apply"
        }))
    }

    pub(super) fn apply_config_document(
        &self,
        expected: ConfigRevision,
        document: &[u8],
    ) -> Result<ConfigApplyOutcome, ManagementHttpError> {
        let control = self.config_control().ok_or_else(config_unavailable)?;
        let store = control.store();
        let _mutation = store.lock_mutation();
        let candidate = store
            .validate_candidate(document)
            .map_err(map_store_error)?;
        let active = store.current_config();
        if candidate.config().check_config {
            return Err(ManagementHttpError::new(
                422,
                "Unprocessable Content",
                "runtime configuration cannot enable check-only mode",
            ));
        }
        if candidate.config().management.listen != active.management.listen
            || candidate.config().management.token != active.management.token
        {
            return Err(ManagementHttpError::new(
                409,
                "Conflict",
                "management listener and authentication changes require local restart",
            ));
        }
        crate::observability::validate_store_path(&candidate.config().logging, store)
            .map_err(map_logging_error)?;
        let logging_changed = candidate.config().logging != active.logging;
        let dynamic_update = self.classify_live_update(&active, candidate.config())?;
        let committed = store
            .replace(expected, candidate)
            .map_err(map_store_error)?;
        let dynamic = committed.changed && dynamic_update.is_some();
        let prepared_logging = if committed.changed && logging_changed {
            match crate::observability::prepare_for_store(&committed.config.logging, store) {
                Ok(prepared) => Some(prepared),
                Err(error) => {
                    if let Err(rollback) = store.rollback_pending() {
                        return Err(logging_rollback_error(error, rollback));
                    }
                    return Err(map_logging_error(error));
                }
            }
        } else {
            None
        };
        if dynamic {
            if let Err(error) = store.activate_desired(committed.revision) {
                let mapped = map_store_error(error);
                if let Err(rollback) = store.rollback_pending() {
                    return Err(activation_rollback_error(mapped, rollback));
                }
                return Err(mapped);
            }
            if let Some(authorities) = dynamic_update
                .expect("dynamic update exists")
                .credential_authorities
            {
                for (server, authority) in self.servers.iter().zip(authorities) {
                    server
                        .publish_credential_authority(authority)
                        .map_err(dynamic_credential_error)?;
                }
            }
            control.publish_runtime_revision(committed.revision);
            if logging_changed && let Some(prepared) = prepared_logging {
                crate::observability::install(prepared);
            }
            crate::observability::emit_lifecycle(
                crate::config::LogLevel::Info,
                "configuration",
                "live_update_activated",
                format_args!(
                    "Activated configuration generation {} without restarting runtime services",
                    committed.revision
                ),
            );
        }
        let pending_revision = store.pending_revision();
        let pending_activation = pending_revision == Some(committed.revision);
        let active_revision = store.active_revision();
        Ok(ConfigApplyOutcome {
            response: json!({
                "state": if dynamic {
                    "activated"
                } else if pending_activation {
                    "persisted"
                } else {
                    "unchanged"
                },
                "desired_revision": committed.revision.to_string(),
                "active_revision": active_revision.to_string(),
                "pending_revision": pending_revision.map(|revision| revision.to_string()),
                "activation": if dynamic {
                    "live-update"
                } else if pending_activation {
                    "pending-generation-reload"
                } else {
                    "already-active"
                }
            }),
            pending_activation,
            request_reload: pending_activation && control.runtime_revision() != committed.revision,
        })
    }

    /// Recognizes the deliberately narrow live-update surface: process logging
    /// and inbound credential authorities. Every routing, DNS, transport,
    /// resource, timeout, client credential, or TLS change retains the clean
    /// generation-replacement path.
    fn classify_live_update(
        &self,
        active: &AppConfig,
        candidate: &AppConfig,
    ) -> Result<Option<DynamicConfigUpdate>, ManagementHttpError> {
        let mut normalized = candidate.clone();
        normalized.logging = active.logging.clone();
        let (
            CommandConfig::Node(active_node),
            CommandConfig::Node(normalized_node),
            CommandConfig::Node(candidate_node),
        ) = (&active.command, &mut normalized.command, &candidate.command);
        // File parsing assigns one fresh, internal identity to routing,
        // balancers, and DNS so proofs cannot cross runtime generations. Those
        // identities are deliberately absent from TOML and are not semantic
        // configuration changes. Compare a live-update candidate using the
        // active identities; every actual policy/spec difference remains in
        // the surrounding values and still requires generation replacement.
        normalized_node.dns_policy.generation = active_node.dns_policy.generation;
        if let (Some(active_policy), Some(normalized_policy)) = (
            active_node.product_policy.as_ref(),
            normalized_node.product_policy.as_mut(),
        ) {
            normalized_policy.generation = active_policy.generation;
        }
        for normalized_balancer in &mut normalized_node.gateway_balancers {
            if let Some(active_balancer) = active_node
                .gateway_balancers
                .iter()
                .find(|active| active.id == normalized_balancer.id)
            {
                normalized_balancer.generation = active_balancer.generation;
            }
        }
        if active_node.servers.len() != normalized_node.servers.len()
            || self.servers.len() != normalized_node.servers.len()
        {
            return Ok(None);
        }
        let authorities = candidate_node
            .servers
            .iter()
            .map(|server| server.security.credential_authority.clone())
            .collect::<Vec<_>>();
        let authorities_changed = active_node
            .servers
            .iter()
            .zip(candidate_node.servers.iter())
            .any(|(active, candidate)| {
                active.security.credential_authority != candidate.security.credential_authority
            });
        for ((active_server, normalized_server), (runtime, authority)) in active_node
            .servers
            .iter()
            .zip(&mut normalized_node.servers)
            .zip(self.servers.iter().zip(&authorities))
        {
            normalized_server.security.credential_authority =
                active_server.security.credential_authority.clone();
            runtime
                .validate_credential_authority_replacement(authority)
                .map_err(dynamic_credential_error)?;
        }
        Ok((normalized == *active).then_some(DynamicConfigUpdate {
            credential_authorities: authorities_changed.then_some(authorities),
        }))
    }

    pub(super) fn request_config_reload(&self) {
        if let Some(control) = self.config_control() {
            control.request_reload();
        }
    }
}

fn dynamic_credential_error(message: &'static str) -> ManagementHttpError {
    ManagementHttpError::new(409, "Conflict", message)
}

fn map_logging_error(error: crate::observability::ConfigureError) -> ManagementHttpError {
    ManagementHttpError::new(
        422,
        "Unprocessable Content",
        format!("logging configuration is unusable: {error}"),
    )
}

fn logging_rollback_error(
    logging: crate::observability::ConfigureError,
    rollback: ConfigStoreError,
) -> ManagementHttpError {
    ManagementHttpError::new(
        500,
        "Internal Server Error",
        format!(
            "logging preflight failed and the pending configuration could not be rolled back: {logging}; rollback: {rollback}"
        ),
    )
}

fn activation_rollback_error(
    activation: ManagementHttpError,
    rollback: ConfigStoreError,
) -> ManagementHttpError {
    ManagementHttpError::new(
        500,
        "Internal Server Error",
        format!(
            "live configuration activation failed and the pending configuration could not be rolled back: {}; rollback: {rollback}",
            activation.message
        ),
    )
}

fn config_unavailable() -> ManagementHttpError {
    ManagementHttpError::new(
        409,
        "Conflict",
        "runtime was not started from a canonical configuration file",
    )
}

fn map_store_error(error: ConfigStoreError) -> ManagementHttpError {
    match error {
        ConfigStoreError::RevisionConflict { expected, actual } => ManagementHttpError::new(
            412,
            "Precondition Failed",
            format!(
                "configuration revision does not match If-Match: expected {expected}, current desired revision is {actual}"
            ),
        ),
        ConfigStoreError::ExternalModification { known, actual } => ManagementHttpError::new(
            409,
            "Conflict",
            format!(
                "configuration file changed outside the active transaction: loaded {known}, disk revision is {actual}"
            ),
        ),
        ConfigStoreError::ActivationPending { desired } => ManagementHttpError::new(
            409,
            "Conflict",
            format!("configuration revision {desired} is still pending activation"),
        ),
        ConfigStoreError::ActivationRevisionConflict { expected, actual } => {
            ManagementHttpError::new(
                409,
                "Conflict",
                format!(
                    "configuration activation revision changed: expected {expected}, desired {actual}"
                ),
            )
        }
        ConfigStoreError::DocumentTooLarge { actual, limit } => ManagementHttpError::new(
            413,
            "Payload Too Large",
            format!("configuration document is {actual} bytes; maximum is {limit}"),
        ),
        ConfigStoreError::Config(error) => ManagementHttpError::new(
            422,
            "Unprocessable Content",
            format!("configuration document is invalid: {error}"),
        ),
        ConfigStoreError::NonUtf8 => ManagementHttpError::new(
            422,
            "Unprocessable Content",
            "configuration document must be valid UTF-8",
        ),
        ConfigStoreError::Io(_)
        | ConfigStoreError::InvalidPath
        | ConfigStoreError::InvalidPendingJournal
        | ConfigStoreError::LastGoodMissing
        | ConfigStoreError::RecoveryConflict(_) => ManagementHttpError::new(
            500,
            "Internal Server Error",
            "configuration transaction failed",
        ),
    }
}

#[cfg(test)]
#[path = "tests_config.rs"]
mod tests;

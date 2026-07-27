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
    pub(super) reload: bool,
}

impl ManagementTarget {
    pub(super) fn config_status_json(&self) -> Result<Value, ManagementHttpError> {
        let control = self.config_control().ok_or_else(config_unavailable)?;
        let store = control.store();
        Ok(json!({
            "schema": "mptunnel.config.v1",
            "path": store.path().display().to_string(),
            "desired_revision": store.revision().to_string(),
            "active_revision": store.active_revision().to_string(),
            "runtime_revision": control.runtime_revision().to_string(),
            "pending_revision": store.pending_revision().map(|revision| revision.to_string()),
            "mutation": {
                "validate": "POST /api/v1/config/validate",
                "apply": "POST /api/v1/config/apply",
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
        Ok(json!({
            "valid": true,
            "revision": candidate.revision().to_string(),
            "activation": "generation-reload"
        }))
    }

    pub(super) fn apply_config_document(
        &self,
        expected: ConfigRevision,
        document: &[u8],
    ) -> Result<ConfigApplyOutcome, ManagementHttpError> {
        let control = self.config_control().ok_or_else(config_unavailable)?;
        let candidate = control
            .store()
            .validate_candidate(document)
            .map_err(map_store_error)?;
        let active = control.store().current_config();
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
        let dynamic_authorities =
            self.dynamic_inbound_credential_update(&active, candidate.config())?;
        let committed = control
            .store()
            .replace(expected, candidate)
            .map_err(map_store_error)?;
        let dynamic = committed.changed && dynamic_authorities.is_some();
        if dynamic {
            control
                .store()
                .activate_desired(committed.revision)
                .map_err(map_store_error)?;
            for (server, authority) in self
                .servers
                .iter()
                .zip(dynamic_authorities.expect("dynamic authorities exist"))
            {
                server
                    .publish_credential_authority(authority)
                    .map_err(dynamic_credential_error)?;
            }
            control.publish_runtime_revision(committed.revision);
        }
        Ok(ConfigApplyOutcome {
            response: json!({
                "state": if dynamic {
                    "activated"
                } else if committed.changed {
                    "persisted"
                } else {
                    "unchanged"
                },
                "desired_revision": committed.revision.to_string(),
                "active_revision": if committed.changed && !dynamic {
                    Value::String(control.store().active_revision().to_string())
                } else {
                    Value::String(committed.revision.to_string())
                },
                "pending_revision": control.store().pending_revision().map(|revision| revision.to_string()),
                "activation": if dynamic {
                    "live-credential-publication"
                } else if committed.changed {
                    "pending-generation-reload"
                } else {
                    "already-active"
                }
            }),
            reload: committed.changed && !dynamic,
        })
    }

    /// Recognizes the deliberately narrow live-update surface: only inbound
    /// credential authorities may change in place. Every routing, DNS,
    /// transport, resource, timeout, client credential, or TLS change retains
    /// the clean generation-replacement path.
    fn dynamic_inbound_credential_update(
        &self,
        active: &AppConfig,
        candidate: &AppConfig,
    ) -> Result<Option<Vec<CredentialAuthority>>, ManagementHttpError> {
        let mut normalized = candidate.clone();
        let (
            CommandConfig::Node(active_node),
            CommandConfig::Node(normalized_node),
            CommandConfig::Node(candidate_node),
        ) = (&active.command, &mut normalized.command, &candidate.command);
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
        Ok((normalized == *active).then_some(authorities))
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
#[path = "config_test.rs"]
mod tests;

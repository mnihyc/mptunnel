//! Explicit management mutations and manual peer-status request selection.
//!
//! Controls select one endpoint-local owner, validate identity before mutation,
//! and refresh the immutable management snapshot only after a committed result.

use super::ManagementTarget;
use super::http::ManagementHttpError;
use super::projection::{PeerPathIdentitySource, peer_status_result};
use crate::protocol::SessionId;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::path_record_failure_cooldown;
use crate::runtime::path::tcp::group::ClientTcpEndpointControlState;
use crate::runtime::peer_status::{PeerStatusBroker, PeerStatusRequestError};
use crate::scheduler::PathState as SchedulerPathState;
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Instant;

impl ManagementTarget {
    pub(super) fn control_path_json(&self, body: &[u8]) -> Result<Value, ManagementHttpError> {
        let request = serde_json::from_slice::<PathControlRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid path control JSON body")
        })?;
        let context = select_control_client_context(&self.clients, &request)?;
        let selection = select_client_path(context, &request.path)?;
        let state = parse_control_state(&request.state)?;
        set_client_path_state(context, &selection, state)?;
        self.refresh_current_snapshot();
        Ok(json!({
            "applied": true,
            "outbound": request.outbound,
            "path": request.path,
            "state": request.state
        }))
    }

    pub(super) async fn peer_diagnostics_json(
        &self,
        body: &[u8],
    ) -> Result<Value, ManagementHttpError> {
        let request = serde_json::from_slice::<PeerDiagnosticsRequest>(body).map_err(|_| {
            ManagementHttpError::new(400, "Bad Request", "invalid peer diagnostics JSON body")
        })?;
        let requested_session = parse_session_id(&request.session_id)?;
        let selected = select_peer_status_broker(self, &request, requested_session)?;
        let result = selected
            .broker
            .request(selected.session_id)
            .await
            .map_err(map_peer_request_error)?;
        self.refresh_current_snapshot();
        serde_json::to_value(peer_status_result(
            result,
            selected.service,
            selected.service_index,
            selected.service_name,
            selected.identity_source,
        ))
        .map_err(|_| {
            ManagementHttpError::new(
                500,
                "Internal Server Error",
                "peer diagnostics response serialization failed",
            )
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PathControlRequest {
    outbound: String,
    path: String,
    state: String,
}

fn select_control_client_context<'a>(
    clients: &'a [ClientPathContext],
    request: &PathControlRequest,
) -> Result<&'a ClientPathContext, ManagementHttpError> {
    let mut matches = clients.iter().filter(|context| {
        context
            .outbound
            .as_ref()
            .is_some_and(|outbound| outbound.as_str() == request.outbound)
    });
    let selected = matches.next().ok_or_else(|| {
        ManagementHttpError::new(404, "Not Found", "outbound does not match an MPP outbound")
    })?;
    if matches.next().is_some() {
        return Err(ManagementHttpError::new(
            409,
            "Conflict",
            "outbound matches more than one MPP path owner",
        ));
    }
    Ok(selected)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PathControlState {
    Enabled,
    Suspect,
    Failed,
    Disabled,
}

#[derive(Debug)]
enum ClientPathSelection {
    Tcp { config_index: usize },
    Udp { index: usize },
}

fn select_client_path(
    context: &ClientPathContext,
    path: &str,
) -> Result<ClientPathSelection, ManagementHttpError> {
    let mut selected = None;
    for endpoint in context.tcp_carrier_groups.iter() {
        if context
            .tcp_path_names
            .get(endpoint.config_index)
            .is_some_and(|name| name == path)
        {
            if selected.is_some() {
                return Err(ManagementHttpError::new(
                    409,
                    "Conflict",
                    "path name is ambiguous within the outbound",
                ));
            }
            selected = Some(ClientPathSelection::Tcp {
                config_index: endpoint.config_index,
            });
        }
    }
    for (index, name) in context.udp_path_names.iter().enumerate() {
        if name == path {
            if selected.is_some() {
                return Err(ManagementHttpError::new(
                    409,
                    "Conflict",
                    "path name is ambiguous within the outbound",
                ));
            }
            selected = Some(ClientPathSelection::Udp { index });
        }
    }
    selected.ok_or_else(|| {
        ManagementHttpError::new(
            404,
            "Not Found",
            "path does not match a configured path for the outbound",
        )
    })
}

fn parse_control_state(value: &str) -> Result<PathControlState, ManagementHttpError> {
    match value {
        "enabled" => Ok(PathControlState::Enabled),
        "suspect" => Ok(PathControlState::Suspect),
        "failed" => Ok(PathControlState::Failed),
        "disabled" => Ok(PathControlState::Disabled),
        _ => Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "state must be enabled, suspect, failed, or disabled",
        )),
    }
}

fn set_client_path_state(
    context: &ClientPathContext,
    selection: &ClientPathSelection,
    state: PathControlState,
) -> Result<(), ManagementHttpError> {
    let index = match *selection {
        ClientPathSelection::Tcp { config_index } => {
            let state = match state {
                PathControlState::Enabled => ClientTcpEndpointControlState::Enabled,
                PathControlState::Suspect => ClientTcpEndpointControlState::Suspect,
                PathControlState::Failed => ClientTcpEndpointControlState::Failed,
                PathControlState::Disabled => ClientTcpEndpointControlState::Disabled,
            };
            context.set_tcp_endpoint_control(config_index, state);
            return Ok(());
        }
        ClientPathSelection::Udp { index } => index,
    };
    let mut health = context
        .health()
        .lock()
        .expect("client path health management lock");
    let records = &mut health.udp;
    let record = records
        .get_mut(index)
        .expect("configured UDP path must have one health record");
    let now = Instant::now();
    record.mutate_eligibility(|record| {
        record.invalidate_path_proofs();
        match state {
            PathControlState::Enabled | PathControlState::Suspect => {
                record.manual_disabled = false;
                record.state = SchedulerPathState::Suspect;
                record.failed_until = None;
            }
            PathControlState::Failed => {
                record.manual_disabled = false;
                record.state = SchedulerPathState::Failed;
                record.failed_until = Some(now + path_record_failure_cooldown(record));
            }
            PathControlState::Disabled => {
                record.manual_disabled = true;
                record.state = SchedulerPathState::Failed;
                record.failed_until = None;
                record.relay_bytes_in_flight = 0;
                record.relay_queue_bytes = 0;
            }
        }
    });
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerDiagnosticsRequest {
    service: String,
    service_name: String,
    session_id: String,
}

fn parse_session_id(value: &str) -> Result<SessionId, ManagementHttpError> {
    value.parse::<u64>().map(SessionId).map_err(|_| {
        ManagementHttpError::new(
            400,
            "Bad Request",
            "session_id must be an unsigned decimal string",
        )
    })
}

struct PeerStatusSelection<'a> {
    broker: PeerStatusBroker,
    session_id: SessionId,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
    identity_source: PeerPathIdentitySource<'a>,
}

fn select_peer_status_broker<'a>(
    target: &'a ManagementTarget,
    request: &PeerDiagnosticsRequest,
    requested_session: SessionId,
) -> Result<PeerStatusSelection<'a>, ManagementHttpError> {
    if !matches!(request.service.as_str(), "mpp_outbound" | "mpp_inbound") {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "service must be mpp_outbound or mpp_inbound",
        ));
    }
    let mut candidates = Vec::new();
    for (index, context) in target.clients.iter().enumerate() {
        candidates.extend(peer_broker_candidates(
            &context.peer_status,
            "mpp_outbound",
            index,
            context
                .outbound
                .as_ref()
                .map(|outbound| outbound.as_str().to_string()),
            PeerPathIdentitySource::Client(context),
            request,
            requested_session,
        ));
    }
    for (index, context) in target.servers.iter().enumerate() {
        candidates.extend(peer_broker_candidates(
            &context.peer_status,
            "mpp_inbound",
            index,
            Some(context.name.clone()),
            PeerPathIdentitySource::Server(context),
            request,
            requested_session,
        ));
    }
    match candidates.as_slice() {
        [selected] => Ok(PeerStatusSelection {
            broker: selected.broker.clone(),
            session_id: selected.session_id,
            service: selected.service,
            service_index: selected.service_index,
            service_name: selected.service_name.clone(),
            identity_source: selected.identity_source,
        }),
        [] => Err(ManagementHttpError::new(
            404,
            "Not Found",
            "no matching authenticated peer session is available",
        )),
        _ => Err(ManagementHttpError::new(
            409,
            "Conflict",
            "peer session is ambiguous for service, service_name, and session_id",
        )),
    }
}

fn peer_broker_candidates<'a>(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_name: Option<String>,
    identity_source: PeerPathIdentitySource<'a>,
    request: &PeerDiagnosticsRequest,
    requested_session: SessionId,
) -> Vec<PeerStatusSelection<'a>> {
    if request.service != service || service_name.as_deref() != Some(request.service_name.as_str())
    {
        return Vec::new();
    }
    broker
        .session_ids()
        .into_iter()
        .filter(|session_id| requested_session == *session_id)
        .map(|session_id| PeerStatusSelection {
            broker: broker.clone(),
            session_id,
            service,
            service_index,
            service_name: service_name.clone(),
            identity_source,
        })
        .collect()
}

fn map_peer_request_error(error: PeerStatusRequestError) -> ManagementHttpError {
    match error {
        PeerStatusRequestError::SessionUnavailable => {
            ManagementHttpError::new(404, "Not Found", "peer session became unavailable")
        }
        PeerStatusRequestError::RequestInProgress => ManagementHttpError::new(
            409,
            "Conflict",
            "peer status request is already in progress for this session",
        ),
        PeerStatusRequestError::NoAvailableCarrier => ManagementHttpError::new(
            503,
            "Service Unavailable",
            "peer session has no available control carrier",
        ),
        PeerStatusRequestError::TimedOut => {
            ManagementHttpError::new(504, "Gateway Timeout", "peer status request timed out")
        }
    }
}

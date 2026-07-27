//! Explicit management mutations and manual peer-status request selection.
//!
//! Controls select one endpoint-local owner, validate identity before mutation,
//! and refresh the immutable management snapshot only after a committed result.

use super::ManagementTarget;
use super::http::ManagementHttpError;
use super::projection::peer_status_result;
use crate::protocol::{SessionId, UnderlayProtocol};
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::path_record_failure_cooldown;
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
        let underlay = parse_underlay(&request.underlay)?;
        let state = parse_control_state(&request.state)?;
        set_client_path_state(context, underlay, request.index, state)?;
        self.refresh_current_snapshot();
        Ok(json!({
            "applied": true,
            "underlay": underlay_name(underlay),
            "index": request.index,
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
        let requested_session = request
            .session_id
            .as_deref()
            .map(parse_session_id)
            .transpose()?;
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
            selected.service_tag,
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
    #[serde(default)]
    client_index: Option<usize>,
    #[serde(default)]
    client_tag: Option<String>,
    underlay: String,
    index: usize,
    state: String,
}

fn select_control_client_context<'a>(
    clients: &'a [ClientPathContext],
    request: &PathControlRequest,
) -> Result<&'a ClientPathContext, ManagementHttpError> {
    if request.client_index.is_some() && request.client_tag.is_some() {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "path control must set at most one of client_index or client_tag",
        ));
    }
    if let Some(tag) = request.client_tag.as_deref() {
        let mut matches = clients.iter().filter(|context| {
            context
                .route_target
                .as_ref()
                .is_some_and(|target| target.tag == tag)
        });
        let selected = matches.next().ok_or_else(|| {
            ManagementHttpError::new(
                404,
                "Not Found",
                "client_tag does not match an MPP outbound or balancer",
            )
        })?;
        if matches.next().is_some() {
            return Err(ManagementHttpError::new(
                409,
                "Conflict",
                "client_tag matches more than one MPP outbound or balancer",
            ));
        }
        return Ok(selected);
    }
    if let Some(index) = request.client_index {
        return clients.get(index).ok_or_else(|| {
            ManagementHttpError::new(
                404,
                "Not Found",
                "client_index does not match an MPP outbound or balancer",
            )
        });
    }
    match clients {
        [context] => Ok(context),
        [] => Err(ManagementHttpError::new(
            409,
            "Conflict",
            "path control requires an existing MPP outbound",
        )),
        _ => Err(ManagementHttpError::new(
            409,
            "Conflict",
            "path control is ambiguous; provide client_index or client_tag",
        )),
    }
}

#[derive(Debug, Clone, Copy)]
enum PathControlState {
    Enabled,
    Suspect,
    Failed,
    Disabled,
}

fn parse_underlay(value: &str) -> Result<UnderlayProtocol, ManagementHttpError> {
    match value {
        "tcp" => Ok(UnderlayProtocol::Tcp),
        "udp" | "quic" => Ok(UnderlayProtocol::Udp),
        _ => Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "underlay must be tcp or udp",
        )),
    }
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
    underlay: UnderlayProtocol,
    index: usize,
    state: PathControlState,
) -> Result<(), ManagementHttpError> {
    let mut health = context
        .health()
        .lock()
        .expect("client path health management lock");
    let records = match underlay {
        UnderlayProtocol::Tcp => &mut health.tcp,
        UnderlayProtocol::Udp => &mut health.udp,
    };
    let Some(record) = records.get_mut(index) else {
        return Err(ManagementHttpError::new(
            404,
            "Not Found",
            "path index does not exist",
        ));
    };
    match state {
        PathControlState::Enabled => {
            record.manual_disabled = false;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Suspect;
            record.failed_until = None;
        }
        PathControlState::Suspect => {
            record.manual_disabled = false;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Suspect;
            record.failed_until = None;
        }
        PathControlState::Failed => {
            record.manual_disabled = false;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Failed;
            record.failed_until = Some(Instant::now() + path_record_failure_cooldown(record));
        }
        PathControlState::Disabled => {
            record.manual_disabled = true;
            record.invalidate_path_proofs();
            record.state = SchedulerPathState::Failed;
            record.failed_until = None;
            record.relay_bytes_in_flight = 0;
            record.relay_queue_bytes = 0;
        }
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PeerDiagnosticsRequest {
    #[serde(default)]
    service: Option<String>,
    #[serde(default)]
    service_index: Option<usize>,
    #[serde(default)]
    session_id: Option<String>,
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

struct PeerStatusSelection {
    broker: PeerStatusBroker,
    session_id: SessionId,
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
}

fn select_peer_status_broker(
    target: &ManagementTarget,
    request: &PeerDiagnosticsRequest,
    requested_session: Option<SessionId>,
) -> Result<PeerStatusSelection, ManagementHttpError> {
    if request.service.is_none() && request.service_index.is_some() {
        return Err(ManagementHttpError::new(
            400,
            "Bad Request",
            "service_index requires service",
        ));
    }
    if request
        .service
        .as_deref()
        .is_some_and(|service| !matches!(service, "mpp_outbound" | "mpp_inbound"))
    {
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
                .route_target
                .as_ref()
                .map(|target| target.tag.clone()),
            request,
            requested_session,
        ));
    }
    for (index, context) in target.servers.iter().enumerate() {
        candidates.extend(peer_broker_candidates(
            &context.peer_status,
            "mpp_inbound",
            index,
            context.tag.clone(),
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
            service_tag: selected.service_tag.clone(),
        }),
        [] => Err(ManagementHttpError::new(
            404,
            "Not Found",
            "no matching authenticated peer session is available",
        )),
        _ => Err(ManagementHttpError::new(
            409,
            "Conflict",
            "peer session is ambiguous; provide service, service_index, and session_id",
        )),
    }
}

fn peer_broker_candidates(
    broker: &PeerStatusBroker,
    service: &'static str,
    service_index: usize,
    service_tag: Option<String>,
    request: &PeerDiagnosticsRequest,
    requested_session: Option<SessionId>,
) -> Vec<PeerStatusSelection> {
    if request
        .service
        .as_deref()
        .is_some_and(|requested| requested != service)
        || request
            .service_index
            .is_some_and(|requested| requested != service_index)
    {
        return Vec::new();
    }
    broker
        .session_ids()
        .into_iter()
        .filter(|session_id| requested_session.is_none_or(|requested| requested == *session_id))
        .map(|session_id| PeerStatusSelection {
            broker: broker.clone(),
            session_id,
            service,
            service_index,
            service_tag: service_tag.clone(),
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

fn underlay_name(underlay: UnderlayProtocol) -> &'static str {
    match underlay {
        UnderlayProtocol::Tcp => "tcp",
        UnderlayProtocol::Udp => "udp",
    }
}

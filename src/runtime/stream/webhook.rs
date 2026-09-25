//! Optional server lifecycle snapshots. Mutation stays under the existing
//! registry/session locks; these helpers own neither timers nor I/O.
use crate::protocol::{PathUsage, SessionId, UnderlayProtocol};
use crate::runtime::path::ServerCarrierPathIdentity;
use crate::runtime::webhook::EventPublisher;
use crate::webhook::EventKind;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone)]
pub(super) struct ServerWebhookContext {
    pub(super) publisher: EventPublisher,
    pub(super) inbound: String,
    pub(super) paths: Arc<Vec<String>>,
}

impl std::fmt::Debug for ServerWebhookContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerWebhookContext")
            .field("inbound", &self.inbound)
            .finish_non_exhaustive()
    }
}

impl ServerWebhookContext {
    pub(super) fn interested(&self) -> bool {
        [
            EventKind::CarrierStateChanged,
            EventKind::CarrierPolicyChanged,
            EventKind::CarrierAddressChanged,
            EventKind::SessionStateChanged,
            EventKind::SessionPeerAddressesChanged,
        ]
        .into_iter()
        .any(|kind| self.publisher.interested(kind))
    }

    pub(super) fn carrier_event(
        &self,
        observation: &mut CarrierObservation,
        identity: ServerCarrierPathIdentity,
        kind: EventKind,
        change: Value,
    ) {
        observation.sequence = observation.sequence.saturating_add(1);
        if !self.publisher.interested(kind) {
            return;
        }
        let transport = transport_name(identity.underlay);
        let reason = change
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or(observation.reason);
        self.publisher.emit(kind,
            &format!("inbound/{}/session/{:016x}/{transport}/{}/{}", self.inbound,
                identity.session_id.0, identity.path_id.0, identity.path_instance_id.as_u64()),
            json!({
                "inbound": {"name": self.inbound},
                "session": {"id": format!("{:016x}", identity.session_id.0)},
                "carrier": {
                    "id": format!("inbound/{}/session/{:016x}/{transport}/{}/{}", self.inbound,
                        identity.session_id.0, identity.path_id.0, identity.path_instance_id.as_u64()),
                    "instance": identity.path_instance_id.as_u64(),
                    "path_id": identity.path_id.0, "configured_slot": observation.configured_slot,
                    "transport": transport, "state": observation.state,
                    "local": socket_value(observation.local), "peer": socket_value(observation.peer),
                    "listen_path": self.paths.get(observation.config_ordinal),
                    "address_revision": observation.address_revision
                    ,"peer_usage": observation.peer_usage.map(usage_name)
                },
                "change": change, "reason": reason,
                "subject_sequence": observation.sequence,
                "initial": kind == EventKind::CarrierStateChanged && change.get("from").is_some_and(Value::is_null)
            }));
    }
}

#[derive(Debug, Clone)]
pub(super) struct CarrierObservation {
    pub(super) local: Option<SocketAddr>,
    pub(super) peer: Option<SocketAddr>,
    pub(super) state: &'static str,
    pub(super) reason: &'static str,
    pub(super) sequence: u64,
    pub(super) address_revision: u64,
    pub(super) config_ordinal: usize,
    pub(super) configured_slot: u64,
    pub(super) peer_usage: Option<PathUsage>,
}

#[derive(Debug)]
pub(super) struct SessionObservation {
    ready: HashMap<ServerCarrierPathIdentity, Option<IpAddr>>,
    lifetime: u64,
    sequence: u64,
    ever_attached: bool,
    retired: bool,
    had_addresses: bool,
}

impl Default for SessionObservation {
    fn default() -> Self {
        static NEXT_LIFETIME: AtomicU64 = AtomicU64::new(1);
        Self {
            ready: HashMap::new(),
            lifetime: NEXT_LIFETIME.fetch_add(1, Ordering::Relaxed),
            sequence: 0,
            ever_attached: false,
            retired: false,
            had_addresses: false,
        }
    }
}

impl SessionObservation {
    pub(super) fn carrier(
        &mut self,
        context: &ServerWebhookContext,
        identity: ServerCarrierPathIdentity,
        peer: Option<SocketAddr>,
        ready: bool,
    ) {
        let before_count = self.ready.len();
        let before_ips = self.peer_ips();
        if ready {
            if self.retired {
                return;
            }
            self.ready.insert(identity, peer.map(|addr| addr.ip()));
        } else {
            self.ready.remove(&identity);
        }
        // Physical cleanup still releases retained membership after the
        // terminal, but cannot create a new session observation afterward.
        if self.retired {
            return;
        }
        let after_count = self.ready.len();
        if !self.retired && before_count == 0 && after_count > 0 {
            let initial = !self.ever_attached;
            self.emit(
                context,
                identity.session_id,
                EventKind::SessionStateChanged,
                json!({"from": if initial {None} else {Some("detached")}, "to":"attached",
                    "reason": if initial {"first_ready_carrier"} else {"reattached"}}),
                initial,
            );
            self.ever_attached = true;
        } else if !self.retired && before_count > 0 && after_count == 0 {
            self.emit(
                context,
                identity.session_id,
                EventKind::SessionStateChanged,
                json!({"from":"attached", "to":"detached", "reason":"last_ready_carrier_left"}),
                false,
            );
        }
        self.address_set(context, identity.session_id, before_ips);
    }

    pub(super) fn address(
        &mut self,
        context: &ServerWebhookContext,
        identity: ServerCarrierPathIdentity,
        peer: SocketAddr,
    ) {
        if self.retired {
            return;
        }
        let before = self.peer_ips();
        let Some(value) = self.ready.get_mut(&identity) else {
            return;
        };
        *value = Some(peer.ip());
        self.address_set(context, identity.session_id, before);
    }

    pub(super) fn retire(&mut self, context: &ServerWebhookContext, session: SessionId) {
        if self.retired {
            return;
        }
        self.retired = true;
        self.emit(context, session, EventKind::SessionStateChanged,
            json!({"from": if !self.ever_attached {None} else if self.ready.is_empty() {Some("detached")} else {Some("attached")},
                "to":"retired", "reason":"session_close"}), !self.ever_attached);
    }

    fn peer_ips(&self) -> BTreeSet<IpAddr> {
        self.ready.values().filter_map(|ip| *ip).collect()
    }

    fn address_set(
        &mut self,
        context: &ServerWebhookContext,
        session: SessionId,
        before: BTreeSet<IpAddr>,
    ) {
        let after = self.peer_ips();
        if before == after {
            return;
        }
        let initial = !self.had_addresses;
        self.had_addresses = true;
        self.emit(context, session, EventKind::SessionPeerAddressesChanged,
            json!({"before": before, "after": after, "components":["ip"], "reason":"ready_carrier_peer_set"}), initial);
    }

    fn emit(
        &mut self,
        context: &ServerWebhookContext,
        session: SessionId,
        kind: EventKind,
        change: Value,
        initial: bool,
    ) {
        self.sequence = self.sequence.saturating_add(1);
        if !context.publisher.interested(kind) {
            return;
        }
        context.publisher.emit(
            kind,
            &format!(
                "inbound/{}/session/{:016x}/{}",
                context.inbound, session.0, self.lifetime
            ),
            json!({
                "inbound": {"name": context.inbound},
                "session": {"id": format!("{:016x}", session.0), "lifetime": self.lifetime,
                    "ready_carriers": self.ready.len(), "peer_ips": self.peer_ips()},
                "change": change, "initial": initial, "subject_sequence": self.sequence
            }),
        );
    }
}

pub(super) fn usage_name(usage: PathUsage) -> &'static str {
    match usage {
        PathUsage::Available => "available",
        PathUsage::Backup => "backup",
    }
}

pub(super) fn transport_name(transport: UnderlayProtocol) -> &'static str {
    match transport {
        UnderlayProtocol::Tcp => "tcp",
        UnderlayProtocol::Udp => "quic",
    }
}

pub(super) fn socket_value(addr: Option<SocketAddr>) -> Value {
    json!({"ip": addr.map(|addr| addr.ip()), "port": addr.map(|addr| addr.port())})
}

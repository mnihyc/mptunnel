//! Optional lifecycle observations for one client MPP path context.
//!
//! This owner is installed only when a generation has interested webhook
//! rules. It tracks configured-path aggregates and exact current carrier
//! incarnations; it never observes Product packets or sampled path metrics.

use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::{PathId, PathUsage, SessionId, UnderlayProtocol};
use crate::runtime::webhook::{EventPublisher, IntervalSnapshotRegistration};
use crate::webhook::EventKind;
use serde_json::{Value, json};
use std::collections::{BTreeSet, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum Availability {
    Unknown,
    Up,
    Down,
    Idle,
}

impl Availability {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Up => "up",
            Self::Down => "down",
            Self::Idle => "idle",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ProbeTrigger {
    Periodic,
    Reconcile,
}

impl ProbeTrigger {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Periodic => "periodic",
            Self::Reconcile => "reconcile",
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct ConfiguredPathObservation {
    pub(super) name: String,
    pub(super) config_ordinal: usize,
    pub(super) members: Vec<RelayPathKey>,
    pub(super) member_slots: HashMap<RelayPathKey, u16>,
    pub(super) transports: Vec<String>,
    pub(super) local_ips: Vec<String>,
}

#[derive(Debug, Clone)]
struct ObservedPath {
    name: String,
    config_ordinal: usize,
    members: Vec<RelayPathKey>,
    member_slots: HashMap<RelayPathKey, u16>,
    transports: Vec<String>,
    configured_local_ips: BTreeSet<IpAddr>,
    state: Availability,
    sequence: u64,
    policy: String,
    last_ready_at: Option<SystemTime>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CarrierState {
    Ready,
    Draining,
}

impl CarrierState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Draining => "draining",
        }
    }
}

#[derive(Debug, Clone)]
struct ObservedCarrier {
    member: RelayPathKey,
    path_id: PathId,
    configured_slot: u16,
    state: CarrierState,
    sequence: u64,
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
    address_revision: u64,
    usage: Option<PathUsage>,
}

#[derive(Debug)]
struct SessionObservation {
    ready_carriers: usize,
    ever_attached: bool,
    had_peer_addresses: bool,
    retired: bool,
    sequence: u64,
    peer_ips: BTreeSet<IpAddr>,
}

#[derive(Debug)]
struct ClientObservationState {
    paths: Vec<ObservedPath>,
    path_by_member: HashMap<RelayPathKey, usize>,
    member_states: HashMap<RelayPathKey, Availability>,
    current_by_member: HashMap<RelayPathKey, CarrierPathInstanceId>,
    carriers: HashMap<CarrierPathInstanceId, ObservedCarrier>,
    session: SessionObservation,
    next_probe_id: u64,
}

/// The client-side observer is bounded by the configured path and carrier
/// inventory. Carrier rows are removed after their exact incarnation closes.
pub(in crate::runtime) struct ClientPathWebhookObserver {
    publisher: EventPublisher,
    outbound: String,
    session_id: SessionId,
    state: Mutex<ClientObservationState>,
    interval_registrations: Mutex<Vec<IntervalSnapshotRegistration>>,
    intervals_registered: AtomicBool,
}

impl ClientPathWebhookObserver {
    pub(super) fn new(
        publisher: EventPublisher,
        outbound: String,
        session_id: SessionId,
        configured_paths: Vec<ConfiguredPathObservation>,
    ) -> Arc<Self> {
        let mut paths = configured_paths
            .into_iter()
            .map(|path| ObservedPath {
                name: path.name,
                config_ordinal: path.config_ordinal,
                members: path.members,
                member_slots: path.member_slots,
                transports: path.transports,
                configured_local_ips: path
                    .local_ips
                    .into_iter()
                    .filter_map(|ip| ip.parse::<IpAddr>().ok())
                    .filter(|ip| !ip.is_unspecified())
                    .collect(),
                state: Availability::Unknown,
                sequence: 0,
                policy: "enabled".to_owned(),
                last_ready_at: None,
            })
            .collect::<Vec<_>>();
        paths.sort_unstable_by_key(|path| path.config_ordinal);
        let path_by_member: HashMap<RelayPathKey, usize> = paths
            .iter()
            .enumerate()
            .flat_map(|(logical_index, path)| {
                path.members
                    .iter()
                    .copied()
                    .map(move |member| (member, logical_index))
            })
            .collect();
        let member_states = path_by_member
            .keys()
            .copied()
            .map(|member| (member, Availability::Unknown))
            .collect();
        Arc::new(Self {
            publisher,
            outbound,
            session_id,
            state: Mutex::new(ClientObservationState {
                paths,
                path_by_member,
                member_states,
                current_by_member: HashMap::new(),
                carriers: HashMap::new(),
                session: SessionObservation {
                    ready_carriers: 0,
                    ever_attached: false,
                    had_peer_addresses: false,
                    retired: false,
                    sequence: 0,
                    peer_ips: BTreeSet::new(),
                },
                next_probe_id: 1,
            }),
            interval_registrations: Mutex::new(Vec::new()),
            intervals_registered: AtomicBool::new(false),
        })
    }

    pub(super) fn wants_carrier_details(&self) -> bool {
        self.publisher.interested(EventKind::PathStateChanged)
            || self.publisher.interested(EventKind::PathPolicyChanged)
            || self.publisher.interested(EventKind::PathProbeCompleted)
            || self.publisher.interested(EventKind::CarrierStateChanged)
            || self.publisher.interested(EventKind::CarrierPolicyChanged)
            || self.publisher.interested(EventKind::CarrierAddressChanged)
            || self
                .publisher
                .interested(EventKind::SessionPeerAddressesChanged)
            || self.publisher.interested(EventKind::PathInterval)
    }

    pub(super) fn wants_validated_addresses(&self) -> bool {
        self.publisher.interested(EventKind::CarrierAddressChanged)
            || self
                .publisher
                .interested(EventKind::SessionPeerAddressesChanged)
    }

    pub(super) fn register_interval_snapshots(self: &Arc<Self>) {
        if !self.publisher.interested(EventKind::PathInterval)
            || self.intervals_registered.swap(true, Ordering::AcqRel)
        {
            return;
        }
        let paths = {
            let state = self.state.lock().expect("client webhook observation lock");
            state
                .paths
                .iter()
                .map(|path| (path.config_ordinal, path.name.clone()))
                .collect::<Vec<_>>()
        };
        let mut registrations = self
            .interval_registrations
            .lock()
            .expect("client webhook interval registration lock");
        for (config_ordinal, name) in paths {
            let weak = Arc::downgrade(self);
            let subject = path_subject(&self.outbound, &name);
            let registration = self.publisher.register_interval_snapshot(
                subject.clone(),
                Arc::new(move || {
                    weak.upgrade()
                        .and_then(|observer| observer.snapshot(config_ordinal))
                }),
            );
            registrations.push(registration);
        }
    }

    pub(super) fn snapshot(&self, config_ordinal: usize) -> Option<Value> {
        let state = self.state.lock().expect("client webhook observation lock");
        let logical_index = state
            .paths
            .iter()
            .position(|path| path.config_ordinal == config_ordinal)?;
        Some(path_observation_data(&state, logical_index, &self.outbound))
    }

    pub(super) fn begin_probe(
        self: &Arc<Self>,
        member: RelayPathKey,
        trigger: ProbeTrigger,
    ) -> Option<ClientProbeAttempt> {
        if !self.publisher.interested(EventKind::PathProbeCompleted) {
            return None;
        }
        let mut state = self.state.lock().expect("client webhook observation lock");
        let logical_index = *state.path_by_member.get(&member)?;
        let path = state.paths.get(logical_index)?;
        let state_at_start = path.state;
        let path_name = path.name.clone();
        let configured_slot = *path.member_slots.get(&member)?;
        let id = state.next_probe_id;
        state.next_probe_id = id.checked_add(1).unwrap_or(1);
        Some(ClientProbeAttempt {
            observer: self.clone(),
            logical_index,
            path_name,
            member,
            configured_slot,
            id,
            trigger,
            state_at_start,
            started_at: SystemTime::now(),
            started_monotonic: std::time::Instant::now(),
            settled: false,
        })
    }

    pub(super) fn mark_establishment_failure(&self, member: RelayPathKey, reason: &'static str) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        if state.member_states.get(&member) == Some(&Availability::Down) {
            return;
        }
        state.member_states.insert(member, Availability::Down);
        self.emit(transition_aggregate_path(
            &self.publisher,
            &self.outbound,
            &mut state,
            logical_index,
            reason,
        ));
    }

    // Preserve the exact readiness transaction's explicit identity/address
    // inputs; the observer does not reach back into transport owners.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn publish_ready(
        &self,
        member: RelayPathKey,
        instance: CarrierPathInstanceId,
        path_id: PathId,
        local: Option<SocketAddr>,
        peer: Option<SocketAddr>,
        address_revision: u64,
        usage: PathUsage,
    ) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        if state.current_by_member.get(&member) == Some(&instance)
            && state
                .carriers
                .get(&instance)
                .is_some_and(|carrier| carrier.state == CarrierState::Ready)
        {
            return;
        }
        let path_name = state.paths[logical_index].name.clone();
        let configured_slot = *state.paths[logical_index]
            .member_slots
            .get(&member)
            .expect("configured client path member has a stable slot");
        let ready_before = ready_carrier_count(&state);
        let mut emissions = Vec::new();
        if let Some(previous_id) = state.current_by_member.get(&member).copied()
            && previous_id != instance
            && let Some(previous) = state.carriers.get_mut(&previous_id)
            && previous.state == CarrierState::Ready
        {
            previous.state = CarrierState::Draining;
            previous.sequence = previous.sequence.saturating_add(1);
            push_carrier_emission(
                &self.publisher,
                &mut emissions,
                EventKind::CarrierStateChanged,
                carrier_subject(self.session_id, member, previous_id),
                carrier_data(
                    &self.outbound,
                    &path_name,
                    self.session_id,
                    member,
                    previous_id,
                    previous.path_id,
                    previous.configured_slot,
                    previous.sequence,
                    Some("ready"),
                    "draining",
                    previous.local,
                    previous.peer,
                    previous.address_revision,
                    Some("replacement_published"),
                    previous.usage,
                ),
            );
        }
        state.current_by_member.insert(member, instance);
        state.member_states.insert(member, Availability::Up);
        state.paths[logical_index].last_ready_at = Some(SystemTime::now());
        state.carriers.insert(
            instance,
            ObservedCarrier {
                member,
                path_id,
                configured_slot,
                state: CarrierState::Ready,
                sequence: 1,
                local,
                peer,
                address_revision,
                usage: Some(usage),
            },
        );
        push_carrier_emission(
            &self.publisher,
            &mut emissions,
            EventKind::CarrierStateChanged,
            carrier_subject(self.session_id, member, instance),
            carrier_data(
                &self.outbound,
                &path_name,
                self.session_id,
                member,
                instance,
                path_id,
                configured_slot,
                1,
                None,
                "ready",
                local,
                peer,
                address_revision,
                Some("authenticated"),
                Some(usage),
            ),
        );
        emissions.extend(transition_aggregate_path(
            &self.publisher,
            &self.outbound,
            &mut state,
            logical_index,
            "carrier_ready",
        ));
        let total_ready = ready_carrier_count(&state);
        if ready_before == 0 && total_ready > 0 && !state.session.retired {
            state.session.ready_carriers = total_ready;
            state.session.sequence = state.session.sequence.saturating_add(1);
            let reattached = state.session.ever_attached;
            let initial = !state.session.ever_attached;
            state.session.ever_attached = true;
            let from = if reattached { Some("detached") } else { None };
            push_session_emission(
                &self.publisher,
                &mut emissions,
                &self.outbound,
                self.session_id,
                state.session.sequence,
                total_ready,
                from,
                "attached",
                initial,
                if reattached { Some("reattached") } else { None },
            );
        } else {
            state.session.ready_carriers = total_ready;
        }
        update_client_peer_ip_set(
            &self.publisher,
            &self.outbound,
            self.session_id,
            &mut state,
            &mut emissions,
            "carrier_ready",
        );
        self.emit(emissions);
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn replace_ready(
        &self,
        member: RelayPathKey,
        predecessor: CarrierPathInstanceId,
        successor: CarrierPathInstanceId,
        successor_path_id: PathId,
        local: Option<SocketAddr>,
        peer: Option<SocketAddr>,
        address_revision: u64,
        usage: PathUsage,
    ) {
        if predecessor == successor {
            return;
        }
        let predecessor_is_current = self
            .state
            .lock()
            .expect("client webhook observation lock")
            .current_by_member
            .get(&member)
            == Some(&predecessor);
        if !predecessor_is_current {
            self.publish_ready(
                member,
                successor,
                successor_path_id,
                local,
                peer,
                address_revision,
                usage,
            );
            return;
        }
        self.publish_ready(
            member,
            successor,
            successor_path_id,
            local,
            peer,
            address_revision,
            usage,
        );
    }

    pub(super) fn begin_retirement(&self, member: RelayPathKey, instance: CarrierPathInstanceId) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        let path_name = state.paths[logical_index].name.clone();
        let ready_before = ready_carrier_count(&state);
        let Some(carrier) = state.carriers.get_mut(&instance) else {
            return;
        };
        if carrier.member != member || carrier.state != CarrierState::Ready {
            return;
        }
        carrier.state = CarrierState::Draining;
        carrier.sequence = carrier.sequence.saturating_add(1);
        let sequence = carrier.sequence;
        let local = carrier.local;
        let peer = carrier.peer;
        let usage = carrier.usage;
        let path_id = carrier.path_id;
        let configured_slot = carrier.configured_slot;
        let address_revision = carrier.address_revision;
        let mut emissions = Vec::new();
        push_carrier_emission(
            &self.publisher,
            &mut emissions,
            EventKind::CarrierStateChanged,
            carrier_subject(self.session_id, member, instance),
            carrier_data(
                &self.outbound,
                &path_name,
                self.session_id,
                member,
                instance,
                path_id,
                configured_slot,
                sequence,
                Some("ready"),
                "draining",
                local,
                peer,
                address_revision,
                Some("planned"),
                usage,
            ),
        );
        emissions.extend(transition_aggregate_path(
            &self.publisher,
            &self.outbound,
            &mut state,
            logical_index,
            "planned_retirement",
        ));
        let total_ready = ready_carrier_count(&state);
        if ready_before > 0 && total_ready == 0 && !state.session.retired {
            state.session.ready_carriers = 0;
            state.session.sequence = state.session.sequence.saturating_add(1);
            push_session_emission(
                &self.publisher,
                &mut emissions,
                &self.outbound,
                self.session_id,
                state.session.sequence,
                0,
                Some("attached"),
                "detached",
                false,
                Some("planned_retirement"),
            );
        } else {
            state.session.ready_carriers = total_ready;
        }
        update_client_peer_ip_set(
            &self.publisher,
            &self.outbound,
            self.session_id,
            &mut state,
            &mut emissions,
            "planned_retirement",
        );
        self.emit(emissions);
    }

    pub(super) fn close(
        &self,
        member: RelayPathKey,
        instance: CarrierPathInstanceId,
        planned: bool,
        reason: &'static str,
    ) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(previous) = state.carriers.remove(&instance) else {
            return;
        };
        if previous.member != member {
            state.carriers.insert(instance, previous);
            return;
        }
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        let path_name = state.paths[logical_index].name.clone();
        let was_current = state.current_by_member.get(&member) == Some(&instance);
        let ready_before =
            ready_carrier_count(&state) + usize::from(previous.state == CarrierState::Ready);
        if was_current {
            state.current_by_member.remove(&member);
            state.member_states.insert(
                member,
                if planned {
                    Availability::Idle
                } else {
                    Availability::Down
                },
            );
        }
        let mut emissions = Vec::new();
        push_carrier_emission(
            &self.publisher,
            &mut emissions,
            EventKind::CarrierStateChanged,
            carrier_subject(self.session_id, member, instance),
            carrier_data(
                &self.outbound,
                &path_name,
                self.session_id,
                member,
                instance,
                previous.path_id,
                previous.configured_slot,
                previous.sequence.saturating_add(1),
                Some(previous.state.as_str()),
                "closed",
                previous.local,
                previous.peer,
                previous.address_revision,
                Some(reason),
                previous.usage,
            ),
        );
        if was_current {
            emissions.extend(transition_aggregate_path(
                &self.publisher,
                &self.outbound,
                &mut state,
                logical_index,
                reason,
            ));
        }
        let total_ready = ready_carrier_count(&state);
        if ready_before > 0 && total_ready == 0 && !state.session.retired {
            state.session.ready_carriers = 0;
            state.session.sequence = state.session.sequence.saturating_add(1);
            push_session_emission(
                &self.publisher,
                &mut emissions,
                &self.outbound,
                self.session_id,
                state.session.sequence,
                0,
                Some("attached"),
                "detached",
                false,
                Some(reason),
            );
        } else {
            state.session.ready_carriers = total_ready;
        }
        update_client_peer_ip_set(
            &self.publisher,
            &self.outbound,
            self.session_id,
            &mut state,
            &mut emissions,
            "carrier_closed",
        );
        self.emit(emissions);
    }

    pub(super) fn update_peer_usage(
        &self,
        member: RelayPathKey,
        instance: CarrierPathInstanceId,
        usage: PathUsage,
    ) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        let path_name = state.paths[logical_index].name.clone();
        let Some(carrier) = state.carriers.get_mut(&instance) else {
            return;
        };
        if carrier.member != member || carrier.usage == Some(usage) {
            return;
        }
        let before = carrier.usage.replace(usage);
        carrier.sequence = carrier.sequence.saturating_add(1);
        let sequence = carrier.sequence;
        let carrier_state = carrier.state.as_str();
        let local = carrier.local;
        let peer = carrier.peer;
        let path_id = carrier.path_id;
        let configured_slot = carrier.configured_slot;
        let data = json!({
            "path": { "name": path_name, "outbound": self.outbound },
            "carrier": {
                "id": instance.as_u64().to_string(),
                "transport": underlay_name(member.underlay),
                "configured_slot": configured_slot,
                "path_id": path_id.0,
                "instance": instance.as_u64(),
                "state": carrier_state,
                "local": address_json(local),
                "peer": address_json(peer),
                "peer_usage": usage_json(Some(usage)),
            },
            "session": { "id": session_id_text(self.session_id) },
            "change": {
                "field": "peer_usage",
                "from": usage_name(before),
                "to": usage_name(Some(usage)),
                "before": usage_name(before),
                "after": usage_name(Some(usage)),
            },
            "reason": "peer_path_usage",
            "subject_sequence": sequence,
        });
        self.emit(vec![(
            EventKind::CarrierPolicyChanged,
            carrier_subject(self.session_id, member, instance),
            data,
        )]);
    }

    pub(super) fn update_path_policy(
        &self,
        config_ordinal: usize,
        policy: &'static str,
        actor: &'static str,
    ) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state
            .paths
            .iter()
            .position(|path| path.config_ordinal == config_ordinal)
        else {
            return;
        };
        if state.paths[logical_index].policy == policy {
            return;
        }
        let before = std::mem::replace(&mut state.paths[logical_index].policy, policy.to_owned());
        state.paths[logical_index].sequence = state.paths[logical_index].sequence.saturating_add(1);
        let sequence = state.paths[logical_index].sequence;
        let name = state.paths[logical_index].name.clone();
        let snapshot = path_observation_data(&state, logical_index, &self.outbound);
        let data = json!({
            "path": snapshot,
            "change": {
                "field": "policy",
                "from": before,
                "to": policy,
                "before": before,
                "after": policy,
            },
            "reason": actor,
            "subject_sequence": sequence,
        });
        self.emit(vec![(
            EventKind::PathPolicyChanged,
            path_subject(&self.outbound, &name),
            data,
        )]);
    }

    pub(super) fn retire_session(&self, reason: &str) {
        let mut state = self.state.lock().expect("client webhook observation lock");
        if state.session.retired {
            return;
        }
        state.session.retired = true;
        state.session.sequence = state.session.sequence.saturating_add(1);
        let from = if !state.session.ever_attached {
            None
        } else if state.session.ready_carriers > 0 {
            Some("attached")
        } else {
            Some("detached")
        };
        let data = json!({
            "outbound": { "name": self.outbound },
            "session": { "id": session_id_text(self.session_id), "ready_carriers": state.session.ready_carriers },
            "change": { "from": from, "to": "retired" },
            "reason": reason,
            "initial": !state.session.ever_attached,
            "subject_sequence": state.session.sequence,
        });
        self.emit(vec![(
            EventKind::SessionStateChanged,
            session_subject(self.session_id),
            data,
        )]);
    }

    pub(super) fn update_peer_address(
        &self,
        member: RelayPathKey,
        instance: CarrierPathInstanceId,
        revision: u64,
        peer: SocketAddr,
        skipped_revisions: u64,
    ) {
        if !self.publisher.interested(EventKind::CarrierAddressChanged)
            && !self
                .publisher
                .interested(EventKind::SessionPeerAddressesChanged)
        {
            return;
        }
        let mut state = self.state.lock().expect("client webhook observation lock");
        let Some(logical_index) = state.path_by_member.get(&member).copied() else {
            return;
        };
        let path_name = state.paths[logical_index].name.clone();
        let Some(carrier) = state.carriers.get_mut(&instance) else {
            return;
        };
        if carrier.member != member {
            return;
        }
        let before = carrier.peer;
        if revision <= carrier.address_revision {
            return;
        }
        let skipped_revisions = skipped_revisions.max(
            revision
                .saturating_sub(carrier.address_revision)
                .saturating_sub(1),
        );
        let unchanged = before == Some(peer);
        if unchanged && skipped_revisions == 0 {
            carrier.address_revision = revision;
            return;
        }
        carrier.peer = Some(peer);
        carrier.address_revision = revision;
        carrier.sequence = carrier.sequence.saturating_add(1);
        let sequence = carrier.sequence;
        let local = carrier.local;
        let usage = carrier.usage;
        let carrier_state = carrier.state.as_str();
        let path_id = carrier.path_id;
        let configured_slot = carrier.configured_slot;
        let mut emissions = Vec::new();
        push_carrier_emission(
            &self.publisher,
            &mut emissions,
            EventKind::CarrierAddressChanged,
            carrier_subject(self.session_id, member, instance),
            json!({
                "path": { "name": path_name, "outbound": self.outbound },
                "carrier": {
                    "id": instance.as_u64().to_string(),
                    "transport": underlay_name(member.underlay),
                    "configured_slot": configured_slot,
                    "path_id": path_id.0,
                    "instance": instance.as_u64(),
                    "state": carrier_state,
                    "local": address_json(local),
                    "peer": address_json(Some(peer)),
                    "peer_usage": usage_json(usage),
                    "address_revision": revision,
                },
                "change": {
                    "before": address_change_json(before),
                    "after": address_change_json(Some(peer)),
                    "components": address_change_components(before, Some(peer)),
                    "skipped_revisions": skipped_revisions,
                    "coalesced": skipped_revisions > 0,
                },
                "reason": "validated_migration",
                "subject_sequence": sequence,
            }),
        );
        update_client_peer_ip_set(
            &self.publisher,
            &self.outbound,
            self.session_id,
            &mut state,
            &mut emissions,
            "validated_migration",
        );
        self.emit(emissions);
    }

    fn emit(&self, emissions: Vec<(EventKind, String, Value)>) {
        for (kind, subject, data) in emissions {
            if self.publisher.interested(kind) {
                self.publisher.emit(kind, &subject, data);
            }
        }
    }
}

impl std::fmt::Debug for ClientPathWebhookObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClientPathWebhookObserver")
            .field("session_id", &self.session_id)
            .field("outbound", &self.outbound)
            .finish_non_exhaustive()
    }
}

pub(in crate::runtime) struct ClientProbeAttempt {
    observer: Arc<ClientPathWebhookObserver>,
    logical_index: usize,
    path_name: String,
    member: RelayPathKey,
    configured_slot: u16,
    id: u64,
    trigger: ProbeTrigger,
    state_at_start: Availability,
    started_at: SystemTime,
    started_monotonic: std::time::Instant,
    settled: bool,
}

impl ClientProbeAttempt {
    pub(super) fn finish(mut self, outcome: &'static str, applied: bool) {
        self.settled = true;
        self.publish(outcome, applied);
    }

    fn publish(&self, outcome: &'static str, applied: bool) {
        let mut state = self
            .observer
            .state
            .lock()
            .expect("client webhook observation lock");
        let Some(path) = state.paths.get_mut(self.logical_index) else {
            return;
        };
        path.sequence = path.sequence.saturating_add(1);
        let name = path.name.clone();
        let sequence = path.sequence;
        let completed = SystemTime::now();
        let elapsed = self.started_monotonic.elapsed();
        let path = path_observation_data(&state, self.logical_index, &self.observer.outbound);
        let data = json!({
            "path": path,
            "probe": {
                "id": self.id,
                "trigger": self.trigger.as_str(),
                "started_at": timestamp(self.started_at),
                "completed_at": timestamp(completed),
                "duration_s": elapsed.as_secs_f64(),
                "state_at_start": self.state_at_start.as_str(),
                "outcome": outcome,
                "applied": applied,
                "transport": underlay_name(self.member.underlay),
                "configured_slot": self.configured_slot,
            },
            "subject_sequence": sequence,
        });
        self.observer.publisher.emit(
            EventKind::PathProbeCompleted,
            &path_subject(&self.observer.outbound, &name),
            data,
        );
    }
}

impl Drop for ClientProbeAttempt {
    fn drop(&mut self) {
        if !self.settled {
            self.settled = true;
            // Probe completion ordering shares the path subject sequence with
            // transitions, so enqueue while holding the observer lock. This is
            // a short synchronous critical section and the publisher itself
            // uses bounded try-enqueue semantics.
            let mut state = self
                .observer
                .state
                .lock()
                .expect("client webhook observation lock");
            let Some(path) = state.paths.get_mut(self.logical_index) else {
                return;
            };
            path.sequence = path.sequence.saturating_add(1);
            let sequence = path.sequence;
            let path_data =
                path_observation_data(&state, self.logical_index, &self.observer.outbound);
            let completed = SystemTime::now();
            let data = json!({
                "path": path_data,
                "probe": {
                    "id": self.id,
                    "trigger": self.trigger.as_str(),
                    "started_at": timestamp(self.started_at),
                    "completed_at": timestamp(completed),
                    "duration_s": self.started_monotonic.elapsed().as_secs_f64(),
                    "state_at_start": self.state_at_start.as_str(),
                    "outcome": "cancelled",
                    "applied": false,
                    "transport": underlay_name(self.member.underlay),
                    "configured_slot": self.configured_slot,
                },
                "subject_sequence": sequence,
            });
            self.observer.publisher.emit(
                EventKind::PathProbeCompleted,
                &path_subject(&self.observer.outbound, &self.path_name),
                data,
            );
        }
    }
}

fn ready_count(state: &ClientObservationState, path: &ObservedPath) -> usize {
    state
        .carriers
        .values()
        .filter(|carrier| {
            path.members.contains(&carrier.member) && carrier.state == CarrierState::Ready
        })
        .count()
}

fn draining_count(state: &ClientObservationState, path: &ObservedPath) -> usize {
    state
        .carriers
        .values()
        .filter(|carrier| {
            path.members.contains(&carrier.member) && carrier.state == CarrierState::Draining
        })
        .count()
}

fn path_counts(state: &ClientObservationState, path: &ObservedPath) -> (usize, usize) {
    (ready_count(state, path), draining_count(state, path))
}

fn ready_carrier_count(state: &ClientObservationState) -> usize {
    state
        .carriers
        .values()
        .filter(|carrier| carrier.state == CarrierState::Ready)
        .count()
}

fn aggregate_path_state(state: &ClientObservationState, path: &ObservedPath) -> Availability {
    if ready_count(state, path) > 0 {
        return Availability::Up;
    }
    if draining_count(state, path) > 0 {
        return Availability::Idle;
    }
    if path
        .members
        .iter()
        .all(|member| state.member_states.get(member) == Some(&Availability::Down))
    {
        return Availability::Down;
    }
    if path
        .members
        .iter()
        .any(|member| state.member_states.get(member) == Some(&Availability::Idle))
    {
        return Availability::Idle;
    }
    Availability::Unknown
}

fn transition_aggregate_path(
    publisher: &EventPublisher,
    outbound: &str,
    state: &mut ClientObservationState,
    logical_index: usize,
    reason: &str,
) -> Vec<(EventKind, String, Value)> {
    let Some(path) = state.paths.get(logical_index) else {
        return Vec::new();
    };
    let next = aggregate_path_state(state, path);
    transition_path(publisher, outbound, state, logical_index, next, reason)
}

fn transition_path(
    publisher: &EventPublisher,
    outbound: &str,
    state: &mut ClientObservationState,
    logical_index: usize,
    to: Availability,
    reason: &str,
) -> Vec<(EventKind, String, Value)> {
    let (from, name, members, sequence) = {
        let Some(path) = state.paths.get_mut(logical_index) else {
            return Vec::new();
        };
        let from = path.state;
        if from == to {
            return Vec::new();
        }
        path.state = to;
        path.sequence = path.sequence.saturating_add(1);
        (from, path.name.clone(), path.members.clone(), path.sequence)
    };
    if !publisher.interested(EventKind::PathStateChanged) {
        return Vec::new();
    }
    let ready_carriers = members
        .iter()
        .filter(|member| {
            state
                .carriers
                .values()
                .any(|carrier| carrier.member == **member && carrier.state == CarrierState::Ready)
        })
        .count();
    let draining_carriers = members
        .iter()
        .filter(|member| {
            state.carriers.values().any(|carrier| {
                carrier.member == **member && carrier.state == CarrierState::Draining
            })
        })
        .count();
    vec![(
        EventKind::PathStateChanged,
        path_subject(outbound, &name),
        json!({
            "path": {
                "name": name,
                "outbound": outbound,
                "state": to.as_str(),
                "ready_carriers": ready_carriers,
                "draining_carriers": draining_carriers,
                "transports": state.paths[logical_index].transports,
                "local_ips": peer_ip_values(&configured_and_observed_local_ips(
                    state,
                    &state.paths[logical_index],
                )),
                "policy": state.paths[logical_index].policy,
                "last_ready_at": state.paths[logical_index].last_ready_at.map(timestamp),
                "subject_sequence": sequence,
            },
            "change": { "from": from.as_str(), "to": to.as_str() },
            "reason": reason,
            "subject_sequence": sequence,
        }),
    )]
}

fn path_observation_data(
    state: &ClientObservationState,
    logical_index: usize,
    outbound: &str,
) -> Value {
    let path = &state.paths[logical_index];
    let (ready_carriers, draining_carriers) = path_counts(state, path);
    json!({
        "name": path.name,
        "outbound": outbound,
        "state": path.state.as_str(),
        "transports": path.transports,
        "local_ips": peer_ip_values(&configured_and_observed_local_ips(state, path)),
        "policy": path.policy,
        "ready_carriers": ready_carriers,
        "draining_carriers": draining_carriers,
        "last_ready_at": path.last_ready_at.map(timestamp),
        "subject_sequence": path.sequence,
    })
}

fn configured_and_observed_local_ips(
    state: &ClientObservationState,
    path: &ObservedPath,
) -> BTreeSet<IpAddr> {
    let mut local_ips = path.configured_local_ips.clone();
    for member in &path.members {
        local_ips.extend(state.carriers.values().filter_map(|carrier| {
            (carrier.member == *member)
                .then(|| {
                    carrier
                        .local
                        .map(|address| address.ip())
                        .filter(|ip| !ip.is_unspecified())
                })
                .flatten()
        }));
    }
    local_ips
}

fn update_client_peer_ip_set(
    publisher: &EventPublisher,
    outbound: &str,
    session_id: SessionId,
    state: &mut ClientObservationState,
    emissions: &mut Vec<(EventKind, String, Value)>,
    reason: &str,
) {
    let next = state
        .carriers
        .values()
        .filter(|carrier| carrier.state == CarrierState::Ready)
        .filter_map(|carrier| carrier.peer.map(|address| address.ip()))
        .collect::<BTreeSet<_>>();
    if next == state.session.peer_ips {
        return;
    }
    let before = std::mem::replace(&mut state.session.peer_ips, next);
    if state.session.retired {
        return;
    }
    state.session.sequence = state.session.sequence.saturating_add(1);
    let initial = !state.session.had_peer_addresses;
    state.session.had_peer_addresses = true;
    if !publisher.interested(EventKind::SessionPeerAddressesChanged) {
        return;
    }
    emissions.push((
        EventKind::SessionPeerAddressesChanged,
        session_subject(session_id),
        json!({
            "outbound": { "name": outbound },
            "session": {
                "id": session_id_text(session_id),
                "ready_carriers": state.session.ready_carriers,
                "peer_ips": peer_ip_values(&state.session.peer_ips),
            },
            "change": {
                "before": peer_ip_values(&before),
                "after": peer_ip_values(&state.session.peer_ips),
                "components": ["ip"],
            },
            "initial": initial,
            "reason": reason,
            "subject_sequence": state.session.sequence,
        }),
    ));
}

fn peer_ip_values(addresses: &BTreeSet<IpAddr>) -> Vec<String> {
    addresses.iter().map(ToString::to_string).collect()
}

fn push_carrier_emission(
    publisher: &EventPublisher,
    emissions: &mut Vec<(EventKind, String, Value)>,
    kind: EventKind,
    subject: String,
    data: Value,
) {
    if !publisher.interested(kind) {
        return;
    }
    emissions.push((kind, subject, data));
}

#[allow(clippy::too_many_arguments)]
fn push_session_emission(
    publisher: &EventPublisher,
    emissions: &mut Vec<(EventKind, String, Value)>,
    outbound: &str,
    session_id: SessionId,
    sequence: u64,
    ready_carriers: usize,
    from: Option<&str>,
    to: &str,
    initial: bool,
    reason: Option<&str>,
) {
    if !publisher.interested(EventKind::SessionStateChanged) {
        return;
    }
    emissions.push((
        EventKind::SessionStateChanged,
        session_subject(session_id),
        json!({
            "outbound": { "name": outbound },
            "session": { "id": session_id_text(session_id), "ready_carriers": ready_carriers },
            "change": { "from": from, "to": to },
            "initial": initial,
            "reason": reason,
            "subject_sequence": sequence,
        }),
    ));
}

// Frozen lifecycle fields are passed explicitly rather than reacquiring a
// mutable transport owner while constructing the event snapshot.
#[allow(clippy::too_many_arguments)]
fn carrier_data(
    outbound: &str,
    path_name: &str,
    session_id: SessionId,
    member: RelayPathKey,
    instance: CarrierPathInstanceId,
    path_id: PathId,
    configured_slot: u16,
    sequence: u64,
    from: Option<&str>,
    state: &str,
    local: Option<SocketAddr>,
    peer: Option<SocketAddr>,
    address_revision: u64,
    reason: Option<&str>,
    usage: Option<PathUsage>,
) -> Value {
    let mut data = carrier_identity(
        session_id,
        member,
        instance,
        path_id,
        configured_slot,
        sequence,
    );
    data["path"] = json!({ "name": path_name, "outbound": outbound });
    if let Some(carrier) = data.get_mut("carrier").and_then(Value::as_object_mut) {
        carrier.insert("state".to_owned(), Value::String(state.to_owned()));
        carrier.insert("local".to_owned(), address_json(local));
        carrier.insert("peer".to_owned(), address_json(peer));
        carrier.insert("address_revision".to_owned(), json!(address_revision));
        carrier.insert("peer_usage".to_owned(), usage_json(usage));
    }
    data["change"] = json!({ "from": from, "to": state });
    data["initial"] = json!(from.is_none());
    data["reason"] = json!(reason);
    data
}

fn carrier_identity(
    session_id: SessionId,
    member: RelayPathKey,
    instance: CarrierPathInstanceId,
    path_id: PathId,
    configured_slot: u16,
    sequence: u64,
) -> Value {
    json!({
        "carrier": {
            "id": instance.as_u64().to_string(),
            "transport": underlay_name(member.underlay),
            "configured_slot": configured_slot,
            "path_id": path_id.0,
            "instance": instance.as_u64(),
        },
        "session": { "id": session_id_text(session_id) },
        "subject_sequence": sequence,
    })
}

fn address_json(address: Option<SocketAddr>) -> Value {
    json!({
        "ip": address
            .filter(|address| !address.ip().is_unspecified())
            .map(|address| address.ip().to_string()),
        "port": address.map(|address| address.port()),
    })
}

fn address_change_json(address: Option<SocketAddr>) -> Value {
    address_json(address)
}

fn address_change_components(
    before: Option<SocketAddr>,
    after: Option<SocketAddr>,
) -> Vec<&'static str> {
    let mut components = Vec::new();
    if before.map(|address| address.ip()) != after.map(|address| address.ip()) {
        components.push("ip");
    }
    if before.map(|address| address.port()) != after.map(|address| address.port()) {
        components.push("port");
    }
    components
}

fn usage_json(usage: Option<PathUsage>) -> Value {
    match usage {
        Some(PathUsage::Available) => Value::String("available".to_owned()),
        Some(PathUsage::Backup) => Value::String("backup".to_owned()),
        None => Value::Null,
    }
}

fn usage_name(usage: Option<PathUsage>) -> Value {
    usage_json(usage)
}

fn carrier_subject(
    session_id: SessionId,
    member: RelayPathKey,
    instance: CarrierPathInstanceId,
) -> String {
    format!(
        "carrier:{:016x}/{}/{}/{}",
        session_id.0,
        underlay_name(member.underlay),
        member.index,
        instance.as_u64()
    )
}

fn path_subject(outbound: &str, name: &str) -> String {
    format!("path:{outbound}/{name}")
}

fn session_subject(session_id: SessionId) -> String {
    format!("session:{:016x}", session_id.0)
}

fn session_id_text(session_id: SessionId) -> String {
    format!("{:016x}", session_id.0)
}

const fn underlay_name(underlay: UnderlayProtocol) -> &'static str {
    match underlay {
        UnderlayProtocol::Tcp => "tcp",
        UnderlayProtocol::Udp => "quic",
    }
}

fn timestamp(time: SystemTime) -> String {
    let since_epoch = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let nanos =
        i128::from(since_epoch.as_secs()) * 1_000_000_000 + i128::from(since_epoch.subsec_nanos());
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map(|value| {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}Z",
                value.year(),
                u8::from(value.month()),
                value.day(),
                value.hour(),
                value.minute(),
                value.second(),
                value.nanosecond(),
            )
        })
        .unwrap_or_else(|_| format!("unix:{}", since_epoch.as_secs()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::webhook::WebhookTestCaptureHandle;

    const OUTBOUND: &str = "edge";
    const PATH_NAME: &str = "wan";
    const SESSION: SessionId = SessionId(0x12ab);
    const LOCAL: SocketAddr =
        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 4)), 31000);
    const PEER_A: SocketAddr =
        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 8)), 443);
    const PEER_B: SocketAddr =
        SocketAddr::new(IpAddr::V4(std::net::Ipv4Addr::new(198, 51, 100, 9)), 443);

    fn member(underlay: UnderlayProtocol, index: usize) -> RelayPathKey {
        RelayPathKey { underlay, index }
    }

    fn observer(
        kinds: &[EventKind],
        members: Vec<RelayPathKey>,
    ) -> (Arc<ClientPathWebhookObserver>, WebhookTestCaptureHandle) {
        let (publisher, capture) = EventPublisher::test_capture(kinds, 64);
        let transports = members
            .iter()
            .map(|member| underlay_name(member.underlay).to_owned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let member_slots = members
            .iter()
            .map(|member| {
                (
                    *member,
                    u16::try_from(member.index).expect("test member slot fits wire slot"),
                )
            })
            .collect();
        let observer = ClientPathWebhookObserver::new(
            publisher,
            OUTBOUND.to_owned(),
            SESSION,
            vec![ConfiguredPathObservation {
                name: PATH_NAME.to_owned(),
                config_ordinal: 0,
                members,
                member_slots,
                transports,
                local_ips: vec![LOCAL.ip().to_string()],
            }],
        );
        (observer, capture)
    }

    fn publish_ready(
        observer: &ClientPathWebhookObserver,
        member: RelayPathKey,
        instance: u64,
        path_id: u16,
        peer: SocketAddr,
        revision: u64,
    ) {
        observer.publish_ready(
            member,
            CarrierPathInstanceId::from_raw(instance),
            PathId(path_id),
            Some(LOCAL),
            Some(peer),
            revision,
            PathUsage::Available,
        );
    }

    fn event_kind(event: &Value) -> &str {
        event
            .pointer("/event/type")
            .and_then(Value::as_str)
            .expect("canonical event type")
    }

    #[test]
    fn probe_only_observer_tracks_ready_members_and_preserves_canonical_envelope() {
        let member = member(UnderlayProtocol::Tcp, 0);
        let (observer, capture) = observer(&[EventKind::PathProbeCompleted], vec![member]);
        assert!(observer.wants_carrier_details());

        let probe = observer
            .begin_probe(member, ProbeTrigger::Periodic)
            .expect("interested background establish probe");
        publish_ready(&observer, member, 7, 11, PEER_A, 1);
        probe.finish("success", true);

        let events = capture.snapshot();
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event_kind(event), "path.probe_completed");
        assert!(event.pointer("/event/id").and_then(Value::as_str).is_some());
        assert_eq!(
            event.pointer("/path/name").and_then(Value::as_str),
            Some(PATH_NAME)
        );
        assert_eq!(
            event.pointer("/path/outbound").and_then(Value::as_str),
            Some(OUTBOUND)
        );
        assert_eq!(
            event.pointer("/path/state").and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            event
                .pointer("/probe/state_at_start")
                .and_then(Value::as_str),
            Some("unknown")
        );
        assert_eq!(
            event.pointer("/probe/outcome").and_then(Value::as_str),
            Some("success")
        );
        assert_eq!(
            event.pointer("/probe/applied").and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            event
                .pointer("/probe/configured_slot")
                .and_then(Value::as_u64),
            Some(0)
        );
        assert_eq!(
            event.pointer("/event/type").and_then(Value::as_str),
            Some("path.probe_completed")
        );
        assert_eq!(
            event.pointer("/event/subject_sequence"),
            event.get("subject_sequence")
        );

        let snapshot = observer.snapshot(0).expect("configured path snapshot");
        assert_eq!(snapshot.get("state").and_then(Value::as_str), Some("up"));
        assert_eq!(
            snapshot.get("ready_carriers").and_then(Value::as_u64),
            Some(1)
        );
        assert_eq!(
            snapshot
                .get("local_ips")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn pooled_path_goes_down_only_after_last_ready_carrier_and_recovers_on_probe() {
        let tcp = member(UnderlayProtocol::Tcp, 0);
        let quic = member(UnderlayProtocol::Udp, 0);
        let (observer, capture) = observer(
            &[EventKind::PathStateChanged, EventKind::PathProbeCompleted],
            vec![tcp, quic],
        );
        publish_ready(&observer, tcp, 10, 1, PEER_A, 1);
        publish_ready(&observer, quic, 20, 2, PEER_B, 1);
        assert_eq!(
            capture.snapshot().len(),
            1,
            "first ready member brings aggregate up once"
        );

        observer.close(
            tcp,
            CarrierPathInstanceId::from_raw(10),
            false,
            "carrier_lost",
        );
        assert_eq!(
            capture.snapshot().len(),
            1,
            "one pooled member remains ready"
        );
        observer.close(
            quic,
            CarrierPathInstanceId::from_raw(20),
            false,
            "carrier_lost",
        );
        let events = capture.snapshot();
        let path_events = events
            .iter()
            .filter(|event| event_kind(event) == "path.state_changed")
            .collect::<Vec<_>>();
        assert_eq!(path_events.len(), 2);
        assert_eq!(
            path_events[0]
                .pointer("/change/from")
                .and_then(Value::as_str),
            Some("unknown")
        );
        assert_eq!(
            path_events[0].pointer("/change/to").and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            path_events[1]
                .pointer("/change/from")
                .and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            path_events[1].pointer("/change/to").and_then(Value::as_str),
            Some("down")
        );

        let probe = observer
            .begin_probe(quic, ProbeTrigger::Reconcile)
            .expect("one owner probe for the vacant member");
        publish_ready(&observer, quic, 21, 3, PEER_B, 1);
        probe.finish("success", true);
        let events = capture.snapshot();
        let path_events = events
            .iter()
            .filter(|event| event_kind(event) == "path.state_changed")
            .collect::<Vec<_>>();
        assert_eq!(path_events.len(), 3);
        assert_eq!(
            path_events[2]
                .pointer("/change/from")
                .and_then(Value::as_str),
            Some("down")
        );
        assert_eq!(
            path_events[2].pointer("/change/to").and_then(Value::as_str),
            Some("up")
        );
        let probe = events
            .iter()
            .find(|event| event_kind(event) == "path.probe_completed")
            .expect("recovery probe completion");
        assert_eq!(
            probe
                .pointer("/probe/state_at_start")
                .and_then(Value::as_str),
            Some("down")
        );
        assert_eq!(
            probe.pointer("/probe/trigger").and_then(Value::as_str),
            Some("reconcile")
        );
    }

    #[test]
    fn replacement_waits_for_exact_predecessor_close_without_path_or_session_flap() {
        let tcp = member(UnderlayProtocol::Tcp, 0);
        let (observer, capture) = observer(
            &[
                EventKind::PathStateChanged,
                EventKind::CarrierStateChanged,
                EventKind::SessionStateChanged,
            ],
            vec![tcp, member(UnderlayProtocol::Udp, 0)],
        );
        publish_ready(&observer, tcp, 31, 4, PEER_A, 1);
        let before_replace = capture.snapshot();
        let initial_carrier = before_replace
            .iter()
            .find(|event| event_kind(event) == "carrier.state_changed")
            .unwrap();
        assert!(initial_carrier.pointer("/change/from").unwrap().is_null());
        assert_eq!(
            initial_carrier
                .pointer("/event/initial")
                .and_then(Value::as_bool),
            Some(true)
        );
        let initial_session = before_replace
            .iter()
            .find(|event| event_kind(event) == "session.state_changed")
            .unwrap();
        assert!(initial_session.pointer("/change/from").unwrap().is_null());
        assert_eq!(
            initial_session
                .pointer("/session/id")
                .and_then(Value::as_str),
            Some("00000000000012ab")
        );
        assert_eq!(
            initial_session
                .pointer("/outbound/name")
                .and_then(Value::as_str),
            Some(OUTBOUND)
        );

        observer.replace_ready(
            tcp,
            CarrierPathInstanceId::from_raw(31),
            CarrierPathInstanceId::from_raw(32),
            PathId(5),
            Some(LOCAL),
            Some(PEER_A),
            1,
            PathUsage::Available,
        );
        let replacing = capture.snapshot();
        assert_eq!(
            replacing.len(),
            before_replace.len() + 2,
            "old drains and new becomes ready; no path/session transition"
        );
        assert_eq!(
            observer
                .snapshot(0)
                .unwrap()
                .get("state")
                .and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            observer
                .snapshot(0)
                .unwrap()
                .get("draining_carriers")
                .and_then(Value::as_u64),
            Some(1)
        );

        observer.close(
            tcp,
            CarrierPathInstanceId::from_raw(31),
            false,
            "physical_close",
        );
        let after_stale_close = capture.snapshot();
        assert_eq!(after_stale_close.len(), replacing.len() + 1);
        assert_eq!(
            observer
                .snapshot(0)
                .unwrap()
                .get("state")
                .and_then(Value::as_str),
            Some("up")
        );
        observer.close(
            tcp,
            CarrierPathInstanceId::from_raw(31),
            false,
            "duplicate_close",
        );
        assert_eq!(
            capture.snapshot().len(),
            after_stale_close.len(),
            "duplicate stale retirement emits nothing"
        );

        observer.close(
            tcp,
            CarrierPathInstanceId::from_raw(32),
            false,
            "carrier_lost",
        );
        let events = capture.snapshot();
        let paths = events
            .iter()
            .filter(|event| event_kind(event) == "path.state_changed")
            .collect::<Vec<_>>();
        assert_eq!(
            paths.len(),
            2,
            "replacement itself leaves the logical path up"
        );
        assert_eq!(
            paths[1].pointer("/change/from").and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            paths[1].pointer("/change/to").and_then(Value::as_str),
            Some("unknown")
        );
        let sessions = events
            .iter()
            .filter(|event| event_kind(event) == "session.state_changed")
            .collect::<Vec<_>>();
        assert_eq!(
            sessions.len(),
            2,
            "one initial attach and one true last-carrier detach"
        );
        assert_eq!(
            sessions[1].pointer("/change/from").and_then(Value::as_str),
            Some("attached")
        );
        assert_eq!(
            sessions[1].pointer("/change/to").and_then(Value::as_str),
            Some("detached")
        );
    }

    #[test]
    fn cancellation_and_policy_address_interval_only_sources_are_idempotent() {
        let tcp = member(UnderlayProtocol::Tcp, 0);

        let (probe_observer, probe_capture) = observer(&[EventKind::PathProbeCompleted], vec![tcp]);
        drop(
            probe_observer
                .begin_probe(tcp, ProbeTrigger::Periodic)
                .expect("probe guard"),
        );
        let cancelled = probe_capture.snapshot();
        assert_eq!(cancelled.len(), 1);
        assert_eq!(event_kind(&cancelled[0]), "path.probe_completed");
        assert_eq!(
            cancelled[0]
                .pointer("/probe/outcome")
                .and_then(Value::as_str),
            Some("cancelled")
        );
        assert_eq!(
            cancelled[0]
                .pointer("/probe/applied")
                .and_then(Value::as_bool),
            Some(false)
        );

        let (policy_observer, policy_capture) =
            observer(&[EventKind::PathPolicyChanged], vec![tcp]);
        policy_observer.update_path_policy(0, "backup", "management");
        policy_observer.update_path_policy(0, "backup", "management");
        let policy_events = policy_capture.snapshot();
        assert_eq!(policy_events.len(), 1, "no-op policy update is silent");
        assert_eq!(event_kind(&policy_events[0]), "path.policy_changed");
        assert_eq!(
            policy_events[0]
                .pointer("/path/policy")
                .and_then(Value::as_str),
            Some("backup")
        );
        assert_eq!(
            policy_events[0]
                .pointer("/change/from")
                .and_then(Value::as_str),
            Some("enabled")
        );
        assert_eq!(
            policy_events[0]
                .pointer("/change/to")
                .and_then(Value::as_str),
            Some("backup")
        );

        let (carrier_policy_observer, carrier_policy_capture) =
            observer(&[EventKind::CarrierPolicyChanged], vec![tcp]);
        publish_ready(&carrier_policy_observer, tcp, 40, 6, PEER_A, 1);
        carrier_policy_observer.update_peer_usage(
            tcp,
            CarrierPathInstanceId::from_raw(40),
            PathUsage::Backup,
        );
        carrier_policy_observer.update_peer_usage(
            tcp,
            CarrierPathInstanceId::from_raw(40),
            PathUsage::Backup,
        );
        let policy_events = carrier_policy_capture.snapshot();
        assert_eq!(policy_events.len(), 1, "peer-policy no-op emits nothing");
        assert_eq!(event_kind(&policy_events[0]), "carrier.policy_changed");
        assert_eq!(
            policy_events[0]
                .pointer("/change/from")
                .and_then(Value::as_str),
            Some("available")
        );
        assert_eq!(
            policy_events[0]
                .pointer("/change/to")
                .and_then(Value::as_str),
            Some("backup")
        );

        let (interval_observer, interval_capture) = observer(&[EventKind::PathInterval], vec![tcp]);
        interval_observer.register_interval_snapshots();
        publish_ready(&interval_observer, tcp, 41, 6, PEER_A, 1);
        let interval_snapshot = interval_observer.snapshot(0).unwrap();
        assert_eq!(
            interval_snapshot.get("state").and_then(Value::as_str),
            Some("up")
        );
        assert_eq!(
            interval_snapshot
                .get("local_ips")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            1
        );
        assert!(
            interval_capture.snapshot().is_empty(),
            "interval owner queries snapshots; it does not emit transitions"
        );

        let (address_observer, address_capture) =
            observer(&[EventKind::CarrierAddressChanged], vec![tcp]);
        publish_ready(&address_observer, tcp, 41, 6, PEER_A, 1);
        publish_ready(&address_observer, tcp, 41, 6, PEER_A, 1);
        assert!(
            address_capture.snapshot().is_empty(),
            "carrier-ready is not duplicated into an address-only rule"
        );
        address_observer.update_peer_address(
            tcp,
            CarrierPathInstanceId::from_raw(41),
            2,
            PEER_B,
            0,
        );
        address_observer.update_peer_address(
            tcp,
            CarrierPathInstanceId::from_raw(41),
            2,
            PEER_B,
            0,
        );
        address_observer.update_peer_address(
            tcp,
            CarrierPathInstanceId::from_raw(41),
            4,
            PEER_B,
            1,
        );
        let events = address_capture.snapshot();
        let address_events = events
            .iter()
            .filter(|event| event_kind(event) == "carrier.address_changed")
            .collect::<Vec<_>>();
        assert_eq!(
            address_events.len(),
            2,
            "duplicate revision is silent; revision gap is reported once"
        );
        assert_eq!(
            address_events[0]
                .pointer("/change/components/0")
                .and_then(Value::as_str),
            Some("ip")
        );
        assert_eq!(
            address_events[0]
                .pointer("/carrier/id")
                .and_then(Value::as_str),
            Some("41")
        );
        assert_eq!(
            address_events[0]
                .pointer("/carrier/instance")
                .and_then(Value::as_u64),
            Some(41)
        );
        assert_eq!(
            address_events[1]
                .pointer("/change/coalesced")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            address_events[1]
                .pointer("/change/skipped_revisions")
                .and_then(Value::as_u64),
            Some(1)
        );

        let (session_observer, session_capture) =
            observer(&[EventKind::SessionPeerAddressesChanged], vec![tcp]);
        publish_ready(&session_observer, tcp, 42, 7, PEER_A, 1);
        session_observer.update_peer_address(
            tcp,
            CarrierPathInstanceId::from_raw(42),
            2,
            PEER_B,
            0,
        );
        session_observer.update_peer_address(
            tcp,
            CarrierPathInstanceId::from_raw(42),
            3,
            PEER_B,
            0,
        );
        let session_addresses = session_capture.snapshot();
        assert_eq!(
            session_addresses.len(),
            2,
            "ready-only peer IP set changes at attach and migration"
        );
        assert_eq!(
            event_kind(&session_addresses[0]),
            "session.peer_addresses_changed"
        );
        assert_eq!(
            session_addresses[0]
                .pointer("/event/initial")
                .and_then(Value::as_bool),
            Some(true)
        );
        assert_eq!(
            session_addresses[1]
                .pointer("/event/initial")
                .and_then(Value::as_bool),
            Some(false)
        );
        assert_eq!(
            session_addresses[1]
                .pointer("/change/components/0")
                .and_then(Value::as_str),
            Some("ip")
        );

        session_observer.begin_retirement(tcp, CarrierPathInstanceId::from_raw(42));
        let after_draining = session_capture.snapshot();
        assert_eq!(
            after_draining
                .iter()
                .filter(|event| event_kind(event) == "session.peer_addresses_changed")
                .count(),
            3,
            "draining carrier leaves the ready-only peer-IP set",
        );
        let final_set = after_draining
            .iter()
            .rfind(|event| event_kind(event) == "session.peer_addresses_changed")
            .unwrap();
        assert_eq!(
            final_set
                .pointer("/session/peer_ips")
                .and_then(Value::as_array)
                .unwrap()
                .len(),
            0
        );

        let (terminal_observer, terminal_capture) = observer(
            &[
                EventKind::SessionStateChanged,
                EventKind::SessionPeerAddressesChanged,
            ],
            vec![tcp],
        );
        publish_ready(&terminal_observer, tcp, 43, 8, PEER_A, 1);
        terminal_observer.retire_session("normal");
        terminal_observer.close(
            tcp,
            CarrierPathInstanceId::from_raw(43),
            false,
            "carrier_lost",
        );
        let terminal_events = terminal_capture.snapshot();
        let session_events = terminal_events
            .iter()
            .filter(|event| event_kind(event) == "session.state_changed")
            .collect::<Vec<_>>();
        assert_eq!(
            session_events.len(),
            2,
            "terminal publication suppresses a later detach"
        );
        assert_eq!(
            session_events[1]
                .pointer("/change/to")
                .and_then(Value::as_str),
            Some("retired")
        );
        assert_eq!(
            terminal_events
                .iter()
                .filter(|event| event_kind(event) == "session.peer_addresses_changed")
                .count(),
            1,
            "carrier cleanup after terminal does not publish a session peer-set update",
        );
    }
}

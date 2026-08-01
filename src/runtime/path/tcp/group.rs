//! Session-owned TCP carrier groups.
//!
//! A group owns configured bounds, configured-minimum member identities, and
//! endpoint establishment policy. Exact carrier actors retain ownership of
//! sockets, wire ordering, Product attachments, and terminal failure.

use crate::model::path::CarrierPathInstanceId;
use crate::protocol::PathId;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::path_record_failure_cooldown;
use crate::scheduler::PathState as SchedulerPathState;
use crate::transport::TcpCarrierRange;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Weak;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ClientTcpEndpointPolicySnapshot {
    pub(in crate::runtime) enabled: bool,
    pub(in crate::runtime) generation: u64,
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpEndpointPolicy {
    changes: watch::Sender<ClientTcpEndpointPolicySnapshot>,
    commitment: Mutex<()>,
}

impl ClientTcpEndpointPolicy {
    fn enabled() -> Arc<Self> {
        let (changes, _) = watch::channel(ClientTcpEndpointPolicySnapshot {
            enabled: true,
            generation: 1,
        });
        Arc::new(Self {
            changes,
            commitment: Mutex::new(()),
        })
    }

    pub(in crate::runtime) fn snapshot(&self) -> ClientTcpEndpointPolicySnapshot {
        *self.changes.borrow()
    }

    pub(in crate::runtime) fn allows(&self, generation: u64) -> bool {
        let snapshot = self.snapshot();
        snapshot.enabled && snapshot.generation == generation
    }

    pub(in crate::runtime) fn with_current<R>(
        &self,
        generation: u64,
        apply: impl FnOnce() -> R,
    ) -> Option<R> {
        let _commitment = self
            .commitment
            .lock()
            .expect("TCP endpoint policy commitment lock");
        let snapshot = self.snapshot();
        (snapshot.enabled && snapshot.generation == generation).then(apply)
    }

    /// Replaces the published policy while the caller owns `commitment`.
    fn replace_enabled(&self, enabled: bool) -> bool {
        let current = self.snapshot();
        if current.enabled == enabled {
            return false;
        }
        self.changes.send_replace(ClientTcpEndpointPolicySnapshot {
            enabled,
            generation: current.generation.wrapping_add(1),
        });
        true
    }

    pub(in crate::runtime) fn subscribe(&self) -> watch::Receiver<ClientTcpEndpointPolicySnapshot> {
        self.changes.subscribe()
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierGroup {
    pub(in crate::runtime) config_index: usize,
    pub(in crate::runtime) range: TcpCarrierRange,
    pub(in crate::runtime) members: Vec<usize>,
    /// Local path-key slots reserved for elastic instances. A slot has no
    /// health record, actor, queue, or ordinary authority while unoccupied.
    pub(in crate::runtime) elastic_slots: Vec<usize>,
    policy: Arc<ClientTcpEndpointPolicy>,
}

impl ClientTcpCarrierGroup {
    pub(in crate::runtime) fn new(
        config_index: usize,
        range: TcpCarrierRange,
        members: Vec<usize>,
    ) -> Self {
        Self {
            config_index,
            range,
            members,
            elastic_slots: Vec::new(),
            policy: ClientTcpEndpointPolicy::enabled(),
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierGroups {
    groups: Box<[ClientTcpCarrierGroup]>,
    elastic_slot_owners: BTreeMap<usize, (usize, u16)>,
    resources: Mutex<ClientTcpCarrierResourceState>,
    changes: watch::Sender<()>,
}

#[derive(Debug)]
struct ClientTcpCarrierResourceState {
    occupied_by_group: Box<[u16]>,
    occupied_path_ids: BTreeSet<u16>,
    occupied_elastic_slots: BTreeSet<usize>,
    next_path_id: u16,
}

/// Exact physical-carrier reservation.
///
/// The reservation starts immediately before connection initiation and stays
/// owned by that one actor through readiness, validation or ordinary use, and
/// ordered drain. Dropping it releases both the group envelope and wire ID.
#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierReservation {
    owner: Weak<ClientTcpCarrierGroups>,
    config_index: usize,
    path_id: PathId,
    elastic_path_index: Option<usize>,
}

impl ClientTcpCarrierReservation {
    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.path_id
    }

    pub(in crate::runtime) fn config_index(&self) -> usize {
        self.config_index
    }

    pub(in crate::runtime) fn elastic_path_index(&self) -> Option<usize> {
        self.elastic_path_index
    }
}

impl Drop for ClientTcpCarrierReservation {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        {
            let mut resources = owner.resources.lock().expect("TCP carrier resource lock");
            let occupied = resources
                .occupied_by_group
                .get_mut(self.config_index)
                .expect("TCP carrier reservation group");
            *occupied = occupied
                .checked_sub(1)
                .expect("TCP carrier group reservation released once");
            assert!(
                resources.occupied_path_ids.remove(&self.path_id.0),
                "TCP wire PathId reservation released once"
            );
            if let Some(path_index) = self.elastic_path_index {
                assert!(
                    resources.occupied_elastic_slots.remove(&path_index),
                    "TCP elastic local path slot released once"
                );
            }
        }
        owner.publish_change();
    }
}

#[derive(Clone, Copy)]
pub(in crate::runtime) struct ClientTcpMinimumRetry {
    endpoint_generation: u64,
    not_before: tokio::time::Instant,
    hop_instance: Option<CarrierPathInstanceId>,
    hop_not_before: Option<tokio::time::Instant>,
    replacement_port: Option<u16>,
}

impl ClientTcpMinimumRetry {
    pub(in crate::runtime) fn new(now: tokio::time::Instant) -> Self {
        Self {
            endpoint_generation: 0,
            not_before: now,
            hop_instance: None,
            hop_not_before: None,
            replacement_port: None,
        }
    }

    pub(in crate::runtime) fn next_maintenance_at(&self) -> Option<tokio::time::Instant> {
        self.hop_not_before
    }
}

impl ClientTcpCarrierGroups {
    pub(in crate::runtime) fn new(groups: Vec<ClientTcpCarrierGroup>) -> Arc<Self> {
        let (changes, _) = watch::channel(());
        let occupied_by_group = vec![0; groups.len()].into_boxed_slice();
        let mut elastic_slot_owners = BTreeMap::new();
        for group in &groups {
            for (offset, &path_index) in group.elastic_slots.iter().enumerate() {
                let member_ordinal = group
                    .range
                    .min()
                    .checked_add(u16::try_from(offset).expect("TCP elastic slot ordinal fits u16"))
                    .expect("TCP elastic member ordinal fits configured range");
                assert!(
                    elastic_slot_owners
                        .insert(path_index, (group.config_index, member_ordinal))
                        .is_none(),
                    "TCP elastic local path slots are unique"
                );
            }
        }
        Arc::new(Self {
            groups: groups.into_boxed_slice(),
            elastic_slot_owners,
            resources: Mutex::new(ClientTcpCarrierResourceState {
                occupied_by_group,
                occupied_path_ids: BTreeSet::new(),
                occupied_elastic_slots: BTreeSet::new(),
                next_path_id: 0,
            }),
            changes,
        })
    }

    /// Reserves one physical carrier and a concurrently unique TCP `PathId`.
    ///
    /// Unoccupied configured maximum capacity has no actor, command queue,
    /// health record, or protocol identity.
    pub(in crate::runtime) fn reserve(
        self: &Arc<Self>,
        config_index: usize,
    ) -> Option<ClientTcpCarrierReservation> {
        self.reserve_inner(config_index, false)
    }

    /// Reserves the physical envelope, wire identity, and one unpublished
    /// local path-key slot for an elastic candidate.
    pub(in crate::runtime) fn reserve_elastic(
        self: &Arc<Self>,
        config_index: usize,
    ) -> Option<ClientTcpCarrierReservation> {
        self.reserve_inner(config_index, true)
    }

    fn reserve_inner(
        self: &Arc<Self>,
        config_index: usize,
        elastic: bool,
    ) -> Option<ClientTcpCarrierReservation> {
        let group = self.get(config_index)?;
        let mut resources = self.resources.lock().expect("TCP carrier resource lock");
        let occupied = *resources.occupied_by_group.get(config_index)?;
        if occupied >= group.range.max() {
            return None;
        }

        let elastic_path_index = if elastic {
            Some(
                group
                    .elastic_slots
                    .iter()
                    .copied()
                    .find(|slot| !resources.occupied_elastic_slots.contains(slot))?,
            )
        } else {
            None
        };

        let mut candidate = resources.next_path_id;
        let path_id = (0..=u16::MAX).find_map(|_| {
            let available = !resources.occupied_path_ids.contains(&candidate);
            let selected = available.then_some(candidate);
            candidate = candidate.wrapping_add(1);
            selected
        })?;
        resources.next_path_id = candidate;
        resources.occupied_path_ids.insert(path_id);
        if let Some(path_index) = elastic_path_index {
            resources.occupied_elastic_slots.insert(path_index);
        }
        resources.occupied_by_group[config_index] = occupied + 1;
        drop(resources);

        Some(ClientTcpCarrierReservation {
            owner: Arc::downgrade(self),
            config_index,
            path_id: PathId(path_id),
            elastic_path_index,
        })
    }

    pub(in crate::runtime) fn occupied(&self, config_index: usize) -> Option<u16> {
        self.resources
            .lock()
            .expect("TCP carrier resource lock")
            .occupied_by_group
            .get(config_index)
            .copied()
    }

    /// Resolves immutable configured ownership without consulting live
    /// reservation state. Callers pair this metadata with active health or an
    /// exact reservation owner before granting any carrier authority.
    pub(in crate::runtime) fn elastic_path_owner(&self, path_index: usize) -> Option<(usize, u16)> {
        self.elastic_slot_owners.get(&path_index).copied()
    }

    pub(in crate::runtime) fn iter(&self) -> std::slice::Iter<'_, ClientTcpCarrierGroup> {
        self.groups.iter()
    }

    pub(in crate::runtime) fn get(&self, config_index: usize) -> Option<&ClientTcpCarrierGroup> {
        self.groups.get(config_index)
    }

    pub(in crate::runtime) fn len(&self) -> usize {
        self.groups.len()
    }

    pub(in crate::runtime) fn endpoint_policy(
        &self,
        config_index: usize,
    ) -> Option<Arc<ClientTcpEndpointPolicy>> {
        self.get(config_index).map(|group| group.policy.clone())
    }

    pub(in crate::runtime) fn publish_change(&self) {
        self.changes.send_modify(|_| {});
    }

    pub(in crate::runtime) fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }

    pub(in crate::runtime) fn next_maintenance_at(
        &self,
        context: &ClientPathContext,
        retry: &[ClientTcpMinimumRetry],
    ) -> Option<tokio::time::Instant> {
        self.iter()
            .filter(|group| {
                group.policy.snapshot().enabled
                    && group.members.iter().all(|path_index| {
                        context
                            .tcp_sessions
                            .get(*path_index)
                            .is_some_and(|session| session.connection_instance_id().is_some())
                    })
            })
            .flat_map(|group| group.members.iter().copied())
            .filter(|path_index| {
                context
                    .tcp_sessions
                    .get(*path_index)
                    .is_some_and(|session| {
                        session.can_plan_replacement() && session.is_product_quiescent()
                    })
            })
            .filter_map(|path_index| retry.get(path_index)?.next_maintenance_at())
            .min()
    }

    /// Reconciles durable minimum capacity and planned ranged-port
    /// replacement. Distinct missing members may establish concurrently; a
    /// minimum member has at most one current establishment or successor.
    pub(in crate::runtime) async fn reconcile(
        &self,
        context: &ClientPathContext,
        connect_timeout: Duration,
        retry_interval: Duration,
        retry: &mut [ClientTcpMinimumRetry],
    ) {
        assert_eq!(
            retry.len(),
            context.tcp_sessions.len(),
            "TCP minimum retry state must match configured members"
        );

        let now = tokio::time::Instant::now();
        let mut minimum_attempts = tokio::task::JoinSet::new();
        for group in self.iter() {
            let policy_snapshot = group.policy.snapshot();
            if !policy_snapshot.enabled {
                continue;
            }
            for &path_index in &group.members {
                let session = context
                    .tcp_sessions
                    .get(path_index)
                    .expect("TCP group member must have one carrier actor");
                let retry = retry
                    .get_mut(path_index)
                    .expect("TCP group member must have retry state");
                if retry.endpoint_generation != policy_snapshot.generation {
                    retry.endpoint_generation = policy_snapshot.generation;
                    retry.not_before = now;
                    retry.hop_instance = None;
                    retry.hop_not_before = None;
                    retry.replacement_port = None;
                }
                let ready_instance = session.connection_instance_id();
                if retry.hop_instance != ready_instance {
                    retry.hop_instance = ready_instance;
                    retry.hop_not_before = ready_instance.and_then(|_| {
                        context
                            .tcp_paths
                            .get(path_index)
                            .and_then(|path| path.port_hop_interval())
                            .map(|interval| now + interval)
                    });
                }
                if ready_instance.is_some() {
                    continue;
                }
                retry.hop_instance = None;
                retry.hop_not_before = None;
                if !session.can_establish_minimum() {
                    continue;
                }
                if self.occupied(group.config_index).unwrap_or_default() >= group.range.max() {
                    continue;
                }
                if retry.not_before > now {
                    continue;
                }
                // The existing probe interval bounds attempt start rate.
                // After that boundary, exact readiness loss wakes immediate
                // replacement; repeated authenticate-then-drop cycles cannot
                // spin.
                retry.not_before = now + retry_interval;
                let session = session.clone();
                let replacement_port = retry.replacement_port.take();
                minimum_attempts.spawn(async move {
                    let deadline = tokio::time::Instant::now() + connect_timeout;
                    session
                        .prepare_connection_for_endpoint_generation_on_port(
                            deadline,
                            policy_snapshot.generation,
                            replacement_port,
                        )
                        .await
                });
            }
        }

        while let Some(attempt) = minimum_attempts.join_next().await {
            match attempt {
                Ok(Ok(_)) | Ok(Err(_)) => {}
                Err(error) => {
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "minimum_reconciliation_task_failed",
                        "configured-minimum TCP carrier task failed: {error}"
                    );
                }
            }
        }

        // Optional port replacement is considered only after every durable
        // minimum member is ready. It can therefore never consume a
        // reservation needed to restore the configured minimum.
        let now = tokio::time::Instant::now();
        let mut replacement_attempts = tokio::task::JoinSet::new();
        for group in self.iter() {
            let policy_snapshot = group.policy.snapshot();
            if !policy_snapshot.enabled
                || !group.members.iter().all(|path_index| {
                    context
                        .tcp_sessions
                        .get(*path_index)
                        .is_some_and(|session| session.connection_instance_id().is_some())
                })
            {
                continue;
            }

            // One planned replacement is initiated per group reconciliation.
            // The successor publication or terminal predecessor release wakes
            // the owner before another member can be considered.
            for &path_index in &group.members {
                let session = context
                    .tcp_sessions
                    .get(path_index)
                    .expect("TCP group member must have one carrier actor");
                let retry = retry
                    .get_mut(path_index)
                    .expect("TCP group member must have retry state");
                let ready_instance = session.connection_instance_id();
                if retry.hop_instance != ready_instance {
                    retry.hop_instance = ready_instance;
                    retry.hop_not_before = ready_instance.and_then(|_| {
                        context
                            .tcp_paths
                            .get(path_index)
                            .and_then(|path| path.port_hop_interval())
                            .map(|interval| now + interval)
                    });
                }
                let Some(hop_not_before) = retry.hop_not_before else {
                    continue;
                };
                if hop_not_before > now
                    || !session.can_plan_replacement()
                    || !session.is_product_quiescent()
                {
                    continue;
                }
                let Some(current_port) = session.connection_remote_port() else {
                    continue;
                };
                let Some(path) = context.tcp_paths.get(path_index) else {
                    continue;
                };
                let Some(interval) = path.port_hop_interval() else {
                    continue;
                };
                let remote_port = match path.endpoint.ports().select_other(current_port) {
                    Ok(remote_port) => remote_port,
                    Err(error) => {
                        retry.hop_not_before = Some(now + interval);
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "port_replacement_selection_failed",
                            "TCP carrier replacement port selection failed: {error}"
                        );
                        continue;
                    }
                };
                if self.occupied(group.config_index).unwrap_or_default() < group.range.max() {
                    let session = session.clone();
                    replacement_attempts.spawn(async move {
                        let deadline = tokio::time::Instant::now() + connect_timeout;
                        (
                            path_index,
                            interval,
                            session
                                .replace_connection_for_endpoint_generation(
                                    deadline,
                                    policy_snapshot.generation,
                                    remote_port,
                                )
                                .await
                                .map(|_| ()),
                        )
                    });
                } else {
                    match session.begin_connection_replacement_if_product_quiescent(
                        policy_snapshot.generation,
                    ) {
                        Ok(true) => {
                            retry.replacement_port = Some(remote_port);
                            retry.hop_not_before = None;
                            retry.not_before = now;
                        }
                        Ok(false) | Err(RuntimeError::NoSchedulableTcpPath) => {}
                        Err(error) => {
                            retry.hop_not_before = Some(now + interval);
                            crate::observability::process_event!(
                                Warn,
                                "tcp",
                                "port_replacement_failed",
                                "planned TCP carrier replacement failed: {error}"
                            );
                        }
                    }
                }
                break;
            }
        }

        while let Some(attempt) = replacement_attempts.join_next().await {
            match attempt {
                Ok((path_index, _, Ok(())))
                | Ok((path_index, _, Err(RuntimeError::NoSchedulableTcpPath))) => {
                    let _ = path_index;
                }
                Ok((path_index, interval, Err(error))) => {
                    if let Some(retry) = retry.get_mut(path_index) {
                        retry.hop_not_before = Some(tokio::time::Instant::now() + interval);
                    }
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "port_replacement_failed",
                        "planned TCP carrier replacement failed: {error}"
                    );
                }
                Err(error) => {
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "port_replacement_task_failed",
                        "planned TCP carrier replacement task failed: {error}"
                    );
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ClientTcpEndpointControlState {
    Enabled,
    Suspect,
    Failed,
    Disabled,
}

impl ClientPathContext {
    pub(in crate::runtime) fn set_tcp_endpoint_control(
        &self,
        config_index: usize,
        state: ClientTcpEndpointControlState,
    ) {
        let group = self
            .tcp_carrier_groups
            .get(config_index)
            .expect("configured TCP endpoint must retain its carrier group");

        let _policy_commitment = group
            .policy
            .commitment
            .lock()
            .expect("TCP endpoint policy commitment lock");
        let enabled = !matches!(state, ClientTcpEndpointControlState::Disabled);
        if !enabled {
            for &index in &group.members {
                self.tcp_sessions
                    .get(index)
                    .expect("TCP group member must have one carrier actor")
                    .begin_path_drain();
            }
            self.tcp_retained_carriers
                .begin_endpoint_drain(config_index);
        }
        let admission_policy_changed = group.policy.replace_enabled(enabled);
        let mut health = self
            .health()
            .lock()
            .expect("client path health management lock");
        let now = std::time::Instant::now();
        let mut ordinary_eligibility_changed = false;
        for &index in group.members.iter().chain(&group.elastic_slots) {
            let Some(record) = health.tcp_record_mut(index) else {
                assert!(
                    group.elastic_slots.contains(&index),
                    "TCP configured-minimum member must have one health record"
                );
                continue;
            };
            let before = record.eligibility_fingerprint();
            match state {
                ClientTcpEndpointControlState::Enabled | ClientTcpEndpointControlState::Suspect => {
                    record.invalidate_path_proofs();
                    let retiring = record.has_physical_carrier()
                        && record.state == SchedulerPathState::Draining;
                    record.manual_disabled = false;
                    if !retiring {
                        record.state = SchedulerPathState::Suspect;
                        record.failed_until = None;
                    }
                }
                ClientTcpEndpointControlState::Failed => {
                    record.invalidate_path_proofs();
                    record.manual_disabled = false;
                    record.state = SchedulerPathState::Failed;
                    record.failed_until = Some(now + path_record_failure_cooldown(record));
                }
                ClientTcpEndpointControlState::Disabled => {
                    record.manual_disabled = true;
                    if record.has_physical_carrier() {
                        record.begin_planned_retirement();
                    } else {
                        record.invalidate_path_proofs();
                        record.state = SchedulerPathState::Failed;
                    }
                    record.failed_until = None;
                    record.relay_bytes_in_flight = 0;
                    record.relay_queue_bytes = 0;
                }
            }
            ordinary_eligibility_changed |= before != record.eligibility_fingerprint();
        }
        self.state.publish_tcp_carrier_policy_changes(
            &mut health,
            ordinary_eligibility_changed,
            admission_policy_changed,
        );
        drop(health);
        drop(_policy_commitment);

        // Control transitions wake the one configured-minimum reconciler.
        // Only Disabled forbids establishment; Failed remains health evidence.
        self.tcp_carrier_groups.publish_change();
    }
}

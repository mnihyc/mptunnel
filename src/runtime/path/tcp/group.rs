//! Session-owned TCP carrier groups.
//!
//! A group owns its configured maximum, durable member identities, and
//! endpoint establishment policy. Exact carrier actors retain ownership of
//! sockets, wire ordering, Product attachments, and terminal failure.

use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::model::timing::path_open_timeout;
use crate::protocol::{PathId, UnderlayProtocol};
use crate::runtime::error::RuntimeError;
use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::path_record_failure_cooldown;
use crate::scheduler::PathState as SchedulerPathState;
use crate::transport::TcpCarrierRange;
use std::collections::BTreeSet;
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
            policy: ClientTcpEndpointPolicy::enabled(),
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierGroups {
    groups: Box<[ClientTcpCarrierGroup]>,
    resources: Mutex<ClientTcpCarrierResourceState>,
    changes: watch::Sender<()>,
}

#[derive(Debug)]
struct ClientTcpCarrierResourceState {
    occupied_by_group: Box<[usize]>,
    replacement_overlap_by_group: Box<[Option<ClientTcpReplacementOverlap>]>,
    occupied_path_ids: BTreeSet<u16>,
    next_path_id: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ClientTcpReplacementOverlap {
    predecessor_path_id: PathId,
    successor_path_id: PathId,
}

/// Exact physical-carrier reservation.
///
/// The reservation starts immediately before connection initiation and stays
/// owned by that one actor through readiness, ordinary use, and ordered drain.
/// Dropping it releases both the group envelope and wire ID.
#[derive(Debug)]
pub(in crate::runtime) struct ClientTcpCarrierReservation {
    owner: Weak<ClientTcpCarrierGroups>,
    config_index: usize,
    path_id: PathId,
}

impl ClientTcpCarrierReservation {
    pub(in crate::runtime) fn path_id(&self) -> PathId {
        self.path_id
    }
}

impl Drop for ClientTcpCarrierReservation {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        {
            let mut resources = owner.resources.lock().expect("TCP carrier resource lock");
            {
                let occupied = resources
                    .occupied_by_group
                    .get_mut(self.config_index)
                    .expect("TCP carrier reservation group");
                *occupied = occupied
                    .checked_sub(1)
                    .expect("TCP carrier group reservation released once");
            }
            let overlap = resources.replacement_overlap_by_group[self.config_index];
            if overlap.is_some_and(|overlap| {
                overlap.predecessor_path_id == self.path_id
                    || overlap.successor_path_id == self.path_id
            }) {
                // Only an exact endpoint of the overlap can end it. An
                // unrelated member failure cannot admit a second successor.
                resources.replacement_overlap_by_group[self.config_index] = None;
            }
            assert!(
                resources.occupied_path_ids.remove(&self.path_id.0),
                "TCP wire PathId reservation released once"
            );
        }
        owner.publish_change();
    }
}

#[derive(Clone, Copy)]
pub(in crate::runtime) struct ClientTcpMemberRetry {
    endpoint_generation: u64,
    not_before: tokio::time::Instant,
    hop_instance: Option<CarrierPathInstanceId>,
    hop_not_before: Option<tokio::time::Instant>,
}

impl ClientTcpMemberRetry {
    pub(in crate::runtime) fn new(now: tokio::time::Instant) -> Self {
        Self {
            endpoint_generation: 0,
            not_before: now,
            hop_instance: None,
            hop_not_before: None,
        }
    }

    pub(in crate::runtime) fn next_maintenance_at(&self) -> Option<tokio::time::Instant> {
        self.hop_not_before
    }

    fn defer_maintenance(&mut self, now: tokio::time::Instant, interval: Duration) {
        self.hop_not_before = Some(now + interval);
    }
}

impl ClientTcpCarrierGroups {
    pub(in crate::runtime) fn new(groups: Vec<ClientTcpCarrierGroup>) -> Arc<Self> {
        let (changes, _) = watch::channel(());
        let occupied_by_group = vec![0; groups.len()].into_boxed_slice();
        let replacement_overlap_by_group = vec![None; groups.len()].into_boxed_slice();
        Arc::new(Self {
            groups: groups.into_boxed_slice(),
            resources: Mutex::new(ClientTcpCarrierResourceState {
                occupied_by_group,
                replacement_overlap_by_group,
                occupied_path_ids: BTreeSet::new(),
                next_path_id: 0,
            }),
            changes,
        })
    }

    /// Reserves one physical carrier and a concurrently unique TCP `PathId`.
    ///
    /// Every configured member has an actor and health identity; only a live
    /// physical connection consumes this reservation.
    pub(in crate::runtime) fn reserve(
        self: &Arc<Self>,
        config_index: usize,
    ) -> Option<ClientTcpCarrierReservation> {
        self.reserve_with_role(config_index, None)
    }

    /// Reserves the sole transient successor permitted for one TCP group.
    ///
    /// The configured maximum remains the number of current carrier members.
    /// Planned maintenance may overlap one authenticated successor with one
    /// retiring predecessor, but a second member in the same group cannot
    /// independently expand that overlap.
    pub(in crate::runtime) fn reserve_planned_replacement(
        self: &Arc<Self>,
        config_index: usize,
        predecessor_path_id: PathId,
    ) -> Option<ClientTcpCarrierReservation> {
        self.reserve_with_role(config_index, Some(predecessor_path_id))
    }

    fn reserve_with_role(
        self: &Arc<Self>,
        config_index: usize,
        planned_replacement: Option<PathId>,
    ) -> Option<ClientTcpCarrierReservation> {
        let group = self.get(config_index)?;
        let mut resources = self.resources.lock().expect("TCP carrier resource lock");
        let occupied = *resources.occupied_by_group.get(config_index)?;
        let configured_max = usize::from(group.range.max());
        if planned_replacement.is_some() {
            let replacement_overlap = resources.replacement_overlap_by_group.get(config_index)?;
            if replacement_overlap.is_some()
                || occupied != configured_max
                || !resources
                    .occupied_path_ids
                    .contains(&planned_replacement?.0)
            {
                return None;
            }
        } else if occupied >= configured_max {
            return None;
        }

        let mut candidate = resources.next_path_id;
        let path_id = (0..=u16::MAX).find_map(|_| {
            let available = !resources.occupied_path_ids.contains(&candidate);
            let selected = available.then_some(candidate);
            candidate = candidate.wrapping_add(1);
            selected
        })?;
        let next_occupied = occupied.checked_add(1)?;
        resources.next_path_id = candidate;
        resources.occupied_path_ids.insert(path_id);
        if let Some(predecessor_path_id) = planned_replacement {
            resources.replacement_overlap_by_group[config_index] =
                Some(ClientTcpReplacementOverlap {
                    predecessor_path_id,
                    successor_path_id: PathId(path_id),
                });
        }
        resources.occupied_by_group[config_index] = next_occupied;
        drop(resources);

        Some(ClientTcpCarrierReservation {
            owner: Arc::downgrade(self),
            config_index,
            path_id: PathId(path_id),
        })
    }

    pub(in crate::runtime) fn occupied(&self, config_index: usize) -> Option<usize> {
        self.resources
            .lock()
            .expect("TCP carrier resource lock")
            .occupied_by_group
            .get(config_index)
            .copied()
    }

    fn has_planned_replacement_overlap(&self, config_index: usize) -> bool {
        self.resources
            .lock()
            .expect("TCP carrier resource lock")
            .replacement_overlap_by_group
            .get(config_index)
            .is_some_and(Option::is_some)
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
        retry: &[ClientTcpMemberRetry],
    ) -> Option<tokio::time::Instant> {
        self.iter()
            .filter(|group| {
                !self.has_planned_replacement_overlap(group.config_index)
                    && group.policy.snapshot().enabled
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
                    .is_some_and(|session| session.can_plan_replacement())
            })
            .filter_map(|path_index| retry.get(path_index)?.next_maintenance_at())
            .min()
    }

    /// Reconciles the bounded healthy target and planned ranged-port
    /// replacement. Every missing member is an equivalent establishment
    /// candidate and independent attempts may proceed concurrently.
    pub(in crate::runtime) async fn reconcile(
        &self,
        context: &ClientPathContext,
        retry_interval: Duration,
        retry: &mut [ClientTcpMemberRetry],
    ) {
        if context.ensure_session_active().is_err() {
            return;
        }
        assert_eq!(
            retry.len(),
            context.tcp_sessions.len(),
            "TCP member retry state must match configured members"
        );

        let now = tokio::time::Instant::now();
        let mut establishment_attempts = tokio::task::JoinSet::new();
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
                if !session.can_establish() {
                    continue;
                }
                if self.occupied(group.config_index).unwrap_or_default()
                    >= usize::from(group.range.max())
                {
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
                let connect_timeout = tcp_carrier_establishment_timeout(context, path_index);
                establishment_attempts.spawn(async move {
                    let deadline = tokio::time::Instant::now() + connect_timeout;
                    session
                        .prepare_connection_for_endpoint_generation_on_port(
                            deadline,
                            policy_snapshot.generation,
                            None,
                        )
                        .await
                });
            }
        }

        while let Some(attempt) = establishment_attempts.join_next().await {
            match attempt {
                Ok(Ok(_)) | Ok(Err(_)) => {}
                Err(error) => {
                    crate::observability::process_event!(
                        Warn,
                        "tcp",
                        "pool_reconciliation_task_failed",
                        "TCP carrier-pool establishment task failed: {error}"
                    );
                }
            }
        }

        // Planned replacement is considered only after the complete healthy
        // target is ready. The earliest-due member rotates first; a successful
        // replacement receives a fresh deadline, so no ordinal remains old.
        let now = tokio::time::Instant::now();
        let mut replacement_attempts = tokio::task::JoinSet::new();
        for group in self.iter() {
            let policy_snapshot = group.policy.snapshot();
            if !policy_snapshot.enabled
                || self.has_planned_replacement_overlap(group.config_index)
                || !group.members.iter().all(|path_index| {
                    context
                        .tcp_sessions
                        .get(*path_index)
                        .is_some_and(|session| session.connection_instance_id().is_some())
                })
            {
                continue;
            }

            let mut rotation_order = group.members.clone();
            for &path_index in &rotation_order {
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
            }
            rotation_order.retain(|path_index| {
                retry
                    .get(*path_index)
                    .is_some_and(|member| member.hop_not_before.is_some())
            });
            rotation_order.sort_by_key(|path_index| {
                retry
                    .get(*path_index)
                    .and_then(|member| member.hop_not_before)
            });

            // One planned replacement is initiated per group reconciliation.
            // The successor publication or terminal predecessor release wakes
            // the owner before another due member can be considered.
            for path_index in rotation_order {
                let session = context
                    .tcp_sessions
                    .get(path_index)
                    .expect("TCP group member must have one carrier actor");
                let retry = retry
                    .get_mut(path_index)
                    .expect("TCP group member must have retry state");
                let Some(hop_not_before) = retry.hop_not_before else {
                    continue;
                };
                if hop_not_before > now || !session.can_plan_replacement() {
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
                        retry.defer_maintenance(now, interval);
                        crate::observability::process_event!(
                            Warn,
                            "tcp",
                            "port_replacement_selection_failed",
                            "TCP carrier replacement port selection failed: {error}"
                        );
                        continue;
                    }
                };
                let session = session.clone();
                let member_ordinal = group
                    .members
                    .iter()
                    .position(|member| *member == path_index)
                    .expect("TCP group contains selected member");
                let connect_timeout = tcp_carrier_establishment_timeout(context, path_index);
                replacement_attempts.spawn(async move {
                    let deadline = tokio::time::Instant::now() + connect_timeout;
                    (
                        path_index,
                        member_ordinal,
                        interval,
                        session
                            .replace_connection_for_endpoint_generation(
                                deadline,
                                policy_snapshot.generation,
                                remote_port,
                            )
                            .await,
                    )
                });
                break;
            }
        }

        while let Some(attempt) = replacement_attempts.join_next().await {
            match attempt {
                Ok((_, member_ordinal, _, Ok(replacement))) => {
                    crate::observability::process_event!(
                        Debug,
                        "tcp",
                        "carrier_port_replaced",
                        "TCP carrier changed destination port by publishing a fresh carrier before draining its predecessor; \
                         group={} member={} old_path_id={} new_path_id={} old_instance_id={} new_instance_id={} old_port={} new_port={}",
                        replacement.group_index,
                        member_ordinal,
                        replacement.predecessor_path_id.0,
                        replacement.successor_path_id.0,
                        replacement.predecessor_instance_id.as_u64(),
                        replacement.successor_instance_id.as_u64(),
                        replacement.predecessor_port,
                        replacement.successor_port,
                    );
                }
                Ok((path_index, _, _, Err(RuntimeError::NoSchedulableTcpPath))) => {
                    let _ = path_index;
                }
                Ok((path_index, _, interval, Err(error))) => {
                    if let Some(retry) = retry.get_mut(path_index) {
                        retry.defer_maintenance(tokio::time::Instant::now(), interval);
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

#[cfg(test)]
#[path = "tests_group.rs"]
mod tests;

/// Prices a complete cold TCP carrier transaction from the same RFC timing
/// model used by demand-driven stream attachment. A health-probe deadline owns
/// only that probe and must never shorten TCP, transport protection, path join,
/// or readiness establishment.
fn tcp_carrier_establishment_timeout(context: &ClientPathContext, path_index: usize) -> Duration {
    let key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: path_index,
    };
    path_open_timeout(context.reliable_path_snapshot(key), false)
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
        }
        group.policy.replace_enabled(enabled);
        let mut health = self
            .health()
            .lock()
            .expect("client path health management lock");
        let now = std::time::Instant::now();
        for &index in &group.members {
            let record = health
                .tcp_record_mut(index)
                .expect("TCP pool member must have one health record");
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
        }
        drop(health);
        drop(_policy_commitment);

        // Control transitions wake the endpoint's bounded-pool reconciler.
        // Only Disabled forbids establishment; Failed remains health evidence.
        self.tcp_carrier_groups.publish_change();
    }
}

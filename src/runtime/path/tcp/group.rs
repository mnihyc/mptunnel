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
    occupied_by_group: Box<[u16]>,
    occupied_path_ids: BTreeSet<u16>,
    next_path_id: u16,
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
    replacement_port: Option<u16>,
}

impl ClientTcpMemberRetry {
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
        Arc::new(Self {
            groups: groups.into_boxed_slice(),
            resources: Mutex::new(ClientTcpCarrierResourceState {
                occupied_by_group,
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
        let group = self.get(config_index)?;
        let mut resources = self.resources.lock().expect("TCP carrier resource lock");
        let occupied = *resources.occupied_by_group.get(config_index)?;
        if occupied >= group.range.max() {
            return None;
        }

        let mut candidate = resources.next_path_id;
        let path_id = (0..=u16::MAX).find_map(|_| {
            let available = !resources.occupied_path_ids.contains(&candidate);
            let selected = available.then_some(candidate);
            candidate = candidate.wrapping_add(1);
            selected
        })?;
        resources.next_path_id = candidate;
        resources.occupied_path_ids.insert(path_id);
        resources.occupied_by_group[config_index] = occupied + 1;
        drop(resources);

        Some(ClientTcpCarrierReservation {
            owner: Arc::downgrade(self),
            config_index,
            path_id: PathId(path_id),
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

    /// Reconciles the bounded healthy target and planned ranged-port
    /// replacement. Every missing member is an equivalent establishment
    /// candidate and independent attempts may proceed concurrently.
    pub(in crate::runtime) async fn reconcile(
        &self,
        context: &ClientPathContext,
        retry_interval: Duration,
        retry: &mut [ClientTcpMemberRetry],
    ) {
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
                if !session.can_establish() {
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
                let connect_timeout = tcp_carrier_establishment_timeout(context, path_index);
                establishment_attempts.spawn(async move {
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
                    let connect_timeout = tcp_carrier_establishment_timeout(context, path_index);
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

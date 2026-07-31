//! Session-owned TCP carrier groups.
//!
//! A group owns configured bounds, configured-minimum member identities, and
//! endpoint establishment policy. Exact carrier actors retain ownership of
//! sockets, wire ordering, Product attachments, and terminal failure.

use crate::runtime::path::ClientPathContext;
use crate::runtime::path::model::path_record_failure_cooldown;
use crate::scheduler::PathState as SchedulerPathState;
use crate::transport::TcpCarrierRange;
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
    fn replace_enabled(&self, enabled: bool) {
        let current = self.snapshot();
        if current.enabled == enabled {
            return;
        }
        self.changes.send_replace(ClientTcpEndpointPolicySnapshot {
            enabled,
            generation: current.generation.wrapping_add(1),
        });
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
    changes: watch::Sender<()>,
}

#[derive(Clone, Copy)]
pub(in crate::runtime) struct ClientTcpMinimumRetry {
    endpoint_generation: u64,
    not_before: tokio::time::Instant,
}

impl ClientTcpMinimumRetry {
    pub(in crate::runtime) fn new(now: tokio::time::Instant) -> Self {
        Self {
            endpoint_generation: 0,
            not_before: now,
        }
    }
}

impl ClientTcpCarrierGroups {
    pub(in crate::runtime) fn new(groups: Vec<ClientTcpCarrierGroup>) -> Arc<Self> {
        let (changes, _) = watch::channel(());
        Arc::new(Self {
            groups: groups.into_boxed_slice(),
            changes,
        })
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

    /// Restores only configured-minimum members. Distinct missing members may
    /// establish concurrently, while each exact actor serializes its commands.
    pub(in crate::runtime) async fn reconcile_configured_minimum(
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
        let mut attempts = tokio::task::JoinSet::new();
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
                }
                if session.is_connection_ready() || retry.not_before > now {
                    continue;
                }
                // The existing probe interval bounds attempt start rate.
                // After that boundary, exact readiness loss wakes immediate
                // replacement; repeated authenticate-then-drop cycles cannot
                // spin.
                retry.not_before = now + retry_interval;
                let session = session.clone();
                attempts.spawn(async move {
                    let deadline = tokio::time::Instant::now() + connect_timeout;
                    let _ = session
                        .prepare_connection_for_endpoint_generation(
                            deadline,
                            policy_snapshot.generation,
                        )
                        .await;
                });
            }
        }

        while let Some(attempt) = attempts.join_next().await {
            if let Err(error) = attempt {
                crate::observability::process_event!(
                    Warn,
                    "tcp",
                    "minimum_reconciliation_task_failed",
                    "configured-minimum TCP carrier task failed: {error}"
                );
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
        }
        group.policy.replace_enabled(enabled);
        let mut health = self
            .health()
            .lock()
            .expect("client path health management lock");
        let now = std::time::Instant::now();
        for &index in &group.members {
            let record = health
                .tcp
                .get_mut(index)
                .expect("TCP group member must have one health record");
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

        // Control transitions wake the one configured-minimum reconciler.
        // Only Disabled forbids establishment; Failed remains health evidence.
        self.tcp_carrier_groups.publish_change();
    }
}

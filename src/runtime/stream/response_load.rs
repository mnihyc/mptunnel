//! Session load accounting and response-flow registration guards.
//! Every counter mutation uses the single tracker-state mutex owned by response_session.

use super::response_session::{ServerPathLaneTracker, ServerPathLaneTrackerState};
use crate::model::path::CarrierPathKey;
use crate::protocol::{SessionId, UnderlayProtocol};
use crate::scheduler::FlowLane;
use std::sync::{Arc, Mutex};

fn response_lane_is_latency_sensitive(lane: FlowLane) -> bool {
    matches!(
        lane,
        FlowLane::Control | FlowLane::Latency | FlowLane::RealtimeDatagram
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct ServerPathLoadKey {
    pub(super) session_id: SessionId,
    pub(super) path: CarrierPathKey,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct ServerPathLaneLoad {
    pub(super) active_flows: u32,
    pub(super) active_latency_sensitive_flows: u32,
}

impl ServerPathLaneLoad {
    fn add(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_add(1);
        if response_lane_is_latency_sensitive(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_add(1);
        }
    }

    fn remove(&mut self, lane: FlowLane) {
        self.active_flows = self.active_flows.saturating_sub(1);
        if response_lane_is_latency_sensitive(lane) {
            self.active_latency_sensitive_flows =
                self.active_latency_sensitive_flows.saturating_sub(1);
        }
    }
}

impl ServerPathLaneTrackerState {
    fn add_response_service(
        &mut self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        self.response_service_loads
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
        self.response_service_session_loads
            .entry(session_id)
            .or_default()
            .add(lane);
        let family = self
            .response_service_family_loads
            .entry(session_id)
            .or_default();
        match path.underlay {
            UnderlayProtocol::Tcp => family.tcp = family.tcp.saturating_add(1),
            UnderlayProtocol::Udp => family.udp = family.udp.saturating_add(1),
        }
    }

    fn remove_response_service(
        &mut self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) -> bool {
        let key = ServerPathLoadKey { session_id, path };
        let Some(path_load) = self.response_service_loads.get_mut(&key) else {
            return false;
        };
        if path_load.active_flows == 0 {
            return false;
        }
        path_load.remove(lane);
        if path_load.active_flows == 0 {
            self.response_service_loads.remove(&key);
        }

        if let Some(session_load) = self.response_service_session_loads.get_mut(&session_id) {
            session_load.remove(lane);
            if session_load.active_flows == 0 {
                self.response_service_session_loads.remove(&session_id);
            }
        }
        if let Some(family) = self.response_service_family_loads.get_mut(&session_id) {
            match path.underlay {
                UnderlayProtocol::Tcp => family.tcp = family.tcp.saturating_sub(1),
                UnderlayProtocol::Udp => family.udp = family.udp.saturating_sub(1),
            }
            if family.tcp == 0 && family.udp == 0 {
                self.response_service_family_loads.remove(&session_id);
            }
        }
        true
    }

    pub(super) fn move_response_service(
        &mut self,
        session_id: SessionId,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) -> bool {
        if from == to {
            return self
                .response_service_loads
                .contains_key(&ServerPathLoadKey {
                    session_id,
                    path: from,
                });
        }
        if !self.remove_response_service(session_id, from, lane) {
            return false;
        }
        self.add_response_service(session_id, to, lane);
        true
    }

    pub(super) fn response_service_session_load(
        &self,
        session_id: SessionId,
    ) -> ServerPathLaneLoad {
        let mut session_load = self
            .response_service_session_loads
            .get(&session_id)
            .copied()
            .unwrap_or_default();
        let realtime_flows = self.realtime_flows.get(&session_id).copied().unwrap_or(0);
        session_load.active_flows = session_load.active_flows.saturating_add(realtime_flows);
        session_load.active_latency_sensitive_flows = session_load
            .active_latency_sensitive_flows
            .saturating_add(realtime_flows);
        session_load
    }
}

impl ServerPathLaneTracker {
    #[cfg(test)]
    pub(super) fn generation_and_active_response_flows(&self, session_id: SessionId) -> (u64, u32) {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state
            .session_generations
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        let active_response_flows = state
            .active_response_flows
            .get(&session_id)
            .copied()
            .unwrap_or(0);
        (generation, active_response_flows)
    }

    pub(super) fn attach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state
            .loads
            .entry(ServerPathLoadKey { session_id, path })
            .or_default()
            .add(lane);
        state.bump_generation(session_id);
    }

    pub(super) fn detach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let key = ServerPathLoadKey { session_id, path };
        let changed = if let Some(load) = state.loads.get_mut(&key) {
            load.remove(lane);
            if load.active_flows == 0 {
                state.loads.remove(&key);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn change_lanes(
        &self,
        session_id: SessionId,
        paths: &[CarrierPathKey],
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let mut changed = false;
        for path in paths {
            if let Some(load) = state.loads.get_mut(&ServerPathLoadKey {
                session_id,
                path: *path,
            }) {
                load.remove(from);
                load.add(to);
                changed = true;
            }
        }
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn attach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let count = state.realtime_flows.entry(session_id).or_default();
        *count = count.saturating_add(1);
        state.bump_generation(session_id);
    }

    pub(super) fn set_response_flow_active(&self, session_id: SessionId, active: bool) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if active {
            let count = state.active_response_flows.entry(session_id).or_default();
            *count = count.saturating_add(1);
            state.bump_generation(session_id);
            return;
        }

        let changed = if let Some(count) = state.active_response_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.active_response_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn detach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let changed = if let Some(count) = state.realtime_flows.get_mut(&session_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.realtime_flows.remove(&session_id);
            }
            true
        } else {
            false
        };
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    #[cfg(test)]
    pub(super) fn snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
    ) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(super) fn session_snapshot(&self, session_id: SessionId) -> ServerPathLaneLoad {
        let state = self.state.lock().expect("server path lane tracker lock");
        let mut total = state
            .loads
            .iter()
            .filter(|(key, _)| key.session_id == session_id)
            .fold(ServerPathLaneLoad::default(), |mut total, (_, load)| {
                total.active_flows = total.active_flows.saturating_add(load.active_flows);
                total.active_latency_sensitive_flows = total
                    .active_latency_sensitive_flows
                    .saturating_add(load.active_latency_sensitive_flows);
                total
            });
        let realtime_flows = state.realtime_flows.get(&session_id).copied().unwrap_or(0);
        total.active_flows = total.active_flows.saturating_add(realtime_flows);
        total.active_latency_sensitive_flows = total
            .active_latency_sensitive_flows
            .saturating_add(realtime_flows);
        total
    }

    pub(super) fn response_service_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
    ) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .response_service_loads
            .get(&ServerPathLoadKey { session_id, path })
            .copied()
            .unwrap_or_default()
    }

    pub(super) fn attach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        state.add_response_service(session_id, path, lane);
        state.bump_generation(session_id);
    }

    pub(super) fn detach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let changed = state.remove_response_service(session_id, path, lane);
        if changed {
            state.bump_generation(session_id);
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn move_response_service(
        &self,
        session_id: SessionId,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if state.move_response_service(session_id, from, to, lane) {
            state.bump_generation(session_id);
        }
    }

    pub(super) fn change_response_service_lane(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        from: FlowLane,
        to: FlowLane,
    ) {
        if from == to {
            return;
        }
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(load) = state
            .response_service_loads
            .get_mut(&ServerPathLoadKey { session_id, path })
        {
            load.remove(from);
            load.add(to);
            if let Some(session_load) = state.response_service_session_loads.get_mut(&session_id) {
                session_load.remove(from);
                session_load.add(to);
            }
            state.bump_generation(session_id);
        }
    }
}

pub(super) struct ServerResponseFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
    active: Mutex<bool>,
    service: Mutex<Option<(CarrierPathKey, FlowLane)>>,
}

impl ServerResponseFlowRegistration {
    pub(super) fn new(
        lane_tracker: Arc<ServerPathLaneTracker>,
        session_id: SessionId,
        service: CarrierPathKey,
        lane: FlowLane,
    ) -> Self {
        lane_tracker.attach_session(session_id);
        lane_tracker.attach_response_service(session_id, service, lane);
        Self {
            lane_tracker,
            session_id,
            active: Mutex::new(false),
            service: Mutex::new(Some((service, lane))),
        }
    }

    pub(super) fn set_active(&self, active: bool) {
        let mut current = self
            .active
            .lock()
            .expect("server response flow registration lock");
        if *current == active {
            return;
        }
        self.lane_tracker
            .set_response_flow_active(self.session_id, active);
        *current = active;
    }

    pub(super) fn set_service(&self, next: Option<(CarrierPathKey, FlowLane)>) {
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        if *current == next {
            return;
        }
        match (*current, next) {
            (Some((from, from_lane)), Some((to, to_lane))) if from == to => {
                self.lane_tracker.change_response_service_lane(
                    self.session_id,
                    from,
                    from_lane,
                    to_lane,
                );
            }
            (Some((from, from_lane)), Some((to, to_lane))) => {
                debug_assert_eq!(from_lane, to_lane);
                self.lane_tracker
                    .move_response_service(self.session_id, from, to, to_lane);
            }
            (Some((from, lane)), None) => {
                self.lane_tracker
                    .detach_response_service(self.session_id, from, lane);
            }
            (None, Some((to, lane))) => {
                self.lane_tracker
                    .attach_response_service(self.session_id, to, lane);
            }
            (None, None) => {}
        }
        *current = next;
    }

    pub(super) fn change_lane_if_present(&self, from: FlowLane, to: FlowLane) {
        if from == to {
            return;
        }
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        let Some((path, registered_lane)) = *current else {
            return;
        };
        debug_assert_eq!(registered_lane, from);
        self.lane_tracker
            .change_response_service_lane(self.session_id, path, registered_lane, to);
        *current = Some((path, to));
    }

    pub(super) fn commit_reserved_service_move(
        &self,
        from: CarrierPathKey,
        to: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut current = self
            .service
            .lock()
            .expect("server response Service registration lock");
        debug_assert_eq!(*current, Some((from, lane)));
        *current = Some((to, lane));
    }
}

impl Drop for ServerResponseFlowRegistration {
    fn drop(&mut self) {
        self.set_active(false);
        self.set_service(None);
        self.lane_tracker.detach_session(self.session_id);
    }
}

pub(in crate::runtime) struct ServerRealtimeFlowRegistration {
    lane_tracker: Arc<ServerPathLaneTracker>,
    session_id: SessionId,
}

impl ServerRealtimeFlowRegistration {
    pub(in crate::runtime::stream) fn new(
        lane_tracker: Arc<ServerPathLaneTracker>,
        session_id: SessionId,
    ) -> Self {
        lane_tracker.attach_realtime_flow(session_id);
        Self {
            lane_tracker,
            session_id,
        }
    }
}

impl Drop for ServerRealtimeFlowRegistration {
    fn drop(&mut self) {
        self.lane_tracker.detach_realtime_flow(self.session_id);
    }
}

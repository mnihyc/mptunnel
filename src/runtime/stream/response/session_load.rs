//! Session load transactions and response-flow registration guards.
//! Counter schemas and invariants live with the `session` mutex; this service
//! exposes balanced mutations and RAII lifetime ownership.

use super::session::{ServerPathLaneLoad, ServerPathLaneTracker};
use crate::model::path::CarrierPathKey;
use crate::protocol::SessionId;
use crate::scheduler::FlowLane;
use std::sync::{Arc, Mutex};

impl ServerPathLaneTracker {
    #[cfg(test)]
    pub(super) fn generation_and_active_response_flows(&self, session_id: SessionId) -> (u64, u32) {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state.generation(session_id);
        let active_response_flows = state
            .session(session_id)
            .map(|session| session.load().active_response_flows())
            .unwrap_or(0);
        (generation, active_response_flows)
    }

    pub(super) fn with_matching_generation_and_min_active_response_flows<R>(
        &self,
        session_id: SessionId,
        expected_generation: u64,
        minimum_active_response_flows: u32,
        apply: impl FnOnce() -> R,
    ) -> Option<R> {
        let state = self.state.lock().expect("server path lane tracker lock");
        let generation = state.generation(session_id);
        let active_response_flows = state
            .session(session_id)
            .map(|session| session.load().active_response_flows())
            .unwrap_or(0);
        if generation != expected_generation
            || active_response_flows < minimum_active_response_flows
        {
            return None;
        }
        let result = apply();
        drop(state);
        Some(result)
    }

    pub(super) fn attach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let session = state.session_mut_or_default(session_id);
        session.load_mut().attach(path, lane);
        session.bump_generation();
    }

    pub(super) fn detach(&self, session_id: SessionId, path: CarrierPathKey, lane: FlowLane) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().detach(path, lane) {
                session.bump_generation();
            }
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
        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().change_attachment_lanes(paths, from, to) {
                session.bump_generation();
            }
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn attach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let session = state.session_mut_or_default(session_id);
        session.load_mut().attach_realtime_flow();
        session.bump_generation();
    }

    pub(super) fn set_response_flow_active(&self, session_id: SessionId, active: bool) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if active {
            let session = state.session_mut_or_default(session_id);
            session.load_mut().set_response_flow_active(true);
            session.bump_generation();
            return;
        }

        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().set_response_flow_active(false) {
                session.bump_generation();
            }
        }
        state.maybe_reclaim_session(session_id);
    }

    pub(super) fn detach_realtime_flow(&self, session_id: SessionId) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().detach_realtime_flow() {
                session.bump_generation();
            }
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
            .session(session_id)
            .map(|session| session.load().attachment_path_load(path))
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(super) fn session_snapshot(&self, session_id: SessionId) -> ServerPathLaneLoad {
        let state = self.state.lock().expect("server path lane tracker lock");
        state
            .session(session_id)
            .map(|session| session.load().attachment_session_load())
            .unwrap_or_default()
    }

    pub(super) fn response_service_snapshot(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
    ) -> ServerPathLaneLoad {
        self.state
            .lock()
            .expect("server path lane tracker lock")
            .session(session_id)
            .map(|session| session.load().response_service_path_load(path))
            .unwrap_or_default()
    }

    pub(super) fn attach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        let session = state.session_mut_or_default(session_id);
        session.load_mut().add_response_service(path, lane);
        session.bump_generation();
    }

    pub(super) fn detach_response_service(
        &self,
        session_id: SessionId,
        path: CarrierPathKey,
        lane: FlowLane,
    ) {
        let mut state = self.state.lock().expect("server path lane tracker lock");
        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().remove_response_service(path, lane) {
                session.bump_generation();
            }
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
        if let Some(session) = state.session_mut(session_id) {
            if session.load_mut().move_response_service(from, to, lane) {
                session.bump_generation();
            }
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
        if let Some(session) = state.session_mut(session_id) {
            if session
                .load_mut()
                .change_response_service_lane(path, from, to)
            {
                session.bump_generation();
            }
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

#[cfg(test)]
#[path = "session_load_test.rs"]
mod tests;

//! Detached native observations for an imminent response Original claim.

use super::snapshot::{
    server_bulk_output_snapshot_at, server_native_bulk_output_snapshot_at,
    server_sender_path_target_at,
};
use super::{ResponseAcquisitionOutputId, ResponseSenderPathTarget, ResponseStreamBinding};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::response::CarrierPathFlightDebt;
use crate::protocol::{PathMetricDirection, UnderlayProtocol};
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[derive(Clone)]
pub(in crate::runtime) struct ResponsePreparedOutput {
    pub(in crate::runtime) identity: ResponseAcquisitionOutputId,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
}

#[cfg(test)]
mod tests {
    use super::super::test_support::native_response_binding_fixture;
    use super::*;
    use crate::mux::stream::ReliableSendStream;
    use crate::protocol::{Frame, StreamId};
    use crate::runtime::path::commands::{ReliablePathCommand, try_recv_reliable_path_command};
    use crate::runtime::path::prepared::PreparedOriginalClaim;
    use crate::runtime::sender::{
        PreparedResponseSource, ResponseProductState, ServerResponseSenderService,
        SharedResponseProduct, publish_prepared_response_work,
    };
    use bytes::Bytes;
    use futures::FutureExt;
    use std::sync::{Arc, Barrier};

    #[tokio::test]
    async fn prepared_response_native_claim_converts_source_without_queue_publication() {
        let mut fixture = native_response_binding_fixture(8, Some(100_000_000));
        let lane = TrafficClass::Throughput;
        let stream_id = StreamId(713);
        let quantum = 1024;
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(fixture.binding.session_id(), stream_id),
                send_stream: ReliableSendStream::new(stream_id, fixture.binding.mux_limits()),
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(lane, quantum),
            },
            fixture.binding.clone(),
        );
        {
            let mut state = owner.lock();
            state
                .sender
                .enqueue_data_for_lane(Bytes::from(vec![0x35; 2 * quantum]), lane);
            publish_prepared_response_work(&mut state, &owner, lane, quantum, true);
            assert_eq!(state.send_stream.next_offset(), 0);
            assert_eq!(state.send_stream.reinjection_bytes(), 0);
            assert_eq!(state.sender.data_bytes(), 2 * quantum);
        }
        let work = match try_recv_reliable_path_command(&mut fixture.receivers) {
            Some(ReliablePathCommand::PreparedOriginal(work)) => work,
            _ => panic!("publication contains only a weak source notice"),
        };
        let identity = work.response_instance().unwrap();
        let frame = match work.try_claim(
            fixture
                .receivers
                .writer_ready_boundary(identity.path_instance_id)
                .unwrap(),
        ) {
            PreparedOriginalClaim::Claimed(frame) => frame,
            _ => panic!("actual current Native writer claims source"),
        };
        assert!(
            matches!(&frame, Frame::StreamData { offset: 0, payload, .. } if payload.len() == quantum)
        );
        let state = owner.lock();
        assert_eq!(state.send_stream.next_offset(), quantum as u64);
        assert_eq!(state.send_stream.reinjection_bytes(), quantum);
        assert_eq!(state.sender.data_bytes(), quantum);
        assert_eq!(
            state.prepared.first_claimed_at,
            state.prepared.last_claimed_at
        );
        assert_eq!(
            fixture
                .binding
                .original_flight_outputs_overlapping_frame(&frame)
                .len(),
            1
        );
        drop(state);
        assert!(
            try_recv_reliable_path_command(&mut fixture.receivers).is_none(),
            "claim does not enqueue a Data command"
        );
        let charge = fixture.receivers.register_claimed_writer_frame(&frame);
        fixture.receivers.release_pending_command_bytes(charge);
    }

    #[tokio::test]
    async fn prepared_response_native_fenced_busy_releases_native_before_owner_wait() {
        let fixture = native_response_binding_fixture(8, Some(100_000_000));
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(
                    fixture.binding.session_id(),
                    StreamId(714),
                ),
                send_stream: ReliableSendStream::new(StreamId(714), fixture.binding.mux_limits()),
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(TrafficClass::Throughput, 1024),
            },
            fixture.binding.clone(),
        );
        let stamp = fixture
            .authority
            .scheduling_shape_snapshot(fixture.scope)
            .unwrap()
            .stamp();
        let guard = owner.lock();
        let entered = Arc::new(Barrier::new(2));
        let proceed = Arc::new(Barrier::new(2));
        let writer_owner = owner.clone();
        let writer_native = fixture.authority.clone();
        let writer_entered = entered.clone();
        let writer_proceed = proceed.clone();
        let writer = std::thread::spawn(move || {
            let attempt = writer_owner.arm_claim();
            writer_native
                .commit_with_current_scheduling_shape(stamp, |_| {
                    writer_entered.wait();
                    writer_proceed.wait();
                    match attempt.try_lock() {
                        Ok(_) => panic!("actor still owns source"),
                        Err(wait) => wait,
                    }
                })
                .unwrap()
        });
        entered.wait();
        proceed.wait();
        assert_eq!(
            fixture
                .authority
                .scheduling_shape_snapshot(fixture.scope)
                .unwrap()
                .stamp(),
            stamp
        );
        let wait = writer.join().expect("writer releases Native on Busy");
        assert_eq!(guard.send_stream.next_offset(), 0);
        drop(guard);
        assert_eq!(
            wait.now_or_never(),
            Some(()),
            "unlock before first poll remains visible"
        );
    }
}

pub(in crate::runtime) struct ResponsePreparedNativeInputs {
    outputs: Vec<(
        ResponsePreparedOutput,
        Option<NativeCarrierSchedulingShapeSnapshot>,
    )>,
}

impl ResponsePreparedNativeInputs {
    /// This is the only native read in the claim's advisory observation. The
    /// caller has released source ownership before resolving these handles.
    pub(in crate::runtime) fn resolve(outputs: Vec<ResponsePreparedOutput>) -> Self {
        Self {
            outputs: outputs
                .into_iter()
                .map(|output| {
                    let shape = output
                        .commands
                        .native_rate_authority()
                        .and_then(|authority| {
                            authority
                                .scheduling_shape_snapshot(CarrierRateAuthorityScope::new(
                                    output.identity.path_instance_id,
                                    PathMetricDirection::ServerToClient,
                                ))
                                .ok()
                        });
                    (output, shape)
                })
                .collect(),
        }
    }

    pub(in crate::runtime) fn replace_fenced_target(
        &mut self,
        identity: ResponseAcquisitionOutputId,
        shape: NativeCarrierSchedulingShapeSnapshot,
    ) {
        for (output, current) in &mut self.outputs {
            if output.identity == identity {
                *current = Some(shape);
            }
        }
    }
}

pub(in crate::runtime) struct ResponsePreparedObservation {
    pub(in crate::runtime) targets: Vec<ResponseSenderPathTarget>,
    pub(in crate::runtime) lower_flights: Vec<CarrierPathFlightDebt>,
    pub(in crate::runtime) model_generation: u64,
}

impl ResponseStreamBinding {
    /// Exact handles only: no Native acquisition or duplicate mutable model.
    pub(in crate::runtime) fn prepared_outputs(&self) -> Vec<ResponsePreparedOutput> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Vec::new();
        }
        outputs
            .entries
            .iter()
            .map(|entry| ResponsePreparedOutput {
                identity: ResponseAcquisitionOutputId::from(entry),
                commands: entry.commands.clone(),
            })
            .collect()
    }

    /// Re-project current Product fields from supplied Native values. A full
    /// membership receipt prevents pairing an old physical handle with a new
    /// incarnation. Unselected Ready changes are not ownership invalidations.
    pub(in crate::runtime) fn observe_prepared_original(
        &self,
        inputs: &ResponsePreparedNativeInputs,
        lane: TrafficClass,
        next_offset: u64,
    ) -> Option<ResponsePreparedObservation> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire)
            || outputs.entries.len() != inputs.outputs.len()
            || !outputs
                .entries
                .iter()
                .zip(&inputs.outputs)
                .all(|(entry, (captured, _))| {
                    ResponseAcquisitionOutputId::from(entry) == captured.identity
                })
        {
            return None;
        }
        let ingress = *self
            .request_feedback_ingress
            .lock()
            .expect("server response ingress lock");
        let now = Instant::now();
        let targets = outputs
            .entries
            .iter()
            .zip(&inputs.outputs)
            .filter(|(entry, _)| !entry.commands.is_closed())
            .map(|(entry, (_, shape))| {
                let native = entry.key.underlay == UnderlayProtocol::Udp
                    && entry.commands.native_rate_authority().is_some();
                let snapshot = if native {
                    server_native_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        *shape,
                    )
                } else {
                    server_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        now,
                    )
                };
                let mut target = server_sender_path_target_at(
                    entry,
                    snapshot,
                    lane,
                    self.mux_limits,
                    ingress,
                    now,
                );
                if native {
                    target.native_authority_stamp = shape.map(|shape| shape.stamp());
                }
                target
            })
            .collect();
        Some(ResponsePreparedObservation {
            targets,
            lower_flights: self.lower_flights_before_offset(next_offset),
            model_generation: self.response_model_generation.load(Ordering::Acquire),
        })
    }
}

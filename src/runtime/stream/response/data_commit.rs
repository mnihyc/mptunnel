//! Atomic commit of newly assigned response data.
//!
//! Path selection is complete before this boundary. The binding validates the
//! selected attachment and model generation, reserves carrier queue capacity,
//! records exact range ownership, and only then publishes the carrier command.

use super::ResponseStreamBinding;
use super::attachment::{
    ResponseDispatchTarget, ResponseStreamOutputEntry, ResponseValidationOutputIdentity,
};
use super::delivery::CarrierPathFlight;
use crate::model::work::CarrierWorkKind;
use crate::protocol::Frame;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::runtime::RuntimeError;
use crate::scheduler::TrafficClass;
use std::num::NonZeroU64;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl ResponseStreamBinding {
    pub(in crate::runtime) fn try_enqueue_data_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
    ) -> Result<(), RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire)
            || reliable_stream_frame_extent(frame).is_none()
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let target_matches = |entry: &ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
        };
        let Some(target_index) = outputs.entries.iter().position(target_matches) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };

        if self.response_model_generation.load(Ordering::Acquire) != expected_model_generation {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let target_commands = outputs.entries[target_index].commands.clone();
        // Reservation is the only fallible mutation. Exact range ownership is
        // visible before the carrier can dequeue the committed command.
        // STREAM_DATA carries an explicit offset, so independent streams may
        // retain their traffic-class priority without changing byte ordering.
        let command = target_commands.try_reserve_admitted_frame(frame.clone(), lane)?;
        self.record_validated_original_flight_with_outputs(&mut outputs, target_index, frame);
        command.commit();
        Ok(())
    }

    /// Commits finite unique-original work only to the exact unpublished TCP
    /// validation output. Ordinary dispatch has no route to this method.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::runtime::stream) fn try_enqueue_validation_data_frame(
        &self,
        identity: ResponseValidationOutputIdentity,
        validation_id: NonZeroU64,
        frame: &Frame,
    ) -> Result<(), RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let lane = self.lane.lock().expect("server reliable stream lane lock");
        if *lane != TrafficClass::Throughput {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let validation = outputs
            .validation
            .as_ref()
            .filter(|entry| {
                entry.key == identity.key
                    && entry.path_instance_id == identity.path_instance_id
                    && entry.incarnation == identity.incarnation
                    && !entry.commands.is_closed()
            })
            .ok_or(RuntimeError::ReliablePathRetired)?;
        let command = validation
            .commands
            .try_reserve_tcp_carrier_validation_data(
                validation_id,
                frame.clone(),
                TrafficClass::Throughput,
            )?;
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key: identity.key,
                output_incarnation: identity.incarnation,
                end,
                bytes,
                sent_at: Instant::now(),
                kind: CarrierWorkKind::OriginalData,
                evidence_eligible: true,
            });
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        command.commit();
        Ok(())
    }
}

#[cfg(test)]
#[path = "data_commit_test.rs"]
mod tests;

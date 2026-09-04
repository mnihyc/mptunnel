//! Atomic commit of newly assigned response data.
//!
//! Path selection is complete before this boundary. The binding validates the
//! selected attachment and model generation, reserves carrier queue capacity,
//! records exact range ownership, and only then publishes the carrier command.

use super::ResponseStreamBinding;
use super::attachment::{ResponseDispatchTarget, ResponseStreamOutputEntry};
use super::evidence::server_output_product_assignment_qualified;
use super::snapshot::{server_bulk_output_snapshot_at, server_native_bulk_output_snapshot_at};
use crate::model::admission::{BulkCandidatePosition, bulk_original_data_assignment_authority};
use crate::model::capacity::reliable_bulk_product_windows;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl ResponseStreamBinding {
    pub(in crate::runtime) fn try_enqueue_data_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
        position: BulkCandidatePosition,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_data_frame_for_dispatch_target_with_apply_clock(
            target,
            frame,
            lane,
            expected_model_generation,
            position,
            Instant::now,
            || {},
        )
    }

    // Reservation, generation, and apply-clock inputs form one atomic dispatch
    // ownership envelope; a wrapper object would obscure that transaction.
    #[allow(clippy::too_many_arguments)]
    fn try_enqueue_data_frame_for_dispatch_target_with_apply_clock(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
        position: BulkCandidatePosition,
        apply_now: impl FnOnce() -> Instant,
        after_reserve: impl FnOnce(),
    ) -> Result<(), RuntimeError> {
        let Some((_, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }

        let target_matches = |entry: &ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
        };
        let target_commands = {
            let outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            if !self.response_stream_open.load(Ordering::Acquire) {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            outputs
                .entries
                .iter()
                .find(|entry| target_matches(entry))
                .map(|entry| entry.commands.clone())
                .ok_or(RuntimeError::SenderServiceBlocked)?
        };
        // The writer's real bounded queue is the native admission and
        // linearization resource. Dropping this reservation on any exact-model
        // failure below refunds its permit and pending-byte accounting.
        let command = target_commands.try_reserve_admitted_frame(frame.clone(), lane)?;
        after_reserve();
        let native_authority = target_commands.native_rate_authority().cloned();
        let expected_native_stamp = target.native_authority_stamp;
        let commit = |current_native_shape: Option<NativeCarrierSchedulingShapeSnapshot>| {
            let mut outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            if !self.response_stream_open.load(Ordering::Acquire)
                || self.response_model_generation.load(Ordering::Acquire)
                    != expected_model_generation
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let Some(target_index) = outputs.entries.iter().position(target_matches) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let entry = &outputs.entries[target_index];
            let native_shape = match (expected_native_stamp, current_native_shape) {
                (Some(stamp), Some(shape)) if shape.stamp() == stamp => Some(shape),
                (None, None) => None,
                _ => return Err(RuntimeError::SenderServiceBlocked),
            };
            // Planning is advisory. After real writer reservation, recompute
            // the exact output at one instant. Native mode uses only the
            // current full shape while its fence is held.
            let now = apply_now();
            let assignment = if expected_native_stamp.is_some() {
                server_native_bulk_output_snapshot_at(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                    native_shape,
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
            let product_assignment_qualified =
                server_output_product_assignment_qualified(entry, self.mux_limits);
            let assignment_authority = bulk_original_data_assignment_authority(
                assignment,
                payload_bytes,
                self.mux_limits,
                position,
                product_assignment_qualified,
            );
            let product_windows = reliable_bulk_product_windows(self.mux_limits);
            if outputs
                .original_data_in_flight_bytes
                .checked_add(payload_bytes as u64)
                .is_none_or(|committed| committed > product_windows.stream_resource_limit_bytes)
                || !assignment_authority
                    .has_headroom(outputs.entries[target_index].original_data_in_flight_bytes)
            {
                return Err(RuntimeError::SenderServiceBlocked);
            }

            // Exact range ownership is visible before the carrier can dequeue
            // the committed command. Lock order for Native is fence ->
            // coordinator -> current shape -> outputs -> flights.
            self.record_validated_original_flight_with_outputs(&mut outputs, target_index, frame)?;
            command.commit();
            Ok(())
        };
        match (native_authority, expected_native_stamp) {
            (Some(authority), Some(stamp)) => authority
                .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
                .map_err(|_| RuntimeError::SenderServiceBlocked)?,
            (None, None) if target.key.underlay == UnderlayProtocol::Tcp => commit(None),
            _ => Err(RuntimeError::SenderServiceBlocked),
        }
    }
}

#[cfg(test)]
#[path = "tests_data_commit.rs"]
mod tests;

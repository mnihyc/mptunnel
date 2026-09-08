//! Atomic commit of newly assigned response data.
//!
//! The imminent writer supplies its Native fence. The binding validates exact
//! attachment/model and Product authority before converting shared source into
//! mux retention and Original flight. Queued publication is a test oracle.

use super::ResponseStreamBinding;
use super::attachment::{ResponseDispatchTarget, ResponseStreamOutputEntry, ResponseStreamOutputs};
use super::evidence::server_output_product_assignment_qualified;
use super::snapshot::{server_bulk_output_snapshot_at, server_native_bulk_output_snapshot_at};
use crate::model::admission::{BulkCandidatePosition, bulk_original_data_assignment_authority};
use crate::model::capacity::reliable_bulk_product_windows;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::writer_boundary::ReliableWriterReadyGuard;
use crate::runtime::sender::{ResponsePreparedCommitError, ResponsePreparedSourceCommit};
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl ResponseStreamBinding {
    #[cfg(test)]
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
    #[cfg(test)]
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
            if !self.original_assignment_has_headroom(
                &outputs,
                entry,
                payload_bytes,
                lane,
                position,
                native_shape,
                now,
            ) {
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

    /// The caller already owns the selected Native fence (or the TCP writer's
    /// imminent transaction) and has acquired source ownership nonblockingly.
    /// This is the same Product authority as queued dispatch, without a future
    /// Data-command reservation. Startup -> outputs -> flights matches FINAL.
    #[allow(clippy::too_many_arguments)]
    pub(in crate::runtime) fn commit_prepared_original_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
        position: BulkCandidatePosition,
        native_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
        ready: &ReliableWriterReadyGuard,
        source: &mut ResponsePreparedSourceCommit<'_>,
    ) -> Result<(), ResponsePreparedCommitError> {
        use ResponsePreparedCommitError::{Blocked, Source};
        let Some((offset, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(Blocked);
        };
        if !source.matches(frame) {
            return Err(Blocked);
        }
        let startup = self
            .response_startup
            .lock()
            .expect("server response startup lock");
        if startup.fresh_data_limit(offset, payload_bytes) != Some(payload_bytes) {
            return Err(Blocked);
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        if !self.response_stream_open.load(Ordering::Acquire)
            || self.response_model_generation.load(Ordering::Acquire) != expected_model_generation
        {
            return Err(Blocked);
        }
        let Some(index) = outputs.entries.iter().position(|entry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
        }) else {
            return Err(Blocked);
        };
        let entry = &outputs.entries[index];
        if !entry.commands.product_admission_active()
            || ready.receipt().instance() != target.path_instance_id
            || !ready.receipt().is_current()
        {
            return Err(Blocked);
        }
        match (target.native_authority_stamp, native_shape) {
            (Some(expected), Some(shape)) if expected == shape.stamp() => {}
            (None, None) if target.key.underlay == UnderlayProtocol::Tcp => {}
            _ => return Err(Blocked),
        }
        if !self.original_assignment_has_headroom(
            &outputs,
            entry,
            payload_bytes,
            lane,
            position,
            native_shape,
            Instant::now(),
        ) {
            return Err(Blocked);
        }
        if !ready.try_consume() {
            return Err(Blocked);
        }
        source
            .send_stream
            .commit_prepared_data(frame)
            .map_err(|error| Source(RuntimeError::Stream(error)))?;
        if let Err(error) =
            self.record_validated_original_flight_with_outputs(&mut outputs, index, frame)
        {
            source
                .send_stream
                .rollback_committed_data(frame)
                .map_err(|error| Source(RuntimeError::Stream(error)))?;
            return Err(match error {
                RuntimeError::SenderServiceBlocked => Blocked,
                error => Source(error),
            });
        }
        source.finish(payload_bytes);
        outputs.data_level_queue_bytes = source.queue.bytes() as u64;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn original_assignment_has_headroom(
        &self,
        outputs: &ResponseStreamOutputs,
        entry: &ResponseStreamOutputEntry,
        payload_bytes: usize,
        lane: TrafficClass,
        position: BulkCandidatePosition,
        native_shape: Option<NativeCarrierSchedulingShapeSnapshot>,
        now: Instant,
    ) -> bool {
        let assignment = if native_shape.is_some() {
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
        let authority = bulk_original_data_assignment_authority(
            assignment,
            payload_bytes,
            self.mux_limits,
            position,
            server_output_product_assignment_qualified(entry, self.mux_limits),
        );
        outputs
            .original_data_in_flight_bytes
            .checked_add(payload_bytes as u64)
            .is_some_and(|committed| {
                committed
                    <= reliable_bulk_product_windows(self.mux_limits).stream_resource_limit_bytes
            })
            && authority.has_headroom(entry.original_data_in_flight_bytes)
    }
}

#[cfg(test)]
#[path = "tests_data_commit.rs"]
mod tests;

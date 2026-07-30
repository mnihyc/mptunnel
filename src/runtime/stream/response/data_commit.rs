//! Atomic commit of newly assigned response data.
//!
//! Path selection is complete before this boundary. The binding validates the
//! selected attachment and model generation, reserves carrier queue capacity,
//! records exact range ownership, and only then publishes the carrier command.

use super::ResponseStreamBinding;
use super::attachment::{ResponseDispatchTarget, ResponseStreamOutputEntry};
use crate::protocol::Frame;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::runtime::RuntimeError;
use crate::runtime::tcp_service::TcpServiceWriterTransaction;
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;

impl ResponseStreamBinding {
    pub(in crate::runtime) fn try_enqueue_data_frame_for_dispatch_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_data_frame_for_dispatch_target_inner::<false>(
            target,
            frame,
            lane,
            expected_model_generation,
            None,
        )
    }

    pub(in crate::runtime) fn try_enqueue_data_frame_for_dispatch_target_with_tcp_service(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
        tcp_service: &mut TcpServiceWriterTransaction<'_>,
    ) -> Result<(), RuntimeError> {
        self.try_enqueue_data_frame_for_dispatch_target_inner::<true>(
            target,
            frame,
            lane,
            expected_model_generation,
            Some(tcp_service),
        )
    }

    fn try_enqueue_data_frame_for_dispatch_target_inner<const OBSERVE_TCP_SERVICE: bool>(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        expected_model_generation: u64,
        tcp_service: Option<&mut TcpServiceWriterTransaction<'_>>,
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
        if OBSERVE_TCP_SERVICE {
            let _recorded_commit =
                outputs.observe_tcp_service_commit(target_index, frame, tcp_service);
        }
        self.record_validated_original_flight_with_outputs(&mut outputs, target_index, frame);
        command.commit();
        Ok(())
    }
}

#[cfg(test)]
#[path = "data_commit_test.rs"]
mod tests;

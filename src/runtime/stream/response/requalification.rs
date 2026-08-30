//! Exact response-attachment requalification transaction.

use super::snapshot::server_bulk_output_snapshot;
use super::{CarrierPathFlight, ResponseStreamBinding};
use crate::model::requalification::{StreamPathQualification, StreamRequalificationProbe};
use crate::model::timing::reliable_path_stale_interval;
use crate::model::work::CarrierWorkKind;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, OffsetRange, StreamId};
use crate::runtime::RuntimeError;
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

impl ResponseStreamBinding {
    fn request_requalification_commands(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
    ) -> Result<crate::runtime::path::commands::ReliablePathCommandSender, RuntimeError> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| {
                entry.key == key
                    && entry.path_instance_id == path_instance_id
                    && !entry.commands.is_closed()
            })
            .map(|entry| entry.commands.clone())
            .ok_or(RuntimeError::ReliablePathSessionClosed)
    }

    /// ACKs one request-direction probe on the exact carrying attachment.
    /// Probe bytes are not delivered and create no Product flight.
    pub(in crate::runtime) fn accept_request_requalification_probe(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        stream_id: StreamId,
        probe: StreamRequalificationProbe,
    ) -> Result<(), RuntimeError> {
        let commands = self.request_requalification_commands(key, path_instance_id)?;
        commands.try_enqueue_admitted_frame(
            Frame::StreamRequalifyAck {
                stream_id,
                probe_id: probe.id,
                offset: probe.offset,
                payload_bytes: probe.payload_bytes,
            },
            TrafficClass::Control,
        )
    }

    /// Exact response-direction probe receipt enters bounded acquisition.
    pub(in crate::runtime) fn acknowledge_response_requalification_probe(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        probe: StreamRequalificationProbe,
    ) -> bool {
        let now = Instant::now();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let changed = outputs.entries.iter_mut().any(|entry| {
            if entry.key != key
                || entry.path_instance_id != path_instance_id
                || entry.commands.is_closed()
            {
                return false;
            }
            if !matches!(
                entry.qualification,
                StreamPathQualification::Requalifying {
                    probe: expected,
                    ..
                } if expected == probe
            ) {
                return false;
            }
            entry.qualification = StreamPathQualification::Acquiring { started_at: now };
            true
        });
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed {
            self.notify_update();
        }
        changed
    }

    pub(in crate::runtime) fn response_requalification_deadline(&self) -> Option<Instant> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .filter_map(|entry| match entry.qualification {
                StreamPathQualification::Requalifying { retry_at, .. } => Some(retry_at),
                _ => None,
            })
            .min()
    }

    /// Queues one bounded data-bearing duplicate on one stale output.
    ///
    /// The copied bytes retain their healthy OriginalData owner.  This method
    /// does not touch the Product flight ledger, so probe arrival, loss, or
    /// replay cannot deliver bytes or establish Data-ACK ownership.
    pub(in crate::runtime) fn try_enqueue_response_requalification_probe(
        &self,
        send_stream: &ReliableSendStream,
        lane: TrafficClass,
        byte_limit: usize,
    ) -> Result<Option<usize>, RuntimeError> {
        if byte_limit == 0 {
            return Ok(None);
        }
        let now = Instant::now();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        for entry in &mut outputs.entries {
            if let StreamPathQualification::Requalifying { retry_at, .. } = entry.qualification
                && retry_at <= now
            {
                entry.qualification = StreamPathQualification::Stale { retry_at };
            }
        }
        if outputs.entries.iter().any(|entry| {
            matches!(
                entry.qualification,
                StreamPathQualification::Requalifying { .. }
            )
        }) {
            return Ok(None);
        }
        let output_count = outputs.entries.len();
        let candidate_indices = (0..output_count)
            .filter_map(|distance| {
                let index =
                    (outputs.next_requalification_candidate_index + distance) % output_count;
                let entry = &outputs.entries[index];
                (entry.commands.product_admission_active()
                    && matches!(
                        entry.qualification,
                        StreamPathQualification::Stale { retry_at } if retry_at <= now
                    ))
                .then_some(index)
            })
            .collect::<Vec<_>>();
        let mut queue_blocked = false;
        for candidate_index in candidate_indices {
            let source = {
                let flights = self
                    .flights
                    .lock()
                    .expect("server reliable stream flight lock");
                requalification_source_range(&flights, byte_limit)
            };
            let Some(source) = source else {
                continue;
            };
            let Some(Frame::StreamData {
                stream_id,
                offset,
                payload,
            }) = send_stream
                .retransmission_frames_for_ranges(&[source], byte_limit)
                .into_iter()
                .next()
            else {
                continue;
            };
            let commands = outputs.entries[candidate_index].commands.clone();
            let preview = Frame::StreamRequalifyData {
                stream_id,
                probe_id: 1,
                offset,
                payload: payload.clone(),
            };
            if !commands.can_enqueue_reinjection_frame_now(&preview) {
                queue_blocked = true;
                continue;
            }
            let Some(probe_id) = outputs.next_requalification_probe_id else {
                return Ok(None);
            };
            outputs.next_requalification_probe_id = probe_id.checked_add(1);
            let payload_bytes = u32::try_from(payload.len())
                .ok()
                .filter(|bytes| *bytes > 0)
                .ok_or(RuntimeError::Protocol("requalification payload overflow"))?;
            let probe = StreamRequalificationProbe {
                id: probe_id,
                offset,
                payload_bytes,
            };
            let snapshot = server_bulk_output_snapshot(
                &outputs.entries[candidate_index],
                outputs.data_level_queue_bytes,
                lane,
                self.mux_limits,
            );
            let retry_after = reliable_path_stale_interval(
                Some(outputs.entries[candidate_index].key.underlay),
                Some(snapshot),
            );
            let frame = Frame::StreamRequalifyData {
                stream_id,
                probe_id,
                offset,
                payload,
            };
            match commands.try_reserve_reinjection_frame(frame, lane) {
                Ok(reservation) => {
                    outputs.entries[candidate_index].qualification =
                        StreamPathQualification::Requalifying {
                            probe,
                            retry_at: now + retry_after,
                        };
                    outputs.next_requalification_candidate_index =
                        (candidate_index + 1) % output_count;
                    reservation.commit();
                    return Ok(Some(payload_bytes as usize));
                }
                Err(RuntimeError::SenderServiceBlocked)
                | Err(RuntimeError::ReliablePathSessionClosed) => {
                    queue_blocked = true;
                    outputs.next_requalification_candidate_index =
                        (candidate_index + 1) % output_count;
                }
                Err(error) => return Err(error),
            }
        }
        if queue_blocked {
            Err(RuntimeError::SenderServiceBlocked)
        } else {
            Ok(None)
        }
    }
}

fn requalification_source_range(
    flights: &std::collections::BTreeMap<u64, Vec<CarrierPathFlight>>,
    byte_limit: usize,
) -> Option<OffsetRange> {
    flights.iter().find_map(|(start, path_flights)| {
        let owner = path_flights
            .iter()
            .rev()
            .find(|flight| flight.kind == CarrierWorkKind::OriginalData)?;
        OffsetRange::new(
            *start,
            owner.end.min(start.saturating_add(byte_limit as u64)),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::MuxLimits;
    use crate::protocol::{PathId, SessionId, UnderlayProtocol};
    use crate::runtime::path::commands::{
        ReliablePathCommand, reliable_path_command_channels, try_recv_reliable_path_command,
    };
    use crate::runtime::sender::ServerReinjectionOutputIdentity;
    use bytes::Bytes;

    fn key(underlay: UnderlayProtocol, path_id: u16) -> crate::model::path::CarrierPathKey {
        crate::model::path::CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        }
    }

    #[test]
    fn response_requalification_needs_exact_probe_then_fresh_original_ack() {
        let stream_id = StreamId(701);
        let candidate = key(UnderlayProtocol::Tcp, 0);
        let healthy = key(UnderlayProtocol::Udp, 1);
        let (candidate_commands, mut candidate_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(701),
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            TrafficClass::Throughput,
        );
        let (healthy_commands, _healthy_receivers) = reliable_path_command_channels(8);
        binding.attach(
            healthy.underlay,
            healthy.path_id,
            healthy_commands,
            TrafficClass::Throughput,
        );
        let candidate_entry = binding
            .outputs
            .lock()
            .expect("response outputs")
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .map(|entry| (entry.path_instance_id, entry.incarnation))
            .expect("candidate output");
        let healthy_path_instance_id = binding
            .outputs
            .lock()
            .expect("response outputs")
            .entries
            .iter()
            .find(|entry| entry.key == healthy)
            .expect("healthy output")
            .path_instance_id;
        let candidate_identity = ServerReinjectionOutputIdentity {
            key: candidate,
            incarnation: candidate_entry.1,
        };

        let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
        let old = send_stream
            .send_data(Bytes::from(vec![0x41; 4096]))
            .expect("pre-stale candidate data");
        let source = send_stream
            .send_data(Bytes::from(vec![0x42; 4096]))
            .expect("healthy retained source");
        let fresh = send_stream
            .send_data(Bytes::from(vec![0x43; 4096]))
            .expect("post-probe candidate data");
        binding.record_original_flight(candidate, &old);
        binding.record_original_flight(healthy, &source);
        {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            entry.original_data_acked_bytes = 1 << 20;
            entry.delivery_samples = 7;
        }
        assert!(binding.mark_output_stale(candidate_identity));
        {
            let outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            assert_eq!(entry.original_data_acked_bytes, 0);
            assert_eq!(entry.delivery_samples, 0);
        }

        assert!(matches!(
            binding.try_enqueue_response_requalification_probe(
                &send_stream,
                TrafficClass::Throughput,
                4096,
            ),
            Ok(Some(4096))
        ));
        let probe = match try_recv_reliable_path_command(&mut candidate_receivers) {
            Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyData {
                probe_id,
                offset,
                payload,
                ..
            })) => StreamRequalificationProbe {
                id: probe_id,
                offset,
                payload_bytes: payload.len() as u32,
            },
            _ => panic!("candidate receives exact response probe"),
        };
        assert_eq!(probe.offset, 0);
        assert_eq!(
            binding.original_flight_outputs_overlapping_frame(&source),
            vec![(
                healthy,
                binding
                    .outputs
                    .lock()
                    .expect("response outputs")
                    .entries
                    .iter()
                    .find(|entry| entry.key == healthy)
                    .expect("healthy output")
                    .incarnation
            )],
            "probe never becomes an alternate Product owner"
        );
        assert!(!binding.acknowledge_response_requalification_probe(
            healthy,
            healthy_path_instance_id,
            probe,
        ));
        assert!(!binding.acknowledge_response_requalification_probe(
            candidate,
            candidate_entry.0,
            StreamRequalificationProbe {
                id: probe.id + 1,
                ..probe
            },
        ));
        assert!(binding.acknowledge_response_requalification_probe(
            candidate,
            candidate_entry.0,
            probe,
        ));
        assert!(
            binding
                .outputs
                .lock()
                .expect("response outputs")
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output")
                .qualification
                .acquiring()
        );

        let old_release = binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 0,
            end: 4096,
        }]);
        assert!(old_release.path_progress_outputs.is_empty());
        {
            let outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            assert!(entry.qualification.acquiring());
            assert_eq!(entry.original_data_acked_bytes, 0);
            assert_eq!(entry.delivery_samples, 0);
        }
        assert!(!binding.acknowledge_response_requalification_probe(
            candidate,
            candidate_entry.0,
            probe,
        ));

        binding.record_original_flight(candidate, &fresh);
        let fresh_release = binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 8192,
            end: 12288,
        }]);
        assert_eq!(
            fresh_release.path_progress_outputs.as_slice(),
            &[candidate_identity]
        );
        let outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        assert_eq!(entry.qualification, StreamPathQualification::Qualified);
        assert_eq!(entry.original_data_acked_bytes, 4096);
    }

    #[test]
    fn sole_stale_response_fallback_can_source_a_non_owning_probe() {
        let stream_id = StreamId(702);
        let candidate = key(UnderlayProtocol::Udp, 0);
        let (commands, mut receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(702),
            candidate.underlay,
            candidate.path_id,
            commands,
            TrafficClass::Throughput,
        );
        let (path_instance_id, incarnation) = {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs.entries.first_mut().expect("sole response output");
            entry.qualification = StreamPathQualification::Stale {
                retry_at: Instant::now(),
            };
            (entry.path_instance_id, entry.incarnation)
        };

        let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
        let fallback = send_stream
            .send_data(Bytes::from(vec![0x61; 4096]))
            .expect("sole-stale response fallback");
        binding.record_original_flight(candidate, &fallback);
        assert!(matches!(
            binding.try_enqueue_response_requalification_probe(
                &send_stream,
                TrafficClass::Throughput,
                4096,
            ),
            Ok(Some(4096))
        ));
        let probe = match try_recv_reliable_path_command(&mut receivers) {
            Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyData {
                probe_id,
                offset,
                payload,
                ..
            })) => StreamRequalificationProbe {
                id: probe_id,
                offset,
                payload_bytes: payload.len() as u32,
            },
            _ => panic!("sole stale response output receives the exact probe"),
        };
        assert_eq!(probe.offset, 0);
        assert_eq!(
            binding.original_flight_outputs_overlapping_frame(&fallback),
            vec![(candidate, incarnation)],
            "the probe does not become a second Product owner"
        );
        assert!(binding.acknowledge_response_requalification_probe(
            candidate,
            path_instance_id,
            probe,
        ));

        let fresh = send_stream
            .send_data(Bytes::from(vec![0x62; 4096]))
            .expect("fresh response acquisition data");
        binding.record_original_flight(candidate, &fresh);
        let release = binding.release_normalized_acked_ranges(&[OffsetRange {
            start: 4096,
            end: 8192,
        }]);
        assert_eq!(
            release.path_progress_outputs.as_slice(),
            &[ServerReinjectionOutputIdentity {
                key: candidate,
                incarnation,
            }]
        );
        assert_eq!(
            binding.outputs.lock().expect("response outputs").entries[0].qualification,
            StreamPathQualification::Qualified
        );
    }

    #[test]
    fn response_requalification_skips_draining_and_full_stale_outputs() {
        let stream_id = StreamId(703);
        let draining = key(UnderlayProtocol::Tcp, 0);
        let full = key(UnderlayProtocol::Tcp, 1);
        let ready = key(UnderlayProtocol::Tcp, 2);
        let (draining_commands, mut draining_receivers) = reliable_path_command_channels(1);
        let binding = ResponseStreamBinding::new(
            SessionId(703),
            draining.underlay,
            draining.path_id,
            draining_commands.clone(),
            TrafficClass::Throughput,
        );
        let (full_commands, mut full_receivers) = reliable_path_command_channels(1);
        binding.attach(
            full.underlay,
            full.path_id,
            full_commands.clone(),
            TrafficClass::Throughput,
        );
        let (ready_commands, mut ready_receivers) = reliable_path_command_channels(1);
        binding.attach(
            ready.underlay,
            ready.path_id,
            ready_commands,
            TrafficClass::Throughput,
        );
        {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            for entry in &mut outputs.entries {
                entry.qualification = StreamPathQualification::Stale {
                    retry_at: Instant::now(),
                };
            }
        }
        draining_commands.begin_path_drain();
        full_commands
            .try_enqueue_reinjection_frame(
                Frame::StreamData {
                    stream_id: StreamId(998),
                    offset: 0,
                    payload: Bytes::from(vec![0x73; 4096]),
                },
                TrafficClass::Throughput,
            )
            .expect("fill first active response output");
        let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
        let source = send_stream
            .send_data(Bytes::from(vec![0x74; 4096]))
            .expect("retained response source");
        binding.record_original_flight(ready, &source);

        assert!(matches!(
            binding.try_enqueue_response_requalification_probe(
                &send_stream,
                TrafficClass::Throughput,
                4096,
            ),
            Ok(Some(4096))
        ));
        assert!(try_recv_reliable_path_command(&mut draining_receivers).is_none());
        assert!(matches!(
            try_recv_reliable_path_command(&mut full_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamData {
                stream_id: StreamId(998),
                ..
            }))
        ));
        assert!(matches!(
            try_recv_reliable_path_command(&mut ready_receivers),
            Some(ReliablePathCommand::SendFrame(
                Frame::StreamRequalifyData { .. }
            ))
        ));
    }

    #[test]
    fn all_full_stale_response_outputs_return_bounded_backpressure() {
        let stream_id = StreamId(704);
        let first = key(UnderlayProtocol::Tcp, 0);
        let second = key(UnderlayProtocol::Tcp, 1);
        let (first_commands, _first_receivers) = reliable_path_command_channels(1);
        let binding = ResponseStreamBinding::new(
            SessionId(704),
            first.underlay,
            first.path_id,
            first_commands.clone(),
            TrafficClass::Throughput,
        );
        let (second_commands, _second_receivers) = reliable_path_command_channels(1);
        binding.attach(
            second.underlay,
            second.path_id,
            second_commands.clone(),
            TrafficClass::Throughput,
        );
        {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            for entry in &mut outputs.entries {
                entry.qualification = StreamPathQualification::Stale {
                    retry_at: Instant::now(),
                };
            }
        }
        for (commands, filler_stream) in [(&first_commands, 996), (&second_commands, 997)] {
            commands
                .try_enqueue_reinjection_frame(
                    Frame::StreamData {
                        stream_id: StreamId(filler_stream),
                        offset: 0,
                        payload: Bytes::from(vec![0x75; 4096]),
                    },
                    TrafficClass::Throughput,
                )
                .expect("fill response reinjection queue");
        }
        let mut send_stream = ReliableSendStream::new(stream_id, MuxLimits::default());
        let source = send_stream
            .send_data(Bytes::from(vec![0x76; 4096]))
            .expect("retained response source");
        binding.record_original_flight(first, &source);

        assert!(matches!(
            binding.try_enqueue_response_requalification_probe(
                &send_stream,
                TrafficClass::Throughput,
                4096,
            ),
            Err(RuntimeError::SenderServiceBlocked)
        ));
        let outputs = binding.outputs.lock().expect("response outputs");
        assert!(
            outputs
                .entries
                .iter()
                .all(|entry| matches!(entry.qualification, StreamPathQualification::Stale { .. }))
        );
    }
}

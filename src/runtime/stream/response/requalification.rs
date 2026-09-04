//! Exact response-attachment requalification transaction.

use super::snapshot::server_bulk_output_snapshot;
use super::{CarrierPathFlight, ResponseStreamBinding};
use crate::model::product_qualification::ProductQualificationAuthority;
use crate::model::requalification::{
    StreamPathQualification, StreamRequalificationProbe, StreamRequalificationReceipt,
};
use crate::model::timing::reliable_path_stale_interval;
use crate::model::work::CarrierWorkKind;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, OffsetRange, StreamId};
use crate::runtime::RuntimeError;
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::runtime::stream::{RequalificationAttempt, TargetCarrierCapacityWait};
use crate::scheduler::TrafficClass;
use std::sync::atomic::Ordering;
use std::time::Instant;

#[derive(Clone)]
struct PendingRequestRequalificationAck {
    preferred_key: crate::model::path::CarrierPathKey,
    preferred_path_instance_id: crate::model::path::CarrierPathInstanceId,
    frame: Frame,
}

#[derive(Default)]
pub(super) struct RequestRequalificationAckPublication {
    highest_accepted_probe: Option<StreamRequalificationProbe>,
    pending: Option<PendingRequestRequalificationAck>,
}

impl ResponseStreamBinding {
    /// ACKs one request-direction probe through any live authenticated output
    /// in the same logical session, preferring its carrying attachment.
    /// Probe bytes are not delivered and create no Product flight.
    pub(in crate::runtime) fn accept_request_requalification_probe(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        stream_id: StreamId,
        probe: StreamRequalificationProbe,
    ) -> Result<(), RuntimeError> {
        let attached = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| entry.key == key && entry.path_instance_id == path_instance_id);
        if !attached {
            return Err(RuntimeError::ReliablePathSessionClosed);
        }
        let ack = Frame::StreamRequalifyAck {
            stream_id,
            probe_id: probe.id,
            offset: probe.offset,
            payload_bytes: probe.payload_bytes,
        };
        let installed = {
            let mut publication = self
                .request_requalification_ack
                .lock()
                .expect("server request requalification ACK lock");
            if let Some(highest) = publication.highest_accepted_probe {
                if probe.id < highest.id {
                    return Ok(());
                }
                if probe.id == highest.id && probe != highest {
                    return Err(RuntimeError::Protocol(
                        "request requalification probe id reused with a different tuple",
                    ));
                }
                if probe == highest {
                    debug_assert!(
                        publication
                            .pending
                            .as_ref()
                            .is_none_or(|pending| pending.frame == ack)
                    );
                    if publication.pending.is_none() {
                        // The exact receipt already entered at least one
                        // reliable carrier queue. Native delivery/loss stays
                        // with that carrier; accepting the same Product probe
                        // again would create unbounded cross-path copies.
                        return Ok(());
                    }
                    false
                } else {
                    // A higher probe supersedes an older receipt that never
                    // entered any return queue. The sender is allowed to
                    // expire that old transaction while reverse service is
                    // unavailable.
                    publication.highest_accepted_probe = Some(probe);
                    publication.pending = Some(PendingRequestRequalificationAck {
                        preferred_key: key,
                        preferred_path_instance_id: path_instance_id,
                        frame: ack,
                    });
                    true
                }
            } else {
                publication.highest_accepted_probe = Some(probe);
                publication.pending = Some(PendingRequestRequalificationAck {
                    preferred_key: key,
                    preferred_path_instance_id: path_instance_id,
                    frame: ack,
                });
                true
            }
        };
        if installed {
            self.notify_update();
        }

        // Queue admission is not delivery evidence: a preferred writer can be
        // arbitrarily delayed while a sibling has bounded reverse service.
        // Publish one identical receipt on every output that admits it in this
        // pass. The receiver's exact tuple makes all copies idempotent.
        let _ = self.retry_pending_request_requalification_ack()?;
        Ok(())
    }

    pub(in crate::runtime) fn has_pending_request_requalification_ack(&self) -> bool {
        self.request_requalification_ack
            .lock()
            .expect("server request requalification ACK lock")
            .pending
            .is_some()
    }

    pub(in crate::runtime) fn pending_request_requalification_ack_capacity_notifies(
        &self,
    ) -> Vec<std::sync::Arc<tokio::sync::Notify>> {
        let publication = self
            .request_requalification_ack
            .lock()
            .expect("server request requalification ACK lock");
        if publication.pending.is_none() {
            return Vec::new();
        }
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .filter(|entry| !entry.commands.control_frame_admission_is_closed())
            .map(|entry| entry.commands.capacity_notify())
            .collect()
    }

    /// Retries the one stream-owned receipt over the current authenticated
    /// attachment set. The preferred ingress controls order only. Any
    /// successful publication completes this transaction; a zero-publication
    /// pass retains it for a capacity or membership wake.
    pub(in crate::runtime) fn retry_pending_request_requalification_ack(
        &self,
    ) -> Result<bool, RuntimeError> {
        let mut publication = self
            .request_requalification_ack
            .lock()
            .expect("server request requalification ACK lock");
        let Some(pending) = publication.pending.as_ref().cloned() else {
            return Ok(false);
        };
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let preferred = outputs.entries.iter().position(|entry| {
            entry.key == pending.preferred_key
                && entry.path_instance_id == pending.preferred_path_instance_id
        });
        let candidates = preferred.into_iter().chain(
            (0..outputs.entries.len()).filter(move |candidate| Some(*candidate) != preferred),
        );
        let mut published = false;
        let mut first_fatal_error = None;
        for candidate in candidates {
            let commands = &outputs.entries[candidate].commands;
            if commands.control_frame_admission_is_closed() {
                continue;
            }
            match commands.try_enqueue_admitted_frame(pending.frame.clone(), TrafficClass::Control)
            {
                Ok(()) => published = true,
                Err(RuntimeError::SenderServiceBlocked)
                | Err(RuntimeError::ReliablePathSessionClosed) => {}
                Err(error) => {
                    if first_fatal_error.is_none() {
                        first_fatal_error = Some(error);
                    }
                }
            }
        }
        drop(outputs);
        if published {
            publication.pending = None;
        }
        drop(publication);
        if published {
            self.notify_update();
            Ok(true)
        } else if let Some(error) = first_fatal_error {
            Err(error)
        } else {
            Ok(false)
        }
    }

    /// Exact response-direction probe receipt enters bounded acquisition.
    pub(in crate::runtime) fn acknowledge_response_requalification_probe(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        probe: StreamRequalificationProbe,
    ) -> bool {
        self.acknowledge_response_requalification_probe_at(
            key,
            path_instance_id,
            probe,
            Instant::now(),
        )
    }

    fn acknowledge_response_requalification_probe_at(
        &self,
        key: crate::model::path::CarrierPathKey,
        path_instance_id: crate::model::path::CarrierPathInstanceId,
        probe: StreamRequalificationProbe,
        now: Instant,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        // The carrying output proves authenticated same-session return
        // service. It does not identify the forward attachment being proved.
        if !outputs
            .entries
            .iter()
            .any(|entry| entry.key == key && entry.path_instance_id == path_instance_id)
        {
            return false;
        }
        let mut changed = false;
        let mut acknowledged = false;
        for entry in &mut outputs.entries {
            match entry.qualification.classify_probe_receipt(probe, now) {
                StreamRequalificationReceipt::Unmatched => continue,
                StreamRequalificationReceipt::Expired { retry_at } => {
                    entry.qualification = StreamPathQualification::Stale { retry_at };
                    changed = true;
                }
                StreamRequalificationReceipt::Timely => {
                    if entry.product_qualification.authority()
                        != ProductQualificationAuthority::Revoked
                        || entry.product_qualification.reactivate_without_evidence() != Ok(true)
                    {
                        break;
                    }
                    entry.qualification = StreamPathQualification::Acquiring { started_at: now };
                    changed = true;
                    acknowledged = true;
                }
            }
            break;
        }
        if changed {
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        }
        drop(outputs);
        if changed {
            self.notify_update();
        }
        acknowledged
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
    ) -> Result<RequalificationAttempt<ServerReinjectionOutputIdentity>, RuntimeError> {
        if byte_limit == 0 {
            return Ok(RequalificationAttempt::Idle);
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
            return Ok(RequalificationAttempt::Idle);
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
        let mut capacity_blocked = Vec::new();
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
            let target = ServerReinjectionOutputIdentity {
                key: outputs.entries[candidate_index].key,
                incarnation: outputs.entries[candidate_index].incarnation,
            };
            let preview = Frame::StreamRequalifyData {
                stream_id,
                probe_id: 1,
                offset,
                payload: payload.clone(),
            };
            // Arm the exact writer edge before observing queue capacity. A
            // release racing this check is then retained by the actor-owned
            // target-local wait instead of being lost.
            let capacity_wait = TargetCarrierCapacityWait::arm(target, commands.capacity_notify());
            if !commands.can_enqueue_reinjection_frame_now(&preview) {
                capacity_blocked.push(capacity_wait);
                continue;
            }
            let Some(probe_id) = outputs.next_requalification_probe_id else {
                return Ok(RequalificationAttempt::Idle);
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
                    return Ok(RequalificationAttempt::Published {
                        target,
                        payload_bytes: payload_bytes as usize,
                    });
                }
                Err(RuntimeError::SenderServiceBlocked) => {
                    capacity_blocked.push(capacity_wait);
                    outputs.next_requalification_candidate_index =
                        (candidate_index + 1) % output_count;
                }
                Err(RuntimeError::ReliablePathSessionClosed) => {
                    outputs.next_requalification_candidate_index =
                        (candidate_index + 1) % output_count;
                }
                Err(error) => return Err(error),
            }
        }
        if capacity_blocked.is_empty() {
            Ok(RequalificationAttempt::Idle)
        } else {
            Ok(RequalificationAttempt::CapacityBlocked {
                targets: capacity_blocked,
            })
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
    use crate::runtime::stream::response::next_server_carrier_path_instance_id;
    use bytes::Bytes;
    use std::time::Duration;

    fn key(underlay: UnderlayProtocol, path_id: u16) -> crate::model::path::CarrierPathKey {
        crate::model::path::CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        }
    }

    #[test]
    fn request_requalification_ack_replicates_once_to_each_queue_admitting_attachment() {
        let stream_id = StreamId(708);
        let carrying = key(UnderlayProtocol::Tcp, 0);
        let sibling = key(UnderlayProtocol::Udp, 1);
        let (carrying_commands, mut carrying_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(708),
            carrying.underlay,
            carrying.path_id,
            carrying_commands,
            TrafficClass::Throughput,
        );
        let carrying_instance =
            binding.outputs.lock().expect("response outputs").entries[0].path_instance_id;
        let (sibling_commands, mut sibling_receivers) = reliable_path_command_channels(8);
        assert!(matches!(
            binding.attach(
                sibling.underlay,
                sibling.path_id,
                sibling_commands,
                TrafficClass::Throughput,
            ),
            crate::runtime::stream::response::ResponseStreamAttachOutcome::Attached
        ));

        let probe = StreamRequalificationProbe {
            id: 19,
            offset: 12_288,
            payload_bytes: 1024,
        };
        binding
            .accept_request_requalification_probe(carrying, carrying_instance, stream_id, probe)
            .expect("publish the exact ACK on the bounded return set");
        let expected = Frame::StreamRequalifyAck {
            stream_id,
            probe_id: probe.id,
            offset: probe.offset,
            payload_bytes: probe.payload_bytes,
        };
        assert!(matches!(
            try_recv_reliable_path_command(&mut carrying_receivers),
            Some(ReliablePathCommand::SendFrame(frame)) if frame == expected
        ));
        assert!(matches!(
            try_recv_reliable_path_command(&mut sibling_receivers),
            Some(ReliablePathCommand::SendFrame(frame)) if frame == expected
        ));
        assert!(
            try_recv_reliable_path_command(&mut carrying_receivers).is_none(),
            "the carrying attachment receives at most one exact ACK copy"
        );
        assert!(
            try_recv_reliable_path_command(&mut sibling_receivers).is_none(),
            "the sibling attachment receives at most one exact ACK copy"
        );
        binding
            .accept_request_requalification_probe(carrying, carrying_instance, stream_id, probe)
            .expect("an exact replay is a bounded no-op after publication");
        assert!(try_recv_reliable_path_command(&mut carrying_receivers).is_none());
        assert!(try_recv_reliable_path_command(&mut sibling_receivers).is_none());
        assert!(matches!(
            binding.accept_request_requalification_probe(
                carrying,
                carrying_instance,
                stream_id,
                StreamRequalificationProbe {
                    offset: probe.offset + 1,
                    ..probe
                },
            ),
            Err(RuntimeError::Protocol(_))
        ));
    }

    #[test]
    fn request_requalification_ack_does_not_complete_on_known_terminal_return_writer() {
        let stream_id = StreamId(710);
        let terminal = key(UnderlayProtocol::Tcp, 0);
        let healthy = key(UnderlayProtocol::Udp, 1);
        let (terminal_commands, mut terminal_receivers) = reliable_path_command_channels(4);
        let binding = ResponseStreamBinding::new(
            SessionId(710),
            terminal.underlay,
            terminal.path_id,
            terminal_commands.clone(),
            TrafficClass::Throughput,
        );
        let (healthy_commands, mut healthy_receivers) = reliable_path_command_channels(1);
        assert!(matches!(
            binding.attach(
                healthy.underlay,
                healthy.path_id,
                healthy_commands.clone(),
                TrafficClass::Throughput,
            ),
            crate::runtime::stream::response::ResponseStreamAttachOutcome::Attached
        ));
        let healthy_instance = binding
            .outputs
            .lock()
            .expect("response outputs")
            .entries
            .iter()
            .find(|entry| entry.key == healthy)
            .expect("healthy response attachment")
            .path_instance_id;

        terminal_commands.terminate_failed_path();
        healthy_commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce: 710 }, TrafficClass::Control)
            .expect("fill the only live reverse-control queue");
        binding
            .accept_request_requalification_probe(
                healthy,
                healthy_instance,
                stream_id,
                StreamRequalificationProbe {
                    id: 23,
                    offset: 40_960,
                    payload_bytes: 1024,
                },
            )
            .expect("the logical stream retains a zero-publication receipt");
        assert!(
            binding.has_pending_request_requalification_ack(),
            "a known-terminal writer cannot complete the receipt while its live sibling is full",
        );
        assert!(
            try_recv_reliable_path_command(&mut terminal_receivers).is_none(),
            "terminal failure cannot accept a receipt that is then cleared globally",
        );
        assert!(matches!(
            try_recv_reliable_path_command(&mut healthy_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 710 }))
        ));
    }

    #[test]
    fn request_requalification_ack_remains_admissible_during_planned_drain() {
        let stream_id = StreamId(711);
        let carrying = key(UnderlayProtocol::Tcp, 0);
        let (commands, mut receivers) = reliable_path_command_channels(4);
        let binding = ResponseStreamBinding::new(
            SessionId(711),
            carrying.underlay,
            carrying.path_id,
            commands.clone(),
            TrafficClass::Throughput,
        );
        let carrying_instance =
            binding.outputs.lock().expect("response outputs").entries[0].path_instance_id;
        commands.begin_path_drain();
        let probe = StreamRequalificationProbe {
            id: 24,
            offset: 45_056,
            payload_bytes: 1024,
        };
        binding
            .accept_request_requalification_probe(carrying, carrying_instance, stream_id, probe)
            .expect("planned drain retains Product-neutral settlement control");
        assert!(!binding.has_pending_request_requalification_ack());
        assert!(matches!(
            try_recv_reliable_path_command(&mut receivers),
            Some(ReliablePathCommand::SendFrame(Frame::StreamRequalifyAck {
                stream_id: received_stream_id,
                probe_id: 24,
                offset: 45_056,
                payload_bytes: 1024,
            })) if received_stream_id == stream_id
        ));
    }

    #[test]
    fn request_requalification_ack_zero_publication_is_retained_until_any_output_admits() {
        let stream_id = StreamId(709);
        let carrying = key(UnderlayProtocol::Tcp, 0);
        let sibling = key(UnderlayProtocol::Udp, 1);
        let (carrying_commands, mut carrying_receivers) = reliable_path_command_channels(1);
        let binding = ResponseStreamBinding::new(
            SessionId(709),
            carrying.underlay,
            carrying.path_id,
            carrying_commands.clone(),
            TrafficClass::Throughput,
        );
        let carrying_instance =
            binding.outputs.lock().expect("response outputs").entries[0].path_instance_id;
        let (sibling_commands, mut sibling_receivers) = reliable_path_command_channels(1);
        assert!(matches!(
            binding.attach(
                sibling.underlay,
                sibling.path_id,
                sibling_commands.clone(),
                TrafficClass::Throughput,
            ),
            crate::runtime::stream::response::ResponseStreamAttachOutcome::Attached
        ));
        carrying_commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce: 1 }, TrafficClass::Control)
            .expect("fill carrying control queue");
        sibling_commands
            .try_enqueue_admitted_frame(Frame::Ping { nonce: 2 }, TrafficClass::Control)
            .expect("fill sibling control queue");

        let probe = StreamRequalificationProbe {
            id: 20,
            offset: 16_384,
            payload_bytes: 512,
        };
        binding
            .accept_request_requalification_probe(carrying, carrying_instance, stream_id, probe)
            .expect("the stream owns a receipt even when every output is full");
        assert!(binding.has_pending_request_requalification_ack());
        assert_eq!(
            binding
                .pending_request_requalification_ack_capacity_notifies()
                .len(),
            2,
            "the retry transaction waits on every current return attachment"
        );

        let successor = StreamRequalificationProbe {
            id: probe.id + 1,
            offset: probe.offset + u64::from(probe.payload_bytes),
            payload_bytes: probe.payload_bytes,
        };
        binding
            .accept_request_requalification_probe(carrying, carrying_instance, stream_id, successor)
            .expect("a newer probe replaces an unpublished receipt");
        binding
            .accept_request_requalification_probe(
                carrying,
                carrying_instance,
                stream_id,
                StreamRequalificationProbe {
                    id: probe.id - 1,
                    ..probe
                },
            )
            .expect("an older probe is a stale no-op");

        assert!(matches!(
            try_recv_reliable_path_command(&mut sibling_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 2 }))
        ));
        assert!(
            binding
                .retry_pending_request_requalification_ack()
                .expect("retry pending receipt"),
            "one queue-admitting sibling completes the publication transaction"
        );
        let expected = Frame::StreamRequalifyAck {
            stream_id,
            probe_id: successor.id,
            offset: successor.offset,
            payload_bytes: successor.payload_bytes,
        };
        assert!(matches!(
            try_recv_reliable_path_command(&mut sibling_receivers),
            Some(ReliablePathCommand::SendFrame(frame)) if frame == expected
        ));
        assert!(!binding.has_pending_request_requalification_ack());

        assert!(matches!(
            try_recv_reliable_path_command(&mut carrying_receivers),
            Some(ReliablePathCommand::SendFrame(Frame::Ping { nonce: 1 }))
        ));
        assert!(
            !binding
                .retry_pending_request_requalification_ack()
                .expect("a completed receipt has no retry")
        );
        assert!(
            try_recv_reliable_path_command(&mut carrying_receivers).is_none(),
            "a later release cannot amplify a receipt completed on its sibling"
        );
    }

    #[test]
    fn response_requalification_ack_return_carrier_does_not_replace_probe_target() {
        let candidate = key(UnderlayProtocol::Tcp, 0);
        let return_carrier = key(UnderlayProtocol::Udp, 1);
        let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(700),
            candidate.underlay,
            candidate.path_id,
            candidate_commands,
            TrafficClass::Throughput,
        );
        let (return_commands, _return_receivers) = reliable_path_command_channels(8);
        binding.attach(
            return_carrier.underlay,
            return_carrier.path_id,
            return_commands,
            TrafficClass::Throughput,
        );
        let probe = StreamRequalificationProbe {
            id: 7,
            offset: 4096,
            payload_bytes: 1024,
        };
        let deadline = Instant::now() + Duration::from_secs(1);
        let return_path_instance_id = {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            let candidate_index = outputs
                .entries
                .iter()
                .position(|entry| entry.key == candidate)
                .expect("candidate output");
            let candidate_entry = &mut outputs.entries[candidate_index];
            candidate_entry.qualification = StreamPathQualification::Requalifying {
                probe,
                retry_at: deadline,
            };
            candidate_entry.product_qualification.revoke();
            outputs.next_requalification_probe_id = Some(probe.id + 1);
            outputs.next_requalification_candidate_index =
                (candidate_index + 1) % outputs.entries.len();
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == return_carrier)
                .expect("authenticated sibling return carrier")
                .path_instance_id
        };

        assert!(
            binding.acknowledge_response_requalification_probe_at(
                return_carrier,
                return_path_instance_id,
                probe,
                deadline - Duration::from_nanos(1),
            ),
            "the exact pending probe, not its authenticated return carrier, identifies the forward attachment"
        );
        let outputs = binding.outputs.lock().expect("response outputs");
        assert!(
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output")
                .qualification
                .acquiring()
        );
        assert_eq!(
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output")
                .product_qualification
                .authority(),
            ProductQualificationAuthority::Active
        );
        assert_eq!(
            outputs
                .entries
                .iter()
                .find(|entry| entry.key == return_carrier)
                .expect("return carrier")
                .qualification,
            StreamPathQualification::Qualified,
            "the ACK carrier is not the requalification target"
        );
    }

    #[test]
    fn response_requalification_ack_at_or_after_deadline_is_stale_noop() {
        for (index, lateness) in [Duration::ZERO, Duration::from_millis(1)]
            .into_iter()
            .enumerate()
        {
            let candidate = key(UnderlayProtocol::Tcp, 0);
            let return_carrier = key(UnderlayProtocol::Udp, 1);
            let (candidate_commands, _candidate_receivers) = reliable_path_command_channels(8);
            let binding = ResponseStreamBinding::new(
                SessionId(704 + index as u64),
                candidate.underlay,
                candidate.path_id,
                candidate_commands,
                TrafficClass::Throughput,
            );
            let (return_commands, _return_receivers) = reliable_path_command_channels(8);
            binding.attach(
                return_carrier.underlay,
                return_carrier.path_id,
                return_commands,
                TrafficClass::Throughput,
            );
            let probe = StreamRequalificationProbe {
                id: 10,
                offset: 12_288,
                payload_bytes: 1024,
            };
            let deadline = Instant::now();
            let (return_path_instance_id, cursor_before, next_probe_id_before) = {
                let mut outputs = binding.outputs.lock().expect("response outputs");
                let candidate_index = outputs
                    .entries
                    .iter()
                    .position(|entry| entry.key == candidate)
                    .expect("candidate output");
                let candidate_entry = &mut outputs.entries[candidate_index];
                candidate_entry.qualification = StreamPathQualification::Requalifying {
                    probe,
                    retry_at: deadline,
                };
                candidate_entry.product_qualification.revoke();
                outputs.next_requalification_probe_id = Some(probe.id + 1);
                outputs.next_requalification_candidate_index =
                    (candidate_index + 1) % outputs.entries.len();
                let return_path_instance_id = outputs
                    .entries
                    .iter()
                    .find(|entry| entry.key == return_carrier)
                    .expect("authenticated sibling return carrier")
                    .path_instance_id;
                (
                    return_path_instance_id,
                    outputs.next_requalification_candidate_index,
                    outputs.next_requalification_probe_id,
                )
            };
            let generation_before = binding.response_model_generation.load(Ordering::Acquire);
            let mut updates = binding.subscribe_updates();

            assert!(!binding.acknowledge_response_requalification_probe_at(
                return_carrier,
                return_path_instance_id,
                probe,
                deadline + lateness,
            ));
            assert_eq!(
                binding.response_model_generation.load(Ordering::Acquire),
                generation_before + 1,
                "expiry publishes the successor service wake"
            );
            assert!(updates.has_changed().expect("response expiry wake"));
            updates.borrow_and_update();
            let outputs = binding.outputs.lock().expect("response outputs");
            let target = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            assert_eq!(
                target.qualification,
                StreamPathQualification::Stale { retry_at: deadline }
            );
            assert_eq!(
                target.product_qualification.authority(),
                ProductQualificationAuthority::Revoked
            );
            assert_eq!(
                outputs.next_requalification_candidate_index, cursor_before,
                "receipt expiry cannot rewind the publication cursor"
            );
            assert_eq!(
                outputs.next_requalification_probe_id, next_probe_id_before,
                "receipt expiry cannot reuse or consume a probe ID"
            );
            drop(outputs);

            let generation_after_expiry = binding.response_model_generation.load(Ordering::Acquire);
            assert!(!binding.acknowledge_response_requalification_probe_at(
                return_carrier,
                return_path_instance_id,
                probe,
                deadline + lateness,
            ));
            assert_eq!(
                binding.response_model_generation.load(Ordering::Acquire),
                generation_after_expiry,
                "an already-retired receipt is an idempotent stale no-op"
            );
            assert!(
                !updates.has_changed().expect("no replay wake"),
                "an already-retired receipt cannot publish another wake"
            );
        }
    }

    #[test]
    fn response_requalification_ack_rejects_same_session_unattached_carrier() {
        let candidate = key(UnderlayProtocol::Tcp, 0);
        let unattached = key(UnderlayProtocol::Udp, 7);
        let (commands, _receivers) = reliable_path_command_channels(8);
        let binding = ResponseStreamBinding::new(
            SessionId(703),
            candidate.underlay,
            candidate.path_id,
            commands,
            TrafficClass::Throughput,
        );
        let probe = StreamRequalificationProbe {
            id: 9,
            offset: 8192,
            payload_bytes: 512,
        };
        let now = Instant::now();
        {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            let candidate = outputs.entries.first_mut().expect("candidate output");
            candidate.qualification = StreamPathQualification::Requalifying {
                probe,
                retry_at: now + Duration::from_secs(1),
            };
            candidate.product_qualification.revoke();
        }

        assert!(
            !binding.acknowledge_response_requalification_probe_at(
                unattached,
                next_server_carrier_path_instance_id(),
                probe,
                now,
            ),
            "same-session authentication does not grant stream-attachment authority"
        );
        assert!(matches!(
            binding
                .outputs
                .lock()
                .expect("response outputs")
                .entries[0]
                .qualification,
            StreamPathQualification::Requalifying {
                probe: pending,
                ..
            } if pending == probe
        ));
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
        let qualification_floor =
            crate::model::capacity::reliable_path_startup_sample_limit_bytes(MuxLimits::default());
        {
            let mut outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            entry.product_qualification = Default::default();
            let seeded_range = OffsetRange {
                start: 1 << 32,
                end: (1 << 32) + qualification_floor,
            };
            let receipt = entry
                .product_qualification
                .tag_admitted_original(
                    qualification_floor,
                    crate::model::capacity::reliable_relay_buffer_len(MuxLimits::default()) as u64,
                    seeded_range,
                )
                .expect("valid qualification seed")
                .expect("seed receipt");
            assert_eq!(
                entry
                    .product_qualification
                    .release_exact(receipt, seeded_range),
                qualification_floor
            );
            assert!(entry.product_qualification.qualified());
            entry.original_data_acked_bytes = 1 << 20;
            entry.delivery_samples = 7;
        }
        assert!(binding.mark_output_stale(candidate_identity, TrafficClass::Throughput,));
        {
            let outputs = binding.outputs.lock().expect("response outputs");
            let entry = outputs
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output");
            assert_eq!(entry.original_data_acked_bytes, 0);
            assert_eq!(entry.delivery_samples, 0);
            assert_eq!(entry.product_qualification.deficit_bytes(), None);
            assert!(!entry.product_qualification.qualified());
        }

        assert!(matches!(
            binding.try_enqueue_response_requalification_probe(
                &send_stream,
                TrafficClass::Throughput,
                4096,
            ),
            Ok(attempt) if attempt.published_payload_bytes() == Some(4096)
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
            StreamRequalificationProbe {
                id: probe.id + 1,
                ..probe
            },
        ));
        assert!(binding.acknowledge_response_requalification_probe(
            healthy,
            healthy_path_instance_id,
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
        assert_eq!(
            binding
                .outputs
                .lock()
                .expect("response outputs")
                .entries
                .iter()
                .find(|entry| entry.key == candidate)
                .expect("candidate output")
                .product_qualification
                .deficit_bytes(),
            None,
            "the exact probe ACK cannot start a Product generation",
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
        assert_eq!(entry.product_qualification.invariant().verified_bytes, 4096);
        assert!(
            !entry.product_qualification.qualified(),
            "one post-probe quantum below F cannot restore Product assignment"
        );
        drop(outputs);

        let remaining = qualification_floor - 4096;
        let finish = send_stream
            .send_data(Bytes::from(vec![
                0x44;
                usize::try_from(remaining)
                    .expect("test qualification remainder")
            ]))
            .expect("finish response qualification volume");
        binding.record_original_flight(candidate, &finish);
        let finish_start = 12_288;
        binding.release_normalized_acked_ranges(&[OffsetRange {
            start: finish_start,
            end: finish_start + remaining,
        }]);
        let outputs = binding.outputs.lock().expect("response outputs");
        let entry = outputs
            .entries
            .iter()
            .find(|entry| entry.key == candidate)
            .expect("candidate output");
        assert!(entry.product_qualification.qualified());
        assert_eq!(
            entry.product_qualification.invariant().verified_bytes,
            qualification_floor
        );
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
            entry.product_qualification.revoke();
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
            Ok(attempt) if attempt.published_payload_bytes() == Some(4096)
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
            Ok(attempt) if attempt.published_payload_bytes() == Some(4096)
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
            Ok(attempt) if attempt.is_capacity_blocked()
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

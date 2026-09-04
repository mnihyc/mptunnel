//! Exact product-range flight and STREAM_ACK ordering ownership.
//! This layer linearizes product ACK release and flight identity; carrier ACK
//! and packet recovery remain below it, while sender ranking remains above it.

use super::ack_clock::apply_response_ack_clock_release_samples;
use super::attachment::ResponseDispatchTarget;
use super::snapshot::{server_native_bulk_output_snapshot_at, server_output_payload_schedulable};
use super::{ResponseStreamBinding, ResponseStreamOutputs};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_diagnostic;
use crate::model::capacity::{reliable_path_startup_sample_limit_bytes, reliable_relay_buffer_len};
use crate::model::path::CarrierPathKey;
use crate::model::product_qualification::ProductQualificationReceipt;
use crate::model::requalification::StreamPathQualification;
use crate::model::response::CarrierPathFlightDebt;
use crate::model::timing::reliable_data_retransmission_interval;
use crate::model::work::{
    CarrierWorkKind, RangeRecoveryState, ReliableFlightSpan, ReliableLiveOwnerFrontier,
    ReliableReinjectionTargetWork, ambiguous_flight_intervals, flight_interval_bytes,
    reliable_live_owner_uniform_frontier, reliable_reinjection_service_limit_bytes,
    split_flight_interval_by_ack,
};
use crate::protocol::frame::{
    normalize_offset_ranges, offset_ranges_not_covered, reliable_stream_frame_accounted_bytes,
    reliable_stream_frame_extent,
};
use crate::protocol::{ConfiguredMemberSlot, Frame, OffsetRange, UnderlayProtocol};
use crate::runtime::RuntimeError;
use crate::runtime::path::authority::NativeCarrierSchedulingShapeSnapshot;
use crate::runtime::path::commands::ReliablePathCommandSender;
use crate::runtime::sender::ServerReinjectionOutputIdentity;
use crate::scheduler::{PathSnapshot, TrafficClass};
use smallvec::SmallVec;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product reinjection state.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlight {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    /// Authenticated configured member identity. Physical replacement changes
    /// exact Apply identity but cannot mint another Product copy in this slot
    /// while the predecessor remains in current scheduling membership.
    pub(super) configured_slot: Option<ConfiguredMemberSlot>,
    pub(super) end: u64,
    pub(super) bytes: usize,
    pub(super) sent_at: Instant,
    pub(super) kind: CarrierWorkKind,
    pub(super) evidence_eligible: bool,
    /// Exact, generation-fenced authority for Product qualification bytes.
    /// Reinjection copies and untagged OriginalData carry no authority.
    pub(super) qualification_receipt: Option<ProductQualificationReceipt>,
    /// Frozen from the selected carrier's exact snapshot at command commit.
    pub(super) reinjection_suppression_deadline: Option<Instant>,
}

/// Lowest original Data Sequence flight currently blocking cumulative delivery.
///
/// Recovery keeps the complete flight identity so ACK progress, attachment
/// replacement, and metric refresh cannot accidentally share one timer epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct ResponseDataAckRecoveryCandidate {
    pub(in crate::runtime) start: u64,
    pub(in crate::runtime) end: u64,
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) output_incarnation: u64,
    pub(in crate::runtime) sent_at: Instant,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ResponseDataAckRelease {
    /// Outputs with exact, unambiguous OriginalData progress.
    pub(in crate::runtime) path_progress_outputs: SmallVec<[ServerReinjectionOutputIdentity; 4]>,
}

impl CarrierPathFlight {
    pub(in crate::runtime::stream) fn fixed_output(
        key: CarrierPathKey,
        end: u64,
        bytes: usize,
        sent_at: Instant,
        kind: CarrierWorkKind,
        reinjection_suppression_interval: Option<Duration>,
    ) -> Self {
        Self {
            key,
            output_incarnation: 0,
            configured_slot: None,
            end,
            bytes,
            sent_at,
            kind,
            evidence_eligible: true,
            qualification_receipt: None,
            reinjection_suppression_deadline: reinjection_suppression_interval
                .and_then(|interval| sent_at.checked_add(interval))
                .or(reinjection_suppression_interval.map(|_| sent_at)),
        }
    }

    pub(in crate::runtime::stream) fn reinjected_data_bytes(self) -> Option<usize> {
        (self.kind == CarrierWorkKind::ReinjectedData).then_some(self.bytes)
    }
}

#[derive(Debug, Clone)]
pub(in crate::runtime) struct CarrierPathReleasedFlight {
    pub(super) flight: CarrierPathFlight,
    pub(super) path_proving: bool,
    /// Exact overlap that cannot advance qualification. The surrounding
    /// released bytes may still move current-generation tag weight into V.
    pub(super) qualification_ambiguous_ranges: SmallVec<[OffsetRange; 2]>,
}

impl CarrierPathReleasedFlight {
    pub(in crate::runtime::stream) fn fixed_output_sample(
        self,
    ) -> (usize, Instant, CarrierWorkKind, bool) {
        (
            self.flight.bytes,
            self.flight.sent_at,
            self.flight.kind,
            self.path_proving,
        )
    }
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathAckedHole {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
    pub(super) end: u64,
    pub(super) bytes: u64,
    pub(super) sent_at: Instant,
    pub(super) kind: CarrierWorkKind,
    pub(super) path_proving: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ResponseAckOrderingState {
    pub(super) contiguous_frontier: u64,
    pub(super) acked_holes: BTreeMap<u64, Vec<CarrierPathAckedHole>>,
}

pub(in crate::runtime) struct ResponseAckOrderingUpdate {
    pub(super) changed: bool,
    pub(super) contiguous_frontier: u64,
    /// Hole volume is observation-only; ordering still computes it locally to
    /// detect semantic state changes in every build.
    #[cfg(feature = "lab-diagnostics")]
    pub(super) acked_hole_bytes: u64,
    pub(super) newly_contiguous: Vec<CarrierPathAckedHole>,
}

impl ResponseAckOrderingState {
    pub(super) fn apply_normalized_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathReleasedFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, release) in released {
            let flight = release.flight;
            let hole = CarrierPathAckedHole {
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                end: flight.end,
                bytes: flight.bytes as u64,
                sent_at: flight.sent_at,
                kind: flight.kind,
                path_proving: release.path_proving,
            };
            if hole.end <= self.contiguous_frontier {
                newly_contiguous.push(hole);
            } else {
                self.acked_holes.entry(*offset).or_default().push(hole);
            }
        }

        self.advance_contiguous_frontier(ranges);
        let frontier = self.contiguous_frontier;
        self.acked_holes.retain(|_, holes| {
            holes.retain(|hole| {
                if hole.end <= frontier {
                    newly_contiguous.push(*hole);
                    false
                } else {
                    true
                }
            });
            !holes.is_empty()
        });
        let acked_hole_bytes = self.acked_hole_bytes();

        ResponseAckOrderingUpdate {
            changed: previous_frontier != self.contiguous_frontier
                || previous_hole_bytes != acked_hole_bytes
                || !newly_contiguous.is_empty(),
            contiguous_frontier: self.contiguous_frontier,
            #[cfg(feature = "lab-diagnostics")]
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in ranges {
                if range.start > next_frontier {
                    break;
                }
                if range.end > next_frontier {
                    next_frontier = range.end;
                }
            }
            for (offset, holes) in self.acked_holes.range(..=next_frontier) {
                if *offset > next_frontier {
                    break;
                }
                for hole in holes {
                    if hole.end > next_frontier {
                        next_frontier = hole.end;
                    }
                }
            }
            if next_frontier == self.contiguous_frontier {
                break;
            }
            self.contiguous_frontier = next_frontier;
        }
    }

    pub(super) fn acked_hole_bytes(&self) -> u64 {
        self.acked_holes
            .values()
            .filter_map(|holes| response_latest_original_hole(holes))
            .map(|hole| hole.bytes)
            .sum()
    }
}

pub(in crate::runtime) fn response_latest_original_hole(
    holes: &[CarrierPathAckedHole],
) -> Option<&CarrierPathAckedHole> {
    holes
        .iter()
        .rev()
        .find(|hole| hole.kind.is_original_transmission())
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn tail_reinjection_snapshot(
        &self,
        ack_frontier: u64,
        lane: TrafficClass,
    ) -> Option<PathSnapshot> {
        let owner = self.blocking_original_path_at_or_after(ack_frontier);
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        owner.and_then(|(key, incarnation)| {
            outputs.snapshot_for_instance(key, incarnation, lane, self.mux_limits)
        })
    }

    pub(in crate::runtime) fn tail_reinjection_original_underlay(
        &self,
        ack_frontier: u64,
    ) -> Option<UnderlayProtocol> {
        self.data_ack_recovery_candidate(ack_frontier)
            .map(|candidate| candidate.key.underlay)
    }

    pub(in crate::runtime) fn data_ack_recovery_candidate(
        &self,
        ack_frontier: u64,
    ) -> Option<ResponseDataAckRecoveryCandidate> {
        self.blocking_original_flight_at_or_after(ack_frontier)
            .map(|(start, flight)| ResponseDataAckRecoveryCandidate {
                start,
                end: flight.end,
                key: flight.key,
                output_incarnation: flight.output_incarnation,
                sent_at: flight.sent_at,
            })
    }

    /// Returns the earliest current-epoch OriginalData flight below a complete
    /// authoritative Data ACK horizon for every exact output that can still
    /// prove progress. Output lifetime and flight evidence are matched by
    /// incarnation. An output already withdrawn from OriginalData placement is
    /// recovery work, not another staleness observation.
    pub(in crate::runtime) fn data_ack_recovery_candidates(
        &self,
        authoritative_horizon: u64,
        lane: TrafficClass,
    ) -> SmallVec<[ResponseDataAckRecoveryCandidate; 4]> {
        if authoritative_horizon == 0 {
            return SmallVec::new();
        }
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut live_outputs = outputs
            .entries
            .iter()
            .filter(|entry| {
                server_output_payload_schedulable(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                ) && !entry.qualification.stale_for_original_data()
            })
            .map(|entry| ((entry.key, entry.incarnation), false))
            .collect::<SmallVec<[_; 4]>>();
        if live_outputs.is_empty() {
            return SmallVec::new();
        }

        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut candidates = SmallVec::<[ResponseDataAckRecoveryCandidate; 4]>::new();
        'flights: for (start, path_flights) in flights.range(..authoritative_horizon) {
            for flight in path_flights {
                if flight.end <= *start
                    || !flight.kind.is_original_transmission()
                    || !flight.evidence_eligible
                {
                    continue;
                }
                let identity = (flight.key, flight.output_incarnation);
                let Some((_, observed)) = live_outputs
                    .iter_mut()
                    .find(|(candidate, _)| *candidate == identity)
                else {
                    continue;
                };
                if *observed {
                    continue;
                }
                *observed = true;
                candidates.push(ResponseDataAckRecoveryCandidate {
                    start: *start,
                    end: flight.end,
                    key: flight.key,
                    output_incarnation: flight.output_incarnation,
                    sent_at: flight.sent_at,
                });
                if candidates.len() == live_outputs.len() {
                    break 'flights;
                }
            }
        }
        candidates
    }

    pub(in crate::runtime) fn has_multipath_reinjection_alternative(&self) -> bool {
        let lane = self.lane();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs
            .entries
            .iter()
            .filter(|entry| {
                server_output_payload_schedulable(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                ) && !entry.qualification.stale_for_original_data()
            })
            .take(2)
            .count()
            > 1
    }

    pub(in crate::runtime) fn has_nonstale_reinjection_alternative(
        &self,
        candidate: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.entries.iter().any(|entry| {
            server_output_payload_schedulable(
                entry,
                outputs.data_level_queue_bytes,
                lane,
                self.mux_limits,
            ) && !entry.qualification.stale_for_original_data()
                && (entry.key != candidate.key || entry.incarnation != candidate.incarnation)
        })
    }

    pub(in crate::runtime) fn mark_output_stale(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> bool {
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let data_level_queue_bytes = outputs.data_level_queue_bytes;
        let has_nonstale_alternative = outputs.entries.iter().any(|entry| {
            server_output_payload_schedulable(entry, data_level_queue_bytes, lane, self.mux_limits)
                && !entry.qualification.stale_for_original_data()
                && (entry.key != identity.key || entry.incarnation != identity.incarnation)
        });
        if !has_nonstale_alternative {
            return false;
        }
        let changed = outputs.entries.iter_mut().any(|entry| {
            if entry.key == identity.key
                && entry.incarnation == identity.incarnation
                && server_output_payload_schedulable(
                    entry,
                    data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                )
                && !entry.qualification.stale_for_original_data()
            {
                entry.qualification = StreamPathQualification::Stale {
                    retry_at: Instant::now(),
                };
                entry.product_rate_epoch = None;
                entry.tcp_product_rate_evidence = None;
                entry.delivery_samples = 0;
                entry.original_data_acked_bytes = 0;
                entry.product_qualification.revoke();
                true
            } else {
                false
            }
        });
        if changed {
            drop(outputs);
            self.invalidate_path_flight_evidence(identity.key, identity.incarnation);
            self.response_model_generation
                .fetch_add(1, Ordering::AcqRel);
        } else {
            drop(outputs);
        }
        if changed {
            self.notify_update();
        }
        changed
    }

    #[cfg(test)]
    pub(in crate::runtime) fn output_is_stale(
        &self,
        identity: ServerReinjectionOutputIdentity,
    ) -> bool {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| {
                entry.key == identity.key
                    && entry.incarnation == identity.incarnation
                    && !entry.commands.is_closed()
                    && entry.qualification.stale_for_original_data()
            })
    }

    pub(in crate::runtime) fn stale_original_outputs(
        &self,
        lane: TrafficClass,
    ) -> Vec<ServerReinjectionOutputIdentity> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let stale_outputs = outputs
            .entries
            .iter()
            .chain(outputs.detaching.iter())
            .filter(|entry| entry.qualification.stale_for_original_data())
            .filter(|stale| {
                outputs.entries.iter().any(|entry| {
                    server_output_payload_schedulable(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                    ) && !entry.qualification.stale_for_original_data()
                        && (entry.key != stale.key || entry.incarnation != stale.incarnation)
                })
            })
            .map(|entry| ServerReinjectionOutputIdentity {
                key: entry.key,
                incarnation: entry.incarnation,
            })
            .collect::<Vec<_>>();
        drop(outputs);
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        stale_outputs
            .into_iter()
            .filter(|identity| {
                flights.values().any(|path_flights| {
                    path_flights.iter().any(|flight| {
                        flight.kind.is_original_transmission()
                            && flight.key == identity.key
                            && flight.output_incarnation == identity.incarnation
                    })
                })
            })
            .collect()
    }

    /// Observes one stale owner's due ranges and next exact-copy expiry in one
    /// ledger pass. The actor must consume both fields from this observation.
    pub(in crate::runtime) fn stale_original_recovery_state(
        &self,
        identity: ServerReinjectionOutputIdentity,
        lane: TrafficClass,
    ) -> RangeRecoveryState {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let owner_is_stale = outputs.entries.iter().any(|entry| {
            entry.key == identity.key
                && entry.incarnation == identity.incarnation
                && entry.qualification.stale_for_original_data()
        });
        if !owner_is_stale {
            return RangeRecoveryState::default();
        }
        let available_outputs = outputs
            .entries
            .iter()
            .filter(|entry| {
                server_output_payload_schedulable(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                ) && !entry.qualification.stale_for_original_data()
            })
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<Vec<_>>();
        if available_outputs.is_empty() {
            return RangeRecoveryState::default();
        }
        let current_copy_outputs = outputs
            .entries
            .iter()
            .filter(|entry| entry.key != identity.key || entry.incarnation != identity.incarnation)
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<Vec<_>>();
        drop(outputs);
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let now = Instant::now();
        let mut stale_original_ranges = Vec::new();
        let mut current_reinjections = Vec::new();
        for (start, path_flights) in flights.iter() {
            if let Some(flight) = path_flights.iter().find(|flight| {
                flight.kind.is_original_transmission()
                    && flight.key == identity.key
                    && flight.output_incarnation == identity.incarnation
            }) {
                stale_original_ranges.push(OffsetRange {
                    start: *start,
                    end: flight.end,
                });
            }
            for flight in path_flights {
                if flight.kind != CarrierWorkKind::ReinjectedData
                    || !current_copy_outputs.contains(&(flight.key, flight.output_incarnation))
                {
                    continue;
                }
                let Some(deadline) = flight.reinjection_suppression_deadline else {
                    continue;
                };
                if deadline > now {
                    current_reinjections.push((
                        OffsetRange {
                            start: *start,
                            end: flight.end,
                        },
                        deadline,
                    ));
                }
            }
        }
        let stale_original_ranges = normalize_offset_ranges(stale_original_ranges);
        if stale_original_ranges.is_empty() {
            return RangeRecoveryState::default();
        }

        let mut retry_deadline = None;
        let mut live_reinjected_ranges = Vec::new();
        let mut original_index = 0usize;
        for (range, deadline) in current_reinjections {
            while original_index < stale_original_ranges.len()
                && stale_original_ranges[original_index].end <= range.start
            {
                original_index += 1;
            }
            if stale_original_ranges
                .get(original_index)
                .is_some_and(|original| original.start < range.end)
            {
                retry_deadline =
                    Some(retry_deadline.map_or(deadline, |current: Instant| current.min(deadline)));
                live_reinjected_ranges.push(range);
            }
        }
        let live_reinjected_ranges = normalize_offset_ranges(live_reinjected_ranges);
        RangeRecoveryState {
            uncovered_ranges: offset_ranges_not_covered(
                &stale_original_ranges,
                &live_reinjected_ranges,
            ),
            retry_deadline,
        }
    }

    pub(in crate::runtime) fn has_reinjection_path_for_frame(&self, frame: &Frame) -> bool {
        let avoid_outputs = self.flight_outputs_overlapping_frame(frame);
        let lane = self.lane();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.entries.iter().any(|entry| {
            server_output_payload_schedulable(
                entry,
                outputs.data_level_queue_bytes,
                lane,
                self.mux_limits,
            ) && !entry.qualification.stale_for_original_data()
                && !avoid_outputs.contains(&(entry.key, entry.incarnation))
        })
    }

    pub(in crate::runtime) fn has_tail_reinjection_output_for_frame(&self, frame: &Frame) -> bool {
        let owner_outputs = self.original_flight_outputs_overlapping_frame(frame);
        if owner_outputs.is_empty() {
            return false;
        }
        let avoid_outputs = self.flight_outputs_overlapping_frame(frame);
        let lane = self.lane();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.entries.iter().any(|entry| {
            server_output_payload_schedulable(
                entry,
                outputs.data_level_queue_bytes,
                lane,
                self.mux_limits,
            ) && !entry.qualification.stale_for_original_data()
                && !avoid_outputs.contains(&(entry.key, entry.incarnation))
        })
    }

    #[cfg(test)]
    pub(in crate::runtime) fn has_recent_reinjection_overlap(&self, frame: &Frame) -> bool {
        self.reinjection_suppression_deadline(frame).is_some()
    }

    /// Earliest frozen accepted-copy expiry overlapping one exact range.
    pub(in crate::runtime) fn reinjection_suppression_deadline(
        &self,
        frame: &Frame,
    ) -> Option<Instant> {
        let (start, end, _) = reliable_stream_frame_extent(frame)?;
        let now = Instant::now();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let current_outputs = outputs
            .entries
            .iter()
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        drop(outputs);
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        product_flights_have_recent_reinjection_overlap(
            &flights,
            start,
            end,
            now,
            |key, incarnation| current_outputs.contains(&(key, incarnation)),
        )
    }

    /// Earliest immutable expiry of any ReinjectedData copy whose exact
    /// attachment remains in current Product scheduling membership.
    pub(in crate::runtime) fn earliest_reinjection_suppression_deadline(&self) -> Option<Instant> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let current_outputs = outputs
            .entries
            .iter()
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        drop(outputs);
        let now = Instant::now();
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| {
                flight.kind == CarrierWorkKind::ReinjectedData
                    && current_outputs.contains(&(flight.key, flight.output_incarnation))
            })
            .filter_map(|flight| flight.reinjection_suppression_deadline)
            .filter(|deadline| *deadline > now)
            .min()
    }

    /// Exact accepted ReinjectedData bytes whose frozen native-recovery
    /// ownership remains live on one output incarnation.
    #[cfg(test)]
    pub(in crate::runtime) fn live_reinjected_data_in_flight_bytes_at(
        &self,
        identity: ServerReinjectionOutputIdentity,
        now: Instant,
    ) -> usize {
        if !self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .any(|entry| entry.key == identity.key && entry.incarnation == identity.incarnation)
        {
            return 0;
        }
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .values()
            .flat_map(|flights| flights.iter())
            .filter(|flight| {
                flight.kind == CarrierWorkKind::ReinjectedData
                    && flight.key == identity.key
                    && flight.output_incarnation == identity.incarnation
                    && flight
                        .reinjection_suppression_deadline
                        .is_some_and(|deadline| deadline > now)
            })
            .fold(0usize, |bytes, flight| bytes.saturating_add(flight.bytes))
    }

    /// Interval-union ReinjectedData debt for the selected output's stable
    /// configured slot.
    ///
    /// A native recovery/suppression deadline cannot renew this debt. Product
    /// DataACK clips the retained intervals; removal from current Product
    /// scheduling membership transfers publication authority to a successor.
    pub(in crate::runtime) fn accepted_reinjected_data_in_flight_bytes_at(
        &self,
        identity: ServerReinjectionOutputIdentity,
    ) -> usize {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let Some((underlay, configured_slot)) = outputs
            .entries
            .iter()
            .find(|entry| entry.key == identity.key && entry.incarnation == identity.incarnation)
            .map(|entry| (entry.key.underlay, entry.configured_slot))
        else {
            return 0;
        };
        let current_slot_outputs = outputs
            .entries
            .iter()
            .filter(|entry| {
                entry.key.underlay == underlay && entry.configured_slot == configured_slot
            })
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        self.retained_reinjected_data_bytes_for_slot(
            underlay,
            configured_slot,
            &current_slot_outputs,
        )
    }

    /// Raw retained range union after one exact target has already been
    /// validated under the output lock. Physical attempts remain separately
    /// retained for exact ACK ambiguity and cumulative wire accounting.
    fn retained_reinjected_data_bytes_for_slot(
        &self,
        underlay: UnderlayProtocol,
        configured_slot: ConfiguredMemberSlot,
        current_slot_outputs: &[(CarrierPathKey, u64)],
    ) -> usize {
        let ranges = self
            .flights
            .lock()
            .expect("server reliable stream flight lock")
            .iter()
            .flat_map(|(start, flights)| {
                flights.iter().filter_map(|flight| {
                    (flight.kind == CarrierWorkKind::ReinjectedData
                        && flight.key.underlay == underlay
                        && flight.configured_slot == Some(configured_slot)
                        && current_slot_outputs.contains(&(flight.key, flight.output_incarnation)))
                    .then_some(OffsetRange {
                        start: *start,
                        end: flight.end,
                    })
                })
            })
            .collect::<Vec<_>>();
        normalize_offset_ranges(ranges)
            .into_iter()
            .fold(0usize, |bytes, range| {
                bytes.saturating_add(flight_interval_bytes(range.start, range.end))
            })
    }

    /// Returns every unacknowledged OriginalData range whose exact attachment
    /// has left Product scheduling membership, excluding bytes already
    /// reinjected by another current publication owner.
    #[cfg(test)]
    pub(in crate::runtime) fn uncovered_failed_original_ranges(&self) -> Vec<OffsetRange> {
        self.failed_original_recovery_state().uncovered_ranges
    }

    /// Failed-owner due ranges and the earliest exact accepted-copy expiry are
    /// observed under one flight snapshot so the actor can wake precisely.
    pub(in crate::runtime) fn failed_original_recovery_state(&self) -> RangeRecoveryState {
        let lane = self.lane();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let available_outputs = outputs
            .entries
            .iter()
            .filter(|entry| {
                server_output_payload_schedulable(
                    entry,
                    outputs.data_level_queue_bytes,
                    lane,
                    self.mux_limits,
                ) && !entry.qualification.stale_for_original_data()
            })
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<Vec<_>>();
        if available_outputs.is_empty() {
            return RangeRecoveryState::default();
        }
        let current_outputs = outputs
            .entries
            .iter()
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let failed_original_ranges = normalize_offset_ranges(
            flights
                .iter()
                .filter_map(|(start, path_flights)| {
                    let original = path_flights
                        .iter()
                        .rev()
                        .find(|flight| flight.kind.is_original_transmission())?;
                    (!current_outputs.contains(&(original.key, original.output_incarnation)))
                        .then_some(OffsetRange {
                            start: *start,
                            end: original.end,
                        })
                })
                .collect(),
        );
        if failed_original_ranges.is_empty() {
            return RangeRecoveryState::default();
        }
        let now = Instant::now();
        let mut retry_deadline = None;
        let mut live_reinjected_ranges = Vec::new();
        for (start, path_flights) in flights.iter() {
            for flight in path_flights {
                let Some(deadline) = flight.reinjection_suppression_deadline else {
                    continue;
                };
                if flight.kind != CarrierWorkKind::ReinjectedData
                    || deadline <= now
                    || !current_outputs.contains(&(flight.key, flight.output_incarnation))
                    || !failed_original_ranges
                        .iter()
                        .any(|original| original.start < flight.end && *start < original.end)
                {
                    continue;
                }
                retry_deadline =
                    Some(retry_deadline.map_or(deadline, |current: Instant| current.min(deadline)));
                live_reinjected_ranges.push(OffsetRange {
                    start: *start,
                    end: flight.end,
                });
            }
        }
        let live_reinjected_ranges = normalize_offset_ranges(live_reinjected_ranges);
        RangeRecoveryState {
            uncovered_ranges: offset_ranges_not_covered(
                &failed_original_ranges,
                &live_reinjected_ranges,
            ),
            retry_deadline,
        }
    }

    pub(in crate::runtime) fn has_untracked_data_reinjection_path_for_frame(
        &self,
        frame: &Frame,
    ) -> bool {
        if !self.all_flight_outputs_overlapping_frame(frame).is_empty() {
            return false;
        }
        let lane = self.lane();
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        outputs.entries.iter().any(|entry| {
            server_output_payload_schedulable(
                entry,
                outputs.data_level_queue_bytes,
                lane,
                self.mux_limits,
            ) && !entry.qualification.stale_for_original_data()
        })
    }

    pub(in crate::runtime) fn release_normalized_acked_ranges(
        &self,
        ranges: &[OffsetRange],
    ) -> ResponseDataAckRelease {
        self.release_normalized_acked_ranges_at_inner(ranges, Instant::now())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn release_normalized_acked_ranges_at(
        &self,
        ranges: &[OffsetRange],
        now: Instant,
    ) -> ResponseDataAckRelease {
        self.release_normalized_acked_ranges_at_inner(ranges, now)
    }

    fn release_normalized_acked_ranges_at_inner(
        &self,
        ranges: &[OffsetRange],
        now: Instant,
    ) -> ResponseDataAckRelease {
        if ranges.is_empty() {
            return ResponseDataAckRelease::default();
        }
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let released = release_carrier_path_flight_ranges(&mut flights, ranges);
        if released.is_empty() {
            drop(flights);
            let ordering_update = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock")
                .apply_normalized_ack(ranges, &[]);
            if ordering_update.changed {
                // Publish the generation only after the coherent ordering view
                // exists. Duplicate ACKs need no fence or shared atomic write.
                self.response_model_generation
                    .fetch_add(1, Ordering::AcqRel);
            }
            drop(outputs);
            if ordering_update.changed {
                self.notify_update();
            }
            return ResponseDataAckRelease::default();
        }
        drop(flights);

        let ordering_update = {
            let mut ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            ordering.apply_normalized_ack(ranges, &released)
        };
        #[cfg(feature = "lab-diagnostics")]
        if ordering_update.acked_hole_bytes > 0 {
            lab_diagnostic(
                "server_ack_ordering_state",
                format_args!(
                    "session_id={} contiguous_frontier={} acked_hole_bytes={} released_flights={}",
                    self.session_id.0,
                    ordering_update.contiguous_frontier,
                    ordering_update.acked_hole_bytes,
                    released.len(),
                ),
            );
        }

        let mut changed = false;
        let mut data_ack_progress_outputs = SmallVec::<[ServerReinjectionOutputIdentity; 4]>::new();
        let mut path_samples =
            HashMap::<(CarrierPathKey, u64), (u64, u64, Instant, Instant)>::new();
        for (released_start, release) in released {
            let flight = release.flight;
            let released_range = OffsetRange {
                start: released_start,
                end: flight.end,
            };
            let identity = (flight.key, flight.output_incarnation);
            if flight.kind.is_original_transmission() {
                // Shared Product debt survives output detach/replacement and is
                // released only by the exact MPP DataACK transaction.
                outputs.original_data_in_flight_bytes = outputs
                    .original_data_in_flight_bytes
                    .checked_sub(flight.bytes as u64)
                    .expect("response shared OriginalData debt covers released flight");
            }
            let ResponseStreamOutputs {
                entries, detaching, ..
            } = &mut *outputs;
            if let Some(entry) = entries
                .iter_mut()
                .chain(detaching.iter_mut())
                .find(|entry| {
                    entry.key == flight.key && entry.incarnation == flight.output_incarnation
                })
            {
                entry.bytes_in_flight = entry.bytes_in_flight.saturating_sub(flight.bytes as u64);
                if flight.kind.is_original_transmission() {
                    let prior_original_flight = entry.original_data_in_flight_bytes;
                    entry.original_data_in_flight_bytes = entry
                        .original_data_in_flight_bytes
                        .saturating_sub(flight.bytes as u64);
                    if prior_original_flight > 0 && entry.original_data_in_flight_bytes == 0 {
                        entry.load_registration.deactivate();
                    }
                }
                let unambiguous_qualification_ranges = offset_ranges_not_covered(
                    &[released_range],
                    &release.qualification_ambiguous_ranges,
                );
                let has_unambiguous_original = flight.kind.is_original_transmission()
                    && !unambiguous_qualification_ranges.is_empty();
                let current_generation_original = has_unambiguous_original
                    && flight.evidence_eligible
                    && entry
                        .qualification
                        .observe_unique_original_progress(flight.sent_at);
                if let Some(receipt) = flight.qualification_receipt {
                    for ambiguous in &release.qualification_ambiguous_ranges {
                        entry
                            .product_qualification
                            .release_ambiguous(receipt, *ambiguous);
                    }
                    if current_generation_original {
                        for exact in unambiguous_qualification_ranges {
                            entry.product_qualification.release_exact(receipt, exact);
                        }
                    }
                }
                let product_evidence_eligible = release.path_proving && current_generation_original;
                if product_evidence_eligible {
                    let progress_identity = ServerReinjectionOutputIdentity {
                        key: flight.key,
                        incarnation: flight.output_incarnation,
                    };
                    if !data_ack_progress_outputs.contains(&progress_identity) {
                        data_ack_progress_outputs.push(progress_identity);
                    }
                    entry.original_data_acked_bytes = entry
                        .original_data_acked_bytes
                        .saturating_add(flight.bytes as u64);
                    let sample = path_samples.entry(identity).or_insert((
                        0_u64,
                        0_u64,
                        flight.sent_at,
                        flight.sent_at,
                    ));
                    sample.0 = sample.0.saturating_add(flight.bytes as u64);
                    sample.1 = sample.1.saturating_add(flight.bytes as u64);
                    sample.2 = sample.2.min(flight.sent_at);
                    sample.3 = sample.3.max(flight.sent_at);
                }
                changed = true;
            }
        }
        apply_response_ack_clock_release_samples(&mut outputs, path_samples, now);
        let mut delivery_sample_outputs = SmallVec::<[ServerReinjectionOutputIdentity; 4]>::new();
        for hole in ordering_update.newly_contiguous {
            if !hole.path_proving {
                continue;
            }
            let identity = ServerReinjectionOutputIdentity {
                key: hole.key,
                incarnation: hole.output_incarnation,
            };
            if let Some(entry) = outputs
                .entries
                .iter_mut()
                .find(|entry| entry.key == hole.key && entry.incarnation == hole.output_incarnation)
                && hole.end <= ordering_update.contiguous_frontier
                && entry
                    .qualification
                    .observe_unique_original_progress(hole.sent_at)
                && !delivery_sample_outputs.contains(&identity)
            {
                delivery_sample_outputs.push(identity);
                entry.delivery_samples = entry.delivery_samples.saturating_add(1);
                changed = true;
            }
        }
        // Scheduling captures this before reading lower flights and path
        // snapshots. Publish it only after both ledgers describe the ACK.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        drop(outputs);
        if changed || ordering_update.changed {
            self.notify_update();
        }
        ResponseDataAckRelease {
            path_progress_outputs: data_ack_progress_outputs,
        }
    }

    pub(super) fn record_validated_original_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        target_index: usize,
        frame: &Frame,
    ) -> Result<(), RuntimeError> {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let max_quantum_bytes = u64::try_from(reliable_relay_buffer_len(self.mux_limits))
            .map_err(|_| RuntimeError::SenderServiceBlocked)?;
        let (key, output_incarnation, configured_slot, evidence_eligible, qualification_receipt) = {
            let entry = outputs
                .entries
                .get_mut(target_index)
                .expect("validated response output index");
            let evidence_eligible = !entry.qualification.stale_for_original_data();
            let qualification_receipt = if evidence_eligible {
                entry
                    .product_qualification
                    .tag_admitted_original(
                        reliable_path_startup_sample_limit_bytes(self.mux_limits),
                        max_quantum_bytes,
                        OffsetRange { start: offset, end },
                    )
                    .map_err(|_| RuntimeError::SenderServiceBlocked)?
            } else {
                None
            };
            // No recoverable operation follows qualification mutation.
            if entry.original_data_in_flight_bytes == 0 {
                entry.load_registration.activate();
            }
            entry.original_data_in_flight_bytes = entry
                .original_data_in_flight_bytes
                .saturating_add(bytes as u64);
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (
                entry.key,
                entry.incarnation,
                entry.configured_slot,
                evidence_eligible,
                qualification_receipt,
            )
        };
        outputs.original_data_in_flight_bytes = outputs
            .original_data_in_flight_bytes
            .saturating_add(bytes as u64);
        self.flights
            .lock()
            .expect("server reliable stream flight lock")
            .entry(offset)
            .or_default()
            .push(CarrierPathFlight {
                key,
                output_incarnation,
                configured_slot: Some(configured_slot),
                end,
                bytes,
                sent_at: Instant::now(),
                kind: CarrierWorkKind::OriginalData,
                evidence_eligible,
                qualification_receipt,
                reinjection_suppression_deadline: None,
            });
        // Keep path counters and the exact range ledger in one published model
        // generation so a concurrent measurement plan cannot mix the views.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(in crate::runtime) fn try_enqueue_reinjected_frame_for_target(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        queued_reinjection_bytes: usize,
        reinjection_debt_bytes: usize,
        bound_expires_at: Option<Instant>,
    ) -> Result<Instant, RuntimeError> {
        self.try_enqueue_reinjected_frame_for_target_with_after_reserve(
            target,
            frame,
            lane,
            queued_reinjection_bytes,
            reinjection_debt_bytes,
            bound_expires_at,
            || {},
        )
    }

    fn try_enqueue_reinjected_frame_for_target_with_after_reserve(
        &self,
        target: &ResponseDispatchTarget,
        frame: &Frame,
        lane: TrafficClass,
        queued_reinjection_bytes: usize,
        reinjection_debt_bytes: usize,
        bound_expires_at: Option<Instant>,
        after_reserve: impl FnOnce(),
    ) -> Result<Instant, RuntimeError> {
        if !self.response_stream_open.load(Ordering::Acquire) {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let target_matches = |entry: &super::attachment::ResponseStreamOutputEntry| {
            entry.key == target.key
                && entry.path_instance_id == target.path_instance_id
                && entry.incarnation == target.incarnation
        };
        let commands = {
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
        let command = commands.try_reserve_reinjection_frame(frame.clone(), lane)?;
        after_reserve();
        let native_authority = commands.native_rate_authority().cloned();
        let expected_native_stamp = target.native_authority_stamp;
        let commit = |current_native_shape: Option<NativeCarrierSchedulingShapeSnapshot>| {
            let mut outputs = self
                .outputs
                .lock()
                .expect("server reliable stream binding lock");
            if !self.response_stream_open.load(Ordering::Acquire) {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let Some(target_index) = outputs.entries.iter().position(target_matches) else {
                return Err(RuntimeError::SenderServiceBlocked);
            };
            let entry = &outputs.entries[target_index];
            let snapshot = match (expected_native_stamp, current_native_shape) {
                (Some(stamp), Some(shape)) if shape.stamp() == stamp => {
                    server_native_bulk_output_snapshot_at(
                        entry,
                        outputs.data_level_queue_bytes,
                        lane,
                        self.mux_limits,
                        Some(shape),
                    )
                }
                (None, None) => outputs
                    .snapshot_for_instance(target.key, target.incarnation, lane, self.mux_limits)
                    .ok_or(RuntimeError::SenderServiceBlocked)?,
                _ => return Err(RuntimeError::SenderServiceBlocked),
            };
            let configured_slot = outputs.entries[target_index].configured_slot;
            let current_slot_outputs = outputs
                .entries
                .iter()
                .filter(|entry| {
                    entry.key.underlay == target.key.underlay
                        && entry.configured_slot == configured_slot
                })
                .map(|entry| (entry.key, entry.incarnation))
                .collect::<SmallVec<[_; 4]>>();
            let accepted_reinjection_bytes = self.retained_reinjected_data_bytes_for_slot(
                target.key.underlay,
                configured_slot,
                &current_slot_outputs,
            );
            let payload_bytes = reliable_stream_frame_accounted_bytes(frame);
            let exact_service = reliable_reinjection_service_limit_bytes(
                ReliableReinjectionTargetWork::new(
                    Some(snapshot),
                    queued_reinjection_bytes,
                    accepted_reinjection_bytes,
                ),
                payload_bytes.min(reinjection_debt_bytes),
                self.mux_limits,
            );
            if exact_service < payload_bytes {
                // Dropping the uncommitted reservation returns carrier capacity.
                return Err(RuntimeError::SenderServiceBlocked);
            }
            let suppression_interval =
                reliable_data_retransmission_interval(Some(target.key.underlay), Some(snapshot));
            let accepted_at = Instant::now();
            if bound_expires_at.is_some_and(|deadline| accepted_at >= deadline) {
                return Err(RuntimeError::SenderServiceBlocked);
            }
            self.record_product_flight_with_outputs(
                &mut outputs,
                target.key,
                target.incarnation,
                &commands,
                frame,
                CarrierWorkKind::ReinjectedData,
                Some((accepted_at, suppression_interval)),
                true,
            )?;
            command.commit();
            Ok(accepted_at
                .checked_add(suppression_interval)
                .unwrap_or(accepted_at))
        };
        match (native_authority, expected_native_stamp) {
            (Some(authority), Some(stamp)) => authority
                .commit_with_current_scheduling_shape(stamp, |shape| commit(Some(shape)))
                .map_err(|_| RuntimeError::SenderServiceBlocked)?,
            (None, None) if target.key.underlay == UnderlayProtocol::Tcp => commit(None),
            _ => Err(RuntimeError::SenderServiceBlocked),
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_original_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.commands.clone()))
            .expect("test owner output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            &commands,
            frame,
            CarrierWorkKind::OriginalData,
        )
        .expect("test OriginalData flight must satisfy qualification bounds")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn record_reinjected_flight(&self, key: CarrierPathKey, frame: &Frame) {
        let (incarnation, commands) = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .entries
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| (entry.incarnation, entry.commands.clone()))
            .expect("test reinjection output must be attached");
        self.record_product_flight(
            key,
            incarnation,
            &commands,
            frame,
            CarrierWorkKind::ReinjectedData,
        )
        .expect("test reinjection flight must be recordable")
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_reinjected_flights_for_test(&self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights {
                if flight.kind == CarrierWorkKind::ReinjectedData {
                    flight.sent_at = sent_at;
                    flight.reinjection_suppression_deadline = flight
                        .reinjection_suppression_deadline
                        .and_then(|deadline| deadline.checked_sub(age));
                }
            }
        }
    }

    #[cfg(test)]
    pub(in crate::runtime) fn age_original_flights_for_test(&self, age: Duration) {
        let sent_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights {
                if flight.kind == CarrierWorkKind::OriginalData {
                    flight.sent_at = sent_at;
                }
            }
        }
    }

    #[cfg(test)]
    fn record_product_flight(
        &self,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
    ) -> Result<(), RuntimeError> {
        let lane = self.lane();
        let mut outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let reinjection_suppression = (kind == CarrierWorkKind::ReinjectedData).then(|| {
            let accepted_at = Instant::now();
            let snapshot =
                outputs.snapshot_for_instance(key, output_incarnation, lane, self.mux_limits);
            (
                accepted_at,
                reliable_data_retransmission_interval(Some(key.underlay), snapshot),
            )
        });
        self.record_product_flight_with_outputs(
            &mut outputs,
            key,
            output_incarnation,
            planned_commands,
            frame,
            kind,
            reinjection_suppression,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn record_product_flight_with_outputs(
        &self,
        outputs: &mut ResponseStreamOutputs,
        key: CarrierPathKey,
        output_incarnation: u64,
        planned_commands: &ReliablePathCommandSender,
        frame: &Frame,
        kind: CarrierWorkKind,
        reinjection_suppression: Option<(Instant, Duration)>,
        enforce_stable_slot_vacancy: bool,
    ) -> Result<(), RuntimeError> {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let max_quantum_bytes = u64::try_from(reliable_relay_buffer_len(self.mux_limits))
            .map_err(|_| RuntimeError::SenderServiceBlocked)?;
        let range = OffsetRange { start: offset, end };
        let Some(target_index) = outputs
            .entries
            .iter()
            .position(|entry| entry.key == key && entry.commands.same_channel(planned_commands))
        else {
            return Err(RuntimeError::SenderServiceBlocked);
        };
        let configured_slot = outputs.entries[target_index].configured_slot;
        let current_slot_outputs = outputs
            .entries
            .iter()
            .filter(|entry| {
                entry.key.underlay == key.underlay && entry.configured_slot == configured_slot
            })
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        if enforce_stable_slot_vacancy
            && kind == CarrierWorkKind::ReinjectedData
            && product_flights_have_current_slot_overlap(
                &flights,
                offset,
                end,
                key.underlay,
                configured_slot,
                &current_slot_outputs,
            )
        {
            return Err(RuntimeError::SenderServiceBlocked);
        }
        let mut qualification_receipt = None;
        let (recorded_incarnation, evidence_eligible) = {
            let entry = &mut outputs.entries[target_index];
            let incarnation_matches = entry.incarnation == output_incarnation;
            if kind.is_original_transmission() {
                if incarnation_matches && !entry.qualification.stale_for_original_data() {
                    qualification_receipt = entry
                        .product_qualification
                        .tag_admitted_original(
                            reliable_path_startup_sample_limit_bytes(self.mux_limits),
                            max_quantum_bytes,
                            range,
                        )
                        .map_err(|_| RuntimeError::SenderServiceBlocked)?;
                }
                // Qualification mutation is the last recoverable operation.
                if entry.original_data_in_flight_bytes == 0 {
                    entry.load_registration.activate();
                }
                entry.original_data_in_flight_bytes = entry
                    .original_data_in_flight_bytes
                    .saturating_add(bytes as u64);
            }
            entry.bytes_in_flight = entry.bytes_in_flight.saturating_add(bytes as u64);
            (
                entry.incarnation,
                incarnation_matches
                    && (!kind.is_original_transmission()
                        || !entry.qualification.stale_for_original_data()),
            )
        };
        if kind.is_original_transmission() {
            outputs.original_data_in_flight_bytes = outputs
                .original_data_in_flight_bytes
                .saturating_add(bytes as u64);
        }
        let reinjection_suppression_deadline = reinjection_suppression
            .and_then(|(accepted_at, interval)| accepted_at.checked_add(interval))
            .or(reinjection_suppression.map(|(accepted_at, _)| accepted_at));
        if kind == CarrierWorkKind::ReinjectedData {
            // An accepted duplicate destroys uniqueness only for the exact
            // OriginalData receipts whose current flights overlap this range.
            // Bare-range broadcast would let one attachment erase another's
            // same-DSN generation.
            let receipts = flights
                .range(..end)
                .flat_map(|(start, path_flights)| {
                    path_flights.iter().filter_map(move |flight| {
                        (flight.kind.is_original_transmission()
                            && *start < end
                            && flight.end > offset)
                            .then_some((
                                flight.key,
                                flight.output_incarnation,
                                flight.qualification_receipt?,
                            ))
                    })
                })
                .collect::<SmallVec<[_; 4]>>();
            for (owner_key, owner_incarnation, receipt) in receipts {
                let ResponseStreamOutputs {
                    entries, detaching, ..
                } = &mut *outputs;
                if let Some(owner) = entries
                    .iter_mut()
                    .chain(detaching.iter_mut())
                    .find(|entry| entry.key == owner_key && entry.incarnation == owner_incarnation)
                {
                    owner
                        .product_qualification
                        .release_ambiguous(receipt, range);
                }
            }
        }
        flights.entry(offset).or_default().push(CarrierPathFlight {
            key,
            output_incarnation: recorded_incarnation,
            configured_slot: Some(configured_slot),
            end,
            bytes,
            sent_at: reinjection_suppression
                .map(|(accepted_at, _)| accepted_at)
                .unwrap_or_else(Instant::now),
            kind,
            evidence_eligible,
            qualification_receipt,
            reinjection_suppression_deadline,
        });
        // The generation becomes visible only after the matching exact range.
        self.response_model_generation
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    pub(super) fn invalidate_path_flight_evidence(
        &self,
        key: CarrierPathKey,
        output_incarnation: u64,
    ) {
        let mut flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for path_flights in flights.values_mut() {
            for flight in path_flights.iter_mut().filter(|flight| {
                flight.key == key && flight.output_incarnation == output_incarnation
            }) {
                flight.evidence_eligible = false;
            }
        }
        drop(flights);
        let mut ordering = self
            .ack_ordering
            .lock()
            .expect("server response ACK ordering lock");
        for holes in ordering.acked_holes.values_mut() {
            for hole in holes
                .iter_mut()
                .filter(|hole| hole.key == key && hole.output_incarnation == output_incarnation)
            {
                // Ordering ownership remains exact; only stale Product
                // evidence authority is revoked.
                hole.path_proving = false;
            }
        }
    }

    pub(in crate::runtime) fn flight_outputs_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<(CarrierPathKey, u64)> {
        // Exact physical attempts remain in the Product ledger until DataACK.
        // The stable-slot variant below decides whether a current successor
        // inherits local publication authority.
        self.all_flight_outputs_overlapping_frame(frame)
    }

    /// Every currently selectable output whose stable configured slot already
    /// owns an overlapping current Product publication. This is an
    /// advisory Decide exclusion; Apply repeats the vacancy check while
    /// holding the flight ledger lock.
    pub(in crate::runtime) fn reinjection_avoid_outputs_for_frame(
        &self,
        frame: &Frame,
    ) -> Vec<(CarrierPathKey, u64)> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let current_outputs = outputs
            .entries
            .iter()
            .map(|entry| (entry.key, entry.incarnation))
            .collect::<SmallVec<[_; 4]>>();
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut occupied_slots = Vec::<(UnderlayProtocol, ConfiguredMemberSlot)>::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                let Some(configured_slot) = flight.configured_slot else {
                    continue;
                };
                let domain = (flight.key.underlay, configured_slot);
                if flight.end > start
                    && current_outputs.contains(&(flight.key, flight.output_incarnation))
                    && !occupied_slots.contains(&domain)
                {
                    occupied_slots.push(domain);
                }
            }
        }
        outputs
            .entries
            .iter()
            .filter(|entry| occupied_slots.contains(&(entry.key.underlay, entry.configured_slot)))
            .map(|entry| (entry.key, entry.incarnation))
            .collect()
    }

    /// Exact current local publication-owner shape at the lowest recovery
    /// frontier. Output incarnation remains part of Apply identity.
    pub(in crate::runtime) fn live_owner_uniform_frontier(
        &self,
        range: OffsetRange,
    ) -> Option<ReliableLiveOwnerFrontier<ServerReinjectionOutputIdentity>> {
        let outputs = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock");
        let current_outputs = outputs
            .entries
            .iter()
            .map(|entry| ServerReinjectionOutputIdentity {
                key: entry.key,
                incarnation: entry.incarnation,
            })
            .collect::<SmallVec<[_; 4]>>();
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let frontier = reliable_live_owner_uniform_frontier(
            range,
            flights.range(..range.end).flat_map(|(start, flights)| {
                flights.iter().filter_map(|flight| {
                    let identity = ServerReinjectionOutputIdentity {
                        key: flight.key,
                        incarnation: flight.output_incarnation,
                    };
                    (flight.end > range.start && current_outputs.contains(&identity)).then_some(
                        ReliableFlightSpan {
                            range: OffsetRange {
                                start: *start,
                                end: flight.end,
                            },
                            identity,
                            kind: flight.kind,
                            sent_at: flight.sent_at,
                        },
                    )
                })
            }),
        );
        drop(flights);
        drop(outputs);
        frontier
    }

    fn all_flight_outputs_overlapping_frame(&self, frame: &Frame) -> Vec<(CarrierPathKey, u64)> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut outputs = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                let output = (flight.key, flight.output_incarnation);
                if flight.end > start && !outputs.contains(&output) {
                    outputs.push(output);
                }
            }
        }
        outputs
    }

    pub(in crate::runtime) fn original_flight_outputs_overlapping_frame(
        &self,
        frame: &Frame,
    ) -> Vec<(CarrierPathKey, u64)> {
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        let mut outputs = Vec::new();
        for (_, path_flights) in flights.range(..end) {
            for flight in path_flights {
                let output = (flight.key, flight.output_incarnation);
                if flight.end <= start
                    || !flight.kind.is_original_transmission()
                    || outputs.contains(&output)
                {
                    continue;
                }
                outputs.push(output);
            }
        }
        outputs
    }

    pub(in crate::runtime) fn lower_flights_before_offset(
        &self,
        offset: u64,
    ) -> Vec<CarrierPathFlightDebt> {
        let mut debts = BTreeMap::<u64, CarrierPathFlightDebt>::new();
        {
            let flights = self
                .flights
                .lock()
                .expect("server reliable stream flight lock");
            for (flight_offset, path_flights) in flights.range(..offset) {
                if let Some(original) = path_flights
                    .iter()
                    .rev()
                    .find(|flight| flight.kind.is_original_transmission())
                {
                    debts.insert(
                        *flight_offset,
                        CarrierPathFlightDebt {
                            key: original.key,
                            output_incarnation: original.output_incarnation,
                            bytes: original.bytes as u64,
                        },
                    );
                }
            }
        }
        {
            let ack_ordering = self
                .ack_ordering
                .lock()
                .expect("server response ACK ordering lock");
            for (hole_offset, holes) in ack_ordering.acked_holes.range(..offset) {
                if let Some(latest) = response_latest_original_hole(holes) {
                    debts.insert(
                        *hole_offset,
                        CarrierPathFlightDebt {
                            key: latest.key,
                            output_incarnation: latest.output_incarnation,
                            bytes: latest.bytes,
                        },
                    );
                }
            }
        }
        debts.into_values().collect()
    }

    pub(in crate::runtime) fn blocking_original_path_at_or_after(
        &self,
        offset: u64,
    ) -> Option<(CarrierPathKey, u64)> {
        self.blocking_original_flight_at_or_after(offset)
            .map(|(_, flight)| (flight.key, flight.output_incarnation))
    }

    fn blocking_original_flight_at_or_after(
        &self,
        offset: u64,
    ) -> Option<(u64, CarrierPathFlight)> {
        let flights = self
            .flights
            .lock()
            .expect("server reliable stream flight lock");
        for (start, path_flights) in flights.iter() {
            for flight in path_flights {
                if flight.kind.is_original_transmission() && flight.end > offset {
                    return Some((*start, *flight));
                }
            }
        }
        None
    }
}

pub(in crate::runtime::stream) fn release_carrier_path_flight_ranges(
    flights: &mut BTreeMap<u64, Vec<CarrierPathFlight>>,
    ranges: &[OffsetRange],
) -> Vec<(u64, CarrierPathReleasedFlight)> {
    if ranges.is_empty() || flights.is_empty() {
        return Vec::new();
    }

    let original_flights = std::mem::take(flights)
        .into_iter()
        .flat_map(|(start, path_flights)| {
            path_flights.into_iter().map(move |flight| (start, flight))
        })
        .collect::<Vec<_>>();
    let ambiguous_intervals = ambiguous_flight_intervals(
        original_flights
            .iter()
            .map(|(start, flight)| (*start, flight.end)),
    );
    let mut released = Vec::new();
    for (start, flight) in original_flights.iter().copied() {
        let split = split_flight_interval_by_ack(start, flight.end, ranges);
        for (acked_start, acked_end) in split.acked {
            let bytes = flight_interval_bytes(acked_start, acked_end);
            if bytes == 0 {
                continue;
            }
            let qualification_ambiguous_ranges = ambiguous_intervals
                .iter()
                .filter_map(|(ambiguous_start, ambiguous_end)| {
                    OffsetRange::new(
                        acked_start.max(*ambiguous_start),
                        acked_end.min(*ambiguous_end),
                    )
                })
                .collect::<SmallVec<[_; 2]>>();
            released.push((
                acked_start,
                CarrierPathReleasedFlight {
                    flight: CarrierPathFlight {
                        end: acked_end,
                        bytes,
                        qualification_receipt: flight.qualification_receipt.and_then(|receipt| {
                            receipt.intersect(OffsetRange {
                                start: acked_start,
                                end: acked_end,
                            })
                        }),
                        ..flight
                    },
                    path_proving: flight.evidence_eligible
                        && flight.kind.is_original_transmission()
                        && qualification_ambiguous_ranges.is_empty(),
                    qualification_ambiguous_ranges,
                },
            ));
        }
        for (retained_start, retained_end) in split.retained {
            let bytes = flight_interval_bytes(retained_start, retained_end);
            if bytes == 0 {
                continue;
            }
            flights
                .entry(retained_start)
                .or_default()
                .push(CarrierPathFlight {
                    end: retained_end,
                    bytes,
                    qualification_receipt: flight.qualification_receipt.and_then(|receipt| {
                        receipt.intersect(OffsetRange {
                            start: retained_start,
                            end: retained_end,
                        })
                    }),
                    ..flight
                });
        }
    }
    released
}

pub(in crate::runtime::stream) fn product_flights_have_recent_reinjection_overlap(
    flights: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    start: u64,
    end: u64,
    now: Instant,
    mut live: impl FnMut(CarrierPathKey, u64) -> bool,
) -> Option<Instant> {
    if start >= end {
        return None;
    }
    let mut earliest_deadline = None;
    for (&offset, path_flights) in flights.range(..end) {
        for flight in path_flights {
            if offset >= end || flight.end <= start {
                continue;
            }
            if flight.kind != CarrierWorkKind::ReinjectedData
                || !live(flight.key, flight.output_incarnation)
            {
                continue;
            }
            if let Some(deadline) = flight.reinjection_suppression_deadline
                && deadline > now
            {
                earliest_deadline = Some(
                    earliest_deadline.map_or(deadline, |current: Instant| current.min(deadline)),
                );
            }
        }
    }
    earliest_deadline
}

fn product_flights_have_current_slot_overlap(
    flights: &BTreeMap<u64, Vec<CarrierPathFlight>>,
    start: u64,
    end: u64,
    underlay: UnderlayProtocol,
    configured_slot: ConfiguredMemberSlot,
    current_slot_outputs: &[(CarrierPathKey, u64)],
) -> bool {
    start < end
        && flights.range(..end).any(|(flight_start, path_flights)| {
            *flight_start < end
                && path_flights.iter().any(|flight| {
                    flight.end > start
                        && flight.key.underlay == underlay
                        && flight.configured_slot == Some(configured_slot)
                        && current_slot_outputs.contains(&(flight.key, flight.output_incarnation))
                })
        })
}

#[cfg(test)]
#[path = "tests_delivery.rs"]
mod tests;

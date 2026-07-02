use super::super::bulk_admission::bulk_service_horizon_payload_bytes;
use super::*;

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) commands: TcpPathSessionCommandSender,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    pub(super) last_delivery_at: Option<Instant>,
    pub(super) path_metrics: Option<ServerPathMetricsEntry>,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
}

#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) commands: TcpPathSessionCommandSender,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) eta_ms: f64,
    pub(in crate::runtime) is_active: bool,
    pub(in crate::runtime) has_sender_evidence: bool,
}

/// Product byte range currently assigned to a carrier path.
///
/// STREAM_ACK releases this ledger entry from product flight; carrier ACKs only
/// update carrier/path evidence and must not release product repair state.
#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlight {
    pub(super) key: CarrierPathKey,
    pub(super) end: u64,
    pub(super) bytes: usize,
    pub(super) sent_at: Instant,
    pub(super) stream_ack_proves_path: bool,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathFlightDebt {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) bytes: u64,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathAckedHole {
    pub(super) key: CarrierPathKey,
    pub(super) end: u64,
    pub(super) bytes: u64,
    pub(super) stream_ack_proves_path: bool,
}

#[derive(Debug, Default)]
pub(in crate::runtime) struct ResponseAckOrderingState {
    pub(super) contiguous_frontier: u64,
    pub(super) acked_holes: BTreeMap<u64, Vec<CarrierPathAckedHole>>,
}

pub(in crate::runtime) struct ResponseAckOrderingUpdate {
    pub(super) changed: bool,
    pub(super) contiguous_frontier: u64,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) acked_hole_bytes: u64,
    pub(super) newly_contiguous: Vec<CarrierPathAckedHole>,
}

impl ResponseAckOrderingState {
    pub(super) fn apply_ack(
        &mut self,
        ranges: &[OffsetRange],
        released: &[(u64, CarrierPathFlight)],
    ) -> ResponseAckOrderingUpdate {
        let previous_frontier = self.contiguous_frontier;
        let previous_hole_bytes = self.acked_hole_bytes();
        let mut newly_contiguous = Vec::new();

        for (offset, flight) in released {
            let hole = CarrierPathAckedHole {
                key: flight.key,
                end: flight.end,
                bytes: flight.bytes as u64,
                stream_ack_proves_path: flight.stream_ack_proves_path,
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
            acked_hole_bytes,
            newly_contiguous,
        }
    }

    fn advance_contiguous_frontier(&mut self, ranges: &[OffsetRange]) {
        let ranges = normalized_offset_ranges(ranges);
        loop {
            let mut next_frontier = self.contiguous_frontier;
            for range in &ranges {
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
            .flat_map(|holes| holes.iter())
            .map(|hole| hole.bytes)
            .sum()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum ServerPathMetricsSource {
    PeerHint,
    LocalSender,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct ServerPathMetricsEntry {
    pub(super) metrics: PathMetrics,
    pub(super) source: ServerPathMetricsSource,
}

impl ResponseStreamOutputs {
    pub(super) fn read_backpressure_snapshot(
        &self,
        session_id: SessionId,
        lane_tracker: &ServerPathLaneTracker,
        lane: FlowLane,
        payload_bytes: usize,
        mux_limits: MuxLimits,
    ) -> Option<PathSnapshot> {
        let now = Instant::now();
        if !relay_lane_is_bulk(lane) {
            return self.entries.last().map(|entry| {
                server_bulk_output_snapshot(entry, session_id, lane, lane_tracker, mux_limits, now)
            });
        }
        let active_key = self.entries.last().map(|entry| entry.key);
        self.entries
            .iter()
            .filter(|entry| {
                Some(entry.key) == active_key || server_output_has_sender_evidence(entry)
            })
            .map(|entry| {
                let snapshot = server_bulk_output_snapshot(
                    entry,
                    session_id,
                    lane,
                    lane_tracker,
                    mux_limits,
                    now,
                );
                let eta_ms = server_bulk_output_eta_ms(
                    entry.key,
                    snapshot,
                    active_key,
                    lane,
                    payload_bytes,
                    mux_limits,
                );
                (eta_ms, snapshot)
            })
            .min_by(|left, right| left.0.total_cmp(&right.0))
            .map(|(_, snapshot)| snapshot)
    }
}

pub(super) fn server_bulk_output_snapshot(
    entry: &ResponseStreamOutputEntry,
    session_id: SessionId,
    lane: FlowLane,
    lane_tracker: &ServerPathLaneTracker,
    mux_limits: MuxLimits,
    now: Instant,
) -> PathSnapshot {
    let local_carrier_metrics = entry.path_metrics.and_then(|path_metrics| {
        (path_metrics.source == ServerPathMetricsSource::LocalSender).then_some(path_metrics)
    });
    let validation_hint_metrics = entry
        .path_metrics
        .and_then(|path_metrics| (entry.delivery_samples == 0).then_some(path_metrics));
    let model_metrics = local_carrier_metrics.or(validation_hint_metrics);
    let srtt_ms = model_metrics.map_or_else(
        || {
            entry
                .srtt_ms
                .unwrap_or_else(|| default_path_srtt_ms(entry.key.underlay))
        },
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = model_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = model_metrics
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let model_rate_bps = model_metrics.map(server_path_metrics_rate_bps);
    let local_sender_rate_bps = local_carrier_metrics
        .map(server_path_metrics_rate_bps)
        .or(entry.delivery_rate_bps);
    let prior_rate_bps =
        model_rate_bps.unwrap_or_else(|| default_path_rate_bps(entry.key.underlay));
    let rate_bps = match entry.key.underlay {
        UnderlayProtocol::Udp => local_sender_rate_bps,
        UnderlayProtocol::Tcp => local_sender_rate_bps.map(|rate| rate.max(prior_rate_bps)),
    }
    .unwrap_or(prior_rate_bps)
    .max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    snapshot.product_progress_rate_bps = entry.product_progress_rate_bps;
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = model_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(entry.commands.pending_bytes());
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        UnderlayProtocol::Tcp => entry.bytes_in_flight,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes =
        model_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes);
    snapshot.confidence = server_output_confidence(entry, now);
    let lane_load = lane_tracker.snapshot(session_id, entry.key);
    let session_lane_load = lane_tracker.session_snapshot(session_id);
    snapshot.active_flows = lane_load.active_flows;
    snapshot.active_latency_sensitive_flows = lane_load.active_latency_sensitive_flows;
    snapshot.session_active_latency_sensitive_flows =
        session_lane_load.active_latency_sensitive_flows;
    let known_bulk_flows = lane_load
        .active_flows
        .saturating_sub(lane_load.active_latency_sensitive_flows);
    if relay_lane_is_bulk(lane)
        && lane_load.active_latency_sensitive_flows > 0
        && known_bulk_flows > 0
    {
        let latency_headroom =
            adaptive_reliable_relay_inflight_bytes(Some(snapshot), FlowLane::Latency, mux_limits)
                as u64;
        let protected_queue =
            latency_headroom.saturating_mul(u64::from(lane_load.active_latency_sensitive_flows));
        snapshot.queue_bytes = snapshot.queue_bytes.saturating_add(protected_queue);
    }
    snapshot
}

pub(super) fn server_bulk_output_eta_ms(
    key: CarrierPathKey,
    snapshot: PathSnapshot,
    active_key: Option<CarrierPathKey>,
    lane: FlowLane,
    payload_bytes: usize,
    mux_limits: MuxLimits,
) -> f64 {
    let queued_bits = snapshot
        .queue_bytes
        .saturating_add(snapshot.product_queue_bytes)
        .saturating_add(snapshot.bytes_in_flight)
        .saturating_mul(8) as f64;
    let scoring_payload_bytes = if relay_lane_is_bulk(lane) {
        bulk_service_horizon_payload_bytes(payload_bytes, mux_limits)
    } else {
        payload_bytes
    };
    let payload_bits = scoring_payload_bytes as f64 * 8.0;
    let mut eta_ms = snapshot.srtt_ms / 2.0;
    let effective_rate_bps = if relay_lane_is_bulk(lane) {
        snapshot
            .delivery_rate_bps
            .max(snapshot.pacing_rate_bps)
            .max(1.0)
    } else {
        snapshot.delivery_rate_bps.max(1.0)
    };
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += snapshot.loss_rate.clamp(0.0, 1.0) * 500.0;
    if key.underlay == UnderlayProtocol::Udp && relay_lane_is_bulk(lane) {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    eta_ms += (1.0 - snapshot.confidence.clamp(0.0, 1.0)) * snapshot.srtt_ms;
    if Some(key) != active_key && snapshot.confidence < 0.5 {
        eta_ms += snapshot.srtt_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms += snapshot.srtt_ms;
        }
    }
    eta_ms
}

fn server_output_confidence(entry: &ResponseStreamOutputEntry, now: Instant) -> f64 {
    let delivery_confidence = (f64::from(entry.delivery_samples) / 8.0).clamp(0.0, 1.0);
    let metric_confidence = match entry.path_metrics {
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
        }) if metrics.has_ack_derived_data_sample => {
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_confidence = (f64::from(metrics.data_sample_count) / 8.0).clamp(0.0, 1.0);
            source_confidence * sample_confidence
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.1,
        _ => 0.0,
    };
    let freshness_confidence = entry
        .last_delivery_at
        .map(|seen| {
            let age = now.saturating_duration_since(seen).as_secs_f64();
            (1.0 - age / 30.0).clamp(0.0, 1.0) * 0.25
        })
        .unwrap_or(0.0);
    delivery_confidence
        .max(metric_confidence)
        .max(freshness_confidence)
        .clamp(0.1, 1.0)
}

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    let delivery_rate_bps = path_metrics.metrics.delivery_rate_bps.max(1) as f64;
    let pacing_rate_bps = path_metrics.metrics.pacing_rate_bps.max(1) as f64;
    if path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.app_limited
    {
        delivery_rate_bps.max(pacing_rate_bps)
    } else {
        delivery_rate_bps
    }
}

pub(in crate::runtime) fn server_output_has_sender_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            entry.path_metrics,
            Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    delivery_rate_bps: 1..,
                    has_ack_derived_data_sample: true,
                    ..
                },
            })
        )
}

pub(in crate::runtime) fn record_server_sender_decision(
    session_id: SessionId,
    stream_id: StreamId,
    key: CarrierPathKey,
    frame: &Frame,
    lane: FlowLane,
    reason: &'static str,
) {
    #[cfg(feature = "lab-diagnostics")]
    lab_sender_service_decision(
        "server",
        Some(session_id.0),
        stream_id.0,
        reason,
        sender_service_frame_kind(frame),
        reliable_stream_frame_payload_bytes(frame),
        format_args!(
            "path_underlay={:?} path_id={} lane={:?} pacing_bytes={}",
            key.underlay,
            key.path_id.0,
            lane,
            frame_pacing_bytes(frame),
        ),
    );
    #[cfg(not(feature = "lab-diagnostics"))]
    let _ = (session_id, stream_id, key, frame, lane, reason);
}

#[cfg(feature = "lab-diagnostics")]
pub(super) fn sender_service_frame_kind(frame: &Frame) -> &'static str {
    match frame {
        Frame::StreamData { .. } => "stream_data",
        Frame::StreamAck { .. } => "stream_ack",
        Frame::StreamMaxData { .. } => "stream_max_data",
        Frame::StreamFin { .. } => "stream_fin",
        Frame::StreamReset { .. } => "stream_reset",
        Frame::StreamDetach { .. } => "stream_detach",
        Frame::DatagramData { .. } => "datagram_data",
        Frame::DatagramFeedback { .. } => "datagram_feedback",
        Frame::DatagramClose { .. } => "datagram_close",
        _ => "control",
    }
}

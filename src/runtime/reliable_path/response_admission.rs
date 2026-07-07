use super::super::bulk_admission::bulk_service_horizon_payload_bytes;
use super::*;

/// One carrier output attached to a response stream.
///
/// It owns carrier command access and sender-evidence fields for this stream on
/// this path. Product repair and ordering identity stay in `ResponseStreamBinding`.
#[derive(Clone)]
pub(in crate::runtime) struct ResponseStreamOutputEntry {
    pub(super) key: CarrierPathKey,
    pub(super) commands: ReliablePathCommandSender,
    pub(super) bytes_in_flight: u64,
    pub(super) product_queue_bytes: u64,
    pub(super) product_progress_rate_bps: Option<f64>,
    pub(super) delivery_rate_bps: Option<f64>,
    pub(super) srtt_ms: Option<f64>,
    pub(super) delivery_samples: u32,
    pub(super) last_delivery_at: Option<Instant>,
    pub(super) local_path_metrics: Option<ServerPathMetricsEntry>,
    pub(super) peer_path_metrics: Option<ServerPathMetricsEntry>,
}

pub(in crate::runtime) struct ResponseStreamOutputs {
    pub(super) entries: Vec<ResponseStreamOutputEntry>,
}

#[derive(Clone)]
pub(in crate::runtime) struct ResponseSenderPathTarget {
    pub(in crate::runtime) key: CarrierPathKey,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) snapshot: PathSnapshot,
    pub(in crate::runtime) eta_ms: f64,
    pub(in crate::runtime) is_active: bool,
    pub(in crate::runtime) has_sender_evidence: bool,
    pub(in crate::runtime) has_bulk_rate_evidence: bool,
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
    pub(super) kind: CarrierWorkKind,
}

#[derive(Debug, Clone, Copy)]
pub(in crate::runtime) struct CarrierPathReleasedFlight {
    pub(super) flight: CarrierPathFlight,
    pub(super) path_proving: bool,
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
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
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
                end: flight.end,
                bytes: flight.bytes as u64,
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
            .filter_map(|holes| response_latest_ordering_hole(holes))
            .map(|hole| hole.bytes)
            .sum()
    }
}

pub(in crate::runtime) fn response_latest_ordering_flight(
    flights: &[CarrierPathFlight],
) -> Option<&CarrierPathFlight> {
    flights
        .iter()
        .rev()
        .find(|flight| flight.kind.is_ordering_owner())
}

pub(in crate::runtime) fn response_latest_ordering_hole(
    holes: &[CarrierPathAckedHole],
) -> Option<&CarrierPathAckedHole> {
    holes
        .iter()
        .rev()
        .find(|hole| hole.kind.is_ordering_owner())
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
    let local_carrier_metrics = entry.local_path_metrics;
    let peer_hint_metrics = (entry.delivery_samples == 0)
        .then_some(entry.peer_path_metrics)
        .flatten();
    let liveness_metrics = local_carrier_metrics.or(peer_hint_metrics);
    let bulk_rate_metrics = local_carrier_metrics
        .filter(|path_metrics| server_path_metrics_has_bulk_rate_evidence(*path_metrics));
    let srtt_ms = liveness_metrics.map_or_else(
        || {
            entry
                .srtt_ms
                .unwrap_or_else(|| default_path_srtt_ms(entry.key.underlay))
        },
        |path_metrics| f64::from(path_metrics.metrics.srtt_us.max(1)) / 1000.0,
    );
    let jitter_ms = liveness_metrics.map_or(0.0, |path_metrics| {
        f64::from(path_metrics.metrics.jitter_us) / 1000.0
    });
    let loss_rate = liveness_metrics
        .filter(|path_metrics| path_metrics.metrics.loss_observed)
        .map_or(0.0, |path_metrics| {
            f64::from(path_metrics.metrics.loss_ppm) / 1_000_000.0
        })
        .clamp(0.0, 1.0);
    let model_rate_bps = bulk_rate_metrics.map(server_path_metrics_rate_bps);
    let peer_hint_rate_bps = peer_hint_metrics
        .filter(|path_metrics| !path_metrics.metrics.app_limited)
        .map(server_path_metrics_rate_bps);
    let prior_rate_bps = model_rate_bps
        .or(peer_hint_rate_bps)
        .unwrap_or_else(|| default_path_rate_bps(entry.key.underlay));
    let rate_bps = match (
        entry.key.underlay,
        bulk_rate_metrics,
        entry.delivery_rate_bps,
    ) {
        (_, Some(path_metrics), _) => Some(server_path_metrics_rate_bps(path_metrics)),
        (UnderlayProtocol::Tcp, None, Some(rate))
            if !super::product_delivery_samples_override_startup_prior(entry.delivery_samples) =>
        {
            Some(rate.max(prior_rate_bps))
        }
        (_, None, Some(rate)) => Some(rate),
        (_, None, None) => None,
    }
    .unwrap_or(prior_rate_bps)
    .max(1.0);
    let mut snapshot = PathSnapshot::new(entry.key.path_id, entry.key.underlay, srtt_ms, rate_bps);
    if let Some(path_metrics) = liveness_metrics {
        snapshot.min_rtt_ms = f64::from(path_metrics.metrics.min_rtt_us.max(1)) / 1000.0;
    }
    snapshot.product_progress_rate_bps = entry.product_progress_rate_bps;
    snapshot.jitter_ms = jitter_ms;
    snapshot.loss_rate = loss_rate;
    if let Some(path_metrics) = bulk_rate_metrics {
        snapshot.pacing_rate_bps =
            (path_metrics.metrics.pacing_rate_bps.max(1) as f64).max(snapshot.delivery_rate_bps);
    }
    if let Some(path_metrics) = liveness_metrics {
        snapshot.app_limited = path_metrics.metrics.app_limited;
    }
    let metric_queue_bytes =
        local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.queue_bytes);
    snapshot.queue_bytes = metric_queue_bytes.saturating_add(entry.commands.pending_bytes());
    snapshot.product_queue_bytes = entry.product_queue_bytes;
    snapshot.bytes_in_flight = match entry.key.underlay {
        UnderlayProtocol::Udp => {
            local_carrier_metrics.map_or(0, |path_metrics| path_metrics.metrics.bytes_in_flight)
        }
        // TCP does not expose packet-level carrier flight to the product layer.
        // Product stream ranges waiting for STREAM_ACK remain in
        // product_bytes_in_flight below; treating them as carrier flight makes
        // the BBR-style send quantum collapse as soon as the product window is
        // full even when the kernel TCP stream is healthy.
        UnderlayProtocol::Tcp => 0,
    };
    snapshot.product_bytes_in_flight = entry.bytes_in_flight;
    snapshot.inflight_limit_bytes = match entry.key.underlay {
        UnderlayProtocol::Udp => local_carrier_metrics
            .map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes),
        UnderlayProtocol::Tcp => {
            bulk_rate_metrics.map_or(0, |path_metrics| path_metrics.metrics.inflight_limit_bytes)
        }
    };
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

pub(in crate::runtime) fn server_bulk_output_eta_ms(
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
    let effective_rate_bps = snapshot.delivery_rate_bps.max(1.0);
    eta_ms += (queued_bits + payload_bits) / effective_rate_bps * 1000.0;
    eta_ms += snapshot.jitter_ms;
    eta_ms += response_loss_penalty_ms(snapshot);
    if key.underlay == UnderlayProtocol::Udp && relay_lane_is_bulk(lane) {
        eta_ms += udp_reliable_stream_loss_repair_penalty_ms(snapshot, payload_bytes);
    }
    let uncertainty = 1.0 - snapshot.confidence.clamp(0.0, 1.0);
    let pto_ms = transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0;
    eta_ms += uncertainty * pto_ms;
    if Some(key) != active_key {
        eta_ms += uncertainty * pto_ms;
        if snapshot.bytes_in_flight > 0 {
            eta_ms +=
                (snapshot.bytes_in_flight as f64 * 8.0 / effective_rate_bps.max(1.0)) * 1000.0;
        }
    }
    eta_ms
}

fn response_loss_penalty_ms(snapshot: PathSnapshot) -> f64 {
    let loss = snapshot.loss_rate.clamp(0.0, 1.0);
    if loss <= f64::EPSILON {
        return 0.0;
    }
    let min_progress = PATH_OPEN_SCORE_BYTES as f64
        / ((snapshot.delivery_rate_bps.max(1.0) / 8.0) * (snapshot.srtt_ms.max(1.0) / 1000.0))
            .max(PATH_OPEN_SCORE_BYTES as f64);
    let expected_repairs = loss / (1.0 - loss).max(min_progress);
    expected_repairs * transport_pto_from_snapshot(Some(snapshot)).as_secs_f64() * 1000.0
}

fn confidence_sample_denominator() -> f64 {
    f64::from(RELIABLE_INITIAL_WINDOW_PACKETS as u32)
}

fn server_output_confidence(entry: &ResponseStreamOutputEntry, _now: Instant) -> f64 {
    let delivery_confidence =
        (f64::from(entry.delivery_samples) / confidence_sample_denominator()).clamp(0.0, 1.0);
    let metric_confidence = match entry.local_path_metrics {
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::LocalSender,
            metrics,
        }) if metrics.has_ack_derived_data_sample || metrics.confidence_ppm > 0 => {
            let source_confidence =
                f64::from(metrics.confidence_ppm).clamp(0.0, 1_000_000.0) / 1_000_000.0;
            let sample_floor = server_path_metrics_bulk_sample_floor_bytes(metrics).max(1) as f64;
            let byte_confidence = (metrics.data_sample_bytes as f64 / sample_floor).clamp(0.0, 1.0);
            let count_confidence = (f64::from(metrics.data_sample_count)
                / confidence_sample_denominator())
            .clamp(0.0, 1.0);
            let sample_confidence = byte_confidence.min(count_confidence);
            if metrics.has_ack_derived_data_sample {
                source_confidence * sample_confidence
            } else {
                source_confidence
            }
        }
        Some(ServerPathMetricsEntry {
            source: ServerPathMetricsSource::PeerHint,
            ..
        }) => 0.0,
        _ => 0.0,
    };
    delivery_confidence.max(metric_confidence).clamp(0.0, 1.0)
}

fn server_path_metrics_rate_bps(path_metrics: ServerPathMetricsEntry) -> f64 {
    path_metrics.metrics.delivery_rate_bps.max(1) as f64
}

fn server_path_metrics_bulk_sample_floor_bytes(metrics: PathMetrics) -> u64 {
    metrics
        .inflight_hi_bytes
        .max(metrics.inflight_limit_bytes)
        .max(PATH_OPEN_SCORE_BYTES as u64)
}

fn server_path_metrics_has_bulk_rate_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.has_ack_derived_data_sample
        && path_metrics.metrics.data_sample_count > 0
        && path_metrics.metrics.data_sample_bytes
            >= server_path_metrics_bulk_sample_floor_bytes(path_metrics.metrics)
}

fn server_path_metrics_has_ack_data_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && path_metrics.metrics.has_ack_derived_data_sample
}

fn server_path_metrics_has_sender_evidence(path_metrics: ServerPathMetricsEntry) -> bool {
    path_metrics.source == ServerPathMetricsSource::LocalSender
        && (server_path_metrics_has_bulk_rate_evidence(path_metrics)
            || server_path_metrics_has_ack_data_evidence(path_metrics)
            || path_metrics.metrics.confidence_ppm > 0)
}

pub(in crate::runtime) fn server_output_has_sender_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            entry.local_path_metrics,
            Some(path_metrics) if server_path_metrics_has_sender_evidence(path_metrics)
        )
}

#[cfg(test)]
fn server_output_has_ack_data_evidence(entry: &ResponseStreamOutputEntry) -> bool {
    entry.delivery_samples > 0
        || entry.delivery_rate_bps.is_some()
        || matches!(
            entry.local_path_metrics,
            Some(path_metrics) if server_path_metrics_has_ack_data_evidence(path_metrics)
        )
}

pub(in crate::runtime) fn server_output_has_bulk_rate_evidence(
    entry: &ResponseStreamOutputEntry,
) -> bool {
    let has_local_carrier_bulk = matches!(
        entry.local_path_metrics,
        Some(path_metrics) if server_path_metrics_has_bulk_rate_evidence(path_metrics)
    );
    match entry.key.underlay {
        UnderlayProtocol::Udp => has_local_carrier_bulk,
        // TCP response output is one encrypted carrier stream, so product
        // STREAM_ACK samples are path-scoped sender evidence. QUIC/UDP exposes
        // carrier ACK-derived samples separately and must use those for
        // ordinary bulk-rate proof.
        UnderlayProtocol::Tcp => {
            entry.delivery_samples > 0
                || entry.delivery_rate_bps.is_some()
                || has_local_carrier_bulk
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn udp_bulk_rate_evidence_requires_local_quic_ack_sample() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let entry = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Udp,
                path_id: PathId(0),
            },
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(500_000_000.0),
            delivery_rate_bps: None,
            srtt_ms: None,
            delivery_samples: 1,
            last_delivery_at: Some(Instant::now()),
            local_path_metrics: None,
            peer_path_metrics: None,
        };

        assert!(
            server_output_has_sender_evidence(&entry),
            "product ACK samples still prove end-to-end sender progress"
        );
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "UDP ordinary bulk-rate evidence must be local QUIC ACK-derived carrier data, not product STREAM_ACK alone"
        );
    }

    #[test]
    fn udp_bulk_rate_evidence_survives_later_app_limited_metric_poll() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(0),
        };
        let entry = ResponseStreamOutputEntry {
            key,
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(500_000_000.0),
            delivery_rate_bps: None,
            srtt_ms: None,
            delivery_samples: 0,
            last_delivery_at: None,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 20_000,
                    srtt_us: 20_000,
                    rttvar_us: 1_000,
                    jitter_us: 1_000,
                    delivery_rate_bps: 200_000_000,
                    pacing_rate_bps: 200_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: PATH_OPEN_SCORE_BYTES as u64,
                    queue_bytes: 0,
                    inflight_limit_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                    inflight_hi_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                    confidence_ppm: 1_000_000,
                    app_limited: true,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 32,
                    data_sample_bytes: 4 * PATH_OPEN_SCORE_BYTES as u64,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(
            server_output_has_bulk_rate_evidence(&entry),
            "a later app-limited metrics poll must not erase existing non-app-limited QUIC bulk evidence"
        );
    }

    #[test]
    fn udp_app_limited_ack_data_snapshot_keeps_carrier_inflight_limit() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(7),
        };
        let entry = ResponseStreamOutputEntry {
            key,
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            srtt_ms: None,
            delivery_samples: 0,
            last_delivery_at: None,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 80_000,
                    srtt_us: 80_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                    pacing_rate_bps: default_path_rate_bps(UnderlayProtocol::Udp).round() as u64,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: 2 * 1024 * 1024,
                    inflight_hi_bytes: 2 * 1024 * 1024,
                    confidence_ppm: 0,
                    app_limited: true,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 0,
                    data_sample_bytes: 0,
                },
            }),
            peer_path_metrics: None,
        };

        let lane_tracker = ServerPathLaneTracker::default();
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );

        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(UnderlayProtocol::Udp),
            "app-limited ACK-data must not create a tiny bulk-rate model"
        );
        assert_eq!(
            snapshot.inflight_limit_bytes,
            2 * 1024 * 1024,
            "carrier inflight credit is path-local QUIC state and remains usable for bounded exploration"
        );
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "ACK-data seen without non-app-limited samples is not ordinary bulk-rate proof"
        );
    }

    #[test]
    fn local_proof_metrics_are_sender_evidence_not_ack_data_evidence() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(10),
        };
        let entry = ResponseStreamOutputEntry {
            key,
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            srtt_ms: None,
            delivery_samples: 0,
            last_delivery_at: None,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 40_000,
                    srtt_us: 40_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: 32_000,
                    pacing_rate_bps: 32_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: PATH_OPEN_SCORE_BYTES as u64,
                    inflight_hi_bytes: PATH_OPEN_SCORE_BYTES as u64,
                    confidence_ppm: 1_000_000,
                    app_limited: true,
                    has_ack_derived_data_sample: false,
                    data_sample_count: 0,
                    data_sample_bytes: 0,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(server_output_has_sender_evidence(&entry));
        assert!(
            !server_output_has_ack_data_evidence(&entry),
            "PATH_PROOF-style liveness evidence must not become ACK-data evidence for unique Subflow ownership"
        );
        assert!(!server_output_has_bulk_rate_evidence(&entry));
    }

    #[test]
    fn udp_tiny_non_app_limited_sample_is_ack_data_not_bulk_rate_evidence() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(9),
        };
        let sample_floor = 2 * 1024 * 1024;
        let entry = ResponseStreamOutputEntry {
            key,
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: None,
            delivery_rate_bps: None,
            srtt_ms: None,
            delivery_samples: 0,
            last_delivery_at: None,
            local_path_metrics: Some(ServerPathMetricsEntry {
                source: ServerPathMetricsSource::LocalSender,
                metrics: PathMetrics {
                    path_id: key.path_id,
                    underlay: key.underlay,
                    direction: PathMetricDirection::ServerToClient,
                    metric_epoch: metric_epoch_now(),
                    metric_age_us: 0,
                    min_rtt_us: 80_000,
                    srtt_us: 80_000,
                    rttvar_us: 2_000,
                    jitter_us: 2_000,
                    delivery_rate_bps: 12_000_000,
                    pacing_rate_bps: 12_000_000,
                    loss_ppm: 0,
                    ecn_ppm: 0,
                    loss_observed: false,
                    ecn_observed: false,
                    bytes_in_flight: 0,
                    queue_bytes: 0,
                    inflight_limit_bytes: sample_floor,
                    inflight_hi_bytes: sample_floor,
                    confidence_ppm: 1_000_000,
                    app_limited: false,
                    has_ack_derived_data_sample: true,
                    data_sample_count: 4,
                    data_sample_bytes: PATH_OPEN_SCORE_BYTES as u64,
                },
            }),
            peer_path_metrics: None,
        };

        assert!(matches!(
            entry.local_path_metrics,
            Some(path_metrics) if server_path_metrics_has_ack_data_evidence(path_metrics)
        ));
        assert!(
            !server_output_has_bulk_rate_evidence(&entry),
            "bulk-rate promotion requires enough ACKed byte volume, not just non-app-limited ACK count"
        );
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &ServerPathLaneTracker::default(),
            MuxLimits::default(),
            Instant::now(),
        );
        assert_eq!(
            snapshot.delivery_rate_bps,
            default_path_rate_bps(UnderlayProtocol::Udp)
        );
        assert!(snapshot.confidence < 1.0);
    }

    #[test]
    fn tcp_response_snapshot_persistent_delivery_samples_override_default_prior() {
        let (commands, _receivers) = reliable_path_command_channels(8);
        let prior_rate = default_path_rate_bps(UnderlayProtocol::Tcp);
        let entry = ResponseStreamOutputEntry {
            key: CarrierPathKey {
                underlay: UnderlayProtocol::Tcp,
                path_id: PathId(0),
            },
            commands,
            bytes_in_flight: 0,
            product_queue_bytes: 0,
            product_progress_rate_bps: Some(prior_rate / 10.0),
            delivery_rate_bps: Some(prior_rate / 10.0),
            srtt_ms: Some(default_path_srtt_ms(UnderlayProtocol::Tcp)),
            delivery_samples: RELIABLE_INITIAL_WINDOW_PACKETS as u32,
            last_delivery_at: Some(Instant::now()),
            local_path_metrics: None,
            peer_path_metrics: None,
        };

        let lane_tracker = ServerPathLaneTracker::default();
        let snapshot = server_bulk_output_snapshot(
            &entry,
            SessionId(77),
            FlowLane::Throughput,
            &lane_tracker,
            MuxLimits::default(),
            Instant::now(),
        );

        assert_eq!(snapshot.delivery_rate_bps, prior_rate / 10.0);
    }

    #[test]
    fn response_eta_uses_delivered_rate_not_inflated_quic_pacing_rate() {
        let key = CarrierPathKey {
            underlay: UnderlayProtocol::Udp,
            path_id: PathId(2),
        };
        let mut baseline = PathSnapshot::new(key.path_id, key.underlay, 100.0, 50_000_000.0);
        baseline.pacing_rate_bps = 50_000_000.0;
        baseline.confidence = 1.0;

        let mut inflated_pacing = baseline;
        inflated_pacing.pacing_rate_bps = 5_000_000_000.0;

        let payload_bytes = 64 * 1024;
        let baseline_eta = server_bulk_output_eta_ms(
            key,
            baseline,
            Some(key),
            FlowLane::Throughput,
            payload_bytes,
            MuxLimits::default(),
        );
        let inflated_eta = server_bulk_output_eta_ms(
            key,
            inflated_pacing,
            Some(key),
            FlowLane::Throughput,
            payload_bytes,
            MuxLimits::default(),
        );

        assert!(
            (baseline_eta - inflated_eta).abs() < 0.001,
            "QUIC pacing is carrier send permission, not delivered product throughput"
        );
    }
}

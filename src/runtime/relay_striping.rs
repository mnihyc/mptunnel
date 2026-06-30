use super::bulk_admission::{
    BulkAdmissionCheck, BulkAdmissionRole, bulk_additional_admission_role,
    bulk_candidate_admission_suppression_with_ordering_debt,
};
use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayPathRelease {
    pub(super) key: RelayPathKey,
    pub(super) bytes: usize,
    #[cfg_attr(not(feature = "lab-diagnostics"), allow(dead_code))]
    pub(super) elapsed: Duration,
}

#[derive(Debug, Default)]
pub(super) struct RelayPathFlightLedger {
    flights: BTreeMap<u64, Vec<RelayPathFlight>>,
}

impl RelayPathFlightLedger {
    pub(super) fn record_frame(&mut self, key: RelayPathKey, frame: &Frame) -> usize {
        let Some((offset, end, bytes)) = reliable_stream_frame_extent(frame) else {
            return 0;
        };
        self.flights
            .entry(offset)
            .or_default()
            .push(RelayPathFlight {
                key,
                end,
                bytes,
                sent_at: Instant::now(),
            });
        bytes
    }

    pub(super) fn release_acked_ranges(&mut self, ranges: &[OffsetRange]) -> Vec<RelayPathRelease> {
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }
        let ranges = normalized_offset_ranges(ranges);
        let mut released = Vec::new();
        let mut acked_offsets = Vec::new();
        for range in &ranges {
            for (offset, flights) in self.flights.range(range.start..) {
                if *offset >= range.end {
                    break;
                }
                if flights.iter().any(|flight| range.end >= flight.end) {
                    acked_offsets.push(*offset);
                }
            }
        }
        acked_offsets.sort_unstable();
        acked_offsets.dedup();
        for offset in acked_offsets {
            if let Some(flights) = self.flights.remove(&offset) {
                let now = Instant::now();
                for flight in flights {
                    released.push(RelayPathRelease {
                        key: flight.key,
                        bytes: flight.bytes,
                        elapsed: now.saturating_duration_since(flight.sent_at),
                    });
                }
            }
        }
        released
    }

    pub(super) fn drain_all(&mut self) -> Vec<RelayPathRelease> {
        let mut released = Vec::new();
        for flights in std::mem::take(&mut self.flights).into_values() {
            for flight in flights {
                released.push(RelayPathRelease {
                    key: flight.key,
                    bytes: flight.bytes,
                    elapsed: Instant::now().saturating_duration_since(flight.sent_at),
                });
            }
        }
        released
    }

    pub(super) fn sent_keys_for_frame(&self, frame: &Frame) -> Vec<RelayPathKey> {
        let Some((offset, end, _)) = reliable_stream_frame_extent(frame) else {
            return Vec::new();
        };
        let mut keys = Vec::new();
        if let Some(flights) = self.flights.get(&offset) {
            for flight in flights {
                if flight.end >= end && !keys.contains(&flight.key) {
                    keys.push(flight.key);
                }
            }
        }
        keys
    }

    pub(super) fn latest_unacked_ranges_for_path(&self, key: RelayPathKey) -> Vec<OffsetRange> {
        let mut ranges = Vec::new();
        for (offset, flights) in &self.flights {
            let Some(latest) = flights.last() else {
                continue;
            };
            if latest.key == key {
                ranges.push(OffsetRange {
                    start: *offset,
                    end: latest.end,
                });
            }
        }
        normalized_offset_ranges(&ranges)
    }

    pub(super) fn ordering_debt_bytes_before_offset(&self, key: RelayPathKey, offset: u64) -> u64 {
        self.flights
            .range(..offset)
            .filter_map(|(_, flights)| {
                let latest = flights.last()?;
                (latest.key != key).then_some(latest.bytes as u64)
            })
            .sum()
    }

    pub(super) fn oldest_lower_flight_owner_before_offset(
        &self,
        offset: u64,
    ) -> Option<RelayPathKey> {
        self.flights
            .range(..offset)
            .find_map(|(_, flights)| flights.last().map(|flight| flight.key))
    }
}

pub(super) fn normalized_offset_ranges(ranges: &[OffsetRange]) -> Vec<OffsetRange> {
    let mut ranges = ranges.to_vec();
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<OffsetRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.start >= range.end {
            continue;
        }
        match merged.last_mut() {
            Some(previous) if previous.end >= range.start => {
                previous.end = previous.end.max(range.end);
            }
            _ => merged.push(range),
        }
    }
    merged
}

#[derive(Debug, Clone, Copy)]
struct RelayPathFlight {
    key: RelayPathKey,
    end: u64,
    bytes: usize,
    sent_at: Instant,
}

pub(super) fn reliable_stream_frame_extent(frame: &Frame) -> Option<(u64, u64, usize)> {
    let Frame::StreamData {
        offset, payload, ..
    } = frame
    else {
        return None;
    };
    let bytes = payload.len();
    if bytes == 0 {
        return None;
    }
    let end = offset.saturating_add(bytes as u64);
    Some((*offset, end, bytes))
}

pub(super) fn reliable_stream_frame_payload_bytes(frame: &Frame) -> usize {
    reliable_stream_frame_extent(frame).map_or(1, |(_, _, bytes)| bytes)
}

pub(super) fn relay_lane_is_bulk(lane: FlowLane) -> bool {
    matches!(lane, FlowLane::Throughput | FlowLane::Background)
}

pub(super) fn relay_frame_is_bulk_stream_data(frame: &Frame, lane: FlowLane) -> bool {
    relay_lane_is_bulk(lane) && matches!(frame, Frame::StreamData { .. })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum BulkRelayPathChoice {
    Selected(usize),
    Blocked,
    NotApplicable,
}

pub(super) struct BulkRelayPathRequest<'a> {
    pub(super) context: &'a ClientPathContext,
    pub(super) paths: &'a [TcpRelayRemotePath],
    pub(super) lane: FlowLane,
    pub(super) offset: u64,
    pub(super) payload_bytes: usize,
    pub(super) cursor: usize,
    pub(super) avoid_keys: &'a [RelayPathKey],
    pub(super) path_flights: Option<&'a RelayPathFlightLedger>,
}

pub(super) fn choose_bulk_relay_path_avoiding(
    context: &ClientPathContext,
    paths: &[TcpRelayRemotePath],
    lane: FlowLane,
    frame: &Frame,
    cursor: usize,
    avoid_keys: &[RelayPathKey],
    path_flights: Option<&RelayPathFlightLedger>,
) -> BulkRelayPathChoice {
    let Some((offset, _, payload_bytes)) = reliable_stream_frame_extent(frame) else {
        return BulkRelayPathChoice::NotApplicable;
    };
    choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        context,
        paths,
        lane,
        offset,
        payload_bytes,
        cursor,
        avoid_keys,
        path_flights,
    })
}

pub(super) fn choose_bulk_relay_path_for_extent_avoiding(
    request: BulkRelayPathRequest<'_>,
) -> BulkRelayPathChoice {
    let BulkRelayPathRequest {
        context,
        paths,
        lane,
        offset,
        payload_bytes,
        cursor,
        avoid_keys,
        path_flights,
    } = request;
    if paths.len() <= 1 || !relay_lane_is_bulk(lane) || payload_bytes == 0 {
        return BulkRelayPathChoice::NotApplicable;
    }
    let policy = SchedulerPolicy::default();
    let active_key = paths.last().map(|path| path.key());
    let normal_bulk_send = avoid_keys.is_empty();
    let admitted_bulk_keys = if normal_bulk_send {
        context.ordered_reliable_bulk_striping_path_keys(payload_bytes)
    } else {
        Vec::new()
    };
    let lead_key = admitted_bulk_keys.first().copied().or(active_key);
    let restrict_to_admitted = normal_bulk_send
        && paths
            .iter()
            .any(|path| admitted_bulk_keys.contains(&path.key()));
    let mut best: Option<(usize, f64, usize)> = None;
    for (position, path) in paths.iter().enumerate() {
        let key = path.key();
        if normal_bulk_send && path.placement == RelayPathPlacement::Repair {
            continue;
        }
        if avoid_keys.contains(&path.key())
            && paths.iter().any(|path| !avoid_keys.contains(&path.key()))
        {
            continue;
        }
        if normal_bulk_send {
            if restrict_to_admitted {
                if !admitted_bulk_keys.contains(&key) {
                    continue;
                }
            } else if Some(key) != active_key {
                continue;
            }
        }
        if normal_bulk_send
            && Some(key) != active_key
            && !context.relay_path_has_bulk_model_evidence(key.underlay, key.index)
        {
            continue;
        }
        let Some(snapshot) = relay_path_snapshot_for_bulk_choice(context, key, active_key) else {
            continue;
        };
        let Some(score) = scheduler::score_path(snapshot, lane, payload_bytes, policy) else {
            continue;
        };
        if normal_bulk_send {
            let role = if Some(key) == lead_key {
                BulkAdmissionRole::ActiveDataPath
            } else if let Some(lead_key) = lead_key {
                bulk_additional_admission_role(lead_key.underlay, key.underlay)
            } else {
                BulkAdmissionRole::ActiveDataPath
            };
            let ordering_debt = path_flights
                .map(|flights| flights.ordering_debt_bytes_before_offset(key, offset))
                .unwrap_or(0);
            let ordering_suppression =
                bulk_candidate_admission_suppression_with_ordering_debt(BulkAdmissionCheck {
                    best_snapshot: snapshot,
                    best_eta_ms: score.eta_ms,
                    candidate_snapshot: snapshot,
                    candidate_eta_ms: score.eta_ms,
                    payload_bytes,
                    mux_limits: context.mux_limits,
                    role,
                    stream_ordering_debt_bytes: ordering_debt,
                });
            if let Some(reason) = ordering_suppression {
                #[cfg(not(feature = "lab-diagnostics"))]
                let _ = reason;
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "bulk_striping_candidate_suppressed",
                    format_args!(
                        "path_underlay={:?} path_index={} role={:?} eta_ms={:.3} best_eta_ms={:.3} stream_ordering_debt={} reason={}",
                        key.underlay,
                        key.index,
                        role,
                        score.eta_ms,
                        score.eta_ms,
                        ordering_debt,
                        reason,
                    ),
                );
                continue;
            }
        }
        let cursor_distance = path_cursor_distance(position, cursor, paths.len());
        match best {
            None => best = Some((position, score.eta_ms, cursor_distance)),
            Some((_, best_eta, best_distance)) => {
                if score.eta_ms < best_eta
                    || (score.eta_ms == best_eta && cursor_distance < best_distance)
                {
                    best = Some((position, score.eta_ms, cursor_distance));
                }
            }
        }
    }
    if let Some((position, _, _)) = best {
        return BulkRelayPathChoice::Selected(position);
    }
    if !normal_bulk_send {
        return BulkRelayPathChoice::NotApplicable;
    }
    if let Some(owner) =
        path_flights.and_then(|flights| flights.oldest_lower_flight_owner_before_offset(offset))
        && let Some(position) = paths.iter().position(|path| {
            path.key() == owner
                && path.placement != RelayPathPlacement::Repair
                && !avoid_keys.contains(&owner)
        })
    {
        return BulkRelayPathChoice::Selected(position);
    }
    BulkRelayPathChoice::Blocked
}

pub(super) fn bulk_relay_send_ready_for_extent(
    context: &ClientPathContext,
    paths: &[TcpRelayRemotePath],
    lane: FlowLane,
    offset: u64,
    payload_bytes: usize,
    cursor: usize,
    path_flights: &RelayPathFlightLedger,
) -> bool {
    match choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
        context,
        paths,
        lane,
        offset,
        payload_bytes,
        cursor,
        avoid_keys: &[],
        path_flights: Some(path_flights),
    }) {
        BulkRelayPathChoice::Selected(_) | BulkRelayPathChoice::NotApplicable => true,
        BulkRelayPathChoice::Blocked => false,
    }
}

fn relay_path_snapshot_for_bulk_choice(
    context: &ClientPathContext,
    key: RelayPathKey,
    active_key: Option<RelayPathKey>,
) -> Option<PathSnapshot> {
    let mut snapshot = relay_path_snapshot(context, key)?;
    if Some(key) != active_key {
        snapshot.active_flows = snapshot.active_flows.saturating_add(1);
    }
    Some(snapshot)
}

fn path_cursor_distance(position: usize, cursor: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SharedSecret;

    fn security() -> SecurityConfig {
        SecurityConfig::encrypted(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        )
    }

    fn context(paths: &[&str]) -> ClientPathContext {
        ClientPathContext::new(
            paths
                .iter()
                .map(|path| path.parse::<PathSpec>().expect("path spec"))
                .collect(),
            security(),
            ResourceLimits::default(),
        )
        .expect("context")
    }

    fn data_frame(offset: u64, len: usize) -> Frame {
        Frame::StreamData {
            stream_id: StreamId(7),
            offset,
            flags: StreamFlags::NONE,
            payload: Bytes::from(vec![0x5a; len]),
        }
    }

    fn relay_path(
        underlay: UnderlayProtocol,
        index: usize,
        placement: RelayPathPlacement,
    ) -> TcpRelayRemotePath {
        let (commands, _receivers) = tcp_path_session_command_channels(8);
        TcpRelayRemotePath {
            path_index: index,
            instance_id: index as u64 + 1,
            placement,
            stream: TcpPathStreamHandle {
                stream_id: StreamId(7),
                max_offset: u64::MAX,
                lane: FlowLane::Throughput,
                underlay,
                max_frame_payload_bytes: 64 * 1024,
                output: TcpPathStreamOutput::Fixed(commands),
            },
        }
    }

    #[test]
    fn ordering_debt_counts_lower_bytes_owned_by_other_paths() {
        let path0 = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 0,
        };
        let path1 = RelayPathKey {
            underlay: UnderlayProtocol::Udp,
            index: 1,
        };
        let path2 = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_frame(path0, &data_frame(0, 4096));
        ledger.record_frame(path1, &data_frame(4096, 4096));

        assert_eq!(ledger.ordering_debt_bytes_before_offset(path0, 8192), 4096);
        assert_eq!(ledger.ordering_debt_bytes_before_offset(path1, 8192), 4096);
        assert_eq!(ledger.ordering_debt_bytes_before_offset(path2, 8192), 8192);
        assert_eq!(
            ledger.oldest_lower_flight_owner_before_offset(8192),
            Some(path0)
        );
    }

    #[test]
    fn bulk_ready_blocks_when_no_attached_path_can_advance_ordered_frontier() {
        let context = context(&[
            "tcp://127.0.0.1:10100?srtt-ms=50&rate-mbps=1",
            "udp://127.0.0.1:10101?srtt-ms=50&rate-mbps=1",
        ]);
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        ];
        let missing_owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 1,
        };
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_frame(missing_owner, &data_frame(0, 64 * 1024));

        assert!(!bulk_relay_send_ready_for_extent(
            &context,
            &paths,
            FlowLane::Throughput,
            64 * 1024,
            64 * 1024,
            0,
            &ledger,
        ));
        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 0,
                avoid_keys: &[],
                path_flights: Some(&ledger),
            }),
            BulkRelayPathChoice::Blocked
        );
    }

    #[test]
    fn bulk_admission_prefers_lower_flight_owner_over_expanding_active_debt() {
        let context = context(&[
            "tcp://127.0.0.1:10110?srtt-ms=50&rate-mbps=1",
            "udp://127.0.0.1:10111?srtt-ms=50&rate-mbps=1",
        ]);
        let lower_owner = RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index: 0,
        };
        let paths = vec![
            relay_path(UnderlayProtocol::Tcp, 0, RelayPathPlacement::Validation),
            relay_path(UnderlayProtocol::Udp, 0, RelayPathPlacement::Active),
        ];
        let mut ledger = RelayPathFlightLedger::default();
        ledger.record_frame(lower_owner, &data_frame(0, 64 * 1024));

        assert_eq!(
            choose_bulk_relay_path_for_extent_avoiding(BulkRelayPathRequest {
                context: &context,
                paths: &paths,
                lane: FlowLane::Throughput,
                offset: 64 * 1024,
                payload_bytes: 64 * 1024,
                cursor: 0,
                avoid_keys: &[],
                path_flights: Some(&ledger),
            }),
            BulkRelayPathChoice::Selected(0)
        );
    }
}

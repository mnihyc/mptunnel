use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy)]
pub(super) struct RelayPathRelease {
    pub(super) key: RelayPathKey,
    pub(super) bytes: usize,
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
            .push(RelayPathFlight { key, end, bytes });
        bytes
    }

    pub(super) fn release_acked_ranges(&mut self, ranges: &[OffsetRange]) -> Vec<RelayPathRelease> {
        if ranges.is_empty() || self.flights.is_empty() {
            return Vec::new();
        }
        let mut released = Vec::new();
        let acked_offsets = self
            .flights
            .iter()
            .filter_map(|(offset, flights)| {
                flights
                    .iter()
                    .any(|flight| {
                        ranges
                            .iter()
                            .any(|range| range.start <= *offset && range.end >= flight.end)
                    })
                    .then_some(*offset)
            })
            .collect::<Vec<_>>();
        for offset in acked_offsets {
            if let Some(flights) = self.flights.remove(&offset) {
                for flight in flights {
                    released.push(RelayPathRelease {
                        key: flight.key,
                        bytes: flight.bytes,
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
                });
            }
        }
        released
    }
}

#[derive(Debug, Clone, Copy)]
struct RelayPathFlight {
    key: RelayPathKey,
    end: u64,
    bytes: usize,
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

pub(super) fn choose_bulk_relay_path(
    context: &ClientPathContext,
    paths: &[TcpRelayRemotePath],
    lane: FlowLane,
    frame: &Frame,
    cursor: usize,
) -> Option<usize> {
    if paths.len() <= 1 || !relay_frame_is_bulk_stream_data(frame, lane) {
        return None;
    }
    let payload_bytes = reliable_stream_frame_payload_bytes(frame);
    let policy = SchedulerPolicy::default();
    let mut best: Option<(usize, f64, usize)> = None;
    for (position, path) in paths.iter().enumerate() {
        let snapshot = relay_path_snapshot(context, path.key())?;
        let score = scheduler::score_path(snapshot, lane, payload_bytes, policy)?;
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
    best.map(|(position, _, _)| position)
}

fn path_cursor_distance(position: usize, cursor: usize, len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    position.wrapping_add(len).wrapping_sub(cursor % len) % len
}

//! Opt-in, bounded evidence for the native-IP request/reply test.
//!
//! This module is compiled only in tests. It observes one outgoing and one
//! incoming packet, never retains payloads, and performs no I/O while recording.

use super::{IpPacketId, IpTunnelId, MAX_NATIVE_FRAGMENTS};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// One send geometry, send completion and receive completion, plus at most one
// acceptance per outgoing fragment and ingress/insert check/outcome/assembly expiry
// per incoming fragment. Repeated IDs or unexpected extra events are counted
// as overflow, so a truncated observation cannot masquerade as complete history.
const EVENT_LIMIT: usize = 3 + 5 * MAX_NATIVE_FRAGMENTS;

#[derive(Clone, Debug)]
pub(crate) struct NativeIpTestObservation {
    started: Instant,
    request_stream_id: u64,
    tunnel_id: IpTunnelId,
    send_packet_id: IpPacketId,
    receive_packet_id: IpPacketId,
    data: Arc<Mutex<IpObservationSnapshot>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct IpObservationSnapshot {
    pub events: Vec<TimedIpEvent>,
    pub overflow: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct TimedIpEvent {
    pub elapsed: Duration,
    pub packet_id: IpPacketId,
    pub kind: IpEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IpEventKind {
    Geometry {
        count: usize,
        fragment_bytes: usize,
        total_bytes: usize,
    },
    Accepted {
        index: usize,
    },
    SendComplete,
    Ingress {
        index: usize,
        count: usize,
        total_bytes: usize,
        deadline: Duration,
    },
    InsertCheck {
        index: usize,
        deadline: Duration,
    },
    Inserted {
        index: usize,
        parts: u64,
        bytes: usize,
    },
    Rejected {
        index: usize,
        reason: IpRejectReason,
    },
    AssemblyExpired {
        parts: u64,
        bytes: usize,
        deadline: Duration,
    },
    Complete {
        bytes: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IpRejectReason {
    Expired,
    NoCapacity,
    ShapeMismatch,
    InvalidIndex,
    BadLength,
}

impl NativeIpTestObservation {
    pub(crate) fn new(
        started: Instant,
        request_stream_id: u64,
        tunnel_id: IpTunnelId,
        send_packet_id: IpPacketId,
        receive_packet_id: IpPacketId,
    ) -> Self {
        Self {
            started,
            request_stream_id,
            tunnel_id,
            send_packet_id,
            receive_packet_id,
            data: Arc::new(Mutex::new(IpObservationSnapshot {
                events: Vec::with_capacity(EVENT_LIMIT),
                overflow: 0,
            })),
        }
    }

    pub(crate) fn elapsed(&self, at: Instant) -> Duration {
        at.saturating_duration_since(self.started)
    }

    pub(crate) fn record(
        &self,
        request_stream_id: u64,
        tunnel_id: IpTunnelId,
        packet_id: IpPacketId,
        at: Instant,
        kind: IpEventKind,
    ) {
        if request_stream_id != self.request_stream_id
            || tunnel_id != self.tunnel_id
            || (packet_id != self.send_packet_id && packet_id != self.receive_packet_id)
        {
            return;
        }
        let mut data = self.data.lock().expect("native IP test observation");
        if data.events.len() == EVENT_LIMIT {
            data.overflow = data.overflow.saturating_add(1);
            return;
        }
        data.events.push(TimedIpEvent {
            elapsed: self.elapsed(at),
            packet_id,
            kind,
        });
    }

    pub(crate) fn snapshot(&self) -> IpObservationSnapshot {
        self.data
            .lock()
            .expect("native IP test observation")
            .clone()
    }

    pub(crate) fn report(&self) {
        let data = self.snapshot();
        eprintln!(
            "native IP observation: request={} tunnel={:?} send={:?} receive={:?} events={} overflow={}",
            self.request_stream_id,
            self.tunnel_id,
            self.send_packet_id,
            self.receive_packet_id,
            data.events.len(),
            data.overflow
        );
        for event in data.events {
            eprintln!(
                "  +{:?} packet={:?} {:?}",
                event.elapsed, event.packet_id, event.kind
            );
        }
    }
}

#[test]
fn observation_filters_identity_and_reports_bounded_overflow() {
    let now = Instant::now();
    let observation =
        NativeIpTestObservation::new(now, 4, IpTunnelId(7), IpPacketId(11), IpPacketId(12));
    for (request, tunnel, packet) in [(0, 7, 11), (4, 8, 11), (4, 7, 13)] {
        observation.record(
            request,
            IpTunnelId(tunnel),
            IpPacketId(packet),
            now,
            IpEventKind::SendComplete,
        );
    }
    assert!(observation.snapshot().events.is_empty());
    for _ in 0..EVENT_LIMIT + 1 {
        observation.record(
            4,
            IpTunnelId(7),
            IpPacketId(11),
            now,
            IpEventKind::SendComplete,
        );
    }
    let snapshot = observation.snapshot();
    assert_eq!(snapshot.events.len(), EVENT_LIMIT);
    assert_eq!(snapshot.overflow, 1);
}

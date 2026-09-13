//! Calibrate observations against actual version2 decode, insertion and expiry.
//!
//! Ingress is supplied at the router helper boundary, not through a socket. To
//! exercise expiry deterministically, only a fixture's already-inserted deadline
//! is moved into the past. These tests prove event/resource distinctions, not
//! elapsed-time reachability, native delivery or the historical macOS cause.

use super::*;

const REQUEST: u64 = 0;
const TUNNEL: IpTunnelId = IpTunnelId(7);
const PARTIAL_PACKET: IpPacketId = IpPacketId(11);
const FRESH_PACKET: IpPacketId = IpPacketId(12);

struct IpCalibration {
    state: Arc<HubState>,
    receiver: NativeDatagramReceiver,
    tx: Option<mpsc::Sender<BudgetedPacket>>,
}

impl IpCalibration {
    fn new() -> Self {
        let state = Arc::new(HubState {
            routing: Mutex::new(RoutingTable::default()),
            next_generation: AtomicU64::new(1),
            buffered_bytes: Arc::new(AtomicUsize::new(0)),
            // Four one-byte fragments and two assembly identities fit without
            // activating a capacity refusal in these branch calibrations.
            max_buffered_bytes: 4 * (NATIVE_IP_FRAGMENT_HEADER_BYTES + 1),
            max_routes: 1,
            max_pending_packets_per_route: 4,
            active_reassemblies: AtomicUsize::new(0),
            max_active_reassemblies: 2,
            dropped_packets: AtomicU64::new(0),
            ip_observation: Arc::new(std::sync::OnceLock::new()),
        });
        assert!(
            state
                .ip_observation
                .set(NativeIpTestObservation::new(
                    Instant::now() - Duration::from_secs(2),
                    REQUEST,
                    TUNNEL,
                    PARTIAL_PACKET,
                    FRESH_PACKET,
                ))
                .is_ok()
        );
        let (tx, rx) = mpsc::channel(4);
        Self {
            receiver: NativeDatagramReceiver {
                request_stream_id: REQUEST,
                generation: 1,
                state: state.clone(),
                rx,
                ip_rx: None,
                reassemblies: HashMap::new(),
                ip_reassemblies: HashMap::new(),
            },
            state,
            tx: Some(tx),
        }
    }

    fn fragment(
        &self,
        packet_id: IpPacketId,
        index: u16,
        received_at: Instant,
        deadline: Instant,
    ) -> BudgetedPacket {
        let mut bytes = Vec::with_capacity(NATIVE_IP_FRAGMENT_HEADER_BYTES + 1);
        bytes.push(NATIVE_IP_PACKET_VERSION);
        bytes.extend_from_slice(&TUNNEL.0.to_be_bytes());
        bytes.extend_from_slice(&packet_id.0.to_be_bytes());
        bytes.extend_from_slice(&index.to_be_bytes());
        bytes.extend_from_slice(&2_u16.to_be_bytes());
        bytes.extend_from_slice(&2_u32.to_be_bytes());
        bytes.push(b'a' + u8::try_from(index).expect("two-fragment index"));
        let bytes = Bytes::from(bytes);
        assert!(reserve_buffered_bytes(&self.state, bytes.len()));
        observe_native_ip_ingress(&self.state, REQUEST, &bytes, received_at, Some(deadline));
        BudgetedPacket {
            bytes,
            buffered_bytes: self.state.buffered_bytes.clone(),
            received_at,
            ip_deadline: Some(deadline),
        }
    }

    fn insert_live(&mut self, packet_id: IpPacketId, index: u16) {
        let received_at = Instant::now();
        // No sleep uses this duration: it keeps the insertion live under test
        // scheduling. Expiry below is explicit fixture-state calibration.
        let deadline = received_at + Duration::from_secs(3_600);
        let packet = self.fragment(packet_id, index, received_at, deadline);
        let fragment = decode_ip_fragment(packet, CodecLimits::default())
            .expect("decode actual version2 envelope");
        assert!(self.receiver.insert_ip_fragment(fragment).is_none());
        assert!(
            self.receiver
                .ip_reassemblies
                .contains_key(&(TUNNEL, packet_id))
        );
    }

    fn expire_inserted(&mut self, packet_id: IpPacketId) {
        let entry = self
            .receiver
            .ip_reassemblies
            .get_mut(&(TUNNEL, packet_id))
            .expect("real insertion precedes deadline calibration");
        entry.deadline = Instant::now() - Duration::from_secs(1);
        self.receiver.expire_reassemblies();
        assert!(
            !self
                .receiver
                .ip_reassemblies
                .contains_key(&(TUNNEL, packet_id))
        );
    }

    fn assert_released(&self) {
        assert!(self.receiver.ip_reassemblies.is_empty());
        assert_eq!(self.state.active_reassemblies.load(Ordering::Acquire), 0);
        assert_eq!(self.state.buffered_bytes.load(Ordering::Acquire), 0);
    }
}

#[tokio::test]
async fn native_ip_observation_calibrates_all_arrived_but_expired() {
    let mut fixture = IpCalibration::new();
    let received_at = Instant::now() - Duration::from_secs(1);
    let deadline = received_at + Duration::from_millis(25);
    for index in 0..2 {
        let packet = fixture.fragment(PARTIAL_PACKET, index, received_at, deadline);
        fixture.tx.as_ref().unwrap().try_send(packet).unwrap();
    }
    drop(fixture.tx.take());
    assert!(matches!(
        fixture.receiver.recv_frame(CodecLimits::default()).await,
        Err(QuicCarrierError::H3DriverClosed)
    ));
    fixture.assert_released();

    let snapshot = fixture.state.ip_observation.get().unwrap().snapshot();
    assert_eq!(snapshot.overflow, 0);
    for index in 0..2 {
        let (ingress_at, ingress_deadline) = snapshot
            .events
            .iter()
            .find_map(|event| match &event.kind {
                IpEventKind::Ingress {
                    index: actual,
                    count: 2,
                    total_bytes: 2,
                    deadline,
                } if event.packet_id == PARTIAL_PACKET && *actual == index => {
                    Some((event.elapsed, *deadline))
                }
                _ => None,
            })
            .expect("fragment ingress observation");
        let (insert_check_at, insert_check_deadline) = snapshot
            .events
            .iter()
            .find_map(|event| match &event.kind {
                IpEventKind::InsertCheck {
                    index: actual,
                    deadline,
                } if event.packet_id == PARTIAL_PACKET && *actual == index => {
                    Some((event.elapsed, *deadline))
                }
                _ => None,
            })
            .expect("fragment consumption observation");
        let rejected_at = snapshot
            .events
            .iter()
            .find_map(|event| match &event.kind {
                IpEventKind::Rejected {
                    index: actual,
                    reason: IpRejectReason::Expired,
                } if event.packet_id == PARTIAL_PACKET && *actual == index => Some(event.elapsed),
                _ => None,
            })
            .expect("fragment expiry rejection observation");
        assert_eq!(ingress_deadline, insert_check_deadline);
        assert!(ingress_at < ingress_deadline);
        assert!(ingress_deadline <= insert_check_at);
        assert_eq!(insert_check_at, rejected_at);
    }
    assert!(!snapshot.events.iter().any(|event| matches!(
        &event.kind,
        IpEventKind::Inserted { .. }
            | IpEventKind::AssemblyExpired { .. }
            | IpEventKind::Complete { .. }
    )));
}

#[test]
fn native_ip_observation_calibrates_missing_fragment_and_assembly_expiry() {
    let mut fixture = IpCalibration::new();
    fixture.insert_live(PARTIAL_PACKET, 0);
    assert_eq!(fixture.state.active_reassemblies.load(Ordering::Acquire), 1);
    assert_eq!(
        fixture.state.buffered_bytes.load(Ordering::Acquire),
        NATIVE_IP_FRAGMENT_HEADER_BYTES + 1
    );
    fixture.expire_inserted(PARTIAL_PACKET);
    fixture.assert_released();

    let snapshot = fixture.state.ip_observation.get().unwrap().snapshot();
    assert_eq!(snapshot.overflow, 0);
    assert!(snapshot.events.iter().any(|event| matches!(
        &event.kind,
        IpEventKind::Inserted {
            index: 0,
            parts: 1,
            bytes: 1
        }
    )));
    assert!(snapshot.events.iter().any(|event| matches!(
        &event.kind,
        IpEventKind::AssemblyExpired {
            parts: 1,
            bytes: 1,
            ..
        }
    )));
    assert!(!snapshot.events.iter().any(|event| matches!(
        &event.kind,
        IpEventKind::Ingress { index: 1, .. }
            | IpEventKind::Rejected { .. }
            | IpEventKind::Complete { .. }
    )));
}

#[tokio::test]
async fn native_ip_observation_keeps_late_cycles_separate_and_fresh_packet_independent() {
    let mut fixture = IpCalibration::new();
    fixture.insert_live(PARTIAL_PACKET, 0);
    fixture.expire_inserted(PARTIAL_PACKET);
    fixture.assert_released();

    // The other fragment arrives after the old assembly is gone. It creates a
    // new incomplete cycle; masks from both cycles must not imply completion.
    fixture.insert_live(PARTIAL_PACKET, 1);
    let late = fixture
        .receiver
        .ip_reassemblies
        .get(&(TUNNEL, PARTIAL_PACKET))
        .unwrap();
    assert!(late.parts[0].is_none());
    assert!(late.parts[1].is_some());
    assert_eq!(late.received_len, 1);

    let received_at = Instant::now();
    let deadline = received_at + Duration::from_secs(3_600);
    for index in 0..2 {
        let packet = fixture.fragment(FRESH_PACKET, index, received_at, deadline);
        fixture.tx.as_ref().unwrap().try_send(packet).unwrap();
    }
    let complete = fixture
        .receiver
        .recv_frame(CodecLimits::default())
        .await
        .unwrap();
    assert_eq!(
        complete.frame,
        Frame::IpPacket {
            tunnel_id: TUNNEL,
            packet_id: FRESH_PACKET,
            payload: Bytes::from_static(b"ab"),
        }
    );
    assert_eq!(fixture.receiver.ip_reassemblies.len(), 1);
    fixture.expire_inserted(PARTIAL_PACKET);
    fixture.assert_released();

    let snapshot = fixture.state.ip_observation.get().unwrap().snapshot();
    assert_eq!(snapshot.overflow, 0);
    let inserted_masks: Vec<_> = snapshot
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            IpEventKind::Inserted { parts, .. } if event.packet_id == PARTIAL_PACKET => {
                Some(*parts)
            }
            _ => None,
        })
        .collect();
    assert_eq!(inserted_masks, [1, 2]);
    let expired_masks: Vec<_> = snapshot
        .events
        .iter()
        .filter_map(|event| match &event.kind {
            IpEventKind::AssemblyExpired { parts, .. } if event.packet_id == PARTIAL_PACKET => {
                Some(*parts)
            }
            _ => None,
        })
        .collect();
    assert_eq!(expired_masks, [1, 2]);
    let completed_ids: Vec<_> = snapshot
        .events
        .iter()
        .filter_map(|event| {
            matches!(&event.kind, IpEventKind::Complete { bytes: 2 }).then_some(event.packet_id)
        })
        .collect();
    assert_eq!(completed_ids, [FRESH_PACKET]);
}

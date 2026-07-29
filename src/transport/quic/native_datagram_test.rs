use super::*;

fn budgeted(bytes: Bytes) -> BudgetedPacket {
    let buffered_bytes = Arc::new(AtomicUsize::new(bytes.len()));
    BudgetedPacket {
        bytes,
        buffered_bytes,
        received_at: Instant::now(),
    }
}

#[test]
fn rfc9297_quarter_stream_id_round_trips_at_every_varint_width() {
    for request_stream_id in [
        0,
        4 * 63,
        4 * 64,
        4 * 16_383,
        4 * 16_384,
        4 * 1_073_741_823,
        4 * 1_073_741_824,
    ] {
        let mut encoded = Vec::new();
        encode_varint(request_stream_id / 4, &mut encoded).expect("encode Quarter Stream ID");
        encoded.extend_from_slice(b"payload");
        let packet = Bytes::from(encoded);
        let (decoded, consumed) =
            decode_quarter_stream_id(&packet).expect("decode Quarter Stream ID");
        assert_eq!(decoded, request_stream_id);
        assert_eq!(&packet[consumed..], b"payload");
    }
}

#[test]
fn rfc9297_quarter_stream_id_enforces_legal_range_and_complete_encoding() {
    let mut maximum = Vec::new();
    encode_varint(MAX_QUARTER_STREAM_ID, &mut maximum).expect("encode maximum legal QSID");
    assert_eq!(
        decode_quarter_stream_id(&Bytes::from(maximum)),
        Ok((MAX_QUARTER_STREAM_ID << 2, 8))
    );

    assert_eq!(
        decode_quarter_stream_id(&Bytes::from_static(&[0x40])),
        Err(QuarterStreamIdError::Truncated)
    );
    let oversized = ((0b11_u64 << 62) | (1_u64 << 60)).to_be_bytes();
    assert_eq!(
        decode_quarter_stream_id(&Bytes::copy_from_slice(&oversized)),
        Err(QuarterStreamIdError::OutOfRange)
    );
}

#[test]
fn native_fragment_contract_is_compact_and_rejects_unbounded_metadata() {
    assert_eq!(NATIVE_FRAGMENT_HEADER_BYTES, 29);

    let mut zero = Vec::new();
    zero.push(NATIVE_DATAGRAM_VERSION);
    zero.extend_from_slice(&7_u64.to_be_bytes());
    zero.extend_from_slice(&11_u64.to_be_bytes());
    zero.extend_from_slice(&1_000_u32.to_be_bytes());
    zero.extend_from_slice(&0_u16.to_be_bytes());
    zero.extend_from_slice(&1_u16.to_be_bytes());
    zero.extend_from_slice(&0_u32.to_be_bytes());
    let fragment =
        decode_fragment(budgeted(Bytes::from(zero)), CodecLimits::default()).expect("zero UDP");
    assert_eq!(fragment.flow_id, DatagramFlowId(7));
    assert_eq!(fragment.datagram_id, DatagramId(11));
    assert_eq!(fragment.count, 1);
    assert_eq!(fragment.total_len, 0);

    let mut excessive = Vec::new();
    excessive.push(NATIVE_DATAGRAM_VERSION);
    excessive.extend_from_slice(&7_u64.to_be_bytes());
    excessive.extend_from_slice(&11_u64.to_be_bytes());
    excessive.extend_from_slice(&1_000_u32.to_be_bytes());
    excessive.extend_from_slice(&0_u16.to_be_bytes());
    excessive.extend_from_slice(&((MAX_NATIVE_FRAGMENTS + 1) as u16).to_be_bytes());
    excessive.extend_from_slice(&65_u32.to_be_bytes());
    excessive.push(1);
    assert!(decode_fragment(budgeted(Bytes::from(excessive)), CodecLimits::default()).is_err());
}

#[tokio::test]
async fn incomplete_native_reassembly_releases_budget_without_a_later_packet() {
    let buffered_bytes = Arc::new(AtomicUsize::new(0));
    let state = Arc::new(HubState {
        routing: Mutex::new(RoutingTable::default()),
        next_generation: AtomicU64::new(1),
        buffered_bytes: buffered_bytes.clone(),
        max_buffered_bytes: 1_024,
        max_routes: 1,
        max_pending_packets_per_route: 1,
        active_reassemblies: AtomicUsize::new(0),
        max_active_reassemblies: 1,
        dropped_packets: AtomicU64::new(0),
    });
    let (tx, rx) = mpsc::channel(1);
    let mut receiver = NativeDatagramReceiver {
        request_stream_id: 0,
        generation: 1,
        state: state.clone(),
        rx,
        reassemblies: HashMap::new(),
    };

    let mut first_fragment = Vec::new();
    first_fragment.push(NATIVE_DATAGRAM_VERSION);
    first_fragment.extend_from_slice(&7_u64.to_be_bytes());
    first_fragment.extend_from_slice(&11_u64.to_be_bytes());
    first_fragment.extend_from_slice(&20_u32.to_be_bytes());
    first_fragment.extend_from_slice(&0_u16.to_be_bytes());
    first_fragment.extend_from_slice(&2_u16.to_be_bytes());
    first_fragment.extend_from_slice(&2_u32.to_be_bytes());
    first_fragment.push(1);
    let first_fragment = Bytes::from(first_fragment);
    buffered_bytes.store(first_fragment.len(), Ordering::Release);
    tx.send(BudgetedPacket {
        bytes: first_fragment,
        buffered_bytes: buffered_bytes.clone(),
        received_at: Instant::now(),
    })
    .await
    .expect("queue incomplete fragment");

    // Keep the channel open: expiry itself, rather than closure or another
    // packet, must wake the receiver and release both byte and assembly caps.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(60),
            receiver.recv_frame(CodecLimits::default())
        )
        .await
        .is_err()
    );
    assert!(receiver.reassemblies.is_empty());
    assert_eq!(state.active_reassemblies.load(Ordering::Acquire), 0);
    assert_eq!(buffered_bytes.load(Ordering::Acquire), 0);
    drop(tx);
}

#[tokio::test]
async fn unregistered_route_packet_expires_under_global_bounds_without_traffic() {
    let packet_bytes = Bytes::from(vec![0_u8; NATIVE_FRAGMENT_HEADER_BYTES]);
    let buffered_bytes = Arc::new(AtomicUsize::new(packet_bytes.len()));
    let state = HubState {
        routing: Mutex::new(RoutingTable::default()),
        next_generation: AtomicU64::new(1),
        buffered_bytes: buffered_bytes.clone(),
        max_buffered_bytes: 1_024,
        max_routes: 1,
        max_pending_packets_per_route: 1,
        active_reassemblies: AtomicUsize::new(0),
        max_active_reassemblies: 1,
        dropped_packets: AtomicU64::new(0),
    };
    let deadline = Instant::now() + Duration::from_millis(10);
    state.routing.lock().expect("routing table").pending.insert(
        4,
        VecDeque::from([PendingPacket {
            deadline,
            packet: BudgetedPacket {
                bytes: packet_bytes,
                buffered_bytes: buffered_bytes.clone(),
                received_at: Instant::now(),
            },
        }]),
    );

    assert_eq!(next_pending_route_expiry(&state), Some(deadline));
    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
    expire_pending_routes(&state, Instant::now());
    assert!(
        state
            .routing
            .lock()
            .expect("routing table")
            .pending
            .is_empty()
    );
    assert_eq!(buffered_bytes.load(Ordering::Acquire), 0);
    assert_eq!(state.dropped_packets.load(Ordering::Acquire), 1);
}

#[test]
fn resolved_request_evicts_pending_route_without_exceeding_global_route_cap() {
    let packet_len = NATIVE_FRAGMENT_HEADER_BYTES;
    let buffered_bytes = Arc::new(AtomicUsize::new(packet_len * 2));
    let state = HubState {
        routing: Mutex::new(RoutingTable::default()),
        next_generation: AtomicU64::new(1),
        buffered_bytes: buffered_bytes.clone(),
        max_buffered_bytes: packet_len * 2,
        max_routes: 2,
        max_pending_packets_per_route: 1,
        active_reassemblies: AtomicUsize::new(0),
        max_active_reassemblies: 1,
        dropped_packets: AtomicU64::new(0),
    };
    let now = Instant::now();
    let mut routing = state.routing.lock().expect("routing table");
    for (request_stream_id, wait) in [(4_u64, 10_u64), (8, 20)] {
        routing.pending.insert(
            request_stream_id,
            VecDeque::from([PendingPacket {
                deadline: now + Duration::from_millis(wait),
                packet: BudgetedPacket {
                    bytes: Bytes::from(vec![0_u8; packet_len]),
                    buffered_bytes: buffered_bytes.clone(),
                    received_at: now,
                },
            }]),
        );
    }

    assert!(make_room_for_active_route(&state, &mut routing));
    assert_eq!(routing.active.len() + routing.pending.len(), 1);
    assert!(!routing.pending.contains_key(&4));
    assert!(routing.pending.contains_key(&8));
    assert_eq!(state.dropped_packets.load(Ordering::Acquire), 1);
    assert_eq!(buffered_bytes.load(Ordering::Acquire), packet_len);

    let (tx, _rx) = mpsc::channel(1);
    routing.active.insert(12, Route { generation: 1, tx });
    assert_eq!(
        routing.active.len() + routing.pending.len(),
        state.max_routes
    );
    routing.pending.clear();
    drop(routing);
    assert_eq!(buffered_bytes.load(Ordering::Acquire), 0);
}

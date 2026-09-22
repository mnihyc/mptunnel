//! Exact-capacity counterexample for advisory registry/Native publication lag.
use super::*;
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::runtime::path::authority::{
    NativeCarrierRateAuthorityHandle, NativeCarrierSchedulingShapeSnapshot,
};
use crate::runtime::path::quic::ip_tunnel::NativePacketTestQueue;

struct Fixture {
    _context: crate::runtime::path::ServerPathContext,
    registration: ServerCarrierPathRegistration,
    port: ServerIpTunnelPort,
    device: ServerIpTunnelDevice,
    _attachment: AcceptedServerIpTunnel,
    authority: Arc<NativeCarrierRateAuthorityHandle>,
    queue: NativePacketTestQueue,
    scope: CarrierRateAuthorityScope,
    rate: u128,
    principal: PrincipalId,
}

impl Fixture {
    fn new() -> Self {
        let (context, security) = super::super::tests::server_context();
        let registration = context.reliable_streams.register_test_carrier_path(
            SessionId(91),
            UnderlayProtocol::Udp,
            PathId(0),
            crate::runtime::path::ServerLocalPathProperties::default(),
        );
        let scope = CarrierRateAuthorityScope::new(
            registration.path_instance_id(),
            PathMetricDirection::ServerToClient,
        );
        let rate = 80_000_000;
        let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
            scope,
            rate as u64,
            1,
            7,
            Some(rate),
        )
        .unwrap();
        let initial = authority
            .refresh_scheduling_shape_for_test(
                scope,
                1,
                7,
                Some(rate),
                Duration::from_millis(100),
                Duration::from_millis(10),
                30_180,
                10_017,
                1_400,
                Some(rate as u64),
                false,
            )
            .unwrap();
        assert!(
            authority
                .commit_if_current(initial.stamp(), || context
                    .reliable_streams
                    .stage_native_scheduling_shape(&registration, initial))
                .unwrap()
        );
        let queue = NativePacketTestQueue::new(authority.clone());
        let (port, device) = ServerIpTunnelService::build(
            super::super::tests::plan(&security),
            context.reliable_streams.clone(),
            4,
            16 * 1_500,
            context.session_retention_timeout,
        );
        let attachment = port
            .open(ServerIpTunnelOpenRequest {
                tunnel_id: IpTunnelId(91),
                path: &registration,
                carrier: queue.carrier(),
            })
            .unwrap();
        Self {
            _context: context,
            registration,
            port,
            device,
            _attachment: attachment,
            authority,
            queue,
            scope,
            rate,
            principal: PrincipalId::parse("test-peer").unwrap(),
        }
    }

    fn publish_rate(&mut self, rate: u128) {
        self.authority
            .publish_observation_for_test(1, 7, Some(rate))
            .unwrap();
        self.rate = rate;
    }

    fn shape(&self, cwnd: u64, flight: u64) -> NativeCarrierSchedulingShapeSnapshot {
        self.authority
            .refresh_scheduling_shape_for_test(
                self.scope,
                1,
                7,
                Some(self.rate),
                Duration::from_millis(100),
                Duration::from_millis(10),
                cwnd,
                flight,
                1_400,
                Some(self.rate as u64),
                false,
            )
            .unwrap()
    }

    fn plan(&self, packet: &Bytes) -> ServerIpDispatchPlan {
        let flow = parse_ip_packet(packet).unwrap().flow_key;
        let (capture, expiry) =
            capture_server_ip_dispatch(&self.device.inner, &self.principal, &flow, Instant::now());
        assert!(expiry.is_none());
        match select_server_carrier(&self.device.inner.paths, capture.unwrap(), packet.len()) {
            ServerIpDispatchSelection::Selected(plan) => plan,
            _ => panic!("fixture must have a current selected Native plan"),
        }
    }

    fn apply(&self, packet: Bytes, plan: ServerIpDispatchPlan) -> ServerIpDispatchOutcome {
        let flow = parse_ip_packet(&packet).unwrap().flow_key;
        let packet_id = IpPacketId(
            self.device
                .inner
                .next_packet_id
                .fetch_add(1, Ordering::Relaxed),
        );
        let permit = self
            .device
            .inner
            .carrier_packet_budget
            .try_reserve(packet.len())
            .unwrap();
        apply_server_ip_dispatch(
            &self.device.inner,
            &self.principal,
            &flow,
            packet_id,
            packet,
            plan,
            Some(permit),
        )
        .unwrap()
        .outcome
    }

    fn is_bound(&self, packet: &Bytes) -> bool {
        let flow = parse_ip_packet(packet).unwrap().flow_key;
        self.device
            .inner
            .state
            .lock()
            .unwrap()
            .tunnels
            .get_mut(&self.principal)
            .and_then(|tunnel| {
                tunnel
                    .flows
                    .planned_current(&flow, Instant::now(), |_| true)
            })
            .is_some()
    }
}

fn packet(bytes: usize, source_port: u16) -> Bytes {
    let mut packet = super::super::tests::ipv4_packet([10, 88, 0, 1], [10, 88, 0, 2]).to_vec();
    packet.resize(bytes, 0);
    packet[2..4].copy_from_slice(&(bytes as u16).to_be_bytes());
    packet[20..22].copy_from_slice(&source_port.to_be_bytes());
    packet[24..26].copy_from_slice(&((bytes - 20) as u16).to_be_bytes());
    packet[10..12].fill(0);
    let mut sum: u32 = packet[..20]
        .chunks_exact(2)
        .map(|word| u32::from(u16::from_be_bytes([word[0], word[1]])))
        .sum();
    while sum > u32::from(u16::MAX) {
        sum = (sum & u32::from(u16::MAX)) + (sum >> 16);
    }
    packet[10..12].copy_from_slice(&(!(sum as u16)).to_be_bytes());
    Bytes::from(packet)
}

#[test]
fn l3_native_registry_lag_replans_and_accepts_current_capacity() {
    let mut fixture = Fixture::new();
    let registry = fixture.registration.apply_authority().snapshot();
    let prior = registry.native_scheduling_shape.unwrap();
    fixture.publish_rate(81_000_000);
    let current = fixture.shape(30_612, 10_368);
    assert_ne!(prior.stamp(), current.stamp());
    assert_eq!(
        prior.stamp().native_activation(),
        current.stamp().native_activation()
    );
    assert_eq!(
        fixture
            .registration
            .apply_authority()
            .snapshot()
            .eligibility_epoch,
        registry.eligibility_epoch
    );
    assert_eq!(
        fixture
            .registration
            .apply_authority()
            .snapshot()
            .native_scheduling_shape
            .unwrap()
            .stamp(),
        prior.stamp()
    );
    assert_eq!(current.current_mtu(), 1_400);
    assert_eq!(
        current.congestion_window() - current.bytes_in_flight(),
        20_244
    );
    assert_eq!(fixture.queue.pending_bytes(), 0);
    let packet = packet(52, 41_000);
    assert!(!fixture.is_bound(&packet));
    assert!(
        fixture.device.try_send_to_peer(packet.clone()).unwrap(),
        "a lagging registry projection cannot veto fresh, fenced Native capacity"
    );
    assert!(fixture.is_bound(&packet));
    assert_eq!(fixture.queue.pending_bytes(), packet.len());
    assert_eq!(fixture.queue.pop().unwrap().2, packet);
    assert!(fixture.queue.pop().is_none());
    assert_eq!(fixture.queue.pending_bytes(), 0);
    assert_eq!(
        fixture.device.inner.carrier_packet_budget.available_bytes(),
        16 * 1_500
    );
}

#[test]
fn l3_native_registry_final_contraction_cannot_spend_old_retention() {
    let mut fixture = Fixture::new();
    let occupied = packet(1_500, 41_000);
    for _ in 0..9 {
        assert!(fixture.device.try_send_to_peer(occupied.clone()).unwrap());
    }
    assert_eq!(fixture.queue.pending_bytes(), 13_500);
    let packet = packet(1_500, 41_001);
    let plan = fixture.plan(&packet);
    let stamp = plan.native_stamp.unwrap();
    let current = fixture.shape(14_000, 0);
    assert_eq!(
        current.stamp(),
        stamp,
        "same-stamp window updates need final shape revalidation"
    );
    let available = fixture.device.inner.carrier_packet_budget.available_bytes();
    assert_eq!(
        fixture.apply(packet.clone(), plan),
        ServerIpDispatchOutcome::Full
    );
    assert_eq!(fixture.queue.pending_bytes(), 13_500);
    assert_eq!(
        fixture.device.inner.carrier_packet_budget.available_bytes(),
        available
    );
    assert!(!fixture.is_bound(&packet));
    for _ in 0..9 {
        assert_eq!(fixture.queue.pop().unwrap().2, occupied);
    }
    assert!(fixture.queue.pop().is_none());
    assert!(fixture.device.try_send_to_peer(packet.clone()).unwrap());
    assert!(fixture.is_bound(&packet));
    assert_eq!(fixture.queue.pop().unwrap().2, packet);
}

#[test]
fn l3_native_registry_final_retirement_preserves_packet_custody() {
    for retire_session in [false, true] {
        let mut fixture = Fixture::new();
        let packet = packet(52, 41_000);
        let plan = fixture.plan(&packet);
        if retire_session {
            fixture
                .port
                .retire_session(SessionId(91), CloseReason::PolicyRejected);
        } else {
            fixture
                .registration
                .apply_authority()
                .advance_eligibility_epoch();
        }
        assert_eq!(
            fixture.apply(packet.clone(), plan),
            ServerIpDispatchOutcome::Stale
        );
        assert!(!fixture.is_bound(&packet));
        assert_eq!(fixture.queue.pending_bytes(), 0);
        assert!(fixture.queue.pop().is_none());
        assert_eq!(
            fixture.device.inner.carrier_packet_budget.available_bytes(),
            16 * 1_500
        );
    }
}

#[test]
fn l3_native_registry_final_revision_and_activation_stay_fenced() {
    for activation_change in [false, true] {
        let mut fixture = Fixture::new();
        let packet = packet(52, 41_000);
        let plan = fixture.plan(&packet);
        if activation_change {
            fixture
                .authority
                .advance_transport_activation_for_test(2)
                .unwrap();
        } else {
            fixture.publish_rate(81_000_000);
            let _ = fixture.shape(30_612, 10_368);
        }
        assert_eq!(
            fixture.apply(packet.clone(), plan),
            ServerIpDispatchOutcome::Stale
        );
        assert!(!fixture.is_bound(&packet));
        assert_eq!(fixture.queue.pending_bytes(), 0);
        assert!(fixture.queue.pop().is_none());
        assert_eq!(
            fixture.device.inner.carrier_packet_budget.available_bytes(),
            16 * 1_500
        );
    }
}

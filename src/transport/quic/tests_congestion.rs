use super::*;
use quinn::congestion::Controller as _;

#[derive(Clone)]
struct FixedPacingController(u64);

impl quinn::congestion::Controller for FixedPacingController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        _space: quinn::congestion::SpaceId,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn pacing_rate(&self) -> Option<u64> {
        Some(self.0)
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        let mut metrics = quinn::congestion::ControllerMetrics::default();
        metrics.congestion_window = self.window();
        metrics.pacing_rate = Some(self.0);
        metrics
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Clone)]
struct MutableOperationalController(Arc<AtomicU64>);

impl quinn::congestion::Controller for MutableOperationalController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        _space: quinn::congestion::SpaceId,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        let mut metrics = quinn::congestion::ControllerMetrics::default();
        metrics.congestion_window = self.window();
        metrics.bandwidth_estimate =
            NonZeroU64::new(self.0.load(Ordering::Relaxed)).map(NonZeroU64::get);
        metrics
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[derive(Debug, Clone, Copy)]
struct ControlledNativeState {
    bandwidth_estimate: Option<u64>,
    sample: Option<quinn::congestion::BandwidthSample>,
}

#[derive(Clone)]
struct ControlledNativeController {
    state: Arc<Mutex<ControlledNativeState>>,
    congestion_window: u64,
    pacing_rate: u64,
}

impl ControlledNativeController {
    fn new(
        congestion_window: u64,
        pacing_rate: u64,
        bandwidth_estimate: Option<u64>,
        sample: Option<quinn::congestion::BandwidthSample>,
    ) -> (Self, Arc<Mutex<ControlledNativeState>>) {
        let state = Arc::new(Mutex::new(ControlledNativeState {
            bandwidth_estimate,
            sample,
        }));
        (
            Self {
                state: state.clone(),
                congestion_window,
                pacing_rate,
            },
            state,
        )
    }
}

impl quinn::congestion::Controller for ControlledNativeController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        _space: quinn::congestion::SpaceId,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        self.congestion_window
    }

    fn metrics(&self) -> quinn::congestion::ControllerMetrics {
        let state = *self.state.lock().expect("controlled native state");
        let mut metrics = quinn::congestion::ControllerMetrics::default();
        metrics.congestion_window = self.congestion_window;
        metrics.pacing_rate = Some(self.pacing_rate);
        metrics.bandwidth_estimate = state.bandwidth_estimate;
        metrics
    }

    fn latest_bandwidth_sample(&self) -> Option<quinn::congestion::BandwidthSample> {
        self.state.lock().expect("controlled native state").sample
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.congestion_window
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

fn controlled_sample(
    revision: u64,
    valid: bool,
    source_space: quinn::congestion::SpaceId,
    source_packet_number: u64,
    source_round: u64,
    app_limited: bool,
) -> quinn::congestion::BandwidthSample {
    quinn::congestion::BandwidthSample {
        revision: NonZeroU64::new(revision).expect("nonzero sample revision"),
        valid,
        source_space,
        source_packet_number,
        source_round,
        app_limited,
    }
}

fn controlled_instrumented_controller(
    inner: ControlledNativeController,
    startup_target: Option<QuicStartupTarget>,
    telemetry: Arc<QuicCarrierTelemetry>,
) -> InstrumentedController {
    let path_telemetry = telemetry.allocate_path_telemetry();
    InstrumentedController::for_path(
        Box::new(inner),
        LossPolicyPercent::default(),
        startup_target,
        telemetry,
        path_telemetry,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_controlled_sample(
    controller: &mut InstrumentedController,
    state: &Arc<Mutex<ControlledNativeState>>,
    revision: u64,
    valid: bool,
    source_space: quinn::congestion::SpaceId,
    source_packet_number: u64,
    source_round: u64,
    app_limited: bool,
    bandwidth_estimate: Option<u64>,
) {
    *state.lock().expect("controlled native state") = ControlledNativeState {
        bandwidth_estimate,
        sample: Some(controlled_sample(
            revision,
            valid,
            source_space,
            source_packet_number,
            source_round,
            app_limited,
        )),
    };
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        Some(source_packet_number),
        quinn::congestion::SpaceId::Data,
    );
}

async fn live_test_controller_activation() -> quinn::congestion::ControllerActivation {
    let mux_limits = crate::mux::MuxLimits::default();
    let server = crate::transport::quic::Endpoint::bind_server(
        "127.0.0.1:0".parse().expect("server address"),
        &crate::transport::encrypted::test_server_tls_config(),
        crate::transport::quic::test_candidate_verifier(),
        mux_limits,
    )
    .await
    .expect("server endpoint");
    let server_addr = server.local_addr().expect("server local address");
    let accepted = tokio::spawn(async move { server.accept().await.expect("server connection") });
    let client = crate::transport::quic::Endpoint::bind_client(
        "127.0.0.1:0".parse().expect("client address"),
        &crate::transport::encrypted::test_client_tls_config(),
        crate::transport::quic::test_candidate_selector(),
        mux_limits,
    )
    .await
    .expect("client endpoint");
    let client_connection = client
        .connect(server_addr)
        .await
        .expect("client connection");
    let server_connection = accepted.await.expect("server task");
    let activation = client_connection
        .native_controller_authority_snapshot()
        .activation();
    client_connection.close();
    server_connection.close();
    activation
}

#[derive(Debug, Default)]
struct ForwardedCallbacks {
    sent_space: Option<quinn::congestion::SpaceId>,
    end_ack_space: Option<quinn::congestion::SpaceId>,
    lost_space: Option<quinn::congestion::SpaceId>,
    congestion: Vec<(bool, quinn::congestion::SpaceId)>,
    spurious: u64,
    abandoned: u64,
    validated_ecn: u64,
    cwnd_limited: u64,
    ack_frequency: Option<(u64, Duration)>,
}

#[derive(Debug, Clone)]
struct RecordingController(Arc<Mutex<ForwardedCallbacks>>);

impl quinn::congestion::Controller for RecordingController {
    fn on_packet_sent(
        &mut self,
        _now: Instant,
        _bytes: u16,
        _prior_in_flight: u64,
        _packet_number: u64,
        space: quinn::congestion::SpaceId,
        _app_limited: bool,
    ) -> Option<quinn::congestion::PacketDeliveryState> {
        self.0.lock().unwrap().sent_space = Some(space);
        None
    }

    fn on_cwnd_limited(&mut self) {
        self.0.lock().unwrap().cwnd_limited += 1;
    }

    fn on_end_acks(
        &mut self,
        _now: Instant,
        _in_flight: u64,
        _app_limited: bool,
        _largest_packet_num_acked: Option<u64>,
        space: quinn::congestion::SpaceId,
    ) {
        self.0.lock().unwrap().end_ack_space = Some(space);
    }

    fn on_packet_lost(
        &mut self,
        _lost_bytes: u16,
        packet_number: u64,
        space: quinn::congestion::SpaceId,
        _now: Instant,
    ) -> Option<quinn::congestion::RecoveryTransactionId> {
        self.0.lock().unwrap().lost_space = Some(space);
        Some(quinn::congestion::RecoveryTransactionId::new(packet_number))
    }

    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: u64,
        space: quinn::congestion::SpaceId,
    ) {
        self.0.lock().unwrap().congestion.push((is_ecn, space));
    }

    fn on_spurious_congestion_event(
        &mut self,
        _transaction: quinn::congestion::RecoveryTransactionId,
    ) -> bool {
        self.0.lock().unwrap().spurious += 1;
        true
    }

    fn on_recovery_transaction_abandoned(
        &mut self,
        _transaction: quinn::congestion::RecoveryTransactionId,
    ) {
        self.0.lock().unwrap().abandoned += 1;
    }

    fn on_validated_ecn_congestion_event(&mut self) {
        self.0.lock().unwrap().validated_ecn += 1;
    }

    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
        self.0.lock().unwrap().ack_frequency =
            Some((ack_eliciting_threshold, requested_max_ack_delay));
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn clone_box(&self) -> Box<dyn quinn::congestion::Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn instrumented_controller_forwards_pacing_rate_through_clones() {
    const RATE: u64 = 12_345_678;
    let controller = InstrumentedController::new(
        Box::new(FixedPacingController(RATE)),
        Arc::new(QuicCarrierTelemetry::default()),
    );

    assert_eq!(controller.pacing_rate(), Some(RATE));
    assert_eq!(controller.clone_box().pacing_rate(), Some(RATE));
    assert_eq!(controller.metrics().pacing_rate, Some(RATE));
    assert_eq!(controller.clone_box().metrics().pacing_rate, Some(RATE));
}

#[test]
fn candidate_allocation_does_not_publish_active_identity_or_consume_ack_cursor() {
    let telemetry = QuicCarrierTelemetry::default();
    assert_eq!(telemetry.current_path_epoch(), 0);
    let path = telemetry.allocate_path_telemetry();
    path.publish_ack_batch(
        QuicAckTelemetryTotals {
            delivery_clock_epoch: 1,
            acked_bytes: 1200,
            sample_count: 1,
            ..QuicAckTelemetryTotals::default()
        },
        0,
        false,
    );
    assert_eq!(telemetry.current_path_epoch(), 0);
    assert_eq!(path.snapshot().newly_acked_bytes, Some(1200));
    assert_eq!(
        path.snapshot().newly_acked_bytes,
        None,
        "only the owning metrics snapshot advances the ACK cursor"
    );
}

#[tokio::test]
async fn valid_same_activation_rate_changes_wake_once_and_absence_never_clears() {
    let rate = Arc::new(AtomicU64::new(100));
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let notify = telemetry.native_authority_notify();
    let mut controller = InstrumentedController::new(
        Box::new(MutableOperationalController(rate.clone())),
        telemetry,
    );
    assert_eq!(controller.last_valid_operational_rate_bps, Some(800));

    rate.store(200, Ordering::Relaxed);
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        None,
        quinn::congestion::SpaceId::Data,
    );
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("a changed valid B_op wakes the coordinator");
    assert_eq!(controller.last_valid_operational_rate_bps, Some(1_600));

    rate.store(0, Ordering::Relaxed);
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        None,
        quinn::congestion::SpaceId::Data,
    );
    assert_eq!(
        controller.last_valid_operational_rate_bps,
        Some(1_600),
        "absence must not clear the last valid change-detector state"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "absence is no authority event"
    );

    rate.store(200, Ordering::Relaxed);
    let _inspection_clone = controller.clone_box();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "inspection clone neither detects nor publishes a rate change"
    );
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        None,
        quinn::congestion::SpaceId::Data,
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "the same valid value after absence is not a fabricated fresh update"
    );
}

#[tokio::test]
async fn finite_startup_authority_requires_two_post_ready_data_rounds() {
    const WINDOW: u64 = 77_777;
    const PACING_BYTES_PER_SECOND: u64 = 12_345;
    const OPERATIONAL_BYTES_PER_SECOND: u64 = 55_000;
    const FLOOR: u64 = 100;
    let activation = live_test_controller_activation().await;
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let notify = telemetry.native_authority_notify();
    let initial_sample =
        controlled_sample(1, true, quinn::congestion::SpaceId::Data, FLOOR, 1, false);
    let (inner, state) = ControlledNativeController::new(
        WINDOW,
        PACING_BYTES_PER_SECOND,
        Some(OPERATIONAL_BYTES_PER_SECOND),
        Some(initial_sample),
    );
    let target = QuicStartupTarget {
        window_bytes: WINDOW,
        pacing_bytes_per_second: PACING_BYTES_PER_SECOND,
    };
    let mut controller = controlled_instrumented_controller(inner, Some(target), telemetry.clone());
    controller.on_activated(activation);

    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::PreReady
    );
    let authority = controller
        .native_authority_snapshot()
        .expect("activated authority snapshot");
    assert_eq!(authority.kind(), NativeControllerObservationKind::Absent);
    assert_eq!(authority.operational_rate_bps(), None);
    let shape = controller
        .native_shape_snapshot(
            Duration::from_millis(80),
            Duration::from_millis(5),
            4_321,
            1_200,
            false,
        )
        .expect("activated shape snapshot");
    assert_eq!(shape.operational_rate_bps(), None);
    assert_eq!(shape.congestion_window(), WINDOW);
    assert_eq!(
        shape.pacing_rate_bps().map(NonZeroU64::get),
        Some(PACING_BYTES_PER_SECOND * 8)
    );
    assert_eq!(controller.window(), WINDOW);
    assert_eq!(controller.pacing_rate(), Some(PACING_BYTES_PER_SECOND));
    assert_eq!(controller.metrics().congestion_window, WINDOW);
    assert_eq!(
        controller.metrics().pacing_rate,
        Some(PACING_BYTES_PER_SECOND)
    );

    *state.lock().expect("controlled native state") = ControlledNativeState {
        bandwidth_estimate: Some(OPERATIONAL_BYTES_PER_SECOND),
        sample: Some(controlled_sample(
            2,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR + 1,
            2,
            false,
        )),
    };
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        Some(FLOOR + 1),
        quinn::congestion::SpaceId::Data,
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::PreReady
    );
    assert_eq!(
        controller.last_bandwidth_sample_revision,
        NonZeroU64::new(2)
    );

    controller.on_application_ready();
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::AwaitFloor
    );
    *state.lock().expect("controlled native state") = ControlledNativeState {
        bandwidth_estimate: Some(OPERATIONAL_BYTES_PER_SECOND),
        sample: Some(controlled_sample(
            3,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR + 2,
            3,
            false,
        )),
    };
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        Some(FLOOR + 2),
        quinn::congestion::SpaceId::Data,
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::AwaitFloor
    );
    assert_eq!(
        controller
            .native_authority_snapshot()
            .expect("pre-floor authority snapshot")
            .operational_rate_bps(),
        None
    );
    assert_eq!(
        controller
            .native_shape_snapshot(
                Duration::from_millis(80),
                Duration::from_millis(5),
                4_321,
                1_200,
                false,
            )
            .expect("pre-floor shape snapshot")
            .operational_rate_bps(),
        None
    );

    controller.on_packet_sent(
        Instant::now(),
        1_200,
        0,
        FLOOR,
        quinn::congestion::SpaceId::Data,
        false,
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::AwaitFirst {
            floor_packet_number: FLOOR
        }
    );

    publish_controlled_sample(
        &mut controller,
        &state,
        4,
        true,
        quinn::congestion::SpaceId::Data,
        FLOOR - 1,
        6,
        false,
        Some(OPERATIONAL_BYTES_PER_SECOND),
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::AwaitFirst {
            floor_packet_number: FLOOR
        }
    );
    publish_controlled_sample(
        &mut controller,
        &state,
        5,
        true,
        quinn::congestion::SpaceId::Data,
        FLOOR,
        7,
        false,
        Some(OPERATIONAL_BYTES_PER_SECOND),
    );
    let armed = StartupAuthorityState::Armed {
        floor_packet_number: FLOOR,
        first_source_round: 7,
    };
    assert_eq!(controller.startup_authority, armed);
    assert_eq!(
        controller
            .native_authority_snapshot()
            .expect("armed authority snapshot")
            .operational_rate_bps(),
        None,
        "one exact eligible round is not yet native authority"
    );

    let clone = controller
        .clone_box()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("instrumented clone");
    assert_eq!(clone.startup_authority, armed);
    assert_eq!(
        clone.last_bandwidth_sample_revision,
        controller.last_bandwidth_sample_revision
    );

    for (revision, valid, space, packet, round, app_limited, rate) in [
        (
            6,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR + 1,
            7,
            false,
            Some(OPERATIONAL_BYTES_PER_SECOND),
        ),
        (
            7,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR + 2,
            8,
            true,
            Some(OPERATIONAL_BYTES_PER_SECOND),
        ),
        (
            8,
            false,
            quinn::congestion::SpaceId::Data,
            FLOOR + 3,
            8,
            false,
            Some(OPERATIONAL_BYTES_PER_SECOND),
        ),
        (
            9,
            true,
            quinn::congestion::SpaceId::Handshake,
            FLOOR + 4,
            8,
            false,
            Some(OPERATIONAL_BYTES_PER_SECOND),
        ),
        (
            10,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR - 1,
            8,
            false,
            Some(OPERATIONAL_BYTES_PER_SECOND),
        ),
        (
            11,
            true,
            quinn::congestion::SpaceId::Data,
            FLOOR + 5,
            8,
            false,
            Some(0),
        ),
    ] {
        publish_controlled_sample(
            &mut controller,
            &state,
            revision,
            valid,
            space,
            packet,
            round,
            app_limited,
            rate,
        );
        assert_eq!(
            controller.startup_authority, armed,
            "ineligible and same-round samples are absence, not revocation"
        );
    }
    *state.lock().expect("controlled native state") = ControlledNativeState {
        bandwidth_estimate: Some(OPERATIONAL_BYTES_PER_SECOND),
        sample: None,
    };
    controller.on_end_acks(
        Instant::now(),
        0,
        false,
        None,
        quinn::congestion::SpaceId::Data,
    );
    assert_eq!(controller.startup_authority, armed);

    publish_controlled_sample(
        &mut controller,
        &state,
        12,
        true,
        quinn::congestion::SpaceId::Data,
        FLOOR + 6,
        8,
        false,
        Some(OPERATIONAL_BYTES_PER_SECOND),
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::Operational
    );
    assert_eq!(
        controller.last_valid_operational_rate_bps,
        Some(OPERATIONAL_BYTES_PER_SECOND * 8)
    );
    tokio::time::timeout(Duration::from_millis(100), notify.notified())
        .await
        .expect("the structural handoff wake is durable without a waiter");
    assert_eq!(
        controller
            .native_authority_snapshot()
            .expect("operational authority snapshot")
            .operational_rate_bps()
            .map(NonZeroU64::get),
        Some(OPERATIONAL_BYTES_PER_SECOND * 8)
    );

    *state.lock().expect("controlled native state") = ControlledNativeState {
        bandwidth_estimate: None,
        sample: None,
    };
    controller.on_end_acks(
        Instant::now(),
        0,
        true,
        None,
        quinn::congestion::SpaceId::Data,
    );
    assert_eq!(
        controller.startup_authority,
        StartupAuthorityState::Operational,
        "idle absence cannot restore the configured prior"
    );
    assert_eq!(
        controller.last_valid_operational_rate_bps,
        Some(OPERATIONAL_BYTES_PER_SECOND * 8)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(20), notify.notified())
            .await
            .is_err(),
        "absence and an already-completed handoff publish no extra wake"
    );

    let fresh = controller
        .fresh_path_box(Instant::now(), 1_400)
        .expect("fresh path controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert_eq!(fresh.startup_target, Some(target));
    assert_eq!(fresh.startup_authority, StartupAuthorityState::AwaitFloor);
    assert_eq!(fresh.last_bandwidth_sample_revision, None);
}

#[tokio::test]
async fn startup_authority_bypass_and_retained_clone_readiness_are_exact() {
    const WINDOW: u64 = 64_000;
    const PACING_BYTES_PER_SECOND: u64 = 25_000;
    const OPERATIONAL_BYTES_PER_SECOND: u64 = 75_000;
    let activation = live_test_controller_activation().await;

    for initial_rate in [
        crate::transport::RateHint::Unknown,
        crate::transport::RateHint::Unlimited,
    ] {
        let metadata = crate::transport::PathMetadata {
            initial_rate,
            ..Default::default()
        };
        let target = metadata
            .quic_startup_target()
            .expect("valid omitted or unlimited target");
        assert_eq!(target, None);
        let telemetry = Arc::new(QuicCarrierTelemetry::default());
        let (inner, _) = ControlledNativeController::new(
            WINDOW,
            PACING_BYTES_PER_SECOND,
            Some(OPERATIONAL_BYTES_PER_SECOND),
            None,
        );
        let mut controller = controlled_instrumented_controller(inner, target, telemetry);
        controller.on_activated(activation);
        assert_eq!(controller.startup_authority, StartupAuthorityState::Bypass);
        assert_eq!(
            controller
                .native_authority_snapshot()
                .expect("bypass authority snapshot")
                .operational_rate_bps()
                .map(NonZeroU64::get),
            Some(OPERATIONAL_BYTES_PER_SECOND * 8)
        );
        let shape = controller
            .native_shape_snapshot(
                Duration::from_millis(100),
                Duration::from_millis(10),
                1_234,
                1_200,
                false,
            )
            .expect("bypass shape snapshot");
        assert_eq!(shape.congestion_window(), WINDOW);
        assert_eq!(
            shape.pacing_rate_bps().map(NonZeroU64::get),
            Some(PACING_BYTES_PER_SECOND * 8)
        );
        assert_eq!(
            shape.operational_rate_bps().map(NonZeroU64::get),
            Some(OPERATIONAL_BYTES_PER_SECOND * 8)
        );
        controller.on_application_ready();
        controller.on_packet_sent(
            Instant::now(),
            1_200,
            0,
            1,
            quinn::congestion::SpaceId::Data,
            false,
        );
        assert_eq!(controller.startup_authority, StartupAuthorityState::Bypass);
    }

    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let (inner, _) = ControlledNativeController::new(WINDOW, PACING_BYTES_PER_SECOND, None, None);
    let target = QuicStartupTarget {
        window_bytes: WINDOW,
        pacing_bytes_per_second: PACING_BYTES_PER_SECOND,
    };
    let mut active = controlled_instrumented_controller(inner, Some(target), telemetry.clone());
    active.on_activated(activation);
    let mut retained = active
        .clone_box()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("retained instrumented clone");
    assert_eq!(retained.startup_authority, StartupAuthorityState::PreReady);
    active.on_application_ready();
    assert_eq!(active.startup_authority, StartupAuthorityState::AwaitFloor);
    assert_eq!(
        retained.startup_authority,
        StartupAuthorityState::PreReady,
        "an inactive clone changes only when it is installed"
    );
    retained.on_activated(activation);
    assert_eq!(
        retained.startup_authority,
        StartupAuthorityState::AwaitFloor,
        "late activation reconciles connection-scoped readiness"
    );
}

#[test]
fn production_initial_and_fresh_paths_both_construct_bbr3() {
    let now = Instant::now();
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::default()),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("instrumented production controller");
    assert!(
        controller
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .is_ok(),
        "initial production controller must be BBR3"
    );

    let fresh = controller
        .fresh_path_box(now + Duration::from_secs(1), 1400)
        .expect("fresh controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert!(
        fresh
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .is_ok(),
        "fresh network path must restart with BBR3"
    );
}

#[test]
fn omitted_and_unlimited_startup_rate_preserve_exact_bbr3_defaults() {
    let now = Instant::now();
    let default = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::Bbr3Config::default()),
        now,
        1200,
    );
    let default_metrics = default.metrics();

    for initial_rate in [
        crate::transport::RateHint::Unknown,
        crate::transport::RateHint::Unlimited,
    ] {
        let metadata = crate::transport::PathMetadata {
            initial_srtt_ms: Some(17),
            initial_rate,
            ..Default::default()
        };
        let controller = quinn::congestion::ControllerFactory::build(
            Arc::new(InstrumentedBbrConfig::for_path(&metadata)),
            now,
            1200,
        )
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("instrumented production controller");
        assert_eq!(controller.initial_window(), default.initial_window());
        assert_eq!(
            controller.metrics().pacing_rate,
            default_metrics.pacing_rate
        );
        assert_eq!(controller.metrics().bandwidth_estimate, None);
        assert_eq!(controller.last_valid_operational_rate_bps, None);
    }
}

#[test]
fn finite_startup_rate_sets_exact_geometry_without_fabricating_bandwidth() {
    let now = Instant::now();
    let metadata = crate::transport::PathMetadata {
        initial_srtt_ms: Some(101),
        initial_rate: crate::transport::RateHint::BitsPerSecond(25_000_003),
        ..Default::default()
    };
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&metadata)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("instrumented production controller");

    // ceil(25_000_003 bit/s * 101 ms / 8_000) and ceil(rate / 8).
    assert_eq!(controller.initial_window(), 315_626);
    assert_eq!(controller.window(), 315_626);
    assert_eq!(controller.metrics().pacing_rate, Some(3_125_001));
    assert_eq!(controller.metrics().bandwidth_estimate, None);
    assert_eq!(controller.last_valid_operational_rate_bps, None);

    let fresh = controller
        .fresh_path_box(now + Duration::from_secs(1), 1400)
        .expect("fresh controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert_eq!(fresh.initial_window(), 315_626);
    assert_eq!(fresh.window(), 315_626);
    assert_eq!(fresh.metrics().pacing_rate, Some(3_125_001));
    assert_eq!(fresh.metrics().bandwidth_estimate, None);
    assert_eq!(fresh.last_valid_operational_rate_bps, None);
}

#[test]
fn finite_startup_rate_uses_333ms_default_and_never_shrinks_iw10() {
    let now = Instant::now();
    let metadata = crate::transport::PathMetadata {
        initial_rate: crate::transport::RateHint::BitsPerSecond(1_000_001),
        ..Default::default()
    };
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&metadata)),
        now,
        1200,
    );
    assert_eq!(controller.initial_window(), 41_626);
    assert_eq!(controller.metrics().pacing_rate, Some(125_001));
    assert_eq!(controller.metrics().bandwidth_estimate, None);

    let tiny = crate::transport::PathMetadata {
        initial_srtt_ms: Some(1),
        initial_rate: crate::transport::RateHint::BitsPerSecond(8),
        ..Default::default()
    };
    let tiny = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&tiny)),
        now,
        1200,
    );
    let default = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::Bbr3Config::default()),
        now,
        1200,
    );
    assert_eq!(tiny.initial_window(), default.initial_window());
    assert_eq!(tiny.metrics().pacing_rate, Some(1));
    assert_eq!(tiny.metrics().bandwidth_estimate, None);
}

#[test]
fn path_loss_compensation_and_startup_target_construct_initial_and_fresh_bbr3() {
    fn assert_bbr3_loss_compensation(controller: &InstrumentedController, expected: &str) {
        let bbr3 = controller
            .inner
            .clone_box()
            .into_any()
            .downcast::<quinn::congestion::Bbr3>()
            .expect("inner BBR3 controller");
        assert!(
            format!("{bbr3:?}").contains(expected),
            "constructed BBR3 must contain {expected}"
        );
    }

    let now = Instant::now();
    let path = concat!(
        "quic://example.com:443?",
        "initial-rate-mbps=25&loss-compensation-percent=5.1234"
    )
    .parse::<crate::transport::PathSpec>()
    .expect("QUIC path metadata");
    let controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&path.metadata)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("instrumented production controller");
    assert_eq!(controller.loss_compensation.ppm(), 51_234);
    assert_bbr3_loss_compensation(&controller, "loss_compensation_floor: 0.051234");
    assert_eq!(controller.initial_window(), 1_040_625);
    assert_eq!(controller.metrics().pacing_rate, Some(3_125_000));
    assert_eq!(controller.metrics().bandwidth_estimate, None);
    let fresh = controller
        .fresh_path_box(now + Duration::from_secs(1), 1400)
        .expect("fresh controller")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");
    assert_eq!(fresh.loss_compensation.ppm(), 51_234);
    assert_bbr3_loss_compensation(&fresh, "loss_compensation_floor: 0.051234");
    assert_eq!(fresh.initial_window(), 1_040_625);
    assert_eq!(fresh.metrics().pacing_rate, Some(3_125_000));
    assert_eq!(fresh.metrics().bandwidth_estimate, None);
    let rate_only = crate::transport::PathMetadata {
        initial_rate: crate::transport::RateHint::BitsPerSecond(25_000_000),
        ..Default::default()
    };
    let default_controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&rate_only)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("default instrumented controller");
    assert_eq!(default_controller.loss_compensation.ppm(), 100_000);
    assert_bbr3_loss_compensation(&default_controller, "loss_compensation_floor: 0.1");
    assert_eq!(default_controller.initial_window(), 1_040_625);
    assert_eq!(default_controller.metrics().pacing_rate, Some(3_125_000));
    assert_eq!(default_controller.metrics().bandwidth_estimate, None);

    let disabled = "quic://example.com:443?loss-compensation-percent=0"
        .parse::<crate::transport::PathSpec>()
        .expect("explicitly disabled QUIC loss compensation");
    let disabled_controller = quinn::congestion::ControllerFactory::build(
        Arc::new(InstrumentedBbrConfig::for_path(&disabled.metadata)),
        now,
        1200,
    )
    .into_any()
    .downcast::<InstrumentedController>()
    .expect("disabled-compensation instrumented controller");
    assert_eq!(disabled_controller.loss_compensation.ppm(), 0);
    assert_bbr3_loss_compensation(&disabled_controller, "loss_compensation_floor: 0.0");
}

#[test]
fn instrumented_controller_forwards_packet_space_and_recovery_callbacks_once() {
    let now = Instant::now();
    let recorded = Arc::new(Mutex::new(ForwardedCallbacks::default()));
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let mut controller =
        InstrumentedController::new(Box::new(RecordingController(recorded.clone())), telemetry);
    controller.on_packet_sent(now, 1200, 0, 7, quinn::congestion::SpaceId::Initial, false);
    controller.on_end_acks(
        now + Duration::from_millis(50),
        0,
        false,
        Some(7),
        quinn::congestion::SpaceId::Initial,
    );
    let transaction = controller
        .on_packet_lost(
            1200,
            8,
            quinn::congestion::SpaceId::Handshake,
            now + Duration::from_millis(50),
        )
        .expect("recording controller recovery transaction");
    assert_eq!(
        controller.snapshot().lost_bytes,
        0,
        "per-packet loss forwarding must not duplicate aggregate loss telemetry"
    );
    controller.on_congestion_event(
        now + Duration::from_millis(50),
        now,
        false,
        false,
        1200,
        8,
        quinn::congestion::SpaceId::Handshake,
    );
    controller.on_congestion_event(
        now + Duration::from_millis(50),
        now,
        false,
        true,
        0,
        9,
        quinn::congestion::SpaceId::Data,
    );
    assert!(controller.on_spurious_congestion_event(transaction));
    controller.on_recovery_transaction_abandoned(transaction);
    controller.on_validated_ecn_congestion_event();
    controller.on_cwnd_limited();
    controller.on_ack_frequency_update(4, Duration::from_millis(25));

    assert_eq!(controller.snapshot().lost_bytes, 1200);
    let callbacks = recorded.lock().unwrap();
    assert_eq!(
        callbacks.sent_space,
        Some(quinn::congestion::SpaceId::Initial)
    );
    assert_eq!(
        callbacks.end_ack_space,
        Some(quinn::congestion::SpaceId::Initial)
    );
    assert_eq!(
        callbacks.lost_space,
        Some(quinn::congestion::SpaceId::Handshake)
    );
    assert_eq!(
        callbacks.congestion,
        vec![
            (false, quinn::congestion::SpaceId::Handshake),
            (true, quinn::congestion::SpaceId::Data),
        ]
    );
    assert_eq!(callbacks.spurious, 1);
    assert_eq!(callbacks.abandoned, 1);
    assert_eq!(callbacks.validated_ecn, 1);
    assert_eq!(callbacks.cwnd_limited, 1);
    assert_eq!(
        callbacks.ack_frequency,
        Some((4, Duration::from_millis(25)))
    );
}

#[test]
fn same_path_clone_preserves_telemetry_epoch() {
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let controller =
        InstrumentedController::new(Box::new(FixedPacingController(1)), telemetry.clone());
    let cloned = controller
        .clone_box()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("instrumented clone");

    assert!(Arc::ptr_eq(&cloned.telemetry, &telemetry));
    assert!(Arc::ptr_eq(
        &cloned.path_telemetry,
        &controller.path_telemetry
    ));
    assert_eq!(
        cloned.path_telemetry.path_epoch,
        controller.path_telemetry.path_epoch
    );
}

#[test]
fn fresh_network_path_keeps_owner_and_isolates_stale_callbacks() {
    let base = Instant::now();
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let inner = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::BbrConfig::default()),
        base,
        1200,
    );
    let mut old = InstrumentedController::new(inner, telemetry.clone());
    let old_epoch = old.path_telemetry.path_epoch;

    let mut fresh = old
        .fresh_path_box(base + Duration::from_secs(1), 1400)
        .expect("instrumented controller supports fresh paths")
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("fresh instrumented controller");

    assert!(Arc::ptr_eq(&fresh.telemetry, &telemetry));
    assert!(!Arc::ptr_eq(&fresh.path_telemetry, &old.path_telemetry));
    assert_eq!(fresh.path_telemetry.path_epoch, old_epoch + 1);

    old.accumulate_ack_telemetry(base, 1200, false);
    old.finish_ack_telemetry(base + Duration::from_millis(10), 0, false);
    old.on_congestion_event(
        base + Duration::from_millis(10),
        base,
        false,
        false,
        1200,
        7,
        quinn::congestion::SpaceId::Data,
    );

    let before_current_ack = fresh.snapshot();
    assert_eq!(before_current_ack.path_epoch, old_epoch + 1);
    assert_eq!(before_current_ack.newly_acked_bytes, None);
    assert_eq!(before_current_ack.lost_bytes, 0);
    assert_eq!(before_current_ack.bytes_in_flight, None);

    fresh.accumulate_ack_telemetry(base + Duration::from_secs(1), 1200, false);
    fresh.finish_ack_telemetry(
        base + Duration::from_secs(1) + Duration::from_millis(10),
        0,
        false,
    );
    assert_eq!(fresh.snapshot().newly_acked_bytes, Some(1200));
}

#[test]
fn instrumented_controller_preserves_compact_bbr_delivery_state() {
    let base = Instant::now();
    let (mut controller, _) = test_instrumented_controller(base);

    let state = controller
        .on_packet_sent(base, 1200, 0, 7, quinn::congestion::SpaceId::Data, false)
        .expect("BBR packet delivery state");

    assert_eq!(state.delivered, 0);
    assert_eq!(state.delivered_time, base);
    assert_eq!(state.send_elapsed_ns, 0);
}

#[tokio::test]
async fn first_write_activity_wakes_idle_metrics_without_byte_attribution() {
    let telemetry = QuicCarrierTelemetry::default();
    let activity = telemetry.write_activity_notify();
    let started = activity.notified();
    tokio::pin!(started);
    started.as_mut().enable();

    telemetry.record_write_activity();

    tokio::time::timeout(Duration::from_secs(1), &mut started)
        .await
        .expect("idle QUIC metrics wake when application writing becomes active");
}

#[test]
fn native_ack_without_product_provenance_cannot_confirm_product_delivery() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.telemetry.record_write_activity();
    controller.accumulate_ack_telemetry(base, 1_200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(10), 0, false);

    let snapshot = telemetry.snapshot();
    assert_eq!(
        snapshot.newly_acked_bytes,
        Some(1_200),
        "the ACK remains visible only as native packet telemetry"
    );
}

#[test]
fn quic_ack_snapshot_keeps_non_app_limited_classification_coherent() {
    const BATCHES: u64 = 20_000;
    const ACK_BYTES: u64 = 1200;
    const NON_APP_ACKS_PER_BATCH: u64 = 2;
    const ACKS_PER_BATCH: u64 = 3;
    const ELAPSED_PER_BATCH: Duration = Duration::from_micros(7);
    let telemetry = QuicCarrierTelemetry::default().allocate_path_telemetry();
    let writer = {
        let telemetry = telemetry.clone();
        std::thread::spawn(move || {
            for index in 0..BATCHES {
                let non_app_limited = index % 2 == 0;
                telemetry.publish_ack_batch(
                    QuicAckTelemetryTotals {
                        delivery_clock_epoch: 1,
                        acked_bytes: ACKS_PER_BATCH * ACK_BYTES,
                        non_app_limited_acked_bytes: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH * ACK_BYTES
                        } else {
                            0
                        },
                        timed_non_app_limited_acked_bytes: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH * ACK_BYTES
                        } else {
                            0
                        },
                        non_app_limited_ack_elapsed_nanos: if non_app_limited {
                            duration_as_u64_nanos(ELAPSED_PER_BATCH)
                        } else {
                            0
                        },
                        sample_count: ACKS_PER_BATCH,
                        non_app_limited_sample_count: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH
                        } else {
                            0
                        },
                        timed_non_app_limited_sample_count: if non_app_limited {
                            NON_APP_ACKS_PER_BATCH
                        } else {
                            0
                        },
                    },
                    0,
                    !non_app_limited,
                );
            }
        })
    };

    let mut acked_bytes = 0_u64;
    let mut non_app_limited_bytes = 0_u64;
    let mut samples = 0_u64;
    let mut non_app_limited_samples = 0_u64;
    let mut current_timed_non_app_limited_bytes = 0_u64;
    let mut current_timed_non_app_limited_samples = 0_u64;
    let mut current_non_app_limited_elapsed = Duration::ZERO;
    while !writer.is_finished() {
        let snapshot = telemetry.snapshot();
        assert_eq!(
            snapshot.newly_acked_bytes.unwrap_or(0),
            snapshot.delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.non_app_limited_acked_bytes.unwrap_or(0),
            snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.timed_non_app_limited_acked_bytes.unwrap_or(0),
            snapshot.timed_non_app_limited_delivery_sample_count * ACK_BYTES
        );
        assert_eq!(
            snapshot.non_app_limited_ack_elapsed.unwrap_or_default(),
            ELAPSED_PER_BATCH
                * (snapshot.timed_non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                    as u32
        );
        acked_bytes = acked_bytes.saturating_add(snapshot.newly_acked_bytes.unwrap_or(0));
        non_app_limited_bytes =
            non_app_limited_bytes.saturating_add(snapshot.non_app_limited_acked_bytes.unwrap_or(0));
        samples = samples.saturating_add(snapshot.delivery_sample_count);
        non_app_limited_samples =
            non_app_limited_samples.saturating_add(snapshot.non_app_limited_delivery_sample_count);
        current_timed_non_app_limited_bytes =
            snapshot.timed_non_app_limited_acked_bytes.unwrap_or(0);
        current_timed_non_app_limited_samples =
            snapshot.timed_non_app_limited_delivery_sample_count;
        current_non_app_limited_elapsed = snapshot.non_app_limited_ack_elapsed.unwrap_or_default();
    }
    writer.join().expect("QUIC ACK telemetry writer");
    let final_snapshot = telemetry.snapshot();
    assert_eq!(
        final_snapshot.newly_acked_bytes.unwrap_or(0),
        final_snapshot.delivery_sample_count * ACK_BYTES
    );
    assert_eq!(
        final_snapshot.non_app_limited_acked_bytes.unwrap_or(0),
        final_snapshot.non_app_limited_delivery_sample_count * ACK_BYTES
    );
    assert_eq!(
        final_snapshot
            .non_app_limited_ack_elapsed
            .unwrap_or_default(),
        ELAPSED_PER_BATCH
            * (final_snapshot.timed_non_app_limited_delivery_sample_count / NON_APP_ACKS_PER_BATCH)
                as u32
    );
    acked_bytes = acked_bytes.saturating_add(final_snapshot.newly_acked_bytes.unwrap_or(0));
    non_app_limited_bytes = non_app_limited_bytes
        .saturating_add(final_snapshot.non_app_limited_acked_bytes.unwrap_or(0));
    samples = samples.saturating_add(final_snapshot.delivery_sample_count);
    non_app_limited_samples = non_app_limited_samples
        .saturating_add(final_snapshot.non_app_limited_delivery_sample_count);
    current_timed_non_app_limited_bytes = final_snapshot
        .timed_non_app_limited_acked_bytes
        .unwrap_or(current_timed_non_app_limited_bytes);
    current_timed_non_app_limited_samples = final_snapshot
        .timed_non_app_limited_delivery_sample_count
        .max(current_timed_non_app_limited_samples);
    current_non_app_limited_elapsed = final_snapshot
        .non_app_limited_ack_elapsed
        .unwrap_or(current_non_app_limited_elapsed);

    assert_eq!(acked_bytes, BATCHES * ACKS_PER_BATCH * ACK_BYTES);
    assert_eq!(
        non_app_limited_bytes,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH * ACK_BYTES
    );
    assert_eq!(samples, BATCHES * ACKS_PER_BATCH);
    assert_eq!(
        non_app_limited_samples,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH
    );
    assert_eq!(
        current_timed_non_app_limited_bytes,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH * ACK_BYTES
    );
    assert_eq!(
        current_timed_non_app_limited_samples,
        BATCHES / 2 * NON_APP_ACKS_PER_BATCH
    );
    assert_eq!(
        current_non_app_limited_elapsed,
        ELAPSED_PER_BATCH * (BATCHES / 2) as u32
    );
}
fn test_instrumented_controller(base: Instant) -> (InstrumentedController, Arc<QuicPathTelemetry>) {
    let telemetry = Arc::new(QuicCarrierTelemetry::default());
    let inner = quinn::congestion::ControllerFactory::build(
        Arc::new(quinn::congestion::BbrConfig::default()),
        base,
        1200,
    );
    let controller = InstrumentedController::new(inner, telemetry);
    let path_telemetry = controller.path_telemetry.clone();
    (controller, path_telemetry)
}
#[test]
fn quic_first_ack_batch_excludes_path_rtt_and_app_limited_idle() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 200, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(4), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(200), 900, false);
    let first = telemetry.snapshot();
    assert_eq!(first.delivery_clock_epoch, 1);
    assert_eq!(first.non_app_limited_acked_bytes, Some(300));
    assert_eq!(first.non_app_limited_delivery_sample_count, 2);
    assert_eq!(
        first.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(4)),
        "the first delivery interval must not include the 200 ms path RTT"
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(205), 25, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(210), 875, true);
    let idle = telemetry.snapshot();
    assert_eq!(idle.delivery_clock_epoch, 1);
    assert_eq!(idle.newly_acked_bytes, Some(25));
    assert_eq!(idle.non_app_limited_acked_bytes, None);
    assert_eq!(idle.timed_non_app_limited_acked_bytes, Some(300));
    assert_eq!(
        idle.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(4))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(300), 250, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(306), 250, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(600), 0, false);
    let after_idle = telemetry.snapshot();
    assert_eq!(after_idle.delivery_clock_epoch, 2);
    assert_eq!(after_idle.non_app_limited_acked_bytes, Some(500));
    assert_eq!(
        after_idle.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(6)),
        "an app-limited end must reset both delivery clocks"
    );
    assert_eq!(after_idle.bytes_in_flight, Some(0));
}

#[test]
fn quic_coalesced_snapshot_exposes_only_current_delivery_clock_epoch() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 700, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(110), 100, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(120), 600, true);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(130), 300, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(135), 300, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(140), 0, false);

    let coalesced = telemetry.snapshot();
    assert_eq!(coalesced.delivery_clock_epoch, 2);
    assert_eq!(coalesced.newly_acked_bytes, Some(900));
    assert_eq!(coalesced.timed_non_app_limited_acked_bytes, Some(600));
    assert_eq!(coalesced.timed_non_app_limited_delivery_sample_count, 2);
    assert_eq!(
        coalesced.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(5))
    );

    controller.finish_ack_telemetry(base + Duration::from_millis(150), 0, true);
    controller.finish_ack_telemetry(base + Duration::from_millis(160), 0, true);
    assert_eq!(
        telemetry.snapshot().delivery_clock_epoch,
        2,
        "repeated idle callbacks arm one boundary but cannot mint epochs"
    );
}

#[test]
fn quic_mixed_app_limited_batch_keeps_only_native_ack_classification() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(5), 100, true);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, true);

    let mixed = telemetry.snapshot();
    assert_eq!(mixed.delivery_clock_epoch, 1);
    assert_eq!(mixed.timed_non_app_limited_acked_bytes, Some(200));
    assert_eq!(
        mixed.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );
}

#[test]
fn quic_final_non_app_batch_publishes_before_delivery_clock_closes() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);
    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, true);

    let closing = telemetry.snapshot();
    assert_eq!(closing.delivery_clock_epoch, 1);
    assert!(closing.app_limited);
    assert_eq!(closing.timed_non_app_limited_acked_bytes, Some(200));

    controller.finish_ack_telemetry(base + Duration::from_millis(110), 0, true);
    assert_eq!(telemetry.snapshot().delivery_clock_epoch, 1);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(120), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(125), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(130), 0, false);
    let reopened = telemetry.snapshot();
    assert_eq!(reopened.delivery_clock_epoch, 2);
}

#[test]
fn quic_ack_send_clock_resists_ack_compression() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(15), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(30), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(30)),
        "the send clock must win when ACK batches are only 1 ms apart"
    );
}

#[test]
fn quic_zero_span_first_ack_batch_is_untimed_but_seeds_clocks() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
    let seed = telemetry.snapshot();
    assert_eq!(seed.non_app_limited_acked_bytes, Some(1200));
    assert_eq!(seed.non_app_limited_delivery_sample_count, 1);
    assert_eq!(seed.timed_non_app_limited_acked_bytes, None);
    assert_eq!(seed.timed_non_app_limited_delivery_sample_count, 0);
    assert_eq!(seed.non_app_limited_ack_elapsed, None);

    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10)),
        "an untimed first batch must still seed both clocks"
    );
}

#[test]
fn quic_untimed_seed_cannot_join_timed_bytes_between_metric_polls() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(250), 0, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 1200, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(260), 0, false);

    let combined = telemetry.snapshot();
    assert_eq!(combined.non_app_limited_acked_bytes, Some(2400));
    assert_eq!(combined.non_app_limited_delivery_sample_count, 2);
    assert_eq!(combined.timed_non_app_limited_acked_bytes, Some(1200));
    assert_eq!(combined.timed_non_app_limited_delivery_sample_count, 1);
    assert_eq!(
        combined.non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );
}

#[test]
fn quic_reordered_ack_batch_cannot_move_send_frontier_backward() {
    let base = Instant::now();
    let (mut controller, telemetry) = test_instrumented_controller(base);

    controller.accumulate_ack_telemetry(base, 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(10), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(100), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(10))
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(8), 100, false);
    controller.accumulate_ack_telemetry(base + Duration::from_millis(2), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(101), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(16)),
        "within-batch send spacing must guard a reordered ACK batch"
    );

    controller.accumulate_ack_telemetry(base + Duration::from_millis(12), 100, false);
    controller.finish_ack_telemetry(base + Duration::from_millis(102), 0, false);
    assert_eq!(
        telemetry.snapshot().non_app_limited_ack_elapsed,
        Some(Duration::from_millis(18)),
        "the send frontier must remain at 10 ms rather than regress to 8 ms"
    );
}

//! Real weak notice and response Product commit coverage for the proposed
//! successor policy. Native scheduling performance remains an end-to-end gate.
use super::test_support::{
    native_response_binding_fixture, qualify_product_assignment, with_output_entry_for_key_mut,
};
use super::*;
use crate::model::capacity::adaptive_reliable_relay_reinjection_bytes;
use crate::mux::stream::ReliableSendStream;
use crate::protocol::{Frame, OffsetRange, StreamId};
use crate::runtime::path::commands::{
    ReliablePathCommand, ReliablePathCommandReceivers, reliable_path_command_channels,
    try_recv_reliable_path_command,
};
use crate::runtime::path::prepared::{PreparedOriginalClaim, PreparedOriginalWork};
use crate::runtime::sender::{
    PreparedResponseSource, ResponseProductState, ServerReinjectionOutputIdentity,
    ServerResponseSenderService, SharedResponseProduct, publish_prepared_response_work,
};
use bytes::Bytes;
use std::time::Duration;

struct Fixture {
    owner: SharedResponseProduct,
    target: ResponseAcquisitionOutputId,
    receivers: ReliablePathCommandReceivers,
    _other_receivers: ReliablePathCommandReceivers,
    quantum: usize,
}

impl Fixture {
    fn new(target_underlay: UnderlayProtocol) -> Self {
        Self::with_tcp_rate(target_underlay, 100_000_000)
    }

    fn with_tcp_rate(target_underlay: UnderlayProtocol, tcp_rate_bps: u64) -> Self {
        let native = native_response_binding_fixture(32, Some(100_000_000));
        let binding = native.binding;
        let (commands, tcp_receivers) = reliable_path_command_channels(32);
        let tcp = CarrierPathKey {
            underlay: UnderlayProtocol::Tcp,
            path_id: PathId(32),
        };
        binding.attach(
            tcp.underlay,
            tcp.path_id,
            commands,
            TrafficClass::Throughput,
        );
        for key in [native.key, tcp] {
            with_output_entry_for_key_mut(&binding, key, |entry| {
                entry.startup_rate_prior = RateHint::BitsPerSecond(if key == tcp {
                    tcp_rate_bps
                } else {
                    100_000_000
                });
                qualify_product_assignment(entry, binding.mux_limits());
            });
        }
        let (target_key, original, receivers, other_receivers) = match target_underlay {
            UnderlayProtocol::Udp => (native.key, tcp, native.receivers, tcp_receivers),
            UnderlayProtocol::Tcp => (tcp, native.key, tcp_receivers, native.receivers),
        };
        let targets = binding.sender_path_targets(TrafficClass::Throughput, 1);
        let target = ResponseAcquisitionOutputId::from(
            targets
                .iter()
                .find(|target| target.observation.key == target_key)
                .unwrap(),
        );
        let quantum = targets
            .iter()
            .map(|target| {
                adaptive_reliable_relay_reinjection_bytes(
                    Some(target.observation.snapshot),
                    TrafficClass::Throughput,
                    binding.mux_limits(),
                )
            })
            .max()
            .unwrap();
        let stream_id = StreamId(831);
        let mut send_stream = ReliableSendStream::new(stream_id, binding.mux_limits());
        let mut frames = Vec::new();
        for _ in 0..3 {
            let frame = send_stream
                .send_data(Bytes::from(vec![0x83; quantum]))
                .unwrap();
            binding.record_original_flight(original, &frame);
            frames.push(frame);
        }
        binding.record_reinjected_flight(target_key, &frames[0]);
        binding.age_original_flights_for_test(Duration::from_secs(2));
        let owner = SharedResponseProduct::new(
            ResponseProductState {
                sender: ServerResponseSenderService::new(binding.session_id(), stream_id),
                send_stream,
                last_send_ack: Default::default(),
                prepared: PreparedResponseSource::new(TrafficClass::Throughput, quantum),
            },
            binding,
        );
        {
            let mut state = owner.lock();
            publish_prepared_response_work(
                &mut state,
                &owner,
                TrafficClass::Throughput,
                quantum,
                true,
            );
        }
        Self {
            owner,
            target,
            receivers,
            _other_receivers: other_receivers,
            quantum,
        }
    }

    fn offer(&mut self) -> PreparedOriginalWork {
        self.receivers
            .prepared_writer_ready_boundary(self.target.path_instance_id, true)
            .unwrap();
        let Some(ReliablePathCommand::PreparedOriginal(work)) =
            try_recv_reliable_path_command(&mut self.receivers)
        else {
            panic!("actor published one weak response repair offer")
        };
        assert!(work.is_repair());
        work
    }

    fn claim(&mut self, work: &PreparedOriginalWork) -> PreparedOriginalClaim {
        let ready = self
            .receivers
            .prepared_writer_ready_boundary(self.target.path_instance_id, true)
            .unwrap();
        work.try_claim(ready)
    }

    fn debt(&self) -> usize {
        self.owner
            .binding()
            .accepted_reinjected_data_in_flight_bytes_at(ServerReinjectionOutputIdentity {
                key: self.target.key,
                incarnation: self.target.incarnation,
            })
    }
}

#[tokio::test]
async fn response_prepared_repair_acquires_successors_without_waiting_for_copy_ack() {
    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut fixture = Fixture::new(underlay);
        let original_credit = fixture.owner.lock().send_stream.send_credit_bytes();
        let initial_debt = fixture.debt();
        let work = fixture.offer();
        for index in 1..=2 {
            let PreparedOriginalClaim::Claimed(Frame::StreamData {
                offset, payload, ..
            }) = fixture.claim(&work)
            else {
                panic!("mature successor has an actual writer opportunity: {underlay:?}")
            };
            assert_eq!(offset, (index * fixture.quantum) as u64);
            assert_eq!(payload.len(), fixture.quantum);
            assert_eq!(fixture.debt(), initial_debt + index * fixture.quantum);
            let state = fixture.owner.lock();
            assert_eq!(state.send_stream.send_credit_bytes(), original_credit);
            assert_eq!(state.send_stream.reinjection_bytes(), 3 * fixture.quantum);
            assert!(state.sender.is_empty());
        }
        assert!(!matches!(
            fixture.claim(&work),
            PreparedOriginalClaim::Claimed(_)
        ));
    }
}

#[tokio::test]
async fn response_prepared_repair_refusal_and_revocation_create_no_copy() {
    let mut fixture = Fixture::new(UnderlayProtocol::Tcp);
    let work = fixture.offer();
    let before = fixture.debt();
    {
        let owner = fixture.owner.clone();
        let state = owner.lock();
        assert!(matches!(
            fixture.claim(&work),
            PreparedOriginalClaim::Busy(_)
        ));
        drop(state);
    }
    fixture.owner.lock().sender.enqueue_data_for_lane(
        Bytes::from_static(b"new foreground"),
        TrafficClass::Throughput,
    );
    assert!(matches!(
        fixture.claim(&work),
        PreparedOriginalClaim::Blocked(_)
    ));
    assert_eq!(fixture.debt(), before);
    assert_eq!(
        fixture.owner.lock().send_stream.next_offset(),
        (3 * fixture.quantum) as u64
    );

    let mut fixture = Fixture::new(UnderlayProtocol::Udp);
    let work = fixture.offer();
    let before = fixture.debt();
    fixture.owner.lock().prepared.claims_active = false;
    assert!(matches!(fixture.claim(&work), PreparedOriginalClaim::Empty));
    assert_eq!(fixture.debt(), before);
}

#[tokio::test]
async fn response_prepared_repair_preserves_existing_quantum_and_refuses_stale_owner() {
    // Throughput repair uses the Latency quantum. Its existing packet floor
    // and ceiling coincide, so unequal path rates do not create unequal quanta
    // within one stream's limits. Preserve that bound at actual acquisition.
    let mut fixture = Fixture::with_tcp_rate(UnderlayProtocol::Udp, 1_000_000);
    let original = fixture
        .owner
        .binding()
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| target.observation.key != fixture.target.key)
        .unwrap();
    let original_quantum = adaptive_reliable_relay_reinjection_bytes(
        Some(original.observation.snapshot),
        TrafficClass::Throughput,
        fixture.owner.binding().mux_limits(),
    );
    assert_eq!(original_quantum, fixture.quantum);
    let work = fixture.offer();
    let before = fixture.debt();
    let PreparedOriginalClaim::Claimed(Frame::StreamData {
        offset, payload, ..
    }) = fixture.claim(&work)
    else {
        panic!("distinct target can acquire the existing repair quantum")
    };
    assert_eq!(offset, fixture.quantum as u64);
    assert_eq!(payload.len(), fixture.quantum);
    assert_eq!(fixture.debt(), before + fixture.quantum);

    for underlay in [UnderlayProtocol::Tcp, UnderlayProtocol::Udp] {
        let mut fixture = Fixture::new(underlay);
        let original = fixture
            .owner
            .binding()
            .prepared_outputs()
            .into_iter()
            .find(|output| output.identity != fixture.target)
            .unwrap()
            .identity;
        let work = fixture.offer();
        let before = fixture.debt();
        assert!(fixture.owner.binding().mark_output_stale(
            ServerReinjectionOutputIdentity {
                key: original.key,
                incarnation: original.incarnation,
            },
            TrafficClass::Throughput,
        ));
        assert!(matches!(
            fixture.claim(&work),
            PreparedOriginalClaim::Blocked(_)
        ));
        assert_eq!(
            fixture.debt(),
            before,
            "an attached stale owner belongs to existing critical recovery"
        );
    }
}

#[tokio::test]
async fn response_prepared_repair_rejects_owner_detach_before_exact_commit() {
    let mut fixture = Fixture::new(UnderlayProtocol::Tcp);
    let _work = fixture.offer();
    let before = fixture.debt();
    let generation = fixture.owner.binding().response_model_generation();
    let target: ResponseDispatchTarget = fixture
        .owner
        .binding()
        .sender_path_targets(TrafficClass::Throughput, 1)
        .into_iter()
        .find(|target| ResponseAcquisitionOutputId::from(target) == fixture.target)
        .unwrap()
        .into();
    let original = fixture
        .owner
        .binding()
        .prepared_outputs()
        .into_iter()
        .find(|output| output.identity != fixture.target)
        .unwrap()
        .identity;
    let (frame, retained) = {
        let state = fixture.owner.lock();
        (
            state
                .send_stream
                .first_retransmission_frame_for_range(
                    OffsetRange {
                        start: fixture.quantum as u64,
                        end: (2 * fixture.quantum) as u64,
                    },
                    fixture.quantum,
                )
                .unwrap(),
            state.send_stream.reinjection_bytes(),
        )
    };
    assert!(
        fixture
            .owner
            .binding()
            .begin_path_detach(original.key, original.path_instance_id,)
            .is_some()
    );
    let ready = fixture
        .receivers
        .prepared_writer_ready_boundary(fixture.target.path_instance_id, true)
        .unwrap();
    assert!(matches!(
        fixture.owner.binding().claim_reinjected_frame_for_target(
            &target,
            &frame,
            TrafficClass::Throughput,
            0,
            retained,
            ready,
            None,
            generation,
        ),
        Err(crate::runtime::RuntimeError::SenderServiceBlocked)
    ));
    assert!(
        ready.receipt().is_current(),
        "a stale model cannot consume writer authority"
    );
    assert_eq!(
        fixture.debt(),
        before,
        "an independent owner detach creates no copy"
    );
}

use super::*;
use crate::model::path::{CarrierPathInstanceId, CarrierPathKey};
use crate::protocol::{OffsetRange, PathId, PathUsage};

fn stable(membership_generation: u64) -> TcpCarrierStableGenerations {
    TcpCarrierStableGenerations {
        membership_generation,
        ordinary_eligibility_generation: NonZeroU64::new(2).expect("nonzero generation"),
        authority_class: PathUsage::Available,
        admission_policy_generation: NonZeroU64::new(3).expect("nonzero generation"),
        resource_policy_generation: NonZeroU64::new(5).expect("nonzero generation"),
    }
}

fn instance(
    underlay: UnderlayProtocol,
    path_id: u16,
    path_instance_id: u64,
    output_incarnation: u64,
) -> ServerTcpCarrierOutputInstance {
    ServerTcpCarrierOutputInstance {
        key: CarrierPathKey {
            underlay,
            path_id: PathId(path_id),
        },
        path_instance_id: CarrierPathInstanceId::from_raw(path_instance_id),
        output_incarnation,
    }
}

fn saturation(
    stable: TcpCarrierStableGenerations,
    services: &[(ServerTcpCarrierOutputInstance, u64)],
) -> ServerTcpCarrierSaturation {
    ServerTcpCarrierSaturation {
        stable,
        ordinary_services: services
            .iter()
            .map(
                |&(instance, service_pipe_bytes)| ServerTcpCarrierOrdinaryService {
                    instance,
                    service_pipe_bytes,
                },
            )
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }
}

fn active_workload(
    service: &Arc<ServerTcpCarrierService>,
    stream_id: u64,
) -> ServerTcpCarrierWorkloadLease {
    let mut workload = service
        .register_workload(StreamId(stream_id))
        .expect("unique response workload");
    assert!(workload.update_demand(TrafficClass::Throughput, true));
    workload
}

#[test]
fn exact_success_to_saturation_publishes_one_monotonic_demand() {
    let service = ServerTcpCarrierService::new();
    let mut demands = service.subscribe_demands();
    let mut workload = active_workload(&service, 0);
    let authority = stable(7);
    let ordinary = instance(UnderlayProtocol::Tcp, 1, 21, 31);

    assert!(workload.record_successful_ordinary_placement(authority));
    let demand = workload
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("one fresh transition publishes demand");
    assert_eq!(demand.request_id.get(), 1);
    assert_eq!(demand.stream_id, Some(StreamId(0)));
    assert_eq!(demands.current(), Some(demand));

    assert!(workload.record_successful_ordinary_placement(authority));
    assert!(
        workload
            .try_issue_saturation_demand(
                saturation(authority, &[(ordinary, 65_536)]),
                MuxLimits::default(),
            )
            .is_none(),
        "unchanged saturation cannot mint another request"
    );

    assert!(workload.update_demand(TrafficClass::Throughput, false));
    assert_eq!(
        demands.current(),
        Some(ServerTcpCarrierDemand {
            request_id: NonZeroU64::new(2).expect("nonzero request"),
            stream_id: None,
        })
    );
}

#[test]
fn changed_stable_generation_supersedes_the_current_request() {
    let service = ServerTcpCarrierService::new();
    let mut workload = active_workload(&service, 12);
    let ordinary = instance(UnderlayProtocol::Tcp, 2, 22, 32);
    let first = stable(8);
    assert!(workload.record_successful_ordinary_placement(first));
    let first_demand = workload
        .try_issue_saturation_demand(
            saturation(first, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("first demand");

    let changed = stable(9);
    assert!(workload.record_successful_ordinary_placement(changed));
    let second_demand = workload
        .try_issue_saturation_demand(
            saturation(changed, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("changed stable generation supersedes demand");
    assert_eq!(first_demand.request_id.get(), 1);
    assert_eq!(second_demand.request_id.get(), 2);

    let candidate = instance(UnderlayProtocol::Tcp, 3, 23, 33);
    assert!(
        service
            .admit_validation(
                first_demand.request_id,
                NonZeroU64::new(41).expect("validation ID"),
                StreamId(12),
                candidate,
                first,
                &[ordinary],
            )
            .is_none(),
        "superseded request cannot admit"
    );
    assert!(
        service
            .admit_validation(
                second_demand.request_id,
                NonZeroU64::new(42).expect("validation ID"),
                StreamId(12),
                candidate,
                changed,
                &[ordinary],
            )
            .is_some(),
        "exact current request admits"
    );
}

#[test]
fn changed_ordinary_set_without_membership_generation_is_rejected() {
    let service = ServerTcpCarrierService::new();
    let mut workload = active_workload(&service, 17);
    let authority = stable(12);
    let ordinary = instance(UnderlayProtocol::Tcp, 10, 30, 40);
    assert!(workload.record_successful_ordinary_placement(authority));
    let demand = workload
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("first exact membership demand");

    let inconsistent = instance(UnderlayProtocol::Tcp, 11, 31, 41);
    assert!(workload.record_successful_ordinary_placement(authority));
    assert!(
        workload
            .try_issue_saturation_demand(
                saturation(authority, &[(inconsistent, 65_536)]),
                MuxLimits::default(),
            )
            .is_none(),
        "an unchanged membership generation cannot describe a new ordinary set"
    );

    let candidate = instance(UnderlayProtocol::Tcp, 12, 32, 42);
    assert!(
        service
            .admit_validation(
                demand.request_id,
                NonZeroU64::new(71).expect("validation ID"),
                StreamId(17),
                candidate,
                authority,
                &[ordinary],
            )
            .is_some(),
        "inconsistent input does not replace the exact current request"
    );
}

#[test]
fn admission_freezes_complete_workloads_and_routes_exact_ack_receipts() {
    let service = ServerTcpCarrierService::new();
    let mut demands = service.subscribe_demands();
    let mut target = active_workload(&service, 13);
    let mut background = service
        .register_workload(StreamId(14))
        .expect("background response workload");
    let authority = stable(10);
    let ordinary = instance(UnderlayProtocol::Udp, 4, 24, 34);
    assert!(target.record_successful_ordinary_placement(authority));
    let demand = target
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 131_072)]),
            MuxLimits::default(),
        )
        .expect("qualified demand");
    let candidate = instance(UnderlayProtocol::Tcp, 5, 25, 35);
    let admission = service
        .admit_validation(
            demand.request_id,
            NonZeroU64::new(51).expect("validation ID"),
            StreamId(13),
            candidate,
            authority,
            &[ordinary],
        )
        .expect("exact validation admission");
    assert_eq!(admission.request_id(), demand.request_id);
    assert_eq!(admission.validation_id().get(), 51);
    assert_eq!(admission.candidate(), candidate);
    assert_eq!(admission.target(), target.identity());
    assert_eq!(admission.stable(), authority);
    assert_eq!(admission.workloads().len(), 2);
    assert_eq!(admission.ordinary_services()[0].instance, ordinary);
    assert!(admission.geometry().cohort_coverage_bytes() > 0);
    assert!(admission.revalidate(authority, &[ordinary]));

    let mut observations = admission
        .activate_observations(2)
        .expect("one bounded observation owner");
    let receipt = ResponseProductAckReceipt {
        identity: target.identity(),
        completed_at: Instant::now(),
        original_releases: SmallVec::new(),
    };
    let target_identity = target.identity();
    let receipt_target = target
        .response_product_ack_receipt_target()
        .expect("capture active");
    assert_eq!(receipt_target.identity, target_identity);
    receipt_target.publish(receipt.clone());
    let ServerTcpCarrierObservation::ProductAck(observed) =
        observations.try_recv().expect("exact receipt is routed");
    assert_eq!(observed, receipt);

    assert!(background.update_demand(TrafficClass::Latency, true));
    assert!(admission.is_withdrawn());
    assert_eq!(
        demands.current(),
        Some(ServerTcpCarrierDemand {
            request_id: NonZeroU64::new(2).expect("nonzero request"),
            stream_id: None,
        })
    );
    assert!(target.response_product_ack_receipt_target().is_none());
}

#[test]
fn malformed_or_nonexact_validation_never_consumes_current_demand() {
    let service = ServerTcpCarrierService::new();
    let mut workload = active_workload(&service, 15);
    let authority = stable(11);
    let ordinary = instance(UnderlayProtocol::Tcp, 6, 26, 36);
    assert!(workload.record_successful_ordinary_placement(authority));
    let demand = workload
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("qualified demand");

    assert!(
        service
            .admit_validation(
                demand.request_id,
                NonZeroU64::new(61).expect("validation ID"),
                StreamId(15),
                ordinary,
                authority,
                &[ordinary],
            )
            .is_none(),
        "candidate cannot alias an ordinary output"
    );
    let candidate = instance(UnderlayProtocol::Tcp, 7, 27, 37);
    assert!(
        service
            .admit_validation(
                NonZeroU64::new(99).expect("wrong request"),
                NonZeroU64::new(62).expect("validation ID"),
                StreamId(15),
                candidate,
                authority,
                &[ordinary],
            )
            .is_none()
    );
    let admission = service
        .admit_validation(
            demand.request_id,
            NonZeroU64::new(63).expect("validation ID"),
            StreamId(15),
            candidate,
            authority,
            &[ordinary],
        )
        .expect("failed references do not consume current demand");
    admission.release();
    assert!(
        service
            .admit_validation(
                demand.request_id,
                NonZeroU64::new(64).expect("validation ID"),
                StreamId(15),
                candidate,
                authority,
                &[ordinary],
            )
            .is_none(),
        "one request admits at most one candidate"
    );
}

#[test]
fn candidate_release_count_requires_exact_unambiguous_output_provenance() {
    let candidate = instance(UnderlayProtocol::Tcp, 8, 28, 38);
    let other = instance(UnderlayProtocol::Tcp, 9, 29, 39);
    let now = Instant::now();
    let receipt = ResponseProductAckReceipt {
        identity: ProductWorkloadIdentity {
            stream_id: StreamId(16),
            lifecycle_generation: NonZeroU64::new(1).expect("workload generation"),
        },
        completed_at: now,
        original_releases: smallvec::smallvec![
            ResponseProductAckOriginalRelease {
                key: candidate.key,
                path_instance_id: Some(candidate.path_instance_id),
                output_incarnation: candidate.output_incarnation,
                range: OffsetRange {
                    start: 0,
                    end: 1024,
                },
                bytes: 1024,
                sent_at: now,
                resolution: ResponseProductAckOriginalResolution::Unambiguous,
            },
            ResponseProductAckOriginalRelease {
                key: candidate.key,
                path_instance_id: Some(candidate.path_instance_id),
                output_incarnation: candidate.output_incarnation,
                range: OffsetRange {
                    start: 1024,
                    end: 2048,
                },
                bytes: 1024,
                sent_at: now,
                resolution: ResponseProductAckOriginalResolution::Ambiguous,
            },
            ResponseProductAckOriginalRelease {
                key: other.key,
                path_instance_id: Some(other.path_instance_id),
                output_incarnation: other.output_incarnation,
                range: OffsetRange {
                    start: 2048,
                    end: 4096,
                },
                bytes: 2048,
                sent_at: now,
                resolution: ResponseProductAckOriginalResolution::Unambiguous,
            },
        ],
    };
    assert_eq!(
        candidate_original_release_bytes(&receipt, candidate),
        Some(1024)
    );
}

#[test]
fn response_ack_capture_cannot_cross_validation_transactions() {
    let service = ServerTcpCarrierService::new();
    let mut first_target = active_workload(&service, 21);
    let mut second_target = service
        .register_workload(StreamId(22))
        .expect("second response workload");
    let authority = stable(14);
    let ordinary = instance(UnderlayProtocol::Tcp, 13, 33, 43);
    assert!(first_target.record_successful_ordinary_placement(authority));
    let first_demand = first_target
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("first demand");
    let first_admission = service
        .admit_validation(
            first_demand.request_id,
            NonZeroU64::new(81).expect("first validation ID"),
            StreamId(21),
            instance(UnderlayProtocol::Tcp, 14, 34, 44),
            authority,
            &[ordinary],
        )
        .expect("first validation");
    let _first_observations = first_admission
        .activate_observations(1)
        .expect("first observation transaction");
    let first_identity = first_target.identity();
    let captured = first_target
        .response_product_ack_receipt_target()
        .expect("capture exact first validation");

    assert!(second_target.update_demand(TrafficClass::Latency, true));
    assert!(first_admission.is_withdrawn());
    first_admission.release();
    assert!(second_target.update_demand(TrafficClass::Throughput, true));
    assert!(second_target.record_successful_ordinary_placement(authority));
    let second_demand = second_target
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("second demand");
    let second_admission = service
        .admit_validation(
            second_demand.request_id,
            NonZeroU64::new(82).expect("second validation ID"),
            StreamId(22),
            instance(UnderlayProtocol::Tcp, 15, 35, 45),
            authority,
            &[ordinary],
        )
        .expect("second validation");
    let mut second_observations = second_admission
        .activate_observations(1)
        .expect("second observation transaction");

    captured.publish(ResponseProductAckReceipt {
        identity: first_identity,
        completed_at: Instant::now(),
        original_releases: SmallVec::new(),
    });
    assert!(
        matches!(
            second_observations.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ),
        "an ACK captured for the first validation cannot enter the second transaction",
    );
}

#[test]
fn realtime_product_lifecycle_withdraws_and_fences_response_validation() {
    let service = ServerTcpCarrierService::new();
    let mut demands = service.subscribe_demands();
    let mut target = active_workload(&service, 23);
    let authority = stable(15);
    let ordinary = instance(UnderlayProtocol::Tcp, 16, 36, 46);
    assert!(target.record_successful_ordinary_placement(authority));
    let demand = target
        .try_issue_saturation_demand(
            saturation(authority, &[(ordinary, 65_536)]),
            MuxLimits::default(),
        )
        .expect("response demand before realtime flow");
    let admission = service
        .admit_validation(
            demand.request_id,
            NonZeroU64::new(83).expect("validation ID"),
            StreamId(23),
            instance(UnderlayProtocol::Tcp, 17, 37, 47),
            authority,
            &[ordinary],
        )
        .expect("validation before realtime flow");

    let realtime = service.register_realtime_workload();
    assert!(admission.is_withdrawn());
    assert_eq!(
        demands.current(),
        Some(ServerTcpCarrierDemand {
            request_id: NonZeroU64::new(2).expect("withdrawal request"),
            stream_id: None,
        })
    );
    admission.release();
    assert!(target.record_successful_ordinary_placement(authority));
    assert!(
        target
            .try_issue_saturation_demand(
                saturation(authority, &[(ordinary, 65_536)]),
                MuxLimits::default(),
            )
            .is_none(),
        "active realtime Product work forbids expansion",
    );

    drop(realtime);
    assert!(target.record_successful_ordinary_placement(authority));
    assert!(
        target
            .try_issue_saturation_demand(
                saturation(authority, &[(ordinary, 65_536)]),
                MuxLimits::default(),
            )
            .is_some(),
        "ending realtime work creates a new coherent workload generation",
    );
}

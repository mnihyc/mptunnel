use super::*;
use crate::model::path::{CarrierPathInstanceId, RelayPathKey};
use crate::protocol::UnderlayProtocol;
use crate::runtime::path::tcp::group::ClientTcpCarrierGroup;
use crate::transport::TcpCarrierRange;

fn groups(ranges: &[(u16, u16)]) -> Arc<ClientTcpCarrierGroups> {
    let mut groups = ranges
        .iter()
        .enumerate()
        .map(|(config_index, &(min, max))| {
            ClientTcpCarrierGroup::new(
                config_index,
                TcpCarrierRange::new(min, max).expect("valid test carrier range"),
                vec![config_index],
            )
        })
        .collect::<Vec<_>>();
    let mut next_elastic_path_index = groups.len();
    for group in &mut groups {
        for _ in group.range.min()..group.range.max() {
            group.elastic_slots.push(next_elastic_path_index);
            next_elastic_path_index += 1;
        }
    }
    ClientTcpCarrierGroups::new(groups)
}

fn stable(membership_generation: u64) -> ClientTcpCarrierStableGenerations {
    ClientTcpCarrierStableGenerations {
        membership_generation,
        ordinary_eligibility_generation: NonZeroU64::new(2).expect("non-zero test generation"),
        authority_class: PathUsage::Available,
        admission_policy_generation: NonZeroU64::new(3).expect("non-zero test generation"),
        resource_policy_generation: NonZeroU64::new(5).expect("non-zero test generation"),
    }
}

fn instance(index: usize, lifetime: u64) -> RelayPathInstance {
    RelayPathInstance {
        key: RelayPathKey {
            underlay: UnderlayProtocol::Tcp,
            index,
        },
        path_instance_id: CarrierPathInstanceId::from_raw(lifetime),
        attachment_id: lifetime + 100,
    }
}

fn saturation(
    stable: ClientTcpCarrierStableGenerations,
    instances: &[(usize, u64, u64)],
    eligible_tcp_groups: &[usize],
) -> ClientTcpCarrierSaturation {
    ClientTcpCarrierSaturation {
        stable,
        ordinary_services: instances
            .iter()
            .map(
                |&(index, lifetime, service_pipe_bytes)| ClientTcpCarrierOrdinaryService {
                    instance: instance(index, lifetime),
                    service_pipe_bytes,
                },
            )
            .collect::<Vec<_>>()
            .into_boxed_slice(),
        eligible_tcp_groups: eligible_tcp_groups.to_vec().into_boxed_slice(),
    }
}

fn active_throughput_workload(
    service: &Arc<ClientTcpCarrierService>,
    stream_id: u64,
) -> ClientTcpCarrierWorkloadLease {
    let mut workload = service
        .register_workload(StreamId(stream_id))
        .expect("unique workload");
    assert!(workload.update_demand(TrafficClass::Throughput, true));
    workload
}

fn occupy_minimum(
    groups: &Arc<ClientTcpCarrierGroups>,
    config_index: usize,
) -> ClientTcpCarrierReservation {
    groups
        .reserve(config_index)
        .expect("configured minimum owns its physical reservation")
}

#[test]
fn exact_success_to_saturation_transition_is_consumed_once() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let minimum = occupy_minimum(&groups, 0);
    let mut workload = active_throughput_workload(&service, 0);
    let authority = stable(7);

    assert!(workload.record_successful_ordinary_placement(authority));
    let admission = workload
        .try_admit_saturation(
            saturation(authority, &[(0, 21, 32_768)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("fresh transition admits one candidate");
    assert_eq!(admission.validation_id().get(), 1);
    assert_eq!(admission.admission_generation().get(), 1);
    assert_eq!(groups.occupied(0), Some(2));

    assert!(
        workload
            .try_admit_saturation(
                saturation(authority, &[(0, 21, 32_768)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "an unchanged blocked observation is not another transition"
    );
    drop(admission);
    assert_eq!(groups.occupied(0), Some(1));

    assert!(workload.record_successful_ordinary_placement(authority));
    assert!(
        workload
            .try_admit_saturation(
                saturation(authority, &[(0, 21, 32_768)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "candidate settlement cannot reauthorize the same generation"
    );
    drop(minimum);
}

#[test]
fn server_demands_are_session_monotonic_and_conflicts_are_rejected() {
    let service = ClientTcpCarrierService::new();
    let demands = service.subscribe_server_demands();
    let current = ClientTcpCarrierDemand {
        request_id: NonZeroU64::new(2).expect("nonzero request"),
        stream_id: Some(StreamId(0)),
    };
    assert!(service.apply_server_demand(current).is_ok());
    assert_eq!(*demands.borrow(), Some(current));

    assert!(service.apply_server_demand(current).is_ok());
    assert!(
        service
            .apply_server_demand(ClientTcpCarrierDemand {
                request_id: NonZeroU64::new(1).expect("older request"),
                stream_id: Some(StreamId(99)),
            })
            .is_ok(),
        "older requests are ignored independent of their content"
    );
    assert_eq!(*demands.borrow(), Some(current));

    assert_eq!(
        service.apply_server_demand(ClientTcpCarrierDemand {
            request_id: current.request_id,
            stream_id: None,
        }),
        Err(ClientTcpCarrierDemandConflict)
    );
    assert_eq!(*demands.borrow(), Some(current));

    let withdrawal = ClientTcpCarrierDemand {
        request_id: NonZeroU64::new(3).expect("newer request"),
        stream_id: None,
    };
    assert!(service.apply_server_demand(withdrawal).is_ok());
    assert_eq!(*demands.borrow(), Some(withdrawal));
}

#[test]
fn demand_or_stable_generation_change_is_required_for_later_admission() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let _minimum = occupy_minimum(&groups, 0);
    let mut workload = active_throughput_workload(&service, 12);
    let first = stable(8);
    assert!(workload.record_successful_ordinary_placement(first));
    let admission = workload
        .try_admit_saturation(
            saturation(first, &[(0, 31, 65_536)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("first admission");
    drop(admission);

    let changed_membership = stable(9);
    assert!(workload.record_successful_ordinary_placement(changed_membership));
    let second = workload
        .try_admit_saturation(
            saturation(changed_membership, &[(0, 32, 65_536)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("stable membership change grants a fresh generation");
    assert_eq!(second.admission_generation().get(), 2);
    drop(second);

    assert!(workload.update_demand(TrafficClass::Latency, false));
    assert!(workload.update_demand(TrafficClass::Throughput, true));
    assert!(workload.record_successful_ordinary_placement(changed_membership));
    let third = workload
        .try_admit_saturation(
            saturation(changed_membership, &[(0, 32, 65_536)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("a new continuous demand episode grants fresh authority");
    assert_eq!(third.admission_generation().get(), 3);
}

#[test]
fn one_candidate_and_validation_own_the_group_reservation_by_raii() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let minimum = occupy_minimum(&groups, 0);
    let mut first_workload = active_throughput_workload(&service, 13);
    let mut second_workload = active_throughput_workload(&service, 14);
    let authority = stable(10);
    assert!(first_workload.record_successful_ordinary_placement(authority));
    let admission = first_workload
        .try_admit_saturation(
            saturation(authority, &[(0, 41, 131_072)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("one candidate");
    assert_eq!(admission.path_id(), PathId(1));
    let ClientTcpCarrierValidationParts {
        admission: mut service_admission,
        reservation,
    } = admission.into_validation_parts();
    assert_eq!(reservation.path_id(), PathId(1));
    assert!(service_admission.begin_validation());
    assert!(
        !service_admission.begin_validation(),
        "one validation begins once"
    );

    assert!(second_workload.record_successful_ordinary_placement(authority));
    assert!(
        second_workload
            .try_admit_saturation(
                saturation(authority, &[(0, 41, 131_072)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "one session owns at most one unretained candidate"
    );
    assert_eq!(groups.occupied(0), Some(2));
    assert!(service_admission.commit_retained());
    assert_eq!(groups.occupied(0), Some(2));
    drop(reservation);
    assert_eq!(groups.occupied(0), Some(1));
    drop(minimum);
}

#[test]
fn group_selection_is_bounded_and_deduplicated() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 1), (1, 2)]);
    let occupied_minimum = groups.reserve(0).expect("fill first group");
    let second_minimum = occupy_minimum(&groups, 1);
    let mut workload = active_throughput_workload(&service, 15);
    let authority = stable(11);
    assert!(workload.record_successful_ordinary_placement(authority));
    let admission = workload
        .try_admit_saturation(
            saturation(authority, &[(0, 51, 262_144)], &[0, 0, 1, 1]),
            &groups,
            MuxLimits::default(),
        )
        .expect("next eligible group has elastic capacity");
    assert_eq!(admission.config_index(), 1);
    assert_eq!(groups.occupied(0), Some(1));
    assert_eq!(groups.occupied(1), Some(2));
    drop(admission);
    drop(occupied_minimum);
    drop(second_minimum);
    assert_eq!(groups.occupied(0), Some(0));
    assert_eq!(groups.occupied(1), Some(0));
}

#[test]
fn frozen_workload_membership_and_geometry_are_revalidated() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let _minimum = occupy_minimum(&groups, 0);
    let mut target = active_throughput_workload(&service, 16);
    let background = service
        .register_workload(StreamId(17))
        .expect("second complete workload");
    let authority = stable(12);
    assert!(target.record_successful_ordinary_placement(authority));
    let mut admission = target
        .try_admit_saturation(
            saturation(authority, &[(1, 62, 32_768), (0, 61, 65_536)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("frozen comparison");
    assert_eq!(admission.workloads().len(), 2);
    assert_eq!(admission.target(), target.identity());
    assert_eq!(admission.ordinary_services()[0].instance, instance(0, 61));
    assert_eq!(admission.ordinary_services()[1].instance, instance(1, 62));
    let ordinary_instances = admission
        .ordinary_services()
        .iter()
        .map(|ordinary| ordinary.instance)
        .collect::<Vec<_>>();
    assert!(admission.revalidate(authority, &ordinary_instances));
    assert!(admission.begin_validation());

    assert!(target.update_demand(TrafficClass::Throughput, false));
    assert!(
        !admission.is_withdrawn() && admission.revalidate(authority, &ordinary_instances),
        "successful placement may drain the bounded queue without ending the throughput episode"
    );
    assert!(target.update_demand(TrafficClass::Throughput, true));

    let added = service
        .register_workload(StreamId(18))
        .expect("new concurrent Product workload");
    assert!(admission.is_withdrawn());
    assert!(!admission.revalidate(authority, &ordinary_instances));
    drop(added);
    drop(background);
}

#[test]
fn latency_work_and_inconsistent_membership_cannot_mint_authority() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let _minimum = occupy_minimum(&groups, 0);
    let mut target = active_throughput_workload(&service, 19);
    let mut latency = service
        .register_workload(StreamId(20))
        .expect("latency workload");
    assert!(latency.update_demand(TrafficClass::Latency, true));
    let authority = stable(13);
    assert!(target.record_successful_ordinary_placement(authority));
    assert!(
        target
            .try_admit_saturation(
                saturation(authority, &[(0, 71, 65_536)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "active latency work excludes expansion"
    );

    assert!(latency.update_demand(TrafficClass::Latency, false));
    assert!(target.record_successful_ordinary_placement(authority));
    assert!(
        target
            .try_admit_saturation(
                saturation(authority, &[(0, 71, 65_536)], &[99]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "the exact generation is recorded even when no eligible group can reserve"
    );

    assert!(target.record_successful_ordinary_placement(authority));
    assert!(
        target
            .try_admit_saturation(
                saturation(authority, &[(0, 72, 65_536)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "changed instances under one membership generation are inconsistent"
    );
}

#[test]
fn fresh_and_retained_direction_validation_share_one_session_transaction() {
    let service = ClientTcpCarrierService::new();
    let groups = groups(&[(1, 3)]);
    let _minimum = occupy_minimum(&groups, 0);
    let mut workload = active_throughput_workload(&service, 21);
    let authority = stable(14);

    let retained = service
        .reserve_retained_direction_validation(PathMetricDirection::ServerToClient)
        .expect("one retained-direction transaction");
    assert_eq!(retained.validation_id().get(), 1);
    assert_eq!(retained.direction(), PathMetricDirection::ServerToClient);

    assert!(workload.record_successful_ordinary_placement(authority));
    assert!(
        workload
            .try_admit_saturation(
                saturation(authority, &[(0, 81, 65_536)], &[0]),
                &groups,
                MuxLimits::default(),
            )
            .is_none(),
        "a retained-direction transaction excludes a fresh candidate"
    );
    drop(retained);

    assert!(workload.record_successful_ordinary_placement(authority));
    let candidate = workload
        .try_admit_saturation(
            saturation(authority, &[(0, 81, 65_536)], &[0]),
            &groups,
            MuxLimits::default(),
        )
        .expect("released transaction admits a fresh candidate");
    assert_eq!(candidate.validation_id().get(), 2);
    assert!(
        service
            .reserve_retained_direction_validation(PathMetricDirection::ClientToServer)
            .is_none(),
        "a fresh candidate excludes retained-direction validation"
    );
    drop(candidate);

    let next = service
        .reserve_retained_direction_validation(PathMetricDirection::ClientToServer)
        .expect("candidate release restores the sole transaction slot");
    assert_eq!(next.validation_id().get(), 3);
}

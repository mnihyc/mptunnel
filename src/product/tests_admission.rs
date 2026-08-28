use super::*;
use std::sync::{Arc, Barrier};

fn principal(value: &str) -> PrincipalId {
    PrincipalId::parse(value).expect("principal")
}

fn target(value: &str) -> ProtocolTarget {
    ProtocolTarget::parse_authority(value).expect("target")
}

fn outbound(value: &str) -> OutboundId {
    OutboundId::parse(value).expect("outbound")
}

fn small_limits() -> ProductAdmissionConfig {
    ProductAdmissionConfig {
        max_live_flows: 2,
        max_concurrent_work: 2,
        max_live_flows_per_principal: 2,
        max_live_flows_per_outbound: 2,
        max_connects_per_outbound: 1,
        max_live_flows_per_target: 2,
        max_connects_per_target: 1,
        max_dns_work: 1,
    }
}

#[test]
fn every_default_product_admission_boundary_is_4096() {
    let limits = ProductAdmissionConfig::default();
    assert_eq!(
        [
            limits.max_live_flows,
            limits.max_concurrent_work,
            limits.max_live_flows_per_principal,
            limits.max_live_flows_per_outbound,
            limits.max_connects_per_outbound,
            limits.max_live_flows_per_target,
            limits.max_connects_per_target,
            limits.max_dns_work,
        ],
        [4_096; 8]
    );
}

#[test]
fn limits_are_strict_and_independent_dimensions_recover_exactly() {
    let admission = ProductAdmission::new(small_limits()).expect("admission");
    let first = admission
        .try_admit_flow(principal("alice"), target("one.example:443"))
        .expect("first flow");
    let first_connect = first
        .try_begin_connect(outbound("edge"))
        .expect("first connect");
    assert_eq!(
        first
            .try_begin_connect(outbound("edge"))
            .expect_err("outbound connect limit")
            .rejection(),
        ProductAdmissionRejection::OutboundConnects
    );
    let competing = admission
        .try_admit_flow(principal("bob"), target("two.example:443"))
        .expect("competing flow");
    let competing_connect = competing
        .try_begin_connect(outbound("backup"))
        .expect("competing connect");
    assert_eq!(
        admission
            .try_admit_dns_work()
            .expect_err("global work is full")
            .rejection(),
        ProductAdmissionRejection::GlobalConcurrentWork
    );

    drop(competing_connect);
    drop(competing);
    drop(first_connect);
    let dns = admission.try_admit_dns_work().expect("DNS work");
    assert_eq!(
        admission
            .try_admit_dns_work()
            .expect_err("DNS limit")
            .rejection(),
        ProductAdmissionRejection::DnsWork
    );
    drop(dns);

    let connected = first
        .try_begin_connect(outbound("edge"))
        .expect("recovered connect")
        .connected();
    let first = first.commit(connected);
    let second = admission
        .try_admit_flow(principal("alice"), target("one.example:443"))
        .expect("second flow");
    assert_eq!(
        admission
            .try_admit_flow(principal("bob"), target("two.example:443"))
            .expect_err("global flow limit")
            .rejection(),
        ProductAdmissionRejection::GlobalLiveFlows
    );
    drop(second);
    drop(first);

    let snapshot = admission.snapshot();
    assert_eq!(snapshot.live_flows, 0);
    assert_eq!(snapshot.concurrent_work, 0);
    assert_eq!(snapshot.dns_work, 0);
    assert!(snapshot.principals.is_empty());
    assert!(snapshot.outbounds.is_empty());
    assert!(snapshot.targets.is_empty());
    assert_eq!(snapshot.rejections.global_live_flows, 1);
    assert_eq!(snapshot.rejections.global_concurrent_work, 1);
    assert_eq!(snapshot.rejections.outbound_connects, 1);
    assert_eq!(snapshot.rejections.dns_work, 1);
}

#[test]
fn concurrent_admission_never_overshoots_and_old_generation_drop_is_isolated() {
    let limits = ProductAdmissionConfig {
        max_live_flows: 8,
        max_live_flows_per_principal: 8,
        max_live_flows_per_outbound: 8,
        max_live_flows_per_target: 8,
        max_concurrent_work: 8,
        max_connects_per_outbound: 8,
        max_connects_per_target: 8,
        max_dns_work: 8,
    };
    let old = ProductAdmission::new(limits).expect("old generation");
    let replacement = ProductAdmission::new(limits).expect("replacement generation");
    let start = Arc::new(Barrier::new(17));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let admission = old.clone();
        let start = start.clone();
        tasks.push(std::thread::spawn(move || {
            start.wait();
            admission
                .try_admit_flow(principal("shared"), target("shared.example:443"))
                .ok()
                .map(|flow| {
                    let connect = flow
                        .try_begin_connect(outbound("edge"))
                        .expect("one connect per admitted flow");
                    flow.commit(connect.connected())
                })
        }));
    }
    start.wait();
    let leases = tasks
        .into_iter()
        .map(|task| task.join().expect("admission worker"))
        .collect::<Vec<_>>();

    assert_eq!(old.snapshot().live_flows, 8);
    assert_eq!(replacement.snapshot().live_flows, 0);
    assert_eq!(leases.iter().filter(|lease| lease.is_some()).count(), 8);
    assert_eq!(old.snapshot().rejections.global_live_flows, 8);

    drop(leases);
    assert_eq!(old.snapshot().live_flows, 0);
    assert_eq!(replacement.snapshot().live_flows, 0);
}

#[tokio::test]
async fn task_cancellation_releases_pending_connect_and_dns_work() {
    let admission = ProductAdmission::new(small_limits()).expect("admission");
    let (ready, ready_rx) = tokio::sync::oneshot::channel();
    let worker_admission = admission.clone();
    let worker = tokio::spawn(async move {
        let flow = worker_admission
            .try_admit_flow(principal("alice"), target("cancel.example:443"))
            .expect("flow");
        let _connect = flow.try_begin_connect(outbound("edge")).expect("connect");
        let _dns = worker_admission.try_admit_dns_work().expect("DNS");
        ready.send(()).expect("ready receiver");
        std::future::pending::<()>().await;
    });
    ready_rx.await.expect("worker ready");
    assert_eq!(admission.snapshot().live_flows, 1);
    assert_eq!(admission.snapshot().concurrent_work, 2);
    worker.abort();
    assert!(worker.await.expect_err("task aborted").is_cancelled());
    let snapshot = admission.snapshot();
    assert_eq!(snapshot.live_flows, 0);
    assert_eq!(snapshot.concurrent_work, 0);
    assert_eq!(snapshot.dns_work, 0);
    assert!(snapshot.outbounds.is_empty());
    assert!(snapshot.targets.is_empty());
}

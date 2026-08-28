use super::*;

fn carrier_group(max: u16, members: Vec<usize>) -> Arc<ClientTcpCarrierGroups> {
    ClientTcpCarrierGroups::new(vec![ClientTcpCarrierGroup::new(
        0,
        TcpCarrierRange::new(max).expect("test TCP carrier maximum"),
        members,
    )])
}

#[test]
fn one_group_owns_at_most_one_transient_planned_successor() {
    let groups = carrier_group(2, vec![0, 1]);
    let first = groups.reserve(0).expect("first configured member");
    let second = groups.reserve(0).expect("second configured member");
    assert!(groups.reserve(0).is_none());

    let successor = groups
        .reserve_planned_replacement(0, first.path_id())
        .expect("one transient successor");
    assert_eq!(groups.occupied(0), Some(3));
    assert!(groups.has_planned_replacement_overlap(0));
    assert!(
        groups
            .reserve_planned_replacement(0, second.path_id())
            .is_none(),
        "another member in the group cannot overlap a second successor"
    );

    drop(first);
    assert_eq!(groups.occupied(0), Some(2));
    assert!(!groups.has_planned_replacement_overlap(0));
    let second_successor = groups
        .reserve_planned_replacement(0, second.path_id())
        .expect("predecessor retirement releases the group-scoped overlap");
    assert_ne!(second_successor.path_id(), successor.path_id());
    assert_ne!(second_successor.path_id(), second.path_id());
    assert!(
        groups
            .reserve_planned_replacement(0, successor.path_id())
            .is_none()
    );

    drop(second_successor);
    assert_eq!(groups.occupied(0), Some(2));
    let retried = groups
        .reserve_planned_replacement(0, second.path_id())
        .expect("failed successor also releases the group-scoped overlap");
    assert_ne!(retried.path_id(), successor.path_id());
    assert_ne!(retried.path_id(), second.path_id());
}

#[test]
fn unrelated_member_failure_cannot_release_an_active_replacement_overlap() {
    let groups = carrier_group(3, vec![0, 1, 2]);
    let predecessor = groups.reserve(0).expect("replacement predecessor");
    let sibling = groups.reserve(0).expect("stable sibling");
    let unrelated = groups.reserve(0).expect("unrelated member");
    let successor = groups
        .reserve_planned_replacement(0, predecessor.path_id())
        .expect("first group-scoped successor");
    assert_eq!(groups.occupied(0), Some(4));

    drop(unrelated);
    assert_eq!(groups.occupied(0), Some(3));
    assert!(
        groups
            .reserve_planned_replacement(0, sibling.path_id())
            .is_none(),
        "an unrelated member drop must not end predecessor/successor overlap"
    );

    drop(predecessor);
    assert_eq!(groups.occupied(0), Some(2));
    let restored = groups.reserve(0).expect("restore failed unrelated member");
    let next_successor = groups
        .reserve_planned_replacement(0, sibling.path_id())
        .expect("exact predecessor retirement ends the first overlap");
    assert_ne!(next_successor.path_id(), successor.path_id());
    assert_ne!(next_successor.path_id(), restored.path_id());
}

#[tokio::test(start_paused = true)]
async fn failed_replacement_is_deferred_by_one_complete_maintenance_interval() {
    let now = tokio::time::Instant::now();
    let interval = Duration::from_secs(300);
    let mut retry = ClientTcpMemberRetry::new(now);

    retry.defer_maintenance(now, interval);

    assert_eq!(retry.next_maintenance_at(), Some(now + interval));
    tokio::time::advance(interval - Duration::from_millis(1)).await;
    assert!(tokio::time::Instant::now() < retry.next_maintenance_at().unwrap());
    tokio::time::advance(Duration::from_millis(1)).await;
    assert_eq!(
        tokio::time::Instant::now(),
        retry.next_maintenance_at().unwrap()
    );
}

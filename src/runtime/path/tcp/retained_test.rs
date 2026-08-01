use super::*;
use crate::runtime::path::commands::reliable_path_command_channels;

fn key(index: usize) -> RelayPathKey {
    RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index,
    }
}

fn instance(raw: u64) -> CarrierPathInstanceId {
    CarrierPathInstanceId::from_raw(raw)
}

fn registry() -> Arc<ClientTcpRetainedCarrierRegistry> {
    ClientTcpRetainedCarrierRegistry::new(ClientTcpCarrierService::new())
}

fn insert(
    registry: &ClientTcpRetainedCarrierRegistry,
    config_index: usize,
    key: RelayPathKey,
    path_instance_id: CarrierPathInstanceId,
    direction: PathMetricDirection,
) {
    let (commands, _receivers) = reliable_path_command_channels(8);
    assert!(registry.insert(config_index, key, path_instance_id, commands, direction,));
}

#[test]
fn directional_settlement_preserves_exact_existing_authority() {
    let registry = registry();
    let key = key(4);
    let path_instance_id = instance(41);
    insert(
        &registry,
        0,
        key,
        path_instance_id,
        PathMetricDirection::ClientToServer,
    );
    assert!(registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ClientToServer
    ));
    assert!(!registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ServerToClient
    ));

    let negative = registry
        .begin_directional_validation(key, path_instance_id, PathMetricDirection::ServerToClient)
        .expect("opposite direction can validate");
    assert_eq!(negative.validation_id().get(), 1);
    assert_eq!(negative.direction(), PathMetricDirection::ServerToClient);
    assert!(negative.settle_without_retain());
    assert!(registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ClientToServer
    ));
    assert!(!registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ServerToClient
    ));

    let retained = registry
        .begin_directional_validation(key, path_instance_id, PathMetricDirection::ServerToClient)
        .expect("a later validation may retry after a negative result");
    assert_eq!(retained.validation_id().get(), 2);
    assert!(retained.commit_retained());
    assert!(registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ClientToServer
    ));
    assert!(registry.direction_authorized(
        key,
        path_instance_id,
        PathMetricDirection::ServerToClient
    ));
    assert!(
        registry
            .begin_directional_validation(
                key,
                path_instance_id,
                PathMetricDirection::ServerToClient,
            )
            .is_none(),
        "acknowledged authority cannot be validated again"
    );
}

#[test]
fn retained_carriers_share_one_session_validation_owner() {
    let registry = registry();
    let first_key = key(5);
    let second_key = key(6);
    let first_instance = instance(51);
    let second_instance = instance(61);
    insert(
        &registry,
        0,
        first_key,
        first_instance,
        PathMetricDirection::ClientToServer,
    );
    insert(
        &registry,
        1,
        second_key,
        second_instance,
        PathMetricDirection::ClientToServer,
    );

    let first = registry
        .begin_directional_validation(
            first_key,
            first_instance,
            PathMetricDirection::ServerToClient,
        )
        .expect("first retained carrier owns the session transaction");
    assert!(
        registry
            .begin_directional_validation(
                second_key,
                second_instance,
                PathMetricDirection::ServerToClient,
            )
            .is_none(),
        "another retained carrier cannot validate concurrently"
    );
    drop(first);

    let second = registry
        .begin_directional_validation(
            second_key,
            second_instance,
            PathMetricDirection::ServerToClient,
        )
        .expect("dropping the exact owner releases the session transaction");
    assert_eq!(second.validation_id().get(), 2);
}

#[test]
fn stale_lease_cannot_settle_a_replacement_instance() {
    let registry = registry();
    let key = key(7);
    let old_instance = instance(71);
    let new_instance = instance(72);
    insert(
        &registry,
        2,
        key,
        old_instance,
        PathMetricDirection::ClientToServer,
    );
    let stale = registry
        .begin_directional_validation(key, old_instance, PathMetricDirection::ServerToClient)
        .expect("old exact instance validates");
    assert!(registry.remove(key, old_instance));

    insert(
        &registry,
        2,
        key,
        new_instance,
        PathMetricDirection::ClientToServer,
    );
    let current = registry
        .begin_directional_validation(key, new_instance, PathMetricDirection::ServerToClient)
        .expect("replacement owns an independent transaction");
    assert_eq!(current.validation_id().get(), 2);
    drop(stale);
    assert!(current.commit_retained());
    assert!(registry.direction_authorized(key, new_instance, PathMetricDirection::ServerToClient));
    assert!(!registry.direction_authorized(key, old_instance, PathMetricDirection::ServerToClient));
}

#[test]
fn endpoint_drain_revokes_placement_and_releases_validation_ownership() {
    let registry = registry();
    let draining_key = key(8);
    let remaining_key = key(9);
    let draining_instance = instance(81);
    let remaining_instance = instance(91);
    insert(
        &registry,
        3,
        draining_key,
        draining_instance,
        PathMetricDirection::ClientToServer,
    );
    insert(
        &registry,
        4,
        remaining_key,
        remaining_instance,
        PathMetricDirection::ClientToServer,
    );
    let abandoned = registry
        .begin_directional_validation(
            draining_key,
            draining_instance,
            PathMetricDirection::ServerToClient,
        )
        .expect("draining carrier initially owns validation");

    registry.begin_endpoint_drain(3);
    assert!(!registry.direction_authorized(
        draining_key,
        draining_instance,
        PathMetricDirection::ClientToServer
    ));
    assert!(
        registry
            .begin_directional_validation(
                draining_key,
                draining_instance,
                PathMetricDirection::ServerToClient,
            )
            .is_none(),
        "retirement cannot be reversed by new validation"
    );

    let remaining = registry
        .begin_directional_validation(
            remaining_key,
            remaining_instance,
            PathMetricDirection::ServerToClient,
        )
        .expect("retirement releases the session owner for another carrier");
    assert_eq!(remaining.validation_id().get(), 2);
    drop(abandoned);
    assert!(remaining.settle_without_retain());
}

use super::*;

#[test]
fn exact_registration_lifetime_distinguishes_initial_state_from_outage() {
    let inventory = AuthenticatedCarrierInventory::default();
    assert_eq!(
        inventory.snapshot(),
        AuthenticatedCarrierSnapshot {
            live_count: 0,
            ever_authenticated: false,
        }
    );
    assert_eq!(
        inventory.snapshot().availability(),
        AuthenticatedCarrierAvailability::AwaitingFirstCarrier
    );

    let first = inventory.register();
    let second = inventory.register();
    assert_eq!(inventory.snapshot().live_count, 2);
    assert_eq!(
        inventory.snapshot().availability(),
        AuthenticatedCarrierAvailability::Available
    );

    drop(first);
    assert_eq!(inventory.snapshot().live_count, 1);
    drop(second);
    assert_eq!(
        inventory.snapshot(),
        AuthenticatedCarrierSnapshot {
            live_count: 0,
            ever_authenticated: true,
        }
    );
    assert_eq!(
        inventory.snapshot().availability(),
        AuthenticatedCarrierAvailability::Offline
    );

    assert_eq!(
        AuthenticatedCarrierInventory::default()
            .snapshot()
            .availability(),
        AuthenticatedCarrierAvailability::AwaitingFirstCarrier,
        "a fresh runtime generation must begin independently"
    );
}

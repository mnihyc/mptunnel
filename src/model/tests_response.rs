use super::*;
use crate::protocol::{PathId, UnderlayProtocol};

fn key(underlay: UnderlayProtocol, path_id: u16) -> CarrierPathKey {
    CarrierPathKey {
        underlay,
        path_id: PathId(path_id),
    }
}

#[test]
fn ordering_debt_counts_only_lower_ranges_on_other_paths() {
    let candidate = key(UnderlayProtocol::Tcp, 0);
    let lower_flights = [
        CarrierPathFlightDebt {
            key: candidate,
            output_incarnation: 7,
            bytes: 32 * 1024,
        },
        CarrierPathFlightDebt {
            key: key(UnderlayProtocol::Tcp, 1),
            output_incarnation: 8,
            bytes: 64 * 1024,
        },
        CarrierPathFlightDebt {
            key: key(UnderlayProtocol::Udp, 0),
            output_incarnation: 9,
            bytes: 128 * 1024,
        },
    ];

    assert_eq!(
        response_ordering_debt_bytes(&lower_flights, candidate, 7),
        192 * 1024
    );
}

#[test]
fn ordering_debt_is_zero_when_one_path_owns_every_lower_range() {
    let candidate = key(UnderlayProtocol::Udp, 2);
    let lower_flights = [
        CarrierPathFlightDebt {
            key: candidate,
            output_incarnation: 7,
            bytes: 16 * 1024,
        },
        CarrierPathFlightDebt {
            key: candidate,
            output_incarnation: 7,
            bytes: 48 * 1024,
        },
    ];

    assert_eq!(
        response_ordering_debt_bytes(&lower_flights, candidate, 7),
        0
    );
}

#[test]
fn oldest_lower_owner_follows_connection_sequence_order() {
    let oldest = key(UnderlayProtocol::Tcp, 3);
    let lower_flights = [
        CarrierPathFlightDebt {
            key: oldest,
            output_incarnation: 11,
            bytes: 4096,
        },
        CarrierPathFlightDebt {
            key: key(UnderlayProtocol::Udp, 4),
            output_incarnation: 12,
            bytes: 4096,
        },
    ];

    assert_eq!(
        response_oldest_lower_flight_owner(&lower_flights),
        Some((oldest, 11))
    );
}

#[test]
fn replacement_with_same_path_id_does_not_inherit_old_ordering_credit() {
    let candidate = key(UnderlayProtocol::Tcp, 5);
    let lower_flights = [CarrierPathFlightDebt {
        key: candidate,
        output_incarnation: 17,
        bytes: 32 * 1024,
    }];

    assert_eq!(
        response_ordering_debt_bytes(&lower_flights, candidate, 18),
        32 * 1024
    );
}

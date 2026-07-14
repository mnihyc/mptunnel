
use super::*;

#[test]
fn extra_traffic_hint_default_is_five_percent() {
    assert_eq!(
        MppPerformanceConfig::default().extra_traffic_hint_percent,
        5
    );
}

#[test]
fn udp_paths_reject_unknown_query_parameters() {
    let default_path = "udp://127.0.0.1:443"
        .parse::<PathSpec>()
        .expect("default udp path parses");

    assert_eq!(
        default_path.underlay,
        crate::protocol::UnderlayProtocol::Udp
    );
    assert!(
        "udp://127.0.0.1:443?unsupported=true"
            .parse::<PathSpec>()
            .is_err()
    );
    assert!(
        "udp://127.0.0.1:443?profile=experimental"
            .parse::<PathSpec>()
            .is_err()
    );
}

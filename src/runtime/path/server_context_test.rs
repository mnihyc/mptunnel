use super::*;

#[test]
fn local_listener_configuration_is_independent_of_peer_path_id() {
    let spec = "udp://127.0.0.1:12900?srtt-ms=73&rate-mbps=420&backup=true&bulk=false"
        .parse::<PathSpec>()
        .expect("server UDP path");
    let local = ServerLocalPath::new(7, spec);
    let metrics = local.startup_metrics(PathId(0));

    assert_eq!(local.config_ordinal(), 7);
    assert_eq!(local.underlay(), UnderlayProtocol::Udp);
    assert_eq!(local.advertised_usage(), crate::protocol::PathUsage::Backup);
    assert!(local.policy().backup);
    assert!(!local.policy().bulk_allowed);
    assert_eq!(metrics.path_id, PathId(0));
    assert_eq!(metrics.underlay, UnderlayProtocol::Udp);
    assert_eq!(metrics.srtt_us, 73_000);
    assert_eq!(metrics.delivery_rate_bps, 420_000_000);
}

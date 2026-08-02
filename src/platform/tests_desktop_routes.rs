use super::*;

fn default_candidate(
    interface_index: u32,
    raw_route_metric: u32,
    interface_metric: u32,
) -> NativeRouteCandidate {
    NativeRouteCandidate {
        route: ProcessNativeRoute::new(
            AddressFamily::Ipv4,
            interface_index,
            Some("192.0.2.1".parse().expect("gateway")),
            raw_route_metric,
        )
        .expect("native route"),
        preference_metric: windows_effective_route_metric(raw_route_metric, interface_metric),
    }
}

#[test]
fn complete_windows_metric_ranks_routes_without_mutating_raw_offset() {
    assert_eq!(
        windows_effective_route_metric(u32::MAX, u32::MAX),
        u64::from(u32::MAX) * 2,
        "preference addition must not overflow or clamp"
    );

    let mut defaults = BTreeMap::new();
    insert_preferred_default(&mut defaults, default_candidate(7, 40, 5)).expect("first route");
    insert_preferred_default(&mut defaults, default_candidate(8, 1, 100))
        .expect("higher complete metric is ignored");

    let selected = defaults
        .get(&AddressFamily::Ipv4)
        .expect("selected default");
    assert_eq!(selected.route.interface_index().get(), 7);
    assert_eq!(
        selected.route.metric(),
        40,
        "bypass recreation retains the raw route offset"
    );
}

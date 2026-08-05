#[cfg(target_os = "windows")]
use super::*;

#[cfg(target_os = "windows")]
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

#[cfg(target_os = "windows")]
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

#[cfg(target_os = "macos")]
#[test]
fn scoped_macos_route_is_not_a_global_route_or_an_unscoped_match() {
    use super::*;

    let unscoped = Route::new("0.0.0.0".parse().expect("destination"), 0).with_if_index(7);
    let scoped = unscoped.clone().with_if_scope(true);

    assert!(route_is_global(&unscoped));
    assert!(!route_is_global(&scoped));
    assert!(!routes_equal(&unscoped, &scoped));
}

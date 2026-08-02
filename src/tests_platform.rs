use super::*;

#[test]
fn platform_report_contains_current_platform_capabilities() {
    let report = PlatformReport::current();
    let text = report.render_text();

    assert!(text.contains(std::env::consts::OS));
    assert!(text.contains(std::env::consts::ARCH));
    assert!(text.contains("managed_vpn:"));
    assert!(text.contains("native-socket VPN bypass"));
    assert_eq!(
        report.managed_vpn_platform,
        VpnPlatform::current()
            .expect("declared test target")
            .as_str()
    );
    assert!(!text.contains("release_targets:"));
}

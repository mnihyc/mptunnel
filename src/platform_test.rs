use super::*;

#[test]
fn platform_report_contains_current_platform_and_targets() {
    let report = PlatformReport::current();
    let text = report.render_text();

    assert!(text.contains(std::env::consts::OS));
    assert!(text.contains(std::env::consts::ARCH));
    assert!(text.contains("release_targets:"));
    assert!(
        RELEASE_TARGETS
            .iter()
            .any(|target| { target.os == "linux" && target.arch == "amd64" })
    );
    assert!(
        RELEASE_TARGETS
            .iter()
            .any(|target| { target.os == "windows" && target.arch == "aarch64" })
    );
    assert!(
        RELEASE_TARGETS
            .iter()
            .any(|target| target.triple == "aarch64-linux-android")
    );
}

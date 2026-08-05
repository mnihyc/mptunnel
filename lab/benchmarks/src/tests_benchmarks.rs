use super::*;

#[test]
fn deterministic_developer_gates_pass() {
    let report = run_benchmarks();
    assert!(
        report.gates.iter().all(|gate| gate.passed),
        "failed deterministic gates: {:?}",
        report
            .gates
            .iter()
            .filter(|gate| !gate.passed)
            .collect::<Vec<_>>()
    );
    assert!(
        report
            .gates
            .iter()
            .any(|gate| gate.name == "page_load_complete")
    );
    assert!(
        report
            .gates
            .iter()
            .any(|gate| gate.name == "stream_ram_budget")
    );
}

#[test]
fn benchmark_json_contains_profile_and_gates() {
    let report = run_benchmarks();
    let json = report.render_json().expect("json");

    assert!(json.contains("\"profile\""));
    assert!(json.contains("\"developer-gates-v1\""));
    assert!(json.contains("\"gates\""));
}

#[test]
fn ablation_report_compares_single_and_multipath_profiles() {
    let report = run_ablation_study();

    assert!(
        report
            .rows
            .iter()
            .any(|row| row.name == "single_low_latency")
    );
    assert!(report.rows.iter().any(|row| row.name == "multipath_full"));
    let json = report.render_json().expect("json");
    assert!(json.contains("deterministic-path-ablation-v2"));
}

use super::*;

fn parse_trace(json: &str) -> ObservationTrace {
    serde_json::from_str(json).expect("valid trace")
}

#[test]
fn replay_is_deterministic_across_observation_and_failure_events() {
    let json = include_str!("../traces/scheduler-failover-v1.json");
    let expected = include_str!("../traces/scheduler-failover-v1.expected.json");

    let first = replay(parse_trace(json)).expect("first replay");
    let second = replay(parse_trace(json)).expect("second replay");

    assert_eq!(first, second);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&render_json(&first).expect("rendered replay"))
            .expect("rendered replay JSON"),
        serde_json::from_str::<serde_json::Value>(expected).expect("expected replay JSON")
    );
    assert!(matches!(
        first.decisions[0],
        ReplayDecision::Route { path_id: 1, .. }
    ));
    assert!(matches!(
        first.decisions[2],
        ReplayDecision::Route { path_id: 2, .. }
    ));
}

#[test]
fn replay_rejects_time_travel() {
    let trace = parse_trace(
        r#"{
          "schema_version": 1,
          "trace_id": "bad-time",
          "initial_paths": [
            {"id": 1, "underlay": "tcp", "state": "active",
             "srtt_ms": 20.0, "jitter_ms": 0.0,
             "delivery_rate_bps": 1000000.0}
          ],
          "events": [
            {"type": "advance", "at_ms": 2.0},
            {"type": "advance", "at_ms": 1.0}
          ]
        }"#,
    );

    assert!(replay(trace).unwrap_err().contains("precedes"));
}

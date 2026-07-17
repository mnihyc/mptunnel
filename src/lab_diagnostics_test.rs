use super::*;
use std::panic::{self, AssertUnwindSafe};

#[test]
fn diagnostic_event_filter_accepts_exact_names_and_wildcard() {
    let exact =
        parse_lab_diagnostic_event_filter(Some("stream_open, path_timeout")).expect("exact filter");
    assert!(exact.contains("stream_open"));
    assert!(exact.contains("path_timeout"));
    assert!(!exact.contains("stream"));
    assert!(parse_lab_diagnostic_event_filter(None).is_none());
    assert!(parse_lab_diagnostic_event_filter(Some("")).is_none());
    assert!(parse_lab_diagnostic_event_filter(Some("*")).is_none());
}

#[test]
fn diagnostic_flags_are_ascii_case_insensitive() {
    for enabled in ["1", "true", "TRUE", "True", "yes", "YES", "YeS"] {
        assert!(env_flag_value(enabled), "{enabled}");
    }
    for disabled in ["", "0", "false", "no", "on"] {
        assert!(!env_flag_value(disabled), "{disabled}");
    }
}

#[test]
fn diagnostic_event_filter_obeys_master_switch_and_exact_selection() {
    let _guard = lab_diag_test_guard();
    // SAFETY: the guard serializes diagnostic environment mutation in tests.
    unsafe {
        std::env::set_var("MPTUNNEL_LAB_DIAG_EVENTS", "stream_open, path_timeout");
    }
    assert!(lab_diagnostic_event_enabled("stream_open"));
    assert!(lab_diagnostic_event_enabled("path_timeout"));
    assert!(!lab_diagnostic_event_enabled("stream"));

    // SAFETY: the guard serializes diagnostic environment mutation in tests.
    unsafe {
        std::env::set_var("MPTUNNEL_LAB_DIAG", "0");
    }
    assert!(!lab_diagnostic_event_enabled("stream_open"));
}

#[test]
fn conformance_tracking_is_opt_in_under_an_exact_filter() {
    let _guard = lab_diag_test_guard();
    // SAFETY: the guard serializes diagnostic environment mutation in tests.
    unsafe {
        std::env::set_var(
            "MPTUNNEL_LAB_DIAG_EVENTS",
            "server_response_stream_data_frame,sender_service_decision",
        );
    }
    lab_server_response_stream_data(17, 19, 0, 1024);
    lab_sender_service_decision(
        "server",
        Some(17),
        19,
        "data_service",
        "stream_data",
        1024,
        Some(true),
        format_args!("path_underlay=Tcp path_id=0"),
    );
    assert_eq!(lab_sender_service_counts_for_test(17, 19), (0, 0));

    // SAFETY: the guard serializes diagnostic environment mutation in tests.
    unsafe {
        std::env::set_var("MPTUNNEL_LAB_DIAG_EVENTS", "sender_service_conformance");
    }
    lab_server_response_stream_data(17, 19, 1024, 1024);
    lab_sender_service_decision(
        "server",
        Some(17),
        19,
        "data_service",
        "stream_data",
        1024,
        Some(true),
        format_args!("path_underlay=Tcp path_id=0"),
    );
    assert_eq!(lab_sender_service_counts_for_test(17, 19), (1, 1));
    lab_assert_server_sender_service_balanced(17, 19);
}

#[test]
fn every_original_data_dispatch_reason_counts_for_conformance() {
    let _guard = lab_diag_test_guard();

    for (index, decision_kind) in ["data_service", "data_path_state", "data_completion_time"]
        .into_iter()
        .enumerate()
    {
        lab_sender_service_decision(
            "server",
            Some(7),
            9,
            decision_kind,
            "stream_data",
            1024,
            Some(index == 0),
            format_args!("path_underlay=Tcp path_id={index}"),
        );
        lab_server_response_stream_data(7, 9, (index as u64) * 1024, 1024);
    }

    assert_eq!(lab_sender_service_counts_for_test(7, 9), (3, 3));
    let counts = LAB_SENDER_SERVICE_COUNTS
        .get()
        .expect("sender-service counts")
        .lock()
        .expect("lab sender-service counts lock")
        .get(&(7, 9))
        .copied()
        .expect("stream counts");
    assert_eq!(counts.service_decisions, 1);
    assert_eq!(counts.service_payload_bytes, 1024);
    assert_eq!(counts.path_state_decisions, 1);
    assert_eq!(counts.path_state_payload_bytes, 1024);
    lab_assert_server_sender_service_balanced(7, 9);
}

#[test]
fn failed_conformance_assertion_does_not_poison_counts_lock() {
    let _guard = lab_diag_test_guard();

    lab_server_response_stream_data(11, 13, 0, 64);
    let failed = panic::catch_unwind(AssertUnwindSafe(|| {
        lab_assert_server_sender_service_balanced(11, 13);
    }));

    assert!(failed.is_err());
    assert_eq!(lab_sender_service_counts_for_test(11, 13), (1, 0));
}

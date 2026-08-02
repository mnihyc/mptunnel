use super::*;

#[test]
fn records_are_bounded_structured_and_redacted_in_both_formats() {
    let mut bounded = BoundedMessage::new(32);
    std::fmt::Write::write_fmt(
        &mut bounded,
        format_args!(
            "authorization: bearer very-secret-value\n{}",
            "x".repeat(128)
        ),
    )
    .expect("format bounded message");
    let message = bound_message(redact_message(&bounded.finish()), 32);
    assert!(message.len() <= 32);
    assert!(!message.contains('\n'));
    assert!(!message.contains("very-secret"));
    assert!(message.contains("<redacted>"));

    let record = LogRecord {
        timestamp_unix_ms: 7,
        level: "warn",
        component: "management",
        event: "request_failed",
        message: &message,
        suppressed: 3,
    };
    let mut json = Vec::new();
    write_record(&mut json, LogFormat::Json, &record);
    let parsed: serde_json::Value = serde_json::from_slice(&json).expect("one JSON record");
    assert_eq!(parsed["timestamp_unix_ms"], 7);
    assert_eq!(parsed["level"], "warn");
    assert_eq!(parsed["component"], "management");
    assert_eq!(parsed["event"], "request_failed");
    assert_eq!(parsed["suppressed"], 3);

    let mut text = Vec::new();
    write_record(&mut text, LogFormat::Text, &record);
    let text = String::from_utf8(text).expect("UTF-8 text record");
    assert!(text.starts_with("timestamp_unix_ms=7 level=warn"));
    assert_eq!(text.lines().count(), 1);
}

#[test]
fn limiter_bounds_each_call_site_and_reports_suppressed_events() {
    let limiter = RateLimiter::with_policy(100, 2);
    assert_eq!(limiter.admit(1), Some(0));
    assert_eq!(limiter.admit(2), Some(0));
    assert_eq!(limiter.admit(3), None);
    assert_eq!(limiter.admit(4), None);
    assert_eq!(limiter.admit(101), Some(2));
}

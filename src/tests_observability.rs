use super::*;

#[test]
fn process_record_renderers_are_stable_bounded_and_human_readable() {
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
    assert_eq!(parsed["message"], message);
    assert_eq!(parsed["suppressed"], 3);
    assert!(!String::from_utf8_lossy(&json).contains("very-secret"));
    assert!(json.ends_with(b"\n"));
    assert_eq!(json.iter().filter(|byte| **byte == b'\n').count(), 1);

    let mut text = Vec::new();
    write_record(&mut text, LogFormat::Text, &record);
    let text = String::from_utf8(text).expect("UTF-8 text record");
    assert!(
        text.starts_with(
            "1970-01-01T00:00:00.007Z WARN  management.request_failed: authorization:"
        )
    );
    assert!(text.ends_with("(3 similar events suppressed)\n"));
    assert!(!text.contains("timestamp_unix_ms="));
    assert!(!text.contains("message="));
    assert_eq!(text.lines().count(), 1);

    let zero_suppression = LogRecord {
        timestamp_unix_ms: 0,
        level: "info",
        component: "process",
        event: "starting",
        message: "MPTUNNEL starting",
        suppressed: 0,
    };
    let mut json = Vec::new();
    write_record(&mut json, LogFormat::Json, &zero_suppression);
    let parsed: serde_json::Value = serde_json::from_slice(&json).expect("one JSON record");
    assert!(parsed.get("suppressed").is_none());
    let mut text = Vec::new();
    write_record(&mut text, LogFormat::Text, &zero_suppression);
    let text = String::from_utf8(text).expect("UTF-8 text record");
    assert_eq!(
        text,
        "1970-01-01T00:00:00.000Z INFO  process.starting: MPTUNNEL starting\n"
    );
    assert!(!text.contains("suppressed"));
}

#[test]
fn secret_forms_and_terminal_controls_are_removed_before_rendering() {
    for source in [
        "Authorization: Basic logging-canary",
        "Proxy-Authorization=Bearer logging-canary",
        "Cookie: session=logging-canary",
        "token=logging-canary",
        "token = \"logging-canary\"",
        "'token' = 'logging-canary'",
        "\"token\":\"logging-canary\"",
        "api_key:logging-canary",
        "access_token=logging-canary",
        "refresh_token=logging-canary",
        "password: logging-canary",
        "credential_secret=logging-canary",
        "credential-secret:logging-canary",
        "transport_shared_secret=logging-canary",
        "private_key=logging-canary",
    ] {
        let redacted = redact_message(source);
        assert!(
            !redacted.contains("logging-canary"),
            "secret form was not redacted: {source:?} -> {redacted:?}"
        );
        assert!(redacted.contains("<redacted>"));
    }

    assert_eq!(
        redact_message("invalid basic string"),
        "invalid basic string",
        "authentication scheme names are sensitive only inside authorization values"
    );

    let sanitized = redact_message("line one\nline two\r\u{1b}[31m");
    assert!(sanitized.contains("line one"));
    assert!(sanitized.contains("line two"));
    assert!(!sanitized.chars().any(char::is_control));
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

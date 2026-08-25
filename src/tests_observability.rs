use super::*;

#[cfg(target_os = "linux")]
#[derive(Default)]
struct RecordingHostLogSink {
    records: Mutex<Vec<(LogLevel, String)>>,
}

#[cfg(target_os = "linux")]
impl HostLogSink for RecordingHostLogSink {
    fn log(&self, level: LogLevel, rendered_record: &str) {
        self.records
            .lock()
            .expect("recording host sink lock")
            .push((level, rendered_record.to_string()));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn embedding_sink_receives_rendered_records_and_stale_clear_is_harmless() {
    let config = LoggingConfig {
        format: LogFormat::Json,
        flow_events: true,
        ..LoggingConfig::default()
    };
    let prepared = prepare_for_host_sink(&config).expect("prepare embedding logger");
    assert!(!prepared.output.console);
    assert_eq!(prepared.level, LogLevel::Info);
    assert_eq!(prepared.output.format, LogFormat::Json);
    assert!(prepared.flow_events);

    let logger = Logger::new();
    let output = Output {
        format: LogFormat::Text,
        console: false,
        file: Mutex::new(None),
    };
    let first = Arc::new(RecordingHostLogSink::default());
    let first_registration = logger.register_host_sink(first.clone());
    logger.write_to_sinks(LogLevel::Debug, &output, b"already rendered\n");
    assert_eq!(
        *first.records.lock().expect("first host records"),
        vec![(LogLevel::Debug, "already rendered".to_string())]
    );

    let second = Arc::new(RecordingHostLogSink::default());
    let second_registration = logger.register_host_sink(second.clone());
    logger.clear_host_sink(first_registration);
    logger.write_to_sinks(LogLevel::Info, &output, b"replacement\n");
    assert_eq!(
        *second.records.lock().expect("second host records"),
        vec![(LogLevel::Info, "replacement".to_string())]
    );

    logger.clear_host_sink(second_registration);
    logger.write_to_sinks(LogLevel::Error, &output, b"after clear\n");
    assert_eq!(
        second.records.lock().expect("cleared host records").len(),
        1
    );
}

#[test]
fn debug_filter_is_stricter_than_the_default_info_filter() {
    assert!(level_enabled(LogLevel::Info, LogLevel::Info as u8));
    assert!(!level_enabled(LogLevel::Debug, LogLevel::Info as u8));
    assert!(level_enabled(LogLevel::Debug, LogLevel::Debug as u8));
    assert!(level_enabled(LogLevel::Info, LogLevel::Debug as u8));
}

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

    let debug = LogRecord {
        timestamp_unix_ms: 8,
        level: "debug",
        component: "inbound",
        event: "accepted",
        message: "connection_id=1 network=tcp inbound=local-socks",
        suppressed: 0,
    };
    let mut text = Vec::new();
    write_record(&mut text, LogFormat::Text, &debug);
    assert_eq!(
        String::from_utf8(text).expect("UTF-8 debug record"),
        "1970-01-01T00:00:00.008Z DEBUG inbound.accepted: connection_id=1 network=tcp inbound=local-socks\n"
    );
}

#[test]
fn connection_debug_records_repeat_immutable_ingress_context_in_text_and_json() {
    let context = ConnectionDebugContext::for_test(
        DebugConnectionId::for_test(17),
        ConnectionDebugContextFields {
            network: "tcp",
            origin: "mpp_inbound",
            inbound: "edge-in",
            principal: "alice",
            source: Some("203.0.113.7:51000"),
            source_kind: Some(ConnectionDebugSourceKind::MppCarrierPeer),
            requested_destination: "example.net:443",
            session_id: Some("91"),
            ingress_underlay: Some("quic"),
            ingress_path: Some("mobile-quic"),
            ingress_path_id: Some("7"),
            ingress_path_instance: Some("44"),
        },
    );
    let inbound =
        ConnectionDebugRecord::new(9, "inbound", InboundDebugEvent::Accepted.as_str(), &context);
    let mut routing = ConnectionDebugRecord::new(
        10,
        "routing",
        RoutingDebugEvent::Selected.as_str(),
        &context,
    );
    routing.rule = Some("private-sites");
    routing.decision = Some("allow");
    routing.egress = Some("balancer:primary");
    routing.target_resolution = Some("route-only");
    let mut balancer = ConnectionDebugRecord::new(11, "balancer", "selected", &context);
    balancer.balancer = Some("primary");
    balancer.outbound = Some("edge-a");
    balancer.attempt = Some(2);

    let unsafe_error = sanitize_connection_field(&format!(
        "Authorization: Bearer logging-canary\n{}",
        "x".repeat(CONNECTION_FIELD_LIMIT * 2)
    ));
    assert!(unsafe_error.len() <= CONNECTION_FIELD_LIMIT);
    assert!(!unsafe_error.contains("logging-canary"));
    assert!(!unsafe_error.contains('\n'));
    let mut outbound = ConnectionDebugRecord::new(
        12,
        "outbound",
        OutboundDebugEvent::Failed.as_str(),
        &context,
    );
    outbound.outbound = Some("edge-a");
    outbound.outbound_destination = Some("198.51.100.8:443");
    outbound.protocol = Some("direct");
    outbound.attempt = Some(2);
    outbound.error = Some(&unsafe_error);

    for record in [&inbound, &routing, &balancer, &outbound] {
        let mut json = Vec::new();
        write_connection_debug_record(&mut json, LogFormat::Json, record);
        let parsed: serde_json::Value = serde_json::from_slice(&json).expect("debug JSON record");
        for (field, expected) in [
            ("connection_id", "17"),
            ("network", "tcp"),
            ("origin", "mpp_inbound"),
            ("inbound", "edge-in"),
            ("principal", "alice"),
            ("source", "203.0.113.7:51000"),
            ("source_kind", "mpp_carrier_peer"),
            ("requested_destination", "example.net:443"),
            ("session_id", "91"),
            ("ingress_underlay", "quic"),
            ("ingress_path", "mobile-quic"),
            ("ingress_path_id", "7"),
            ("ingress_path_instance", "44"),
        ] {
            assert_eq!(
                parsed[field], expected,
                "{} omitted {field}",
                record.component
            );
        }
        if record.component == "outbound" {
            assert_eq!(parsed["requested_destination"], "example.net:443");
            assert_eq!(parsed["outbound_destination"], "198.51.100.8:443");
        }
        assert!(parsed.get("credential_id").is_none());

        let mut text = Vec::new();
        write_connection_debug_record(&mut text, LogFormat::Text, record);
        let text = String::from_utf8(text).expect("debug text record");
        for field in [
            "origin=\"mpp_inbound\"",
            "inbound=\"edge-in\"",
            "principal=\"alice\"",
            "source=\"203.0.113.7:51000\"",
            "source_kind=\"mpp_carrier_peer\"",
            "requested_destination=\"example.net:443\"",
            "session_id=\"91\"",
            "ingress_underlay=\"quic\"",
            "ingress_path=\"mobile-quic\"",
            "ingress_path_id=\"7\"",
            "ingress_path_instance=\"44\"",
        ] {
            assert!(
                text.contains(field),
                "{} omitted {field}: {text}",
                record.component
            );
        }
        if record.component == "outbound" {
            assert!(text.contains("outbound_destination=\"198.51.100.8:443\""));
        }
    }

    let local = ConnectionDebugContext::for_test(
        DebugConnectionId::for_test(18),
        ConnectionDebugContextFields {
            network: "udp",
            origin: "local_inbound",
            inbound: "local-socks",
            principal: "anonymous",
            source: Some("127.0.0.1:52000"),
            source_kind: Some(ConnectionDebugSourceKind::LocalPeer),
            requested_destination: "dns.example:53",
            session_id: None,
            ingress_underlay: None,
            ingress_path: None,
            ingress_path_id: None,
            ingress_path_instance: None,
        },
    );
    let mut local_outbound = ConnectionDebugRecord::new(13, "outbound", "connected", &local);
    local_outbound.outbound = Some("direct");
    local_outbound.outbound_destination = Some("dns.example:53");
    local_outbound.attempt = Some(1);
    let mut json = Vec::new();
    write_connection_debug_record(&mut json, LogFormat::Json, &local_outbound);
    let parsed: serde_json::Value = serde_json::from_slice(&json).expect("local JSON record");
    assert_eq!(parsed["source"], "127.0.0.1:52000");
    assert_eq!(parsed["source_kind"], "local_peer");
    assert!(parsed.get("session_id").is_none());
}

#[test]
fn connection_debug_records_repeat_immutable_ingress_context_release_oracle() {
    connection_debug_records_repeat_immutable_ingress_context_in_text_and_json();
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

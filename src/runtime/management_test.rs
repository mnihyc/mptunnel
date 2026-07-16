use super::*;
use crate::config::{
    ClientPathConfig, LocalIngressConfig, ResourceLimits, RouteTarget, RouteTargetKind,
    SharedSecret,
};
use crate::ingress::IngressConfig;
use crate::runtime::management::snapshot::SessionInventory;

#[test]
fn auth_accepts_bearer_and_rejects_wrong_token() {
    let request = ManagementRequest {
        method: "GET".to_string(),
        path: "/api/status".to_string(),
        headers: vec![(
            "authorization".to_string(),
            "Bearer correct-token".to_string(),
        )],
        body: Vec::new(),
    };
    let token = "correct-token";
    let wrong = "wrong-token";

    assert!(management_auth_ok(&request, Some(token)));
    assert!(!management_auth_ok(&request, Some(wrong)));
    assert!(!management_auth_ok(&request, None));

    let mut lower_scheme = request;
    lower_scheme.headers[0].1 = "bearer correct-token".to_string();
    assert!(management_auth_ok(&lower_scheme, Some(token)));
}

#[test]
fn enabling_a_path_requires_fresh_liveness_evidence() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget::Client {
        context: context.clone(),
        state: ManagementState::new("client"),
    };

    target
        .control_path_json(br#"{"underlay":"tcp","index":0,"state":"disabled"}"#)
        .expect("disable path");
    target
        .control_path_json(br#"{"underlay":"tcp","index":0,"state":"enabled"}"#)
        .expect("enable path");

    let health = context.health().lock().expect("health");
    assert!(!health.tcp[0].manual_disabled);
    assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
}

#[test]
fn node_path_control_requires_a_selector_when_clients_are_ambiguous() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget::Node {
        clients: vec![context.clone(), context],
        servers: Vec::new(),
        state: ManagementState::new("node"),
    };

    let error = target
        .control_path_json(br#"{"underlay":"tcp","index":0,"state":"disabled"}"#)
        .expect_err("ambiguous clients");
    assert_eq!(error.status, 409);
}

#[test]
fn node_path_control_can_select_client_by_route_target_tag() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_target(
        vec![ClientPathConfig {
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: "edge-mpp".to_string(),
        }),
        Vec::new(),
    )
    .expect("context");
    let target = ManagementTarget::Node {
        clients: vec![context.clone()],
        servers: Vec::new(),
        state: ManagementState::new("node"),
    };

    target
        .control_path_json(
            br#"{"client_tag":"edge-mpp","underlay":"tcp","index":0,"state":"disabled"}"#,
        )
        .expect("control path");

    let health = context.health().lock().expect("health");
    assert!(health.tcp[0].manual_disabled);
    assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
}

#[test]
fn client_status_exposes_inbound_tags_without_credentials() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_target(
        vec![ClientPathConfig {
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: "edge-mpp".to_string(),
        }),
        vec![LocalIngressConfig {
            tag: Some("local-socks".to_string()),
            config: IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("listen")],
                proxy_auth: ProxyAuthConfig::required("operator".to_string(), "secret".to_string()),
            },
        }],
    )
    .expect("context");
    let target = ManagementTarget::Client {
        context,
        state: ManagementState::new("client"),
    };

    target.refresh_sample_snapshot();
    let status = target.snapshot();
    assert_eq!(status.local_inbounds.len(), 1);
    assert_eq!(status.local_inbounds[0].tag.as_deref(), Some("local-socks"));
    assert_eq!(status.local_inbounds[0].protocol, "socks5");
    assert!(status.local_inbounds[0].auth_required);
    let encoded = serde_json::to_string(&*status).expect("serialize snapshot");
    assert!(!encoded.contains("operator"));
    assert!(!encoded.contains("secret"));
}

#[test]
fn status_separates_sessions_flows_and_exclusive_path_states() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    context.health().lock().expect("health").tcp[0].manual_disabled = true;
    let _peer_carrier = context.peer_status.register(context.session_id);
    let target = ManagementTarget::Client {
        context,
        state: ManagementState::new("client"),
    };

    target.refresh_sample_snapshot();
    let status = target.snapshot();
    assert_eq!(status.summary.configured_path_count, 1);
    assert_eq!(status.summary.disabled_paths, 1);
    assert_eq!(status.summary.failed_paths, 0);
    assert_eq!(status.paths[0].state, "disabled");
    assert_eq!(status.sessions.len(), 1);
    assert!(status.flows.is_empty());
    assert_eq!(status.diagnostics.peer_sessions.len(), 1);
    assert_eq!(status.diagnostics.peer_sessions[0].service, "mpp_outbound");
    assert_eq!(status.diagnostics.peer_sessions[0].service_index, 0);
}

#[test]
fn control_refresh_does_not_advance_one_hertz_trends() {
    let security = SecurityConfig::encrypted(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget::Client {
        context,
        state: ManagementState::new("client"),
    };
    target.refresh_sample_snapshot();
    assert_eq!(target.snapshot().traffic.trends.len(), 1);

    target
        .control_path_json(br#"{"underlay":"tcp","index":0,"state":"disabled"}"#)
        .expect("control");
    assert_eq!(target.snapshot().traffic.trends.len(), 1);

    target.refresh_sample_snapshot();
    assert_eq!(target.snapshot().traffic.trends.len(), 2);
}

#[test]
fn session_flow_counts_report_when_detail_is_incomplete() {
    let mut sessions = SessionInventory::default();
    sessions.insert(
        "mpp_outbound",
        0,
        Some("edge".to_string()),
        "42".to_string(),
        "connected",
        2,
        Some(1),
    );

    let sessions = sessions.finish(&[], false);

    assert_eq!(sessions.len(), 1);
    assert!(!sessions[0].active_flow_counts_complete);
    let encoded = serde_json::to_value(&sessions[0]).expect("serialize session");
    assert_eq!(encoded["active_flow_counts_complete"], false);
}

use super::*;
use crate::config::SharedSecret;

#[test]
fn auth_accepts_bearer_and_rejects_wrong_token() {
    let request = ManagementRequest {
        method: "GET".to_string(),
        path: "/status".to_string(),
        headers: vec![(
            "authorization".to_string(),
            "Bearer correct-token".to_string(),
        )],
        body: Vec::new(),
    };
    let token = "correct-token".to_string();
    let wrong = "wrong-token".to_string();

    assert!(management_auth_ok(&request, Some(&token)));
    assert!(!management_auth_ok(&request, Some(&wrong)));
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

    let status = target.status_json();
    let inbounds = status["inbounds"].as_array().expect("inbounds");
    assert_eq!(inbounds.len(), 1);
    assert_eq!(inbounds[0]["tag"], "local-socks");
    assert_eq!(inbounds[0]["protocol"], "socks5");
    assert_eq!(inbounds[0]["auth_required"], true);
    assert!(inbounds[0].get("username").is_none());
    assert!(inbounds[0].get("password").is_none());
}

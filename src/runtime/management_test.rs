use super::*;
use crate::config::{
    ClientPathConfig, LocalIngressConfig, ResourceLimits, RouteTarget, RouteTargetKind,
    SharedSecret,
};
use crate::ingress::{IngressConfig, LocalProxyUser, ProxyAuthConfig};
use crate::runtime::management::snapshot::SessionInventory;
use crate::runtime::outbound_registry::{
    RuntimeOutboundLeaf, RuntimeOutboundRegistry, test_dns_generation,
};
use crate::runtime::readiness::RuntimeGenerationControl;
use crate::{
    config::GatewayBalancerConfig,
    outbound::OutboundConfig,
    product::{
        BalancerId, GatewayBalancerSpec, GatewayMemberSpec, GatewayStrategy, NetworkSet, OutboundId,
    },
};
use std::time::Duration;

fn local_proxy_auth() -> ProxyAuthConfig {
    let user = LocalProxyUser::new(
        "operator".to_string(),
        crate::product::PrincipalId::parse("daily-user").expect("principal"),
        "operator".to_string(),
        "secret".to_string(),
    )
    .expect("local user");
    ProxyAuthConfig::required([user]).expect("proxy auth")
}

#[test]
fn auth_accepts_bearer_and_rejects_wrong_token() {
    let request = ManagementRequest {
        method: "GET".to_string(),
        path: "/api/v1/status".to_string(),
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
    assert!(!format!("{request:?}").contains("correct-token"));

    let mut lower_scheme = request;
    lower_scheme.headers[0].1 = "bearer correct-token".to_string();
    assert!(management_auth_ok(&lower_scheme, Some(token)));
}

#[test]
fn gateway_status_and_actions_share_the_generation_owned_balancer() {
    let first = OutboundId::parse("edge-a").expect("first");
    let second = OutboundId::parse("edge-b").expect("second");
    let balancer = BalancerId::parse("daily-egress").expect("balancer");
    let runtime = RuntimeOutboundRegistry::compile(
        [first.clone(), second.clone()].map(|id| RuntimeOutboundLeaf::Local {
            id,
            config: OutboundConfig::Direct,
            connect_timeout: Duration::from_secs(1),
            native_sockets: Arc::new(crate::transport::SystemNativeSocketConfigurator),
        }),
        &[GatewayBalancerConfig {
            id: balancer,
            generation: 7,
            spec: GatewayBalancerSpec::new(
                GatewayStrategy::RoundRobin,
                vec![
                    GatewayMemberSpec::new(first, 1, NetworkSet::TCP_UDP),
                    GatewayMemberSpec::new(second, 1, NetworkSet::TCP_UDP),
                ],
            ),
        }],
        test_dns_generation(),
    )
    .expect("runtime registry");
    let target = ManagementTarget {
        clients: Vec::new(),
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: Some(runtime.gateway_control()),
        dns: None,
        product_admission: runtime.product_admission().clone(),
        generation: RuntimeGenerationControl::new(),
    };

    target.refresh_sample_snapshot();
    let initial = target.snapshot();
    assert_eq!(initial.schema, "mptunnel.management.v4");
    assert_eq!(initial.services.gateway_balancers, 1);
    assert!(initial.controls.gateway.supported);
    assert_eq!(initial.gateways[0].ready_members, 2);

    let response = target
        .control_gateway_json(
            br#"{"balancer":"daily-egress","action":"drain-member","member":"edge-b"}"#,
        )
        .expect("drain");
    assert_eq!(response["scope"], "runtime-generation");
    let drained = target.gateway_status_json().expect("gateway status");
    assert_eq!(drained["schema"], "mptunnel.gateway.v1");
    assert_eq!(
        drained["gateways"][0]["members"][1]["readiness"],
        "draining"
    );
    assert!(
        target
            .control_gateway_json(
                br#"{"balancer":"daily-egress","action":"pin-member","member":"edge-b"}"#,
            )
            .is_err(),
        "a non-enabled member cannot become the manual override"
    );

    target
        .control_gateway_json(
            br#"{"balancer":"daily-egress","action":"enable-member","member":"edge-b"}"#,
        )
        .expect("enable");
    target
        .control_gateway_json(
            br#"{"balancer":"daily-egress","action":"pin-member","member":"edge-b"}"#,
        )
        .expect("pin");
    assert_eq!(
        target.gateway_status_json().expect("pinned status")["gateways"][0]["manual_member"],
        "edge-b"
    );
    target
        .control_gateway_json(br#"{"balancer":"daily-egress","action":"automatic"}"#)
        .expect("automatic");
    assert!(
        target.gateway_status_json().expect("automatic status")["gateways"][0]["manual_member"]
            .is_null()
    );
}

#[tokio::test]
async fn dns_management_contract_explains_queries_observes_and_flushes_one_generation() {
    let dns = DnsGeneration::from_test_answers(std::collections::HashMap::from([(
        "managed.example".to_string(),
        vec!["192.0.2.53".parse().expect("address")],
    )]));
    let target = ManagementTarget {
        clients: Vec::new(),
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: Some(dns),
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    let initial = target.dns_status_json().expect("initial DNS status");
    assert_eq!(initial["schema"], "mptunnel.dns.status.v1");
    assert_eq!(initial["generation"], 1);
    assert_eq!(initial["plans"][0]["id"], "test-default");
    assert_eq!(initial["plans"][0]["cache"]["entries"], 0);

    let explanation = target
        .dns_explain_json("managed.example")
        .expect("DNS explanation");
    assert_eq!(explanation["schema"], "mptunnel.dns.explain.v1");
    assert_eq!(explanation["plan"], "test-default");
    assert_eq!(explanation["match"], "default");
    assert_eq!(explanation["upstreams"][0]["id"], "test-static");

    let queried = target
        .dns_query_json(br#"{"name":"managed.example","type":"A"}"#)
        .await
        .expect("typed DNS query");
    assert_eq!(queried["schema"], "mptunnel.dns.query.v1");
    assert_eq!(queried["plan"], "test-default");
    assert_eq!(queried["rcode"], 0);
    assert_eq!(queried["rcode_name"], "NOERROR");
    assert_eq!(queried["answers"][0]["type"], "A");
    assert_eq!(queried["answers"][0]["data"], "192.0.2.53");

    let observed = target.dns_status_json().expect("observed DNS status");
    assert_eq!(observed["plans"][0]["cache"]["entries"], 1);
    assert_eq!(observed["plans"][0]["upstreams"][0]["attempts"], "1");
    assert_eq!(observed["plans"][0]["upstreams"][0]["successes"], "1");

    let flushed = target.dns_flush_json(br#"{}"#).expect("flush all plans");
    assert_eq!(flushed["schema"], "mptunnel.dns.flush.v1");
    assert_eq!(flushed["plans"], 1);
    assert_eq!(flushed["removed_entries"], 1);
    assert_eq!(
        target.dns_status_json().expect("flushed DNS status")["plans"][0]["cache"]["entries"],
        0
    );

    assert_eq!(
        target
            .dns_query_json(br#"{"name":"managed.example","type":"A","legacy":true}"#)
            .await
            .expect_err("unknown query fields must fail closed")
            .status,
        400
    );
    assert_eq!(
        target
            .dns_flush_json(br#"{"plan":"missing"}"#)
            .expect_err("unknown plan")
            .status,
        404
    );
}

#[test]
fn enabling_a_path_requires_fresh_liveness_evidence() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context.clone()],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
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
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context.clone(), context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    let error = target
        .control_path_json(br#"{"underlay":"tcp","index":0,"state":"disabled"}"#)
        .expect_err("ambiguous clients");
    assert_eq!(error.status, 409);
}

#[test]
fn node_path_control_can_select_client_by_route_target_tag() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_target(
        vec![ClientPathConfig {
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: "edge-mpp".to_string(),
        }),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context.clone()],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
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
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let local_inbounds = vec![
        LocalIngressConfig {
            tag: Some("local-socks".to_string()),
            config: IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("listen")],
                proxy_auth: local_proxy_auth(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
        },
        LocalIngressConfig {
            tag: Some("local-forward".to_string()),
            config: IngressConfig::TcpForward(
                crate::ingress::TcpForwardConfig::with_defaults(
                    vec!["127.0.0.1:8443".parse().expect("forward listen")],
                    crate::ingress::PortForwardTarget::parse("SERVICE.Example.:443")
                        .expect("forward target"),
                )
                .expect("forward config"),
            ),
        },
    ];
    let outbound_configs = vec![crate::config::OutboundLeafConfig::Local {
        id: OutboundId::parse("daily-direct").expect("outbound"),
        config: OutboundConfig::Direct,
        connect_timeout: Duration::from_secs(3),
    }];
    let context = ClientPathContext::new_with_path_configs_and_target(
        vec![ClientPathConfig {
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(RouteTarget {
            kind: RouteTargetKind::Outbound,
            tag: "edge-mpp".to_string(),
        }),
    )
    .expect("context");
    let product_admission = ProductAdmission::default();
    let _private_flow = product_admission
        .try_admit_flow(
            crate::product::PrincipalId::parse("private-principal").expect("principal"),
            crate::product::ProtocolTarget::parse_authority("private-target.example:443")
                .expect("target"),
        )
        .expect("admit one live flow");
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::from_config(&local_inbounds, &outbound_configs),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission,
        generation: RuntimeGenerationControl::new(),
    };

    target.refresh_sample_snapshot();
    let status = target.snapshot();
    assert_eq!(status.local_inbounds.len(), 2);
    assert_eq!(status.local_inbounds[0].tag.as_deref(), Some("local-socks"));
    assert_eq!(status.local_inbounds[0].protocol, "socks5");
    assert!(status.local_inbounds[0].target.is_none());
    assert!(status.local_inbounds[0].auth_required);
    assert_eq!(status.local_inbounds[1].protocol, "tcp-forward");
    assert_eq!(
        status.local_inbounds[1].target.as_deref(),
        Some("service.example:443")
    );
    assert!(status.local_inbounds[1].name.is_none());
    assert!(!status.local_inbounds[1].auth_required);
    assert_eq!(status.services.outbounds, 1);
    assert_eq!(status.services.local_outbounds, 1);
    assert_eq!(status.outbounds[0].tag, "daily-direct");
    assert_eq!(status.outbounds[0].protocol, "direct");
    assert_eq!(status.outbounds[0].networks, ["tcp", "udp"]);
    assert_eq!(status.admission.live_flows, 1);
    assert_eq!(status.admission.tracked_principals, 1);
    assert_eq!(status.admission.tracked_targets, 1);
    let encoded = serde_json::to_string(&*status).expect("serialize snapshot");
    assert!(!encoded.contains("operator"));
    assert!(!encoded.contains("secret"));
    assert!(!encoded.contains("private-principal"));
    assert!(!encoded.contains("private-target.example"));
}

#[test]
fn status_separates_sessions_flows_and_exclusive_path_states() {
    let security = ClientSecurityConfig::for_test(
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
    let product_telemetry = RuntimeTelemetry::generation_owner(4);
    let product_flow = crate::product::FlowContext::new(
        crate::product::Network::Tcp,
        crate::product::ProtocolTarget::parse_authority("service.example:443").expect("target"),
        crate::product::SourceEndpoint::new("127.0.0.1".parse().expect("source"), 42000),
        crate::product::PrincipalId::parse("daily-user").expect("principal"),
        crate::product::InboundId::parse("local-socks").expect("inbound"),
    );
    let scope = crate::runtime::telemetry::ProductFlowScope::from_flow(
        crate::runtime::telemetry::ProductFlowOriginKind::LocalInbound,
        &product_flow,
        OutboundId::parse("edge-b").expect("outbound"),
        Some(BalancerId::parse("daily-egress").expect("balancer")),
    );
    let _active_flow = product_telemetry.scoped(scope).open_reliable_flow(
        Some(context.session_id),
        crate::protocol::StreamId(9),
        crate::protocol::TargetAddr::Ip("127.0.0.1:443".parse().expect("literal")),
    );
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry,
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    target.refresh_sample_snapshot();
    let status = target.snapshot();
    assert_eq!(status.summary.configured_path_count, 1);
    assert_eq!(status.summary.disabled_paths, 1);
    assert_eq!(status.summary.failed_paths, 0);
    assert_eq!(status.paths[0].state, "disabled");
    assert_eq!(status.sessions.len(), 1);
    assert_eq!(status.sessions[0].active_reliable_flows, 1);
    assert_eq!(status.summary.active_flows, 1);
    assert_eq!(status.flows.len(), 1);
    assert_eq!(status.flows[0].flow_id, "1");
    assert_eq!(status.flows[0].network, "tcp");
    assert_eq!(status.flows[0].inbound.as_deref(), Some("local-socks"));
    assert_eq!(status.flows[0].outbound.as_deref(), Some("edge-b"));
    assert_eq!(status.flows[0].balancer.as_deref(), Some("daily-egress"));
    assert_eq!(status.flows[0].balancer_member.as_deref(), Some("edge-b"));
    assert_eq!(
        status.flows[0].target.as_deref(),
        Some("service.example:443")
    );
    assert_eq!(status.diagnostics.peer_sessions.len(), 1);
    assert_eq!(status.diagnostics.peer_sessions[0].service, "mpp_outbound");
    assert_eq!(status.diagnostics.peer_sessions[0].service_index, 0);
}

#[test]
fn control_refresh_does_not_advance_one_hertz_trends() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new(
        vec!["tcp://127.0.0.1:443".parse().expect("path")],
        security,
        ResourceLimits::default(),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
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

use super::*;
use crate::config::{ClientPathConfig, LocalIngressConfig, ResourceLimits, SharedSecret};
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
        path: "/api/v3/status".to_string(),
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
fn balancer_status_and_actions_share_the_generation_owned_balancer() {
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
        tun_l3_inventory: TunL3RuntimeInventory::default(),
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
    assert_eq!(initial.schema, "mptunnel.management.v3");
    assert_eq!(initial.services.balancers, 1);
    assert!(initial.controls.balancer.supported);
    assert_eq!(initial.balancers[0].ready_members, 2);

    let response = target
        .control_balancer_json(
            br#"{"balancer":"daily-egress","action":"drain-member","outbound":"edge-b"}"#,
        )
        .expect("drain");
    assert_eq!(response["scope"], "runtime-generation");
    let drained = target.balancer_status_json().expect("balancer status");
    assert_eq!(drained["schema"], "mptunnel.balancer.v3");
    assert_eq!(
        drained["balancers"][0]["members"][1]["readiness"],
        "draining"
    );
    assert!(
        target
            .control_balancer_json(
                br#"{"balancer":"daily-egress","action":"pin-member","outbound":"edge-b"}"#,
            )
            .is_err(),
        "a non-enabled member cannot become the manual override"
    );

    target
        .control_balancer_json(
            br#"{"balancer":"daily-egress","action":"enable-member","outbound":"edge-b"}"#,
        )
        .expect("enable");
    target
        .control_balancer_json(
            br#"{"balancer":"daily-egress","action":"pin-member","outbound":"edge-b"}"#,
        )
        .expect("pin");
    assert_eq!(
        target.balancer_status_json().expect("pinned status")["balancers"][0]["manual_outbound"],
        "edge-b"
    );
    target
        .control_balancer_json(br#"{"balancer":"daily-egress","action":"automatic"}"#)
        .expect("automatic");
    assert!(
        target.balancer_status_json().expect("automatic status")["balancers"][0]["manual_outbound"]
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
        tun_l3_inventory: TunL3RuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: Some(dns),
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    let initial = target.dns_status_json().expect("initial DNS status");
    assert_eq!(initial["schema"], "mptunnel.dns.status.v3");
    assert_eq!(initial["generation"], 1);
    assert_eq!(initial["policies"][0]["name"], "test-default");
    assert_eq!(initial["policies"][0]["cache"]["entries"], 0);
    assert_eq!(initial["policies"][0]["servers"][0]["protocol"], "system");
    assert!(initial["policies"][0]["servers"][0]["address"].is_null());

    let explanation = target
        .dns_explain_json("managed.example")
        .expect("DNS explanation");
    assert_eq!(explanation["schema"], "mptunnel.dns.explain.v3");
    assert_eq!(explanation["domain"], "managed.example");
    assert_eq!(explanation["policy"], "test-default");
    assert_eq!(explanation["match"], "default");
    assert_eq!(explanation["servers"][0]["name"], "test-static");

    let queried = target
        .dns_query_json(br#"{"domain":"managed.example","type":"A"}"#)
        .await
        .expect("typed DNS query");
    assert_eq!(queried["schema"], "mptunnel.dns.query.v3");
    assert_eq!(queried["domain"], "managed.example");
    assert_eq!(queried["policy"], "test-default");
    assert_eq!(queried["rcode"], 0);
    assert_eq!(queried["rcode_name"], "NOERROR");
    assert_eq!(queried["answers"][0]["type"], "A");
    assert_eq!(queried["answers"][0]["owner_name"], "managed.example.");
    assert_eq!(queried["answers"][0]["data"], "192.0.2.53");

    let observed = target.dns_status_json().expect("observed DNS status");
    assert_eq!(observed["policies"][0]["cache"]["entries"], 1);
    assert_eq!(observed["policies"][0]["servers"][0]["attempts"], "1");
    assert_eq!(observed["policies"][0]["servers"][0]["successes"], "1");

    let flushed = target.dns_flush_json(br#"{}"#).expect("flush all plans");
    assert_eq!(flushed["schema"], "mptunnel.dns.flush.v3");
    assert_eq!(flushed["flushed_policies"], 1);
    assert_eq!(flushed["removed_entries"], 1);
    assert_eq!(
        target.dns_status_json().expect("flushed DNS status")["policies"][0]["cache"]["entries"],
        0
    );

    assert_eq!(
        target
            .dns_query_json(br#"{"domain":"managed.example","type":"A","legacy":true}"#)
            .await
            .expect_err("unknown query fields must fail closed")
            .status,
        400
    );
    assert_eq!(
        target
            .dns_flush_json(br#"{"policy":"missing"}"#)
            .expect_err("unknown policy")
            .status,
        404
    );
    assert_eq!(
        target
            .dns_flush_json(br#"{"dns_plan":"test-default"}"#)
            .expect_err("removed DNS JSON names must fail closed")
            .status,
        400
    );
}

#[test]
fn enabling_a_path_requires_fresh_liveness_evidence() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?max-tcp-carriers=3"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context.clone()],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        tun_l3_inventory: TunL3RuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    let disabled = target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"disabled"}"#)
        .expect("disable path");
    assert_eq!(disabled["outbound"], "edge-mpp");
    assert_eq!(disabled["path"], "primary");
    {
        let health = context.health().lock().expect("disabled health");
        assert_eq!(health.tcp.len(), 3);
        assert!(health.tcp.iter().all(|record| record.manual_disabled));
        assert!(
            health
                .tcp
                .iter()
                .all(|record| record.state == SchedulerPathState::Failed)
        );
    }
    target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"enabled"}"#)
        .expect("enable path");

    let health = context.health().lock().expect("health");
    assert!(health.tcp.iter().all(|record| !record.manual_disabled));
    assert!(
        health
            .tcp
            .iter()
            .all(|record| record.state == SchedulerPathState::Suspect)
    );
}

#[test]
fn node_path_control_can_select_client_by_outbound_name() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "path-1".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?max-tcp-carriers=1"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context.clone()],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        tun_l3_inventory: TunL3RuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: None,
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"path-1","state":"disabled"}"#)
        .expect("control path");

    let health = context.health().lock().expect("health");
    assert!(health.tcp[0].manual_disabled);
    assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
}

#[test]
fn client_status_exposes_named_inventory_without_credentials() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let local_inbounds = vec![
        LocalIngressConfig {
            name: "local-socks".to_string(),
            config: IngressConfig::Socks5 {
                listen: vec!["127.0.0.1:1080".parse().expect("listen")],
                proxy_auth: local_proxy_auth(),
                admission: crate::ingress::LocalIngressAdmissionConfig::default(),
            },
        },
        LocalIngressConfig {
            name: "local-forward".to_string(),
            config: IngressConfig::TcpForward(
                crate::ingress::TcpForwardConfig::with_defaults(
                    vec!["127.0.0.1:8443".parse().expect("forward listen")],
                    crate::ingress::PortForwardTarget::parse("SERVICE.Example.:443")
                        .expect("forward target"),
                )
                .expect("forward config"),
            ),
        },
        LocalIngressConfig {
            name: "local-mixed-forward".to_string(),
            config: IngressConfig::MixedForward(
                crate::ingress::MixedForwardConfig::with_defaults(
                    vec!["127.0.0.1:853".parse().expect("mixed forward listen")],
                    crate::ingress::PortForwardTarget::parse("DNS.Example.:853")
                        .expect("mixed forward target"),
                )
                .expect("mixed forward config"),
            ),
        },
    ];
    let outbound_configs = vec![crate::config::OutboundLeafConfig::Local {
        id: OutboundId::parse("daily-direct").expect("outbound"),
        config: OutboundConfig::Direct,
        connect_timeout: Duration::from_secs(3),
    }];
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "path-1".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?max-tcp-carriers=1"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
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
        tun_l3_inventory: TunL3RuntimeInventory {
            services: Arc::new(vec![
                TunL3ServiceInventory {
                    role: TunL3ServiceRole::Client,
                    name: "packet-client".to_string(),
                    interface_name: Some("mptun-client".to_string()),
                    mpp_binding: "edge-mpp".to_string(),
                    mtu: None,
                    allocation_count: None,
                },
                TunL3ServiceInventory {
                    role: TunL3ServiceRole::Server,
                    name: "packet-server".to_string(),
                    interface_name: Some("mptun-server".to_string()),
                    mpp_binding: "packet-server".to_string(),
                    mtu: Some(1_400),
                    allocation_count: Some(3),
                },
            ]),
        },
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
    assert_eq!(status.local_inbounds.len(), 3);
    assert_eq!(status.local_inbounds[0].name, "local-socks");
    assert_eq!(status.local_inbounds[0].protocol, "socks5");
    assert!(status.local_inbounds[0].target.is_none());
    assert!(status.local_inbounds[0].auth_required);
    assert_eq!(status.local_inbounds[1].protocol, "tcp-forward");
    assert_eq!(
        status.local_inbounds[1].target.as_deref(),
        Some("service.example:443")
    );
    assert!(status.local_inbounds[1].interface_name.is_none());
    assert!(!status.local_inbounds[1].auth_required);
    assert_eq!(status.local_inbounds[2].name, "local-mixed-forward");
    assert_eq!(status.local_inbounds[2].protocol, "mixed-forward");
    assert_eq!(
        status.local_inbounds[2].target.as_deref(),
        Some("dns.example:853")
    );
    assert!(!status.local_inbounds[2].auth_required);
    assert_eq!(status.services.outbounds, 1);
    assert_eq!(status.services.local_outbounds, 1);
    assert_eq!(status.services.tun_l3_services, 2);
    assert_eq!(status.tun_l3_services[0].role, "client");
    assert_eq!(status.tun_l3_services[0].mpp_binding, "edge-mpp");
    assert_eq!(status.tun_l3_services[1].role, "server");
    assert_eq!(status.tun_l3_services[1].mtu, Some(1_400));
    assert_eq!(status.tun_l3_services[1].allocation_count, Some(3));
    assert_eq!(status.sessions.len(), 1);
    assert_eq!(status.sessions[0].state, "connecting");
    assert_eq!(status.sessions[0].carrier_count, 0);
    assert_eq!(status.summary.configured_path_count, 1);
    assert_eq!(status.summary.path_count, 1);
    assert_eq!(status.paths.len(), 1);
    assert_eq!(status.paths[0].path, "path-1");
    assert_eq!(status.paths[0].tcp_carrier_ordinal, Some(1));
    assert_eq!(status.paths[0].max_tcp_carriers, Some(1));
    assert_eq!(status.outbounds[0].name, "daily-direct");
    assert_eq!(status.outbounds[0].protocol, "direct");
    assert_eq!(status.outbounds[0].networks, ["tcp", "udp"]);
    assert_eq!(status.admission.live_flows, 1);
    assert_eq!(status.admission.tracked_principals, 1);
    assert_eq!(status.admission.tracked_targets, 1);
    let encoded = serde_json::to_string(&*status).expect("serialize snapshot");
    assert!(encoded.contains("\"max_tcp_carriers\":1"));
    assert!(!encoded.contains("tcp_carriers_max"));
    assert!(encoded.contains("\"allow_bulk\":true"));
    assert!(encoded.contains("\"control_only\":false"));
    assert!(encoded.contains("\"allow_datagrams\":true"));
    assert!(!encoded.contains("bulk_allowed"));
    assert!(!encoded.contains("probe_only"));
    assert!(!encoded.contains("no_udp"));
    assert!(!encoded.contains("operator"));
    assert!(!encoded.contains("secret"));
    assert!(!encoded.contains("private-principal"));
    assert!(!encoded.contains("private-target.example"));
}

#[test]
fn status_projects_the_bounded_tcp_carrier_pool() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?max-tcp-carriers=3"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        tun_l3_inventory: TunL3RuntimeInventory::default(),
        product_telemetry: RuntimeTelemetry::new(8),
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
    assert_eq!(status.summary.path_count, 3);
    assert_eq!(status.paths.len(), 3);
    assert_eq!(status.paths[1].path, "primary");
    assert_eq!(status.paths[1].tcp_carrier_ordinal, Some(2));
    assert_eq!(status.paths[1].max_tcp_carriers, Some(3));
    assert_eq!(status.paths[2].tcp_carrier_ordinal, Some(3));
}

#[test]
fn status_separates_sessions_flows_and_exclusive_path_states() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?max-tcp-carriers=1"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    context.health().lock().expect("health").tcp[0].manual_disabled = true;
    let authenticated_carrier = context.authenticated_carriers.register();
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
        tun_l3_inventory: TunL3RuntimeInventory::default(),
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
    assert_eq!(status.summary.path_count, 1);
    assert_eq!(status.summary.disabled_paths, 1);
    assert_eq!(status.summary.failed_paths, 0);
    assert_eq!(status.paths[0].state, "disabled");
    assert_eq!(status.paths[0].path, "primary");
    assert_eq!(status.sessions.len(), 1);
    assert_eq!(status.sessions[0].state, "connected");
    assert_eq!(status.sessions[0].carrier_count, 1);
    assert_eq!(status.sessions[0].active_reliable_flows, 1);
    assert_eq!(status.summary.active_flows, 1);
    assert_eq!(status.flows.len(), 1);
    assert_eq!(status.flows[0].flow_id, "1");
    assert_eq!(status.flows[0].network, "tcp");
    assert_eq!(status.flows[0].inbound.as_deref(), Some("local-socks"));
    assert_eq!(status.flows[0].outbound.as_deref(), Some("edge-b"));
    assert_eq!(status.flows[0].balancer.as_deref(), Some("daily-egress"));
    assert_eq!(
        status.flows[0].target.as_deref(),
        Some("service.example:443")
    );
    assert_eq!(status.diagnostics.peer_sessions.len(), 1);
    assert_eq!(status.diagnostics.peer_sessions[0].service, "mpp_outbound");
    assert_eq!(status.diagnostics.peer_sessions[0].service_index, 0);

    drop(authenticated_carrier);
    target.refresh_sample_snapshot();
    let offline = target.snapshot();
    assert_eq!(offline.sessions[0].state, "offline");
    assert_eq!(offline.sessions[0].carrier_count, 0);
    assert_eq!(offline.sessions[0].active_reliable_flows, 1);
    assert_eq!(offline.diagnostics.peer_sessions[0].carrier_count, 1);
}

#[test]
fn control_refresh_does_not_advance_one_hertz_trends() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let target = ManagementTarget {
        clients: vec![context],
        servers: Vec::new(),
        inventory: ProductRuntimeInventory::default(),
        tun_l3_inventory: TunL3RuntimeInventory::default(),
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
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"disabled"}"#)
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

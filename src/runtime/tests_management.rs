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
        path: "/api/v4/status".to_string(),
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
    assert_eq!(initial.schema, "mptunnel.management.v4");
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
    assert_eq!(drained["schema"], "mptunnel.balancer.v4");
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
    assert_eq!(initial["schema"], "mptunnel.dns.status.v4");
    assert_eq!(initial["generation"], 1);
    assert_eq!(initial["policies"][0]["name"], "test-default");
    assert_eq!(initial["policies"][0]["cache"]["entries"], 0);
    assert_eq!(initial["policies"][0]["servers"][0]["protocol"], "system");
    assert!(initial["policies"][0]["servers"][0]["address"].is_null());

    let explanation = target
        .dns_explain_json("managed.example")
        .expect("DNS explanation");
    assert_eq!(explanation["schema"], "mptunnel.dns.explain.v4");
    assert_eq!(explanation["domain"], "managed.example");
    assert_eq!(explanation["policy"], "test-default");
    assert_eq!(explanation["selector"], "default");
    assert!(explanation["dns_rule"].is_null());
    assert_eq!(explanation["servers"][0]["name"], "test-static");

    let queried = target
        .dns_query_json(br#"{"domain":"managed.example","type":"A"}"#)
        .await
        .expect("typed DNS query");
    assert_eq!(queried["schema"], "mptunnel.dns.query.v4");
    assert_eq!(queried["domain"], "managed.example");
    assert_eq!(queried["policy"], "test-default");
    assert_eq!(queried["selector"], "default");
    assert!(queried["dns_rule"].is_null());
    assert!(queried["matched_domain"].is_null());
    assert!(queried["override_record"].is_null());
    assert!(queried["synthetic_capture"].is_null());
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
    assert_eq!(flushed["schema"], "mptunnel.dns.flush.v4");
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
            spec: "tcp://127.0.0.1:443-445?max-tcp-carriers=1"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    {
        let health = context.health();
        let mut health = health.lock().expect("health");
        // Model a partial macOS-like snapshot: RTT/window shape is native, but
        // exact flight and unsent queue are independently unavailable.
        health.tcp[0].carrier_srtt_ms = Some(20.0);
        health.tcp[0].carrier_rttvar_ms = Some(5.0);
        health.tcp[0].carrier_inflight_limit_bytes = 512 * 1024;
    }
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
    assert_eq!(status.paths[0].direction, Some("client_to_server"));
    assert!(status.paths[0].port_hopping);
    assert_eq!(status.paths[0].active_port, None);
    assert_eq!(
        status.paths[0].delivery_rate_source,
        Some("scheduler_default")
    );
    assert_eq!(status.paths[0].delivery_rate_scope, Some("path_capacity"));
    assert_eq!(status.paths[0].pacing_rate_bps, None);
    assert_eq!(status.paths[0].queue_bytes, None);
    assert_eq!(status.paths[0].bytes_in_flight, None);
    assert_eq!(status.paths[0].loss_ppm, None);
    assert_eq!(status.paths[0].loss_observed, Some(false));
    assert_eq!(status.paths[0].delivery_samples, None);
    assert_eq!(status.paths[0].data_sample_bytes, None);
    assert_eq!(status.paths[0].last_delivery_age_ms, None);
    assert_eq!(status.paths[0].pacing_age_ms, None);
    assert_eq!(status.paths[0].freshness_horizon_ms, None);
    assert_eq!(status.summary.path_pacing_rate_bps, None);
    assert_eq!(status.outbounds[0].name, "daily-direct");
    assert_eq!(status.outbounds[0].protocol, "direct");
    assert_eq!(status.outbounds[0].networks, ["tcp", "udp"]);
    assert_eq!(status.admission.live_flows, 1);
    assert_eq!(status.admission.tracked_principals, 1);
    assert_eq!(status.admission.tracked_targets, 1);
    let encoded = serde_json::to_string(&*status).expect("serialize snapshot");
    assert!(encoded.contains("\"max_tcp_carriers\":1"));
    assert!(encoded.contains("\"active_port\":null"));
    assert!(encoded.contains("\"pacing_rate_bps\":null"));
    assert!(encoded.contains("\"loss_ppm\":null"));
    assert!(encoded.contains("\"delivery_samples\":null"));
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

    {
        let health = target.clients[0].health();
        let mut health = health.lock().expect("health");
        health.tcp[0].carrier_bytes_in_flight_observed = true;
        health.tcp[0].carrier_queue_bytes_observed = true;
    }
    target.refresh_sample_snapshot();
    let observed_zero = target.snapshot();
    assert_eq!(observed_zero.paths[0].bytes_in_flight.as_deref(), Some("0"));
    assert_eq!(observed_zero.paths[0].queue_bytes.as_deref(), Some("0"));
}

#[test]
fn client_status_retains_stale_raw_rate_with_provenance_without_reentering_scheduler() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "measured-quic".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "quic://127.0.0.1:7443-7445".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let sample_at = std::time::Instant::now() - Duration::from_secs(10);
    let sample_deadline = sample_at + Duration::from_secs(3);
    {
        let mut health = context.health().lock().expect("health");
        let record = &mut health.udp[0];
        record.carrier_srtt_ms = Some(10.0);
        record.carrier_rttvar_ms = Some(1.0);
        record.carrier_loss_rate = Some(0.0);
        record.carrier_delivery_rate_bps = Some(123_000.0);
        record.carrier_pacing_rate_bps = Some(200_000.0);
        record.carrier_delivery_samples = 2;
        record.carrier_delivery_sample_bytes = 4_096;
        record.carrier_delivery_window_covered = true;
        record.carrier_last_delivery_at = Some(sample_at);
        record.carrier_bulk_proof_expires_at = Some(sample_deadline);
        record.carrier_app_limited = false;
        record.carrier_ack_derived_data_seen = true;
    }
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
    let path = &status.paths[0];
    assert_eq!(path.delivery_rate_bps.as_deref(), Some("123000"));
    assert_eq!(path.delivery_rate_source, Some("native_carrier"));
    assert_eq!(path.delivery_rate_scope, Some("path_capacity"));
    assert_eq!(path.pacing_rate_bps.as_deref(), Some("200000"));
    assert_eq!(path.pacing_rate_source, Some("native_carrier"));
    assert_eq!(path.delivery_samples, Some(2));
    assert_eq!(path.data_sample_bytes.as_deref(), Some("4096"));
    assert_eq!(path.direction, Some("client_to_server"));
    assert!(path.last_delivery_age_ms.is_some_and(|age| age >= 1_000));
    assert!(path.pacing_age_ms.is_some_and(|age| age >= 1_000));
    assert_eq!(path.pacing_age_ms, path.last_delivery_age_ms);
    assert_eq!(
        path.freshness_horizon_ms,
        Some(3_000),
        "QUIC diagnostics must retain the sample-time proof horizon, not recompute it from current RTT"
    );
    assert_eq!(path.metric_age_scope, Some("delivery"));
    assert_eq!(path.loss_ppm, Some(0));
    assert_eq!(path.loss_source, Some("native_carrier"));
    assert_eq!(path.native_delivery_observed, Some(true));
    assert_eq!(path.ack_derived_data_observed, Some(true));
    assert_ne!(status.summary.path_delivery_rate_bps, "123000");
    assert_eq!(status.summary.path_pacing_rate_bps, None);
    assert!(path.port_hopping);
    assert_eq!(path.active_port, None);
}

#[test]
fn client_status_does_not_mix_native_pacing_with_newer_product_delivery_epoch() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "mixed-evidence-quic".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "quic://127.0.0.1:7443".parse().expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let now = std::time::Instant::now();
    let carrier_at = now - Duration::from_millis(200);
    let product_at = now - Duration::from_millis(100);
    {
        let mut health = context.health().lock().expect("health");
        let record = &mut health.udp[0];
        record.carrier_delivery_rate_bps = Some(120_000_000.0);
        record.carrier_pacing_rate_bps = Some(180_000_000.0);
        record.carrier_delivery_samples = 4;
        record.carrier_delivery_sample_bytes = 256 * 1024;
        record.carrier_last_delivery_at = Some(carrier_at);
        record.carrier_bulk_proof_expires_at = Some(carrier_at + Duration::from_secs(2));
        record.carrier_app_limited = false;
        record.measured_rate_bps = Some(80_000_000.0);
        record.product_delivery_rate_bps = Some(80_000_000.0);
        record.delivery_samples = 1;
        record.product_delivery_sample_bytes = 64 * 1024;
        record.last_delivery_at = Some(product_at);
        record.delivery_rate_expires_at = Some(product_at + Duration::from_secs(1));
    }
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
    let path = &status.paths[0];
    assert_eq!(path.delivery_rate_bps.as_deref(), Some("80000000"));
    assert_eq!(path.delivery_rate_source, Some("product_goodput"));
    assert_eq!(path.delivery_rate_scope, Some("per_flow_goodput"));
    assert_eq!(path.freshness_horizon_ms, Some(1_000));
    assert_eq!(path.pacing_rate_bps, None);
    assert_eq!(path.pacing_rate_source, None);
    assert_eq!(path.pacing_age_ms, None);
    assert_eq!(status.summary.path_pacing_rate_bps, None);
}

#[test]
fn server_tcp_app_limited_refresh_preserves_frozen_rate_and_pacing_provenance() {
    let path: crate::transport::PathSpec = "tcp://127.0.0.1:7443".parse().expect("server path");
    let crate::runtime::node::server::ServerIdentityRuntime {
        paths: context,
        reliable_relay: _,
    } = crate::runtime::node::server::new_identity_runtime(
        vec![path],
        OutboundConfig::Direct,
        crate::config::DEFAULT_OUTBOUND_CONNECT_TIMEOUT,
        crate::config::ServerSecurityConfig::for_test(
            SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
        ),
        crate::config::MppPerformanceConfig::default(),
        ResourceLimits::default(),
    );
    let session_id = crate::protocol::SessionId(91);
    let path_id = crate::protocol::PathId(0);
    let registration = context.reliable_streams.register_test_carrier_path(
        session_id,
        crate::protocol::UnderlayProtocol::Tcp,
        path_id,
        crate::runtime::path::ServerLocalPathProperties {
            config_ordinal: 0,
            ..crate::runtime::path::ServerLocalPathProperties::default()
        },
    );
    let observed_at = std::time::Instant::now() - Duration::from_secs(10);
    let expires_at = observed_at + Duration::from_secs(3);
    context
        .reliable_streams
        .record_local_path_metrics_with_delivery_rate_sample(
            &registration,
            crate::protocol::PathMetrics {
                path_id,
                underlay: crate::protocol::UnderlayProtocol::Tcp,
                direction: crate::protocol::PathMetricDirection::ServerToClient,
                metric_epoch: 1,
                metric_age_us: 0,
                rate_valid_for_us: 0,
                rate_observed: false,
                srtt_us: 2_000_000,
                rttvar_us: 500_000,
                jitter_us: 500_000,
                delivery_rate_bps: 1_000_000,
                pacing_rate_bps: 2_000_000,
                pacing_rate_observed: false,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight_observed: false,
                queue_observed: false,
                bytes_in_flight: 64 * 1024,
                queue_bytes: 32 * 1024,
                inflight_limit_bytes: 512 * 1024,
                inflight_hi_bytes: 512 * 1024,
                confidence_ppm: 1_000_000,
                app_limited: true,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
            false,
            Some(crate::runtime::path::CarrierDeliveryRateSample {
                delivery_rate_bps: 123_000_000,
                pacing_rate_bps: Some(200_000_000),
                sample_count: 8,
                sample_bytes: 512 * 1024,
                delivery_window_covered: true,
                observed_at,
                expires_at,
            }),
        );
    let target = ManagementTarget {
        clients: Vec::new(),
        servers: vec![context.clone()],
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
    let carrier = status
        .paths
        .iter()
        .find(|path| path.session_id.as_deref() == Some("91"))
        .expect("server carrier row");
    assert_eq!(carrier.delivery_rate_bps.as_deref(), Some("123000000"));
    assert_eq!(carrier.pacing_rate_bps.as_deref(), Some("200000000"));
    assert_eq!(carrier.delivery_rate_source, Some("native_carrier"));
    assert_eq!(carrier.pacing_rate_source, Some("native_carrier"));
    assert_eq!(carrier.direction, Some("server_to_client"));
    assert_eq!(carrier.delivery_samples, Some(8));
    assert_eq!(carrier.data_sample_bytes.as_deref(), Some("524288"));
    assert!(
        carrier
            .last_delivery_age_ms
            .is_some_and(|age| age >= 10_000)
    );
    assert_eq!(carrier.pacing_age_ms, carrier.last_delivery_age_ms);
    assert_eq!(carrier.freshness_horizon_ms, Some(3_000));
    assert_eq!(carrier.metric_age_scope, Some("delivery"));
    assert_eq!(carrier.bytes_in_flight, None);
    assert_eq!(carrier.queue_bytes, None);
    assert_ne!(
        status.summary.path_delivery_rate_bps, "123000000",
        "stale raw server diagnostics cannot inflate the live aggregate"
    );

    context
        .reliable_streams
        .record_local_path_metrics_with_delivery_rate_sample(
            &registration,
            context
                .reliable_streams
                .management_snapshot()
                .paths
                .into_iter()
                .find(|path| path.path_instance_id == registration.path_instance_id())
                .and_then(|path| path.metrics)
                .expect("retained metrics"),
            false,
            Some(crate::runtime::path::CarrierDeliveryRateSample {
                delivery_rate_bps: 123_000_000,
                pacing_rate_bps: None,
                sample_count: 8,
                sample_bytes: 512 * 1024,
                delivery_window_covered: true,
                observed_at,
                expires_at,
            }),
        );
    target.refresh_sample_snapshot();
    let no_pacing = target.snapshot();
    let carrier = no_pacing
        .paths
        .iter()
        .find(|path| path.session_id.as_deref() == Some("91"))
        .expect("server carrier row");
    assert_eq!(carrier.pacing_rate_bps, None);
    assert_eq!(carrier.pacing_rate_source, None);
    assert_eq!(carrier.pacing_age_ms, None);
}

#[test]
fn management_rate_freshness_uses_the_runtime_three_pto_inputs() {
    assert_eq!(
        super::projection::client_metric_freshness_horizon_ms(
            crate::runtime::path::model::ClientPathObservation::default()
        ),
        1_573
    );
    assert_eq!(
        super::projection::client_metric_freshness_horizon_ms(
            crate::runtime::path::model::ClientPathObservation {
                carrier_srtt_ms: Some(10.0),
                carrier_rttvar_ms: Some(1.0),
                ..crate::runtime::path::model::ClientPathObservation::default()
            }
        ),
        120
    );
}

#[test]
fn peer_status_projects_local_path_identity_for_a_draining_authenticated_assignment() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![
            ClientPathConfig {
                name: "primary-tcp".to_string(),
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec: "tcp://127.0.0.1:7443-7445?max-tcp-carriers=3"
                    .parse()
                    .expect("TCP path"),
                security: security.clone(),
            },
            ClientPathConfig {
                name: "backup-quic".to_string(),
                tls: crate::transport::encrypted::test_client_tls_config(),
                spec: "quic://127.0.0.1:7444".parse().expect("QUIC path"),
                security,
            },
        ],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let draining = context.peer_status.register_path(
        context.session_id,
        crate::protocol::UnderlayProtocol::Tcp,
        crate::protocol::PathId(47),
        2,
        Some(7444),
    );
    let _keeper = context.peer_status.register_path(
        context.session_id,
        crate::protocol::UnderlayProtocol::Udp,
        crate::protocol::PathId(0),
        0,
        Some(7444),
    );
    let result = crate::runtime::peer_status::PeerStatusResult {
        session_id: context.session_id,
        request_id: 9,
        code: crate::protocol::PeerStatusCode::Ok,
        paths: vec![crate::protocol::PeerPathStatus {
            state: crate::protocol::PeerPathState::Draining,
            usage: crate::protocol::PathUsage::Available,
            metrics: crate::protocol::PathMetrics {
                path_id: crate::protocol::PathId(47),
                underlay: crate::protocol::UnderlayProtocol::Tcp,
                direction: crate::protocol::PathMetricDirection::ServerToClient,
                metric_epoch: 1,
                metric_age_us: 0,
                rate_valid_for_us: 0,
                rate_observed: false,
                srtt_us: 10_000,
                rttvar_us: 0,
                jitter_us: 0,
                delivery_rate_bps: 10_000_000,
                pacing_rate_bps: 10_000_000,
                pacing_rate_observed: false,
                loss_ppm: 0,
                ecn_ppm: 0,
                loss_observed: false,
                ecn_observed: false,
                bytes_in_flight_observed: true,
                queue_observed: true,
                bytes_in_flight: 0,
                queue_bytes: 0,
                inflight_limit_bytes: 0,
                inflight_hi_bytes: 0,
                confidence_ppm: 0,
                app_limited: true,
                has_ack_derived_data_sample: false,
                data_sample_count: 0,
                data_sample_bytes: 0,
            },
        }],
        local_paths: std::collections::BTreeMap::from([(
            (
                crate::protocol::UnderlayProtocol::Tcp,
                crate::protocol::PathId(47),
            ),
            crate::runtime::peer_status::PeerStatusLocalPathSnapshot {
                local_path_index: 2,
                active_port: Some(7444),
                retired: false,
            },
        )]),
        received_at: std::time::SystemTime::now(),
    };
    drop(draining);
    let mut absent_result = result.clone();
    absent_result.paths[0].metrics.bytes_in_flight_observed = false;
    absent_result.paths[0].metrics.queue_observed = false;
    absent_result.paths[0].metrics.has_ack_derived_data_sample = true;
    absent_result.paths[0].metrics.data_sample_count = 1;
    absent_result.paths[0].metrics.data_sample_bytes = 1_024;
    absent_result.paths[0].metrics.metric_age_us = 1_500;
    absent_result.paths[0].metrics.rate_valid_for_us = 2_001;
    absent_result.paths[0].metrics.rate_observed = true;
    absent_result.paths[0].metrics.pacing_rate_observed = true;
    let projected = super::projection::peer_status_result(
        result,
        "mpp_outbound",
        0,
        Some("edge-mpp".to_string()),
        super::projection::PeerPathIdentitySource::Client(&context),
    );

    assert_eq!(projected.paths.len(), 1);
    assert_eq!(projected.paths[0].path.as_deref(), Some("primary-tcp"));
    assert_eq!(
        projected.paths[0].endpoint.as_deref(),
        Some("tcp://127.0.0.1:7443-7445")
    );
    let encoded = serde_json::to_value(projected).expect("peer status JSON");
    assert_eq!(encoded["paths"][0]["path"], "primary-tcp");
    assert_eq!(encoded["paths"][0]["endpoint"], "tcp://127.0.0.1:7443-7445");
    assert_eq!(encoded["paths"][0]["port_hopping"], true);
    assert_eq!(encoded["paths"][0]["active_port"], 7444);
    assert_eq!(encoded["paths"][0]["active_port_retired"], false);
    assert_eq!(encoded["paths"][0]["srtt_us"], 10_000);
    assert_eq!(encoded["paths"][0]["rttvar_us"], 0);
    assert_eq!(encoded["paths"][0]["jitter_us"], 0);
    assert_eq!(encoded["paths"][0]["usage_direction"], "client_to_server");
    assert_eq!(encoded["paths"][0]["direction"], "server_to_client");
    assert_eq!(encoded["paths"][0]["delivery_rate_source"], "peer_advisory");
    assert_eq!(encoded["paths"][0]["delivery_rate_scope"], "advisory");
    assert!(encoded["paths"][0]["pacing_rate_bps"].is_null());
    assert!(encoded["paths"][0]["pacing_rate_source"].is_null());
    assert!(encoded["paths"][0]["freshness_horizon_ms"].is_null());
    assert_eq!(encoded["paths"][0]["metric_age_scope"], "path_metrics");
    assert!(encoded["paths"][0]["loss_ppm"].is_null());
    assert!(encoded["paths"][0]["ecn_ppm"].is_null());
    assert!(encoded["paths"][0]["inflight_limit_bytes"].is_null());
    assert!(encoded["paths"][0]["data_sample_count"].is_null());
    assert!(encoded["paths"][0]["data_sample_bytes"].is_null());
    assert_eq!(encoded["paths"][0]["loss_observed"], false);
    assert_eq!(encoded["paths"][0]["ecn_observed"], false);
    assert_eq!(encoded["paths"][0]["ack_derived_data_observed"], false);
    assert_eq!(encoded["paths"][0]["bytes_in_flight"], "0");
    assert_eq!(encoded["paths"][0]["queue_bytes"], "0");

    let absent = super::projection::peer_status_result(
        absent_result,
        "mpp_outbound",
        0,
        Some("edge-mpp".to_string()),
        super::projection::PeerPathIdentitySource::Client(&context),
    );
    let absent = serde_json::to_value(absent).expect("partial peer status JSON");
    assert!(absent["paths"][0]["bytes_in_flight"].is_null());
    assert!(absent["paths"][0]["queue_bytes"].is_null());
    assert_eq!(absent["paths"][0]["pacing_rate_bps"], "10000000");
    assert_eq!(absent["paths"][0]["pacing_rate_source"], "peer_advisory");
    assert_eq!(absent["paths"][0]["freshness_horizon_ms"], 4);
}

#[test]
fn mpp_flow_projects_the_authenticated_opening_carrier_as_typed_source() {
    let now = std::time::Instant::now();
    let status = super::projection::flow_status(
        crate::runtime::telemetry::ActiveProductFlowSnapshot {
            display_id: 17,
            session_id: Some(crate::protocol::SessionId(91)),
            flow_id: crate::runtime::telemetry::ProductFlowId::Reliable(crate::protocol::StreamId(
                4,
            )),
            network: crate::product::Network::Tcp,
            target: Some(crate::protocol::TargetAddr::Domain {
                host: "service.example".to_string(),
                port: 443,
            }),
            origin: Some(crate::runtime::telemetry::ProductFlowOrigin {
                kind: crate::runtime::telemetry::ProductFlowOriginKind::MppInbound,
                inbound: crate::product::InboundId::parse("edge-in").expect("inbound"),
                source: crate::runtime::telemetry::ProductFlowSource::mpp_carrier_peer(
                    "203.0.113.7:51000".parse().expect("carrier peer"),
                ),
            }),
            selection: Some(crate::runtime::telemetry::ProductFlowSelection {
                outbound: OutboundId::parse("direct").expect("outbound"),
                balancer: None,
                member: None,
            }),
            started_at: now,
            last_activity_at: now,
            io: crate::runtime::telemetry::ProductIoSnapshot::default(),
        },
        now,
    )
    .expect("scoped MPP flow");

    assert_eq!(status.source_kind, "mpp_carrier_peer");
    assert_eq!(status.source, "203.0.113.7:51000");
    let encoded = serde_json::to_value(status).expect("MPP flow JSON");
    assert_eq!(encoded["source_kind"], "mpp_carrier_peer");
    assert_eq!(encoded["source"], "203.0.113.7:51000");
}

#[test]
fn unscoped_internal_telemetry_is_not_projected_as_an_inbound_row() {
    let telemetry = RuntimeTelemetry::new(2);
    let _internal = telemetry.open_reliable_flow(
        None,
        crate::protocol::StreamId(7),
        crate::protocol::TargetAddr::Ip("127.0.0.1:853".parse().expect("internal target")),
    );
    let mut aggregate = super::snapshot::TelemetryAggregate::default();
    aggregate.add(telemetry.snapshot(), std::time::Instant::now());

    assert!(
        aggregate.flows.is_empty(),
        "unscoped DNS/probe/test transport work has no inbound source authority"
    );
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
        crate::runtime::telemetry::ProductFlowSource::local_peer(
            "127.0.0.1:42000".parse().expect("local peer"),
        ),
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
    assert_eq!(status.flows[0].inbound_kind, "local");
    assert_eq!(status.flows[0].inbound, "local-socks");
    assert_eq!(status.flows[0].source_kind, "local_peer");
    assert_eq!(status.flows[0].source, "127.0.0.1:42000");
    assert_eq!(status.flows[0].outbound.as_deref(), Some("edge-b"));
    assert_eq!(status.flows[0].balancer.as_deref(), Some("daily-egress"));
    assert_eq!(
        status.flows[0].target.as_deref(),
        Some("service.example:443")
    );
    let encoded_flow = serde_json::to_value(&status.flows[0]).expect("flow JSON");
    assert_eq!(encoded_flow["source_kind"], "local_peer");
    assert_eq!(encoded_flow["source"], "127.0.0.1:42000");
    assert_eq!(encoded_flow["inbound_kind"], "local");
    assert_eq!(encoded_flow["inbound"], "local-socks");
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

use super::*;
use crate::config::{ClientPathConfig, LocalIngressConfig, ResourceLimits, SharedSecret};
use crate::ingress::{IngressConfig, LocalProxyUser, ProxyAuthConfig};
use crate::model::capacity::reliable_relay_buffer_len;
use crate::model::path::{RelayPathKey, next_carrier_path_instance_id};
use crate::model::tcp_service::{
    TcpServiceDataAckEvent, TcpServiceWithdrawalReason, TcpServiceWriterLifecycle,
};
use crate::protocol::{
    AuthNonce, PathId, PathMetricDirection, PathUsage, StreamId, UnderlayProtocol,
};
use crate::runtime::management::snapshot::SessionInventory;
use crate::runtime::outbound_registry::{
    RuntimeOutboundLeaf, RuntimeOutboundRegistry, test_dns_generation,
};
use crate::runtime::path::ClientRequestTcpServiceLifecycleState;
use crate::runtime::path::commands::reliable_path_command_channels;
use crate::runtime::readiness::RuntimeGenerationControl;
use crate::runtime::stream::{
    OpenedRemoteStream, ReliablePathStream, ReliablePathStreamOutput, ReliableRelayRemoteSet,
};
use crate::runtime::tcp_service::{
    RequestTcpServiceSnapshotRequest, TcpServiceAckDisposition, TcpServiceDataAckSink,
    TcpServiceFlightSidecarError, TcpServiceWriterCoordinator,
};
use crate::scheduler::TrafficClass;
use crate::{
    config::GatewayBalancerConfig,
    outbound::OutboundConfig,
    product::{
        BalancerId, GatewayBalancerSpec, GatewayMemberSpec, GatewayStrategy, NetworkSet, OutboundId,
    },
};
use std::time::Duration;
use tokio::sync::mpsc;

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

#[derive(Debug)]
struct IgnoreManagementTcpServiceAck;

impl TcpServiceDataAckSink for IgnoreManagementTcpServiceAck {
    fn apply_data_ack(
        &self,
        _event: TcpServiceDataAckEvent,
        _now: std::time::Instant,
    ) -> Result<TcpServiceAckDisposition, TcpServiceFlightSidecarError> {
        Ok(TcpServiceAckDisposition::Continue)
    }
}

#[test]
fn auth_accepts_bearer_and_rejects_wrong_token() {
    let request = ManagementRequest {
        method: "GET".to_string(),
        path: "/api/v2/status".to_string(),
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
    assert_eq!(initial.schema, "mptunnel.management.v5");
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
    assert_eq!(drained["schema"], "mptunnel.balancer.v1");
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
        product_telemetry: RuntimeTelemetry::new(8),
        state: ManagementState::new("node"),
        config_control: None,
        gateway_control: None,
        dns: Some(dns),
        product_admission: ProductAdmission::default(),
        generation: RuntimeGenerationControl::new(),
    };

    let initial = target.dns_status_json().expect("initial DNS status");
    assert_eq!(initial["schema"], "mptunnel.dns.status.v2");
    assert_eq!(initial["generation"], 1);
    assert_eq!(initial["plans"][0]["name"], "test-default");
    assert_eq!(initial["plans"][0]["cache"]["entries"], 0);

    let explanation = target
        .dns_explain_json("managed.example")
        .expect("DNS explanation");
    assert_eq!(explanation["schema"], "mptunnel.dns.explain.v2");
    assert_eq!(explanation["domain"], "managed.example");
    assert_eq!(explanation["dns_plan"], "test-default");
    assert_eq!(explanation["match"], "default");
    assert_eq!(explanation["upstreams"][0]["name"], "test-static");

    let queried = target
        .dns_query_json(br#"{"domain":"managed.example","type":"A"}"#)
        .await
        .expect("typed DNS query");
    assert_eq!(queried["schema"], "mptunnel.dns.query.v2");
    assert_eq!(queried["domain"], "managed.example");
    assert_eq!(queried["dns_plan"], "test-default");
    assert_eq!(queried["rcode"], 0);
    assert_eq!(queried["rcode_name"], "NOERROR");
    assert_eq!(queried["answers"][0]["type"], "A");
    assert_eq!(queried["answers"][0]["owner_name"], "managed.example.");
    assert_eq!(queried["answers"][0]["data"], "192.0.2.53");

    let observed = target.dns_status_json().expect("observed DNS status");
    assert_eq!(observed["plans"][0]["cache"]["entries"], 1);
    assert_eq!(observed["plans"][0]["upstreams"][0]["attempts"], "1");
    assert_eq!(observed["plans"][0]["upstreams"][0]["successes"], "1");

    let flushed = target.dns_flush_json(br#"{}"#).expect("flush all plans");
    assert_eq!(flushed["schema"], "mptunnel.dns.flush.v2");
    assert_eq!(flushed["flushed_dns_plan_count"], 1);
    assert_eq!(flushed["removed_entries"], 1);
    assert_eq!(
        target.dns_status_json().expect("flushed DNS status")["plans"][0]["cache"]["entries"],
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
            .dns_flush_json(br#"{"dns_plan":"missing"}"#)
            .expect_err("unknown plan")
            .status,
        404
    );
}

#[tokio::test]
async fn enabling_a_path_requires_fresh_liveness_evidence() {
    let security = ClientSecurityConfig::for_test(
        SharedSecret::new(b"0123456789abcdef0123456789abcdef".to_vec()).expect("secret"),
    );
    let context = ClientPathContext::new_with_path_configs_and_outbound(
        vec![ClientPathConfig {
            name: "primary".to_string(),
            tls: crate::transport::encrypted::test_client_tls_config(),
            spec: "tcp://127.0.0.1:443?tcp-carriers=2-3"
                .parse()
                .expect("path"),
            security,
        }],
        ResourceLimits::default(),
        ProxyAuthConfig::disabled(),
        Some(OutboundId::parse("edge-mpp").expect("outbound")),
    )
    .expect("context");
    let dormant_instance = next_carrier_path_instance_id();
    context.install_authenticated_path_for_test(
        UnderlayProtocol::Tcp,
        2,
        PathId(12),
        AuthNonce([12; 16]),
        dormant_instance,
        0,
        PathUsage::Available,
    );
    let dormant_key = RelayPathKey {
        underlay: UnderlayProtocol::Tcp,
        index: 2,
    };
    let initial_candidate = context
        .current_request_tcp_service_candidate(dormant_key)
        .expect("ready dormant candidate");
    let stream_id = StreamId(903);
    let (commands, _receivers) = reliable_path_command_channels(8);
    let (_frames_tx, frames_rx) = mpsc::channel(1);
    let remotes = ReliableRelayRemoteSet::new(
        OpenedRemoteStream::pending(
            ReliablePathStream {
                stream_id,
                max_offset: context.mux_limits.max_stream_window_bytes,
                lane: TrafficClass::Throughput,
                underlay: UnderlayProtocol::Tcp,
                max_frame_payload_bytes: reliable_relay_buffer_len(context.mux_limits),
                output: ReliablePathStreamOutput::fixed(
                    UnderlayProtocol::Tcp,
                    PathId(0),
                    commands,
                    context.mux_limits,
                ),
                frames: frames_rx.into(),
            },
            0,
        ),
        8,
    );
    let accepted_instance = remotes.paths[0].instance();
    context.install_authenticated_path_for_test(
        UnderlayProtocol::Tcp,
        0,
        PathId(10),
        AuthNonce([10; 16]),
        accepted_instance.path_instance_id,
        0,
        PathUsage::Available,
    );
    let _writer_registration = context
        .register_tcp_service_writer(stream_id, remotes.tcp_service_writer())
        .expect("register managed-path test actor");
    let frozen = remotes
        .snapshot_tcp_service_stream(
            &context,
            RequestTcpServiceSnapshotRequest {
                carrier_group_id: context
                    .tcp_service_carrier_group_id(0)
                    .expect("configured carrier group"),
                candidate: initial_candidate,
                max_accepted_paths: 3,
            },
            1,
            1024,
        )
        .expect("freeze managed-path lifecycle");
    let lifecycle = TcpServiceWriterLifecycle::for_runtime_test(
        context.session_id,
        1,
        PathMetricDirection::ClientToServer,
    );
    let coordinator = Arc::new(TcpServiceWriterCoordinator::new(
        lifecycle,
        Arc::new(IgnoreManagementTcpServiceAck),
    ));
    context
        .arm_request_tcp_service_lifecycle(&[frozen], coordinator.clone())
        .expect("arm before explicit management failure");
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
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"failed"}"#)
        .expect("fail path");
    assert_eq!(
        context.request_tcp_service_lifecycle_state(lifecycle),
        Some(ClientRequestTcpServiceLifecycleState::Withdrawn(
            TcpServiceWithdrawalReason::FenceChanged
        ))
    );
    assert_eq!(
        coordinator.failure(),
        Some(TcpServiceFlightSidecarError::ObserverStopped)
    );
    assert!(
        context
            .begin_request_tcp_service_cleanup(lifecycle, None)
            .expect("clean never-installed managed lifecycle")
            .is_empty()
    );
    assert!(context.disarm_request_tcp_service_lifecycle(lifecycle));
    {
        let health = context.health().lock().expect("failed health");
        assert_eq!(health.tcp.len(), 3);
        assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[1].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[2].state, SchedulerPathState::Failed);
        assert!(health.tcp.iter().all(|record| !record.manual_disabled));
    }
    assert_eq!(
        context.current_request_tcp_service_candidate(dormant_key),
        None,
        "a failed endpoint cannot retain dormant validation authority"
    );
    target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"enabled"}"#)
        .expect("re-enable failed path");
    assert!(
        context
            .current_request_tcp_service_candidate(dormant_key)
            .expect("re-enabled dormant candidate")
            .eligibility_generation
            > initial_candidate.eligibility_generation
    );

    let disabled = target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"disabled"}"#)
        .expect("disable path");
    assert_eq!(disabled["outbound"], "edge-mpp");
    assert_eq!(disabled["path"], "primary");
    {
        let health = context.health().lock().expect("disabled health");
        assert_eq!(health.tcp.len(), 3);
        assert!(health.tcp.iter().all(|record| record.manual_disabled));
        assert_eq!(health.tcp[0].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[1].state, SchedulerPathState::Failed);
        assert_eq!(health.tcp[2].state, SchedulerPathState::Draining);
    }
    target
        .control_path_json(br#"{"outbound":"edge-mpp","path":"primary","state":"enabled"}"#)
        .expect("enable path");

    let health = context.health().lock().expect("health");
    assert!(health.tcp.iter().all(|record| !record.manual_disabled));
    assert_eq!(health.tcp[0].state, SchedulerPathState::Suspect);
    assert_eq!(health.tcp[1].state, SchedulerPathState::Suspect);
    assert_eq!(health.tcp[2].state, SchedulerPathState::Draining);
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
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
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
            spec: "tcp://127.0.0.1:443".parse().expect("path"),
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
    assert_eq!(status.services.outbounds, 1);
    assert_eq!(status.services.local_outbounds, 1);
    assert_eq!(status.summary.configured_path_count, 1);
    assert_eq!(status.summary.path_count, 1);
    assert_eq!(status.paths.len(), 1);
    assert_eq!(status.paths[0].path, "path-1");
    assert_eq!(status.paths[0].tcp_carrier_ordinal, Some(1));
    assert_eq!(status.paths[0].tcp_carriers_min, Some(1));
    assert_eq!(status.paths[0].tcp_carriers_max, Some(3));
    assert_eq!(status.outbounds[0].name, "daily-direct");
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
    assert_eq!(status.summary.path_count, 1);
    assert_eq!(status.summary.disabled_paths, 1);
    assert_eq!(status.summary.failed_paths, 0);
    assert_eq!(status.paths[0].state, "disabled");
    assert_eq!(status.paths[0].path, "primary");
    assert_eq!(status.sessions.len(), 1);
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

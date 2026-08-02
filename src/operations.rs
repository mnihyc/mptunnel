//! Bounded, read-mostly operator commands.
//!
//! This module adapts the canonical Product configuration and versioned local
//! management API to a small CLI surface. It never owns runtime or Core state.

use crate::cli::{
    Cli, DnsCommand, DnsExplainArgs, DnsFlushArgs, DnsQueryArgs, DoctorArgs, ManagementClientArgs,
    RouteCommand, RouteExplainArgs,
};
use crate::config::{
    AppConfig, CommandConfig, ConfigFileError, DEFAULT_CONFIG_PATH, ManagementConfig, NodeConfig,
    OutboundLeafConfig, load_config_toml,
};
use crate::product::{EgressAction, FlowContext, RouteInput, SourceEndpoint, TrafficIntent};
use crate::protocol::UnderlayProtocol;
use crate::transport::Endpoint;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::time::Duration;

const DEFAULT_MANAGEMENT_ADDRESS: &str = "127.0.0.1:7600";
const MANAGEMENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const MANAGEMENT_IO_TIMEOUT: Duration = Duration::from_secs(5);
const MANAGEMENT_HEADER_LIMIT: usize = 32 * 1024;
const MANAGEMENT_BODY_LIMIT: usize = 4 * 1024 * 1024;
const MANAGEMENT_REQUEST_BODY_LIMIT: usize = 64 * 1024;
const MANAGEMENT_PATH_LIMIT: usize = 2 * 1024;
const MANAGEMENT_HEADER_COUNT_LIMIT: usize = 64;
const DOCTOR_ENDPOINT_LIMIT: usize = 32;
const DOCTOR_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(3);
const DNS_RECORD_TYPE_LIMIT: usize = 16;

pub(crate) fn execute(cli: &Cli, output: &mut dyn Write) -> Result<(), OperationError> {
    match &cli.command {
        crate::cli::Command::Status(args) => run_status(cli, args, output),
        crate::cli::Command::Doctor(args) => run_doctor(cli, args, output),
        crate::cli::Command::Route(args) => match &args.command {
            RouteCommand::Explain(args) => run_route_explain(cli, args, output),
        },
        crate::cli::Command::Dns(args) => run_dns(cli, args, output),
        crate::cli::Command::Client(_)
        | crate::cli::Command::Server(_)
        | crate::cli::Command::Platform(_) => Err(OperationError::NotOperational),
    }
}

fn run_status(
    cli: &Cli,
    args: &ManagementClientArgs,
    output: &mut dyn Write,
) -> Result<(), OperationError> {
    let client = management_client(cli, args.address)?;
    let value = client.get("/api/v2/status")?;
    write_json(output, &value, client.token())
}

fn run_dns(
    cli: &Cli,
    args: &crate::cli::DnsArgs,
    output: &mut dyn Write,
) -> Result<(), OperationError> {
    let client = management_client(cli, args.address)?;
    let value = match &args.command {
        DnsCommand::Status => client.get("/api/v2/dns/status")?,
        DnsCommand::Explain(args) => dns_explain(&client, args)?,
        DnsCommand::Query(args) => dns_query(&client, args)?,
        DnsCommand::Flush(args) => dns_flush(&client, args)?,
    };
    write_json(output, &value, client.token())
}

fn dns_explain(client: &ManagementClient, args: &DnsExplainArgs) -> Result<Value, OperationError> {
    let encoded = utf8_percent_encode(args.domain.as_str(), NON_ALPHANUMERIC);
    client.get(&format!("/api/v2/dns/explain?domain={encoded}"))
}

fn dns_query(client: &ManagementClient, args: &DnsQueryArgs) -> Result<Value, OperationError> {
    if args.record_type.is_empty()
        || args.record_type.len() > DNS_RECORD_TYPE_LIMIT
        || !args
            .record_type
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric())
    {
        return Err(OperationError::InvalidDnsRecordType);
    }
    client.post(
        "/api/v2/dns/query",
        &json!({
            "domain": args.domain.as_str(),
            "type": args.record_type.to_ascii_uppercase(),
        }),
    )
}

fn dns_flush(client: &ManagementClient, args: &DnsFlushArgs) -> Result<Value, OperationError> {
    client.post(
        "/api/v2/dns/cache/flush",
        &json!({"dns_plan": args.dns_plan.as_ref().map(|plan| plan.as_str())}),
    )
}

fn run_route_explain(
    cli: &Cli,
    args: &RouteExplainArgs,
    output: &mut dyn Write,
) -> Result<(), OperationError> {
    let path = canonical_config_path(cli);
    let config = load_operation_config(&path)?;
    let CommandConfig::Node(node) = &config.command;
    let policy = node
        .product_policy
        .as_ref()
        .ok_or(OperationError::NoProductRouting)?
        .compile()
        .map_err(|error| OperationError::RoutePolicy(error.to_string()))?;

    let flow = FlowContext::new(
        args.network.into(),
        args.target.clone(),
        SourceEndpoint::from_socket_addr(args.source),
        crate::product::PrincipalId::parse(&cli.principal_id)
            .map_err(|error| OperationError::RouteInput(error.to_string()))?,
        args.inbound.clone(),
    );
    let input = args.resolved_ip.map_or_else(
        || RouteInput::pre_resolution(&flow),
        |address| RouteInput::post_resolution(&flow, address),
    );
    let explanation = policy.routes().explain(input);
    render_route_explanation(node, &policy, &flow, input, &explanation, output)
}

fn render_route_explanation(
    node: &NodeConfig,
    policy: &crate::product::ProductPolicyGeneration,
    flow: &FlowContext,
    input: RouteInput<'_>,
    explanation: &crate::product::RouteExplanation<'_>,
    output: &mut dyn Write,
) -> Result<(), OperationError> {
    let selected = explanation.selected();
    let action = selected.action();
    let action_name = egress_action_name(action.egress());
    let resolution = policy.routes().classify(RouteInput::pre_resolution(flow));
    let resolution_action = resolution.action();
    let resolution_dns_plan = resolution_action
        .dns_plan()
        .unwrap_or(&node.dns_policy.spec.default_plan);
    writeln!(output, "route:")?;
    writeln!(output, "  generation: {}", explanation.generation())?;
    writeln!(
        output,
        "  stage: {}",
        match input.stage() {
            crate::product::RouteStage::PreResolution => "pre-resolution",
            crate::product::RouteStage::PostResolution => "post-resolution",
        }
    )?;
    writeln!(output, "  network: {}", flow.network())?;
    writeln!(output, "  target: {}", flow.target().authority())?;
    writeln!(
        output,
        "  resolved_ip: {}",
        input
            .resolved_ip()
            .map_or_else(|| "none".to_string(), |address| address.to_string())
    )?;
    writeln!(
        output,
        "  source: {}",
        SocketAddr::new(flow.source().address(), flow.source().port())
    )?;
    writeln!(output, "  principal: {}", flow.principal())?;
    writeln!(output, "  inbound: {}", flow.inbound())?;
    writeln!(output, "resolution:")?;
    writeln!(output, "  rule: {}", resolution.rule_id())?;
    writeln!(
        output,
        "  dns_plan: {}{}",
        resolution_dns_plan,
        if resolution_action.dns_plan().is_some() {
            ""
        } else {
            " (policy default)"
        }
    )?;
    writeln!(output, "selected:")?;
    writeln!(output, "  rule: {}", selected.rule_id())?;
    writeln!(output, "  action: {action_name}")?;
    match action.egress() {
        EgressAction::Outbound(outbound) => writeln!(output, "  outbound: {outbound}")?,
        EgressAction::Balancer(balancer) => writeln!(output, "  balancer: {balancer}")?,
        EgressAction::Direct | EgressAction::Reject | EgressAction::Drop => {}
    }
    writeln!(
        output,
        "  traffic_intent: {}",
        traffic_intent_name(action.traffic_intent())
    )?;
    writeln!(output, "  explanation: {}", selected.explanation())?;
    writeln!(output, "rules:")?;
    for trace in explanation.rules() {
        writeln!(output, "  - id: {}", trace.rule_id())?;
        if trace.selected() {
            writeln!(output, "    result: selected")?;
        } else if let Some(mismatch) = trace.first_mismatch() {
            writeln!(output, "    result: mismatch ({mismatch})")?;
        } else {
            writeln!(output, "    result: matched (earlier rule selected)")?;
        }
        for (scope, sets) in [
            ("domain", trace.domain_rule_sets()),
            ("destination_ip", trace.destination_rule_sets()),
        ] {
            for set in sets {
                writeln!(
                    output,
                    "    signed_set: scope={scope} id={} publisher={} revision={} expires={} sha256={}",
                    set.id(),
                    set.publisher(),
                    set.revision(),
                    set.expires_at_unix_secs()
                        .map_or_else(|| "none".to_string(), |expiry| expiry.to_string()),
                    lower_hex(set.checksum_sha256()),
                )?;
            }
        }
    }
    Ok(())
}

fn egress_action_name(action: &EgressAction) -> &'static str {
    match action {
        EgressAction::Direct => "direct",
        EgressAction::Reject => "reject",
        EgressAction::Drop => "drop",
        EgressAction::Outbound(_) => "outbound",
        EgressAction::Balancer(_) => "balancer",
    }
}

const fn traffic_intent_name(intent: TrafficIntent) -> &'static str {
    match intent {
        TrafficIntent::Interactive => "interactive",
        TrafficIntent::Throughput => "throughput",
        TrafficIntent::Realtime => "realtime",
        TrafficIntent::Background => "background",
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    let mut value = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(value, "{byte:02x}");
    }
    value
}

fn run_doctor(cli: &Cli, args: &DoctorArgs, output: &mut dyn Write) -> Result<(), OperationError> {
    let path = canonical_config_path(cli);
    let mut report = DoctorReport::default();
    let config = match load_operation_config(&path) {
        Ok(config) => {
            report.pass(
                "config",
                format!(
                    "{} is strict, complete, and all referenced secret/TLS material is valid",
                    path.display()
                ),
            );
            config
        }
        Err(error) => {
            report.fail("config", error.to_string());
            report.render(output)?;
            return Err(OperationError::DoctorFailed);
        }
    };

    match crate::platform::validate_vpn_generation(&config) {
        Ok(()) => report.pass(
            "vpn",
            "configured VPN lifecycle is supported by this target contract",
        ),
        Err(error) => report.fail("vpn", error.to_string()),
    }
    let platform = crate::platform::PlatformReport::current();
    report.note(
        "platform",
        format!(
            "{} {} | TUN: {} | {}",
            platform.os, platform.arch, platform.tun_backend, platform.tun_device_probe
        ),
    );

    let endpoints = configured_probe_endpoints(&config, &mut report);
    match probe_endpoints(endpoints) {
        Ok(results) => {
            for result in results {
                match result.outcome {
                    EndpointProbeOutcome::Reachable(message) => report.pass(&result.label, message),
                    EndpointProbeOutcome::Resolved(message) => report.pass(&result.label, message),
                    EndpointProbeOutcome::Skipped(message) => report.note(&result.label, message),
                    EndpointProbeOutcome::Unavailable(message) => {
                        report.warn(&result.label, message)
                    }
                }
            }
        }
        Err(error) => report.warn("endpoint probes", error.to_string()),
    }

    doctor_management_checks(cli, args, &config, &mut report);
    let failed = report.failed();
    report.render(output)?;
    if failed {
        Err(OperationError::DoctorFailed)
    } else {
        Ok(())
    }
}

fn doctor_management_checks(
    cli: &Cli,
    args: &DoctorArgs,
    config: &AppConfig,
    report: &mut DoctorReport,
) {
    let explicit = args.management_address.is_some();
    let cli_token = match cli.management.resolve_token() {
        Ok(token) => token,
        Err(error) => {
            report.fail("management", error.to_string());
            return;
        }
    };
    let token = cli_token.or_else(|| config.management.token.clone());
    let addresses = if let Some(address) = args.management_address {
        vec![address]
    } else {
        config.management.listen.clone()
    };
    if addresses.is_empty() {
        report.note(
            "management",
            "not configured; no runtime health/readiness endpoint was requested",
        );
        return;
    }
    let Some(token) = token else {
        report.fail(
            "management",
            "a configured/requested management check requires a token file or environment reference",
        );
        return;
    };
    for address in addresses {
        let label = format!("management {address}");
        let result = ManagementClient::new(address, token.clone())
            .and_then(|client| client.request("GET", "/api/v2/health", None, true));
        match result {
            Ok(response) => {
                let live = response.body.get("live").and_then(Value::as_bool);
                let ready = response.body.get("ready").and_then(Value::as_bool);
                let degraded = response.body.get("degraded").and_then(Value::as_bool);
                match (live, ready, degraded) {
                    (Some(true), Some(true), Some(false)) if response.status == 200 => {
                        report.pass(&label, "live, ready, and not degraded")
                    }
                    (Some(true), Some(true), Some(true)) if response.status == 200 => {
                        report.warn(&label, "live and ready, but degraded")
                    }
                    (Some(live), Some(ready), _) => {
                        let detail = format!(
                            "health returned HTTP {} (live={live}, ready={ready})",
                            response.status
                        );
                        if explicit {
                            report.fail(&label, detail);
                        } else {
                            report.warn(&label, detail);
                        }
                    }
                    _ if explicit => {
                        report.fail(&label, "health response did not match the v2 schema")
                    }
                    _ => report.warn(&label, "health response did not match the v2 schema"),
                }
            }
            Err(error) if explicit => report.fail(&label, error.to_string()),
            Err(error) => report.warn(
                &label,
                format!("configured runtime is not currently reachable: {error}"),
            ),
        }
    }
}

fn configured_probe_endpoints(config: &AppConfig, report: &mut DoctorReport) -> Vec<ProbeEndpoint> {
    let CommandConfig::Node(node) = &config.command;
    let mut probes = Vec::new();
    let mut seen = HashSet::new();
    for outbound in &node.outbounds {
        match outbound {
            OutboundLeafConfig::Mpp { id, config } => {
                for path in &config.paths {
                    let connect = path.spec.underlay == UnderlayProtocol::Tcp;
                    let skip_connect = path.spec.binding.source_ip.is_some()
                        || !path.spec.endpoint.ports().is_single();
                    push_probe(
                        &mut probes,
                        &mut seen,
                        ProbeEndpoint {
                            label: format!(
                                "MPP outbound {id} path {} ({})",
                                path.name,
                                path.spec.endpoint.authority()
                            ),
                            authority: path.spec.endpoint.authority(),
                            endpoint: path.spec.endpoint.first_endpoint(),
                            connect,
                            skip_connect,
                        },
                    );
                }
            }
            OutboundLeafConfig::Local { id, config, .. } => {
                if let Some(endpoint) = config.native_proxy_endpoint() {
                    push_probe(
                        &mut probes,
                        &mut seen,
                        ProbeEndpoint {
                            label: format!("native outbound {id} proxy"),
                            authority: endpoint.authority(),
                            endpoint: endpoint.clone(),
                            connect: true,
                            skip_connect: false,
                        },
                    );
                }
            }
        }
    }
    for upstream in &node.dns_policy.spec.upstreams {
        let Some(bootstrap) = upstream.endpoint.bootstrap() else {
            continue;
        };
        let connect = matches!(
            upstream.endpoint.transport(),
            crate::product::DnsTransport::Tcp
                | crate::product::DnsTransport::UdpTcp
                | crate::product::DnsTransport::Tls
                | crate::product::DnsTransport::Https
        );
        let routed = matches!(upstream.egress, crate::product::DnsEgressSpec::Outbound(_));
        let endpoint = Endpoint {
            host: bootstrap.ip().to_string(),
            port: bootstrap.port(),
        };
        push_probe(
            &mut probes,
            &mut seen,
            ProbeEndpoint {
                label: format!("DNS upstream {}", upstream.id),
                authority: endpoint.authority(),
                endpoint,
                connect,
                skip_connect: routed,
            },
        );
    }
    if probes.len() > DOCTOR_ENDPOINT_LIMIT {
        report.warn(
            "endpoint inventory",
            format!(
                "{} endpoints configured; probing the first {DOCTOR_ENDPOINT_LIMIT}",
                probes.len()
            ),
        );
        probes.truncate(DOCTOR_ENDPOINT_LIMIT);
    } else if probes.is_empty() {
        report.note(
            "endpoint inventory",
            "no outbound control endpoints configured",
        );
    }
    probes
}

fn push_probe(
    probes: &mut Vec<ProbeEndpoint>,
    seen: &mut HashSet<(String, bool, bool)>,
    probe: ProbeEndpoint,
) {
    let key = (probe.authority.clone(), probe.connect, probe.skip_connect);
    if seen.insert(key) {
        probes.push(probe);
    }
}

fn probe_endpoints(
    endpoints: Vec<ProbeEndpoint>,
) -> Result<Vec<EndpointProbeResult>, OperationError> {
    if endpoints.is_empty() {
        return Ok(Vec::new());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(OperationError::BuildRuntime)?;
    runtime.block_on(async move {
        let probes = endpoints.into_iter().map(probe_endpoint);
        Ok(futures::future::join_all(probes).await)
    })
}

async fn probe_endpoint(probe: ProbeEndpoint) -> EndpointProbeResult {
    let label = probe.label.clone();
    let Ok(ip) = probe.endpoint.host.parse::<IpAddr>() else {
        return EndpointProbeResult {
            label,
            outcome: EndpointProbeOutcome::Skipped(format!(
                "{} is a domain endpoint; host DNS and direct probing were skipped because configured runtime DNS and routing own resolution and connection setup",
                probe.authority
            )),
        };
    };
    let outcome = match tokio::time::timeout(DOCTOR_ENDPOINT_TIMEOUT, async {
        let addresses = [SocketAddr::new(ip, probe.endpoint.port)];
        if probe.skip_connect {
            return Ok(EndpointProbeOutcome::Skipped(format!(
                "{} maps to {}; direct probing was skipped because configured carrier selection, routing, or source binding owns the connection",
                probe.authority,
                display_addresses(&addresses)
            )));
        }
        if !probe.connect {
            return Ok(EndpointProbeOutcome::Resolved(format!(
                "{} is a literal address ({})",
                probe.endpoint.authority(),
                display_addresses(&addresses)
            )));
        }
        let mut last_error = None;
        for address in &addresses {
            match tokio::net::TcpStream::connect(address).await {
                Ok(stream) => {
                    drop(stream);
                    return Ok(EndpointProbeOutcome::Reachable(format!(
                        "{} resolved and accepted TCP at {address}",
                        probe.endpoint.authority()
                    )));
                }
                Err(error) => last_error = Some(error),
            }
        }
        Err(format!(
            "{} resolved to {}, but TCP connect failed: {}",
            probe.endpoint.authority(),
            display_addresses(&addresses),
            last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "unknown error".to_string())
        ))
    })
    .await
    {
        Ok(Ok(outcome)) => outcome,
        Ok(Err(error)) => EndpointProbeOutcome::Unavailable(error),
        Err(_) => EndpointProbeOutcome::Unavailable(format!(
            "{} probe timed out after {} ms",
            probe.endpoint.authority(),
            DOCTOR_ENDPOINT_TIMEOUT.as_millis()
        )),
    };
    EndpointProbeResult { label, outcome }
}

fn display_addresses(addresses: &[SocketAddr]) -> String {
    addresses
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(Debug)]
struct ProbeEndpoint {
    label: String,
    authority: String,
    endpoint: Endpoint,
    connect: bool,
    skip_connect: bool,
}

#[derive(Debug)]
struct EndpointProbeResult {
    label: String,
    outcome: EndpointProbeOutcome,
}

#[derive(Debug)]
enum EndpointProbeOutcome {
    Reachable(String),
    Resolved(String),
    Skipped(String),
    Unavailable(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DoctorSeverity {
    Pass,
    Warn,
    Fail,
    Note,
}

impl DoctorSeverity {
    const fn label(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
            Self::Note => "INFO",
        }
    }
}

#[derive(Debug)]
struct DoctorCheck {
    severity: DoctorSeverity,
    name: String,
    detail: String,
}

#[derive(Debug, Default)]
struct DoctorReport {
    checks: Vec<DoctorCheck>,
}

impl DoctorReport {
    fn pass(&mut self, name: impl Into<String>, detail: impl Into<String>) {
        self.push(DoctorSeverity::Pass, name, detail);
    }

    fn warn(&mut self, name: impl Into<String>, detail: impl Into<String>) {
        self.push(DoctorSeverity::Warn, name, detail);
    }

    fn fail(&mut self, name: impl Into<String>, detail: impl Into<String>) {
        self.push(DoctorSeverity::Fail, name, detail);
    }

    fn note(&mut self, name: impl Into<String>, detail: impl Into<String>) {
        self.push(DoctorSeverity::Note, name, detail);
    }

    fn push(
        &mut self,
        severity: DoctorSeverity,
        name: impl Into<String>,
        detail: impl Into<String>,
    ) {
        self.checks.push(DoctorCheck {
            severity,
            name: name.into(),
            detail: detail.into(),
        });
    }

    fn failed(&self) -> bool {
        self.checks
            .iter()
            .any(|check| check.severity == DoctorSeverity::Fail)
    }

    fn warned(&self) -> bool {
        self.checks
            .iter()
            .any(|check| check.severity == DoctorSeverity::Warn)
    }

    fn render(&self, output: &mut dyn Write) -> Result<(), OperationError> {
        for check in &self.checks {
            writeln!(
                output,
                "[{}] {}: {}",
                check.severity.label(),
                terminal_text(&check.name),
                terminal_text(&check.detail)
            )?;
        }
        let outcome = if self.failed() {
            "FAIL"
        } else if self.warned() {
            "WARN"
        } else {
            "PASS"
        };
        writeln!(output, "doctor: {outcome}")?;
        Ok(())
    }
}

fn terminal_text(value: &str) -> String {
    let mut sanitized = String::with_capacity(value.len());
    for character in value.chars() {
        if character.is_control() {
            sanitized.extend(character.escape_default());
        } else {
            sanitized.push(character);
        }
    }
    sanitized
}

fn management_client(
    cli: &Cli,
    requested_address: Option<SocketAddr>,
) -> Result<ManagementClient, OperationError> {
    let cli_token = cli.management.resolve_token()?;
    let must_load_config =
        cli.config_file.is_some() && (requested_address.is_none() || cli_token.is_none());
    let config = if must_load_config {
        Some(load_operation_config(&canonical_config_path(cli))?)
    } else {
        None
    };
    let address = requested_address
        .or_else(|| {
            config
                .as_ref()
                .and_then(|config| config.management.listen.first().copied())
        })
        .unwrap_or_else(|| {
            DEFAULT_MANAGEMENT_ADDRESS
                .parse()
                .expect("static management address")
        });
    let token = cli_token
        .or_else(|| config.and_then(|config| config.management.token))
        .ok_or(OperationError::ManagementTokenRequired)?;
    ManagementClient::new(address, token)
}

fn canonical_config_path(cli: &Cli) -> PathBuf {
    cli.config_file
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH))
}

fn load_operation_config(path: &Path) -> Result<AppConfig, OperationError> {
    load_config_toml(path).map_err(|source| OperationError::ConfigFile {
        path: path.to_path_buf(),
        source: Box::new(source),
    })
}

fn write_json(output: &mut dyn Write, value: &Value, token: &str) -> Result<(), OperationError> {
    let mut redacted = value.clone();
    redact_json_token(&mut redacted, token);
    let rendered =
        serde_json::to_string_pretty(&redacted).map_err(OperationError::SerializeJson)?;
    output.write_all(rendered.as_bytes())?;
    output.write_all(b"\n")?;
    Ok(())
}

fn redact_json_token(value: &mut Value, token: &str) {
    if token.is_empty() {
        return;
    }
    match value {
        Value::String(value) => {
            *value = value.replace(token, "<redacted>");
        }
        Value::Array(values) => {
            for value in values {
                redact_json_token(value, token);
            }
        }
        Value::Object(values) => {
            let original = std::mem::take(values);
            for (key, mut value) in original {
                redact_json_token(&mut value, token);
                values.insert(key.replace(token, "<redacted>"), value);
            }
        }
        Value::Number(number) => {
            let rendered = number.to_string();
            if rendered.contains(token) {
                *value = Value::String(rendered.replace(token, "<redacted>"));
            }
        }
        Value::Null | Value::Bool(_) => {}
    }
}

struct ManagementClient {
    address: SocketAddr,
    token: String,
}

impl std::fmt::Debug for ManagementClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ManagementClient")
            .field("address", &self.address)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl ManagementClient {
    fn new(address: SocketAddr, token: String) -> Result<Self, OperationError> {
        ManagementConfig {
            listen: vec![address],
            token: Some(token.clone()),
            dashboard: false,
            allow_peer_diagnostics: false,
        }
        .validate()
        .map_err(|error| OperationError::ManagementConfig(error.to_string()))?;
        Ok(Self { address, token })
    }

    fn token(&self) -> &str {
        &self.token
    }

    fn get(&self, path: &str) -> Result<Value, OperationError> {
        let response = self.request("GET", path, None, false)?;
        Ok(response.body)
    }

    fn post(&self, path: &str, body: &Value) -> Result<Value, OperationError> {
        let body = serde_json::to_vec(body).map_err(OperationError::SerializeJson)?;
        let response = self.request("POST", path, Some(&body), false)?;
        Ok(response.body)
    }

    fn request(
        &self,
        method: &'static str,
        path: &str,
        body: Option<&[u8]>,
        accept_error_status: bool,
    ) -> Result<ManagementResponse, OperationError> {
        validate_management_path(path)?;
        let body = body.unwrap_or_default();
        if body.len() > MANAGEMENT_REQUEST_BODY_LIMIT {
            return Err(OperationError::ManagementRequestTooLarge);
        }
        let mut stream = TcpStream::connect_timeout(&self.address, MANAGEMENT_CONNECT_TIMEOUT)
            .map_err(|source| OperationError::ManagementIo {
                operation: "connect",
                source,
            })?;
        stream
            .set_read_timeout(Some(MANAGEMENT_IO_TIMEOUT))
            .map_err(|source| OperationError::ManagementIo {
                operation: "set read timeout",
                source,
            })?;
        stream
            .set_write_timeout(Some(MANAGEMENT_IO_TIMEOUT))
            .map_err(|source| OperationError::ManagementIo {
                operation: "set write timeout",
                source,
            })?;
        let host = if self.address.is_ipv6() {
            format!("[{}]:{}", self.address.ip(), self.address.port())
        } else {
            self.address.to_string()
        };
        let content_headers = if method == "POST" {
            format!(
                "Content-Type: application/json\r\nContent-Length: {}\r\n",
                body.len()
            )
        } else {
            String::new()
        };
        let header = format!(
            "{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {}\r\nAccept: application/json\r\n{content_headers}Connection: close\r\n\r\n",
            self.token
        );
        stream
            .write_all(header.as_bytes())
            .and_then(|_| stream.write_all(body))
            .map_err(|source| OperationError::ManagementIo {
                operation: "write request",
                source,
            })?;
        read_management_response(&mut stream, accept_error_status)
    }
}

struct ManagementResponse {
    status: u16,
    body: Value,
}

fn validate_management_path(path: &str) -> Result<(), OperationError> {
    if path.is_empty()
        || path.len() > MANAGEMENT_PATH_LIMIT
        || !path.starts_with("/api/v2/")
        || !path.is_ascii()
        || path
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b' ')
    {
        return Err(OperationError::InvalidManagementPath);
    }
    Ok(())
}

fn read_management_response(
    stream: &mut TcpStream,
    accept_error_status: bool,
) -> Result<ManagementResponse, OperationError> {
    let mut bytes = Vec::with_capacity(4096);
    let (header_end, head) = loop {
        if bytes.len() > MANAGEMENT_HEADER_LIMIT {
            return Err(OperationError::ManagementResponseHeadersTooLarge);
        }
        if let Some(header_end) = find_header_end(&bytes) {
            let head = parse_response_head(&bytes[..header_end])?;
            break (header_end, head);
        }
        let mut chunk = [0_u8; 4096];
        let count = stream
            .read(&mut chunk)
            .map_err(|source| OperationError::ManagementIo {
                operation: "read response headers",
                source,
            })?;
        if count == 0 {
            return Err(OperationError::TruncatedManagementResponse);
        }
        bytes.extend_from_slice(&chunk[..count]);
    };
    if head.content_length > MANAGEMENT_BODY_LIMIT {
        return Err(OperationError::ManagementResponseBodyTooLarge);
    }
    let expected = header_end
        .checked_add(head.content_length)
        .ok_or(OperationError::ManagementResponseBodyTooLarge)?;
    while bytes.len() < expected {
        let remaining = expected - bytes.len();
        let mut chunk = [0_u8; 8192];
        let count = stream
            .read(&mut chunk[..remaining.min(8192)])
            .map_err(|source| OperationError::ManagementIo {
                operation: "read response body",
                source,
            })?;
        if count == 0 {
            return Err(OperationError::TruncatedManagementResponse);
        }
        bytes.extend_from_slice(&chunk[..count]);
    }
    if bytes.len() != expected {
        return Err(OperationError::AmbiguousManagementResponse);
    }
    let body = serde_json::from_slice::<Value>(&bytes[header_end..])
        .map_err(|_| OperationError::InvalidManagementJson)?;
    if !(200..300).contains(&head.status) && !accept_error_status {
        return Err(OperationError::ManagementStatus(head.status));
    }
    Ok(ManagementResponse {
        status: head.status,
        body,
    })
}

struct ResponseHead {
    status: u16,
    content_length: usize,
}

fn parse_response_head(bytes: &[u8]) -> Result<ResponseHead, OperationError> {
    let mut headers = [httparse::EMPTY_HEADER; MANAGEMENT_HEADER_COUNT_LIMIT];
    let mut response = httparse::Response::new(&mut headers);
    let parsed = response
        .parse(bytes)
        .map_err(|_| OperationError::InvalidManagementResponse)?;
    if !parsed.is_complete() || response.version != Some(1) {
        return Err(OperationError::InvalidManagementResponse);
    }
    let status = response
        .code
        .ok_or(OperationError::InvalidManagementResponse)?;
    let mut content_length = None;
    let mut content_type = None;
    for header in response.headers.iter() {
        if header.name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(OperationError::AmbiguousManagementResponse);
        }
        if header.name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(OperationError::AmbiguousManagementResponse);
            }
            let value = std::str::from_utf8(header.value)
                .map_err(|_| OperationError::InvalidManagementResponse)?;
            content_length = Some(
                value
                    .parse::<usize>()
                    .map_err(|_| OperationError::InvalidManagementResponse)?,
            );
        }
        if header.name.eq_ignore_ascii_case("content-type") {
            if content_type.is_some() {
                return Err(OperationError::AmbiguousManagementResponse);
            }
            content_type = Some(
                std::str::from_utf8(header.value)
                    .map_err(|_| OperationError::InvalidManagementResponse)?,
            );
        }
    }
    let content_type = content_type.ok_or(OperationError::InvalidManagementResponse)?;
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        return Err(OperationError::InvalidManagementResponse);
    }
    Ok(ResponseHead {
        status,
        content_length: content_length.ok_or(OperationError::InvalidManagementResponse)?,
    })
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
}

#[derive(Debug)]
pub enum OperationError {
    NotOperational,
    ConfigFile {
        path: PathBuf,
        source: Box<ConfigFileError>,
    },
    CliConfig(crate::cli::CliConfigError),
    NoProductRouting,
    RoutePolicy(String),
    RouteInput(String),
    InvalidDnsRecordType,
    ManagementTokenRequired,
    ManagementConfig(String),
    InvalidManagementPath,
    ManagementRequestTooLarge,
    ManagementIo {
        operation: &'static str,
        source: std::io::Error,
    },
    ManagementResponseHeadersTooLarge,
    ManagementResponseBodyTooLarge,
    TruncatedManagementResponse,
    AmbiguousManagementResponse,
    InvalidManagementResponse,
    InvalidManagementJson,
    ManagementStatus(u16),
    SerializeJson(serde_json::Error),
    Output(std::io::Error),
    BuildRuntime(std::io::Error),
    DoctorFailed,
}

impl From<crate::cli::CliConfigError> for OperationError {
    fn from(value: crate::cli::CliConfigError) -> Self {
        Self::CliConfig(value)
    }
}

impl From<std::io::Error> for OperationError {
    fn from(value: std::io::Error) -> Self {
        Self::Output(value)
    }
}

impl std::fmt::Display for OperationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotOperational => formatter.write_str("command is not an operational command"),
            Self::ConfigFile { path, source } => {
                write!(formatter, "failed to load {}: {source}", path.display())
            }
            Self::CliConfig(error) => error.fmt(formatter),
            Self::NoProductRouting => {
                formatter.write_str("configuration has no local routing policy")
            }
            Self::RoutePolicy(error) => write!(formatter, "failed to compile routing: {error}"),
            Self::RouteInput(error) => write!(formatter, "invalid route input: {error}"),
            Self::InvalidDnsRecordType => formatter.write_str(
                "DNS record type must contain 1-16 ASCII letters or digits",
            ),
            Self::ManagementTokenRequired => formatter.write_str(
                "management command requires --management-token-file, --management-token-env, or a --config containing the token reference",
            ),
            Self::ManagementConfig(error) => {
                write!(formatter, "invalid management client configuration: {error}")
            }
            Self::InvalidManagementPath => {
                formatter.write_str("invalid versioned management API path")
            }
            Self::ManagementRequestTooLarge => {
                formatter.write_str("management request body exceeds the 65536-byte limit")
            }
            Self::ManagementIo { operation, source } => {
                write!(formatter, "management API {operation} failed: {source}")
            }
            Self::ManagementResponseHeadersTooLarge => formatter.write_str(
                "management response headers exceed the 32768-byte limit",
            ),
            Self::ManagementResponseBodyTooLarge => formatter.write_str(
                "management response body exceeds the 4194304-byte limit",
            ),
            Self::TruncatedManagementResponse => {
                formatter.write_str("management API returned a truncated response")
            }
            Self::AmbiguousManagementResponse => {
                formatter.write_str("management API returned an ambiguous HTTP response")
            }
            Self::InvalidManagementResponse => {
                formatter.write_str("management API returned an invalid HTTP/1.1 JSON response")
            }
            Self::InvalidManagementJson => {
                formatter.write_str("management API returned invalid JSON")
            }
            Self::ManagementStatus(status) => {
                write!(formatter, "management API returned HTTP {status}")
            }
            Self::SerializeJson(error) => {
                write!(formatter, "failed to serialize operation output: {error}")
            }
            Self::Output(error) => write!(formatter, "failed to write operation output: {error}"),
            Self::BuildRuntime(error) => {
                write!(formatter, "failed to build doctor probe runtime: {error}")
            }
            Self::DoctorFailed => formatter.write_str("doctor found failed checks"),
        }
    }
}

impl std::error::Error for OperationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConfigFile { source, .. } => Some(source.as_ref()),
            Self::CliConfig(error) => Some(error),
            Self::ManagementIo { source, .. }
            | Self::Output(source)
            | Self::BuildRuntime(source) => Some(source),
            Self::SerializeJson(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests_operations.rs"]
mod tests;

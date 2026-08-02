use super::*;
use crate::platform::{BypassReason, BypassReasons};
use std::collections::VecDeque;

const RESOLVED_STUB_STATUS: &str = "Global\n  resolv.conf mode: stub\n";

enum ScriptResult {
    Output(CommandOutput),
    Io(io::ErrorKind),
}

struct ExpectedCall {
    program: &'static str,
    args: Vec<String>,
    result: ScriptResult,
}

impl ExpectedCall {
    fn success(program: &'static str, args: &[&str], stdout: &str) -> Self {
        Self {
            program,
            args: strings(args),
            result: ScriptResult::Output(CommandOutput::success(stdout.as_bytes().to_vec())),
        }
    }

    fn failure(program: &'static str, args: &[&str], code: i32, stderr: &str) -> Self {
        Self {
            program,
            args: strings(args),
            result: ScriptResult::Output(CommandOutput::failure(code, stderr.as_bytes().to_vec())),
        }
    }

    fn io_error(program: &'static str, args: &[&str], kind: io::ErrorKind) -> Self {
        Self {
            program,
            args: strings(args),
            result: ScriptResult::Io(kind),
        }
    }
}

#[derive(Default)]
struct ScriptedRunner {
    expected: VecDeque<ExpectedCall>,
    seen: Vec<(String, Vec<String>)>,
}

impl ScriptedRunner {
    fn new(expected: Vec<ExpectedCall>) -> Self {
        Self {
            expected: expected.into(),
            seen: Vec::new(),
        }
    }

    fn assert_done(&self) {
        assert!(
            self.expected.is_empty(),
            "{} scripted command(s) were not executed",
            self.expected.len()
        );
    }
}

impl CommandRunner for ScriptedRunner {
    fn run(&mut self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
        let expected = self
            .expected
            .pop_front()
            .unwrap_or_else(|| panic!("unexpected command: {}", render_command(program, args)));
        assert_eq!(program, expected.program);
        assert_eq!(args, expected.args.as_slice());
        assert!(
            !args.iter().any(|arg| arg == "replace" || arg == "flush"),
            "backend must never issue broad or replacing mutations"
        );
        self.seen.push((program.to_string(), args.to_vec()));
        match expected.result {
            ScriptResult::Output(output) => Ok(output),
            ScriptResult::Io(kind) => Err(io::Error::new(kind, "scripted I/O error")),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct FakeDevice(u32);

#[derive(Default)]
struct FakeTunFactory {
    calls: Vec<(String, u16)>,
    error: Option<io::ErrorKind>,
    next_device: u32,
}

impl TunDeviceFactory for FakeTunFactory {
    type Device = FakeDevice;

    fn create(&mut self, interface: &LinuxInterfaceName, mtu: u16) -> io::Result<Self::Device> {
        self.calls.push((interface.as_str().to_string(), mtu));
        if let Some(kind) = self.error.take() {
            return Err(io::Error::new(kind, "scripted TUN error"));
        }
        let device = FakeDevice(self.next_device);
        self.next_device += 1;
        Ok(device)
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn interface(value: &str) -> LinuxInterfaceName {
    LinuxInterfaceName::parse(value).unwrap()
}

fn network(value: &str) -> IpNet {
    value.parse().unwrap()
}

fn address(value: &str) -> IpAddr {
    value.parse().unwrap()
}

fn backend(calls: Vec<ExpectedCall>) -> LinuxHostNetworkBackend<ScriptedRunner, FakeTunFactory> {
    LinuxHostNetworkBackend::new(ScriptedRunner::new(calls), FakeTunFactory::default())
}

fn backend_owning(
    calls: Vec<ExpectedCall>,
) -> LinuxHostNetworkBackend<ScriptedRunner, FakeTunFactory> {
    let mut backend = backend(calls);
    backend.owned_tun = Some((interface("mptun0"), 42));
    backend
}

fn backend_with_native_rule(
    calls: Vec<ExpectedCall>,
    family: AddressFamily,
) -> LinuxHostNetworkBackend<ScriptedRunner, FakeTunFactory> {
    let mut backend = backend(calls);
    backend
        .active_native_rules
        .insert(family, (LinuxSocketMark::new(0x4d50_5455).unwrap(), 9_999));
    backend
}

#[test]
fn parses_best_defaults_and_canonical_direct_networks() {
    let snapshot = parse_ip_route_snapshot(
        AddressFamily::Ipv4,
        br#"[
            {"dst":"default","gateway":"192.0.2.1","dev":"eth0","protocol":"static",
             "prefsrc":"192.0.2.10","metric":200,"flags":[],"future_field":true},
            {"dst":"default","gateway":"198.51.100.1","dev":"eth1","protocol":"dhcp",
             "prefsrc":"198.51.100.10","metric":50,"flags":[]},
            {"dst":"10.1.2.99/24","dev":"eth1","protocol":"kernel",
             "prefsrc":"10.1.2.3","metric":7,"flags":[]},
            {"dst":"172.17.0.0/16","dev":"docker0","protocol":"kernel",
             "prefsrc":"172.17.0.1","flags":["linkdown"]},
            {"type":"blackhole","dst":"203.0.113.0/24","protocol":"static","flags":[]}
        ]"#,
    )
    .unwrap();

    let default = snapshot.default_route.unwrap();
    assert_eq!(default.interface(), &interface("eth1"));
    assert_eq!(default.gateway(), Some(address("198.51.100.1")));
    assert_eq!(default.preferred_source(), Some(address("198.51.100.10")));
    assert_eq!(default.metric(), 50);
    assert_eq!(snapshot.local_networks.len(), 1);
    assert_eq!(snapshot.local_networks[0].prefix(), network("10.1.2.0/24"));
    assert_eq!(
        snapshot.local_networks[0].route().interface(),
        &interface("eth1")
    );
}

#[test]
fn route_parser_accepts_numeric_kernel_protocol_and_host_prefix() {
    let snapshot = parse_ip_route_snapshot(
        AddressFamily::Ipv6,
        br#"[{"dst":"2001:db8::1234","dev":"eth0","protocol":2,
              "prefsrc":"2001:db8::1234","flags":[]}]"#,
    )
    .unwrap();
    assert!(snapshot.default_route.is_none());
    assert_eq!(
        snapshot.local_networks[0].prefix(),
        network("2001:db8::1234/128")
    );
}

#[test]
fn route_parser_accepts_link_scope_and_preserves_onlink_gateway() {
    let snapshot = parse_ip_route_snapshot(
        AddressFamily::Ipv4,
        br#"[
            {"dst":"default","gateway":"198.51.100.1","dev":"eth0",
             "protocol":"static","metric":10,"flags":["onlink"]},
            {"dst":"10.20.0.0/16","dev":"eth0","protocol":"static",
             "scope":"link","flags":[]}
        ]"#,
    )
    .unwrap();
    let default = snapshot.default_route.unwrap();
    assert!(default.onlink());
    assert_eq!(snapshot.local_networks.len(), 1);
    assert_eq!(snapshot.local_networks[0].prefix(), network("10.20.0.0/16"));
    assert_eq!(
        bypass_route_args("add", 51_820, network("203.0.113.7/32"), &default),
        strings(&[
            "-4",
            "route",
            "add",
            "203.0.113.7/32",
            "table",
            "51820",
            "proto",
            "242",
            "via",
            "198.51.100.1",
            "dev",
            "eth0",
            "onlink",
            "metric",
            "10",
        ])
    );
}

#[test]
fn route_parser_skips_unscoped_link_local_networks() {
    let snapshot = parse_ip_route_snapshot(
        AddressFamily::Ipv6,
        br#"[
            {"dst":"fe80::/64","dev":"eth0","protocol":"kernel","metric":256,"flags":[]},
            {"dst":"fe80::/64","dev":"wlan0","protocol":"kernel","metric":256,"flags":[]},
            {"dst":"2001:db8:1::/64","dev":"eth0","protocol":"kernel","flags":[]}
        ]"#,
    )
    .unwrap();
    assert_eq!(snapshot.local_networks.len(), 1);
    assert_eq!(
        snapshot.local_networks[0].prefix(),
        network("2001:db8:1::/64")
    );
}

#[test]
fn route_parser_rejects_malformed_ambiguous_and_indirect_defaults() {
    assert!(matches!(
        parse_ip_route_snapshot(AddressFamily::Ipv4, b"not-json"),
        Err(LinuxBackendError::RouteSnapshot(_))
    ));
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv4,
            br#"[
                {"dst":"default","gateway":"192.0.2.1","dev":"eth0","metric":10},
                {"dst":"default","gateway":"198.51.100.1","dev":"eth1","metric":10}
            ]"#
        ),
        Err(LinuxBackendError::AmbiguousDefaultRoute(
            AddressFamily::Ipv4
        ))
    ));
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv4,
            br#"[{"dst":"default","dev":"eth0","nexthops":[{"dev":"eth0"}]}]"#
        ),
        Err(LinuxBackendError::MultipathDefaultUnsupported(
            AddressFamily::Ipv4
        ))
    ));
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv6,
            br#"[{"dst":"default","dev":"eth0","nhid":17}]"#
        ),
        Err(LinuxBackendError::MultipathDefaultUnsupported(
            AddressFamily::Ipv6
        ))
    ));
}

#[test]
fn route_parser_rejects_missing_fields_and_family_mismatches() {
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv4,
            br#"[{"dev":"eth0","protocol":"kernel"}]"#
        ),
        Err(LinuxBackendError::RouteSnapshot(_))
    ));
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv4,
            br#"[{"dst":"default","gateway":"2001:db8::1","dev":"eth0"}]"#
        ),
        Err(LinuxBackendError::RouteSnapshot(_))
    ));
    assert!(matches!(
        parse_ip_route_snapshot(
            AddressFamily::Ipv6,
            br#"[{"dst":"192.0.2.0/24","dev":"eth0","protocol":"kernel"}]"#
        ),
        Err(LinuxBackendError::RouteSnapshot(_))
    ));
}

#[test]
fn environment_snapshot_uses_only_exact_read_only_commands() {
    let mut runner = ScriptedRunner::new(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "-4", "route", "show", "table", "main"],
            r#"[{"dst":"default","gateway":"192.0.2.1","dev":"eth0","metric":10},
                {"dst":"192.0.2.0/24","dev":"eth0","protocol":"kernel",
                 "prefsrc":"192.0.2.10","flags":[]}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "-6", "route", "show", "table", "main"],
            r#"[{"dst":"default","gateway":"2001:db8::1","dev":"eth0","metric":20}]"#,
        ),
    ]);

    let environment = snapshot_linux_environment(&mut runner).unwrap();
    assert_eq!(
        environment
            .default_route(AddressFamily::Ipv4)
            .unwrap()
            .gateway(),
        Some(address("192.0.2.1"))
    );
    assert_eq!(
        environment
            .default_route(AddressFamily::Ipv6)
            .unwrap()
            .gateway(),
        Some(address("2001:db8::1"))
    );
    assert_eq!(environment.local_networks().len(), 1);
    runner.assert_done();
}

#[test]
fn tun_creation_preflights_name_hands_off_once_and_deletes_exact_link() {
    let mut backend = backend(vec![
        ExpectedCall::success("ip", &["-json", "link", "show"], "[]"),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success("ip", &["link", "delete", "dev", "mptun0"], ""),
    ]);
    let operation = LinuxHostOperation::CreateTun {
        interface: interface("mptun0"),
        mtu: 1400,
    };

    let token = backend.apply(&operation).unwrap();
    assert!(matches!(token, LinuxRollbackToken::Tun { ifindex: 42, .. }));
    assert_eq!(backend.factory.calls, vec![("mptun0".to_string(), 1400)]);
    assert_eq!(backend.take_prepared_device().unwrap(), FakeDevice(0));
    assert!(matches!(
        backend.take_prepared_device(),
        Err(LinuxBackendError::PacketDeviceUnavailable)
    ));
    backend.rollback(&operation, &token).unwrap();
    assert!(backend.owned_tun.is_none());
    backend.runner().assert_done();
}

#[test]
fn tun_creation_refuses_existing_name_without_opening_device() {
    let mut backend = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "link", "show"],
        r#"[{"ifindex":7,"ifname":"mptun0"}]"#,
    )]);
    let operation = LinuxHostOperation::CreateTun {
        interface: interface("mptun0"),
        mtu: 1400,
    };
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::InterfaceAlreadyExists(_))
    ));
    assert!(backend.factory.calls.is_empty());
    backend.runner().assert_done();
}

#[test]
fn tun_creation_drops_device_when_post_create_snapshot_fails() {
    let mut backend = backend(vec![
        ExpectedCall::success("ip", &["-json", "link", "show"], "[]"),
        ExpectedCall::success("ip", &["-json", "link", "show"], "not-json"),
    ]);
    let operation = LinuxHostOperation::CreateTun {
        interface: interface("mptun0"),
        mtu: 1400,
    };
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::LinkSnapshot(_))
    ));
    assert!(backend.prepared_device.is_none());
    assert!(backend.owned_tun.is_none());
    assert_eq!(backend.factory.calls.len(), 1);
    backend.runner().assert_done();
}

#[test]
fn tun_factory_permission_failure_is_clear_and_atomic() {
    let mut backend = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "link", "show"],
        "[]",
    )]);
    backend.factory.error = Some(io::ErrorKind::PermissionDenied);
    let operation = LinuxHostOperation::CreateTun {
        interface: interface("mptun0"),
        mtu: 1400,
    };
    let error = backend.apply(&operation).unwrap_err();
    assert!(matches!(error, LinuxBackendError::TunCreate { .. }));
    assert!(error.to_string().contains("CAP_NET_ADMIN"));
    assert!(backend.prepared_device.is_none());
    backend.runner().assert_done();
}

#[test]
fn address_and_link_rollback_use_exact_identity_and_arguments() {
    let same_link = r#"[{"ifindex":42,"ifname":"mptun0"}]"#;
    let mut backend = backend_owning(vec![
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success(
            "ip",
            &["-4", "address", "add", "10.0.0.1/24", "dev", "mptun0"],
            "",
        ),
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success(
            "ip",
            &["-4", "address", "del", "10.0.0.1/24", "dev", "mptun0"],
            "",
        ),
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success("ip", &["link", "set", "dev", "mptun0", "up"], ""),
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success("ip", &["link", "set", "dev", "mptun0", "down"], ""),
    ]);
    let address_operation = LinuxHostOperation::AddAddress {
        interface: interface("mptun0"),
        address: network("10.0.0.1/24"),
    };
    let address_token = backend.apply(&address_operation).unwrap();
    backend
        .rollback(&address_operation, &address_token)
        .unwrap();
    let link_operation = LinuxHostOperation::SetLinkUp {
        interface: interface("mptun0"),
    };
    let link_token = backend.apply(&link_operation).unwrap();
    backend.rollback(&link_operation, &link_token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn rollback_never_mutates_a_reused_interface_name() {
    let mut backend = backend_owning(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &["-6", "address", "add", "fd00::1/64", "dev", "mptun0"],
            "",
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":99,"ifname":"mptun0"}]"#,
        ),
    ]);
    let operation = LinuxHostOperation::AddAddress {
        interface: interface("mptun0"),
        address: network("fd00::1/64"),
    };
    let token = backend.apply(&operation).unwrap();
    backend.rollback(&operation, &token).unwrap();
    assert_eq!(backend.runner().seen.len(), 3);
    backend.runner().assert_done();
}

#[test]
fn apply_never_mutates_a_missing_or_reused_interface_name() {
    for links in ["[]", r#"[{"ifindex":99,"ifname":"mptun0"}]"#] {
        let mut backend = backend_owning(vec![ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            links,
        )]);
        let operation = LinuxHostOperation::SetLinkUp {
            interface: interface("mptun0"),
        };
        assert!(matches!(
            backend.apply(&operation),
            Err(LinuxBackendError::InterfaceOwnershipChanged { expected: 42, .. })
        ));
        backend.runner().assert_done();
    }
}

#[test]
fn routes_use_private_table_protocol_and_exact_reverse_commands() {
    let native = LinuxNativeRoute::new(
        AddressFamily::Ipv4,
        interface("eth0"),
        Some(address("192.0.2.1")),
        Some(address("192.0.2.10")),
        20,
    )
    .unwrap();
    let bypass = LinuxHostOperation::AddBypassRoute {
        table: 51_820,
        destination: network("203.0.113.7/32"),
        native,
        reasons: BypassReasons::one(BypassReason::CarrierEndpoint),
    };
    let capture = LinuxHostOperation::AddCaptureRoute(LinuxCaptureRoute {
        table: 51_820,
        destination: network("0.0.0.0/0"),
        interface: interface("mptun0"),
    });
    let mut backend = backend_owning(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "-N", "-4", "route", "show", "table", "all"],
            "[]",
        ),
        ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], "[]"),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "route",
                "add",
                "203.0.113.7/32",
                "table",
                "51820",
                "proto",
                "242",
                "via",
                "192.0.2.1",
                "dev",
                "eth0",
                "src",
                "192.0.2.10",
                "metric",
                "20",
            ],
            "",
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "route",
                "add",
                "0.0.0.0/0",
                "table",
                "51820",
                "proto",
                "242",
                "dev",
                "mptun0",
            ],
            "",
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "route",
                "del",
                "0.0.0.0/0",
                "table",
                "51820",
                "proto",
                "242",
                "dev",
                "mptun0",
            ],
            "",
        ),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "route",
                "del",
                "203.0.113.7/32",
                "table",
                "51820",
                "proto",
                "242",
                "via",
                "192.0.2.1",
                "dev",
                "eth0",
                "src",
                "192.0.2.10",
                "metric",
                "20",
            ],
            "",
        ),
    ]);

    let bypass_token = backend.apply(&bypass).unwrap();
    let capture_token = backend.apply(&capture).unwrap();
    backend.rollback(&capture, &capture_token).unwrap();
    backend.rollback(&bypass, &bypass_token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn occupied_or_malformed_route_table_is_rejected_before_mutation() {
    let operation = LinuxHostOperation::AddBypassRoute {
        table: 51_820,
        destination: network("203.0.113.7/32"),
        native: LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth0"), None, None, 0)
            .unwrap(),
        reasons: BypassReasons::one(BypassReason::CarrierEndpoint),
    };
    let mut occupied = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "-N", "-4", "route", "show", "table", "all"],
        r#"[{"dst":"default","table":"51820"}]"#,
    )]);
    assert!(matches!(
        occupied.apply(&operation),
        Err(LinuxBackendError::RouteTableNotEmpty {
            family: AddressFamily::Ipv4,
            table: 51_820
        })
    ));
    occupied.runner().assert_done();

    let mut malformed = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "-N", "-4", "route", "show", "table", "all"],
        "not-json",
    )]);
    assert!(matches!(
        malformed.apply(&operation),
        Err(LinuxBackendError::RouteSnapshot(_))
    ));
    malformed.runner().assert_done();
}

#[test]
fn route_table_referenced_by_existing_rule_is_never_populated_during_prepare() {
    let operation = LinuxHostOperation::AddBypassRoute {
        table: 51_820,
        destination: network("203.0.113.7/32"),
        native: LinuxNativeRoute::new(AddressFamily::Ipv4, interface("eth0"), None, None, 0)
            .unwrap(),
        reasons: BypassReasons::one(BypassReason::CarrierEndpoint),
    };
    let mut backend = backend(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "-N", "-4", "route", "show", "table", "all"],
            "[]",
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "-N", "-4", "rule", "show"],
            r#"[{"priority":9000,"table":"51820"}]"#,
        ),
    ]);
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::RouteTableReferenced {
            family: AddressFamily::Ipv4,
            table: 51_820
        })
    ));
    backend.runner().assert_done();
}

#[test]
fn capture_rule_is_tagged_preflighted_verified_and_exactly_deleted() {
    let operation = LinuxHostOperation::ActivateCaptureRule {
        family: AddressFamily::Ipv4,
        table: 51_820,
        priority: 10_000,
    };
    let mut backend = backend_with_native_rule(
        vec![
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-4", "rule", "show"],
                r#"[{"priority":0,"table":"255"}]"#,
            ),
            ExpectedCall::success(
                "ip",
                &[
                    "-4", "rule", "add", "priority", "10000", "lookup", "51820", "protocol", "242",
                ],
                "",
            ),
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-4", "rule", "show"],
                r#"[{"priority":10000,"src":"all","table":"51820","protocol":"242"}]"#,
            ),
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-4", "rule", "show"],
                r#"[{"priority":10000,"src":"all","table":"51820","protocol":"242"}]"#,
            ),
            ExpectedCall::success(
                "ip",
                &[
                    "-4", "rule", "del", "priority", "10000", "lookup", "51820", "protocol", "242",
                ],
                "",
            ),
        ],
        AddressFamily::Ipv4,
    );
    let token = backend.apply(&operation).unwrap();
    backend.rollback(&operation, &token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn native_egress_rule_has_exact_mark_mask_main_table_and_reverse_command() {
    let mark = LinuxSocketMark::new(0x4d50_5455).unwrap();
    let operation = LinuxHostOperation::ActivateNativeEgressRule {
        family: AddressFamily::Ipv4,
        mark,
        priority: 9_999,
    };
    let owned_rule = r#"[{"priority":9999,"src":"all","fwmark":"0x4d505455",
             "table":"254","protocol":"242"}]"#;
    let mut backend = backend(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "-N", "-4", "rule", "show"],
            r#"[{"priority":0,"src":"all","table":"255"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "rule",
                "add",
                "priority",
                "9999",
                "fwmark",
                "0x4d505455/0xffffffff",
                "lookup",
                "254",
                "protocol",
                "242",
            ],
            "",
        ),
        ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], owned_rule),
        ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], owned_rule),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "rule",
                "del",
                "priority",
                "9999",
                "fwmark",
                "0x4d505455/0xffffffff",
                "lookup",
                "254",
                "protocol",
                "242",
            ],
            "",
        ),
    ]);
    let token = backend.apply(&operation).unwrap();
    assert_eq!(
        token,
        LinuxRollbackToken::NativeEgressRule {
            family: AddressFamily::Ipv4,
            mark,
            priority: 9_999,
        }
    );
    backend.rollback(&operation, &token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn native_egress_rule_rejects_any_existing_selector_matching_owned_mark() {
    let mark = LinuxSocketMark::new(0x4d50_5455).unwrap();
    let operation = LinuxHostOperation::ActivateNativeEgressRule {
        family: AddressFamily::Ipv6,
        mark,
        priority: 9_999,
    };
    let mut backend = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "-N", "-6", "rule", "show"],
        r#"[{"priority":7000,"src":"all","fwmark":"0x4d500000",
             "fwmask":"0xffff0000","table":"100"}]"#,
    )]);
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::SocketMarkRuleInUse {
            family: AddressFamily::Ipv6,
            mark: collision,
            priority: Some(7_000),
        }) if collision == mark
    ));
    backend.runner().assert_done();
}

#[test]
fn native_egress_rule_postcondition_requires_full_mask() {
    let mark = LinuxSocketMark::new(0x1234).unwrap();
    let operation = LinuxHostOperation::ActivateNativeEgressRule {
        family: AddressFamily::Ipv4,
        mark,
        priority: 9_999,
    };
    let mut backend = backend(vec![
        ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], "[]"),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "rule",
                "add",
                "priority",
                "9999",
                "fwmark",
                "0x1234/0xffffffff",
                "lookup",
                "254",
                "protocol",
                "242",
            ],
            "",
        ),
        ExpectedCall::success(
            "ip",
            &["-json", "-N", "-4", "rule", "show"],
            r#"[{"priority":9999,"src":"all","fwmark":"0x1234",
                 "fwmask":"0xffffff00","table":"254","protocol":"242"}]"#,
        ),
        ExpectedCall::success(
            "ip",
            &[
                "-4",
                "rule",
                "del",
                "priority",
                "9999",
                "fwmark",
                "0x1234/0xffffffff",
                "lookup",
                "254",
                "protocol",
                "242",
            ],
            "",
        ),
    ]);
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::RulePostcondition {
            family: AddressFamily::Ipv4,
            priority: 9_999,
            cleanup: None,
            ..
        })
    ));
    backend.runner().assert_done();
}

#[test]
fn absent_native_egress_rule_is_retry_safe_without_interface_state() {
    let mark = LinuxSocketMark::new(0x1234).unwrap();
    let mut backend = backend(vec![ExpectedCall::success(
        "ip",
        &["-json", "-N", "-4", "rule", "show"],
        "[]",
    )]);
    backend
        .rollback_token(&LinuxRollbackToken::NativeEgressRule {
            family: AddressFamily::Ipv4,
            mark,
            priority: 9_999,
        })
        .unwrap();
    backend.runner().assert_done();
}

#[test]
fn backend_enforces_publish_and_reverse_rule_order_without_commands() {
    let mark = LinuxSocketMark::new(0x1234).unwrap();
    let capture = LinuxHostOperation::ActivateCaptureRule {
        family: AddressFamily::Ipv4,
        table: 51_820,
        priority: 10_000,
    };
    let mut backend = backend(Vec::new());
    assert!(matches!(
        backend.apply(&capture),
        Err(LinuxBackendError::NativeEgressRuleRequired {
            family: AddressFamily::Ipv4
        })
    ));

    backend
        .active_native_rules
        .insert(AddressFamily::Ipv4, (mark, 10_000));
    assert!(matches!(
        backend.apply(&capture),
        Err(LinuxBackendError::ManagedRuleOrderInvalid {
            family: AddressFamily::Ipv4,
            native_priority: 10_000,
            capture_priority: 10_000,
        })
    ));

    backend
        .active_capture_rules
        .insert(AddressFamily::Ipv4, (51_820, 10_001));
    assert!(matches!(
        backend.rollback_token(&LinuxRollbackToken::NativeEgressRule {
            family: AddressFamily::Ipv4,
            mark,
            priority: 10_000,
        }),
        Err(LinuxBackendError::CaptureRuleStillActive {
            family: AddressFamily::Ipv4
        })
    ));
    backend.runner().assert_done();
}

#[test]
fn policy_rule_refuses_occupied_priority_and_changed_ownership() {
    let operation = LinuxHostOperation::ActivateCaptureRule {
        family: AddressFamily::Ipv6,
        table: 51_820,
        priority: 10_000,
    };
    let mut occupied = backend_with_native_rule(
        vec![ExpectedCall::success(
            "ip",
            &["-json", "-N", "-6", "rule", "show"],
            r#"[{"priority":10000,"table":"123"}]"#,
        )],
        AddressFamily::Ipv6,
    );
    assert!(matches!(
        occupied.apply(&operation),
        Err(LinuxBackendError::RulePriorityInUse {
            family: AddressFamily::Ipv6,
            priority: 10_000
        })
    ));
    occupied.runner().assert_done();

    let mut changed = backend_with_native_rule(
        vec![
            ExpectedCall::success("ip", &["-json", "-N", "-6", "rule", "show"], "[]"),
            ExpectedCall::success(
                "ip",
                &[
                    "-6", "rule", "add", "priority", "10000", "lookup", "51820", "protocol", "242",
                ],
                "",
            ),
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-6", "rule", "show"],
                r#"[{"priority":10000,"src":"all","table":"51820","protocol":"242"}]"#,
            ),
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-6", "rule", "show"],
                r#"[{"priority":10000,"src":"all","table":"51820",
                     "protocol":"242","fwmark":"0x1"}]"#,
            ),
        ],
        AddressFamily::Ipv6,
    );
    let token = changed.apply(&operation).unwrap();
    assert!(matches!(
        changed.rollback(&operation, &token),
        Err(LinuxBackendError::RuleOwnershipChanged {
            family: AddressFamily::Ipv6,
            priority: 10_000
        })
    ));
    changed.runner().assert_done();
}

#[test]
fn policy_rule_postcondition_conflict_is_exactly_cleaned() {
    let operation = LinuxHostOperation::ActivateCaptureRule {
        family: AddressFamily::Ipv4,
        table: 51_820,
        priority: 10_000,
    };
    let mut backend = backend_with_native_rule(
        vec![
            ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], "[]"),
            ExpectedCall::success(
                "ip",
                &[
                    "-4", "rule", "add", "priority", "10000", "lookup", "51820", "protocol", "242",
                ],
                "",
            ),
            ExpectedCall::success(
                "ip",
                &["-json", "-N", "-4", "rule", "show"],
                r#"[
                    {"priority":10000,"src":"all","table":"51820","protocol":"242"},
                    {"priority":10000,"src":"all","table":"123"}
                ]"#,
            ),
            ExpectedCall::success(
                "ip",
                &[
                    "-4", "rule", "del", "priority", "10000", "lookup", "51820", "protocol", "242",
                ],
                "",
            ),
        ],
        AddressFamily::Ipv4,
    );
    assert!(matches!(
        backend.apply(&operation),
        Err(LinuxBackendError::RulePostcondition {
            family: AddressFamily::Ipv4,
            priority: 10_000,
            cleanup: None,
            ..
        })
    ));
    backend.runner().assert_done();
}

#[test]
fn resolved_dns_is_published_and_reverted_only_on_original_link() {
    let operation = LinuxHostOperation::ConfigureDns {
        interface: interface("mptun0"),
        servers: vec![address("9.9.9.9"), address("2620:fe::fe")],
        route_all: true,
    };
    let mut backend = backend_owning(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "resolvectl",
            &["status", "--no-pager"],
            RESOLVED_STUB_STATUS,
        ),
        ExpectedCall::success(
            "resolvectl",
            &["dns", "mptun0", "9.9.9.9", "2620:fe::fe"],
            "",
        ),
        ExpectedCall::success("resolvectl", &["domain", "mptun0", "~."], ""),
        ExpectedCall::success("resolvectl", &["default-route", "mptun0", "yes"], ""),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success("resolvectl", &["revert", "mptun0"], ""),
    ]);
    let token = backend.apply(&operation).unwrap();
    backend.rollback(&operation, &token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn dns_route_all_false_never_installs_catch_all_domain() {
    let operation = LinuxHostOperation::ConfigureDns {
        interface: interface("mptun0"),
        servers: vec![address("9.9.9.9")],
        route_all: false,
    };
    let same_link = r#"[{"ifindex":42,"ifname":"mptun0"}]"#;
    let mut backend = backend_owning(vec![
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success(
            "resolvectl",
            &["status", "--no-pager"],
            RESOLVED_STUB_STATUS,
        ),
        ExpectedCall::success("resolvectl", &["dns", "mptun0", "9.9.9.9"], ""),
        ExpectedCall::success("resolvectl", &["default-route", "mptun0", "no"], ""),
        ExpectedCall::success("ip", &["-json", "link", "show"], same_link),
        ExpectedCall::success("resolvectl", &["revert", "mptun0"], ""),
    ]);
    let token = backend.apply(&operation).unwrap();
    backend.rollback(&operation, &token).unwrap();
    backend.runner().assert_done();
}

#[test]
fn dns_partial_publish_reverts_and_reports_revert_failure() {
    let operation = LinuxHostOperation::ConfigureDns {
        interface: interface("mptun0"),
        servers: vec![address("9.9.9.9")],
        route_all: true,
    };
    let mut backend = backend_owning(vec![
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success(
            "resolvectl",
            &["status", "--no-pager"],
            RESOLVED_STUB_STATUS,
        ),
        ExpectedCall::success("resolvectl", &["dns", "mptun0", "9.9.9.9"], ""),
        ExpectedCall::failure(
            "resolvectl",
            &["domain", "mptun0", "~."],
            1,
            "link vanished",
        ),
        ExpectedCall::failure("resolvectl", &["revert", "mptun0"], 1, "revert failed"),
        ExpectedCall::success(
            "ip",
            &["-json", "link", "show"],
            r#"[{"ifindex":42,"ifname":"mptun0"}]"#,
        ),
        ExpectedCall::success("resolvectl", &["revert", "mptun0"], ""),
        ExpectedCall::success("ip", &["-json", "-N", "-4", "rule", "show"], "[]"),
    ]);
    let error = backend.apply(&operation).unwrap_err();
    match error {
        LinuxBackendError::DnsPublish { step, revert, .. } => {
            assert_eq!(step, "domain");
            assert!(revert.is_some());
        }
        other => panic!("unexpected error: {other}"),
    }
    assert!(backend.pending_dns_revert.is_some());
    backend
        .rollback_token(&LinuxRollbackToken::CaptureRule {
            family: AddressFamily::Ipv4,
            table: 51_820,
            priority: 10_000,
        })
        .unwrap();
    assert!(backend.pending_dns_revert.is_none());
    backend.runner().assert_done();
}

#[test]
fn resolved_preflight_fails_closed_when_system_stub_is_bypassed() {
    let mut backend = backend(vec![ExpectedCall::success(
        "resolvectl",
        &["status", "--no-pager"],
        "Global\n  resolv.conf mode: foreign\n",
    )]);
    assert!(matches!(
        backend.apply(&LinuxHostOperation::CheckResolvedSupport),
        Err(LinuxBackendError::ResolvedUnavailable(error))
            if matches!(*error, LinuxBackendError::ResolvedStubInactive { .. })
    ));
    backend.runner().assert_done();
}

#[test]
fn exact_deletes_treat_only_known_absence_as_retry_success() {
    let mut runner = ScriptedRunner::new(vec![
        ExpectedCall::failure(
            "ip",
            &["-4", "address", "del", "10.0.0.1/24", "dev", "mptun0"],
            2,
            "RTNETLINK answers: Cannot assign requested address",
        ),
        ExpectedCall::failure(
            "ip",
            &[
                "-4",
                "route",
                "del",
                "0.0.0.0/0",
                "table",
                "51820",
                "proto",
                "242",
                "dev",
                "mptun0",
            ],
            2,
            "RTNETLINK answers: No such process",
        ),
    ]);
    run_exact_delete(
        &mut runner,
        "ip",
        strings(&["-4", "address", "del", "10.0.0.1/24", "dev", "mptun0"]),
    )
    .unwrap();
    run_exact_delete(
        &mut runner,
        "ip",
        strings(&[
            "-4",
            "route",
            "del",
            "0.0.0.0/0",
            "table",
            "51820",
            "proto",
            "242",
            "dev",
            "mptun0",
        ]),
    )
    .unwrap();
    runner.assert_done();
}

#[test]
fn socket_mark_api_reports_success_and_permission_without_owning_fd() {
    let mark = LinuxSocketMark::new(0x1234).unwrap();
    apply_linux_socket_mark_with(77, mark, |fd, value| {
        assert_eq!(fd, 77);
        assert_eq!(value, 0x1234);
        Ok(())
    })
    .unwrap();

    let error = apply_linux_socket_mark_with(77, mark, |_, _| {
        Err(io::Error::from_raw_os_error(libc::EPERM))
    })
    .unwrap_err();
    assert!(matches!(
        &error,
        LinuxSocketMarkApplyError::PermissionDenied {
            mark: denied_mark,
            ..
        } if *denied_mark == mark
    ));
    assert!(error.to_string().contains("CAP_NET_RAW"));
}

#[test]
fn public_socket_mark_api_preserves_invalid_raw_fd_ownership() {
    let mark = LinuxSocketMark::new(0x1234).unwrap();
    let error = apply_linux_socket_mark(-1, mark).unwrap_err();
    assert!(matches!(
        error,
        LinuxSocketMarkApplyError::System {
            mark: failed_mark,
            ..
        } if failed_mark == mark
    ));
}

#[test]
fn missing_resolved_and_ip_permissions_have_actionable_errors() {
    let mut backend = backend_owning(vec![ExpectedCall::io_error(
        "resolvectl",
        &["status", "--no-pager"],
        io::ErrorKind::NotFound,
    )]);
    let error = backend
        .apply(&LinuxHostOperation::CheckResolvedSupport)
        .unwrap_err();
    assert!(matches!(
        error,
        LinuxBackendError::ResolvedUnavailable(error)
            if matches!(*error, LinuxBackendError::ToolMissing { program: "resolvectl" })
    ));
    backend.runner().assert_done();

    let mut runner = ScriptedRunner::new(vec![ExpectedCall::failure(
        "ip",
        &["link", "set", "dev", "mptun0", "up"],
        2,
        "RTNETLINK answers: Operation not permitted",
    )]);
    let error = run_checked(
        &mut runner,
        "ip",
        strings(&["link", "set", "dev", "mptun0", "up"]),
    )
    .unwrap_err();
    assert!(matches!(
        error,
        LinuxBackendError::PermissionDenied { program: "ip", .. }
    ));
    assert!(error.to_string().contains("CAP_NET_ADMIN"));
    runner.assert_done();
}

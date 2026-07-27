use crate::platform::{
    AddressFamily, LinuxCaptureRoute, LinuxHostMutationBackend, LinuxHostOperation,
    LinuxInterfaceName, LinuxNativeNetwork, LinuxNativeRoute, LinuxNativeRouteError,
    LinuxSocketMark, LinuxVpnEnvironment,
};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::net::IpAddr;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd};
use std::process::Command;

const IP_TOOL: &str = "ip";
const RESOLVECTL_TOOL: &str = "resolvectl";
const MPTUNNEL_ROUTE_PROTOCOL: u32 = 242;
const LINUX_MAIN_ROUTE_TABLE: u32 = 254;
const LINUX_FULL_FWMASK: u32 = u32::MAX;
const MAX_DIAGNOSTIC_BYTES: usize = 4 * 1024;

/// Captured output from one direct executable invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandOutput {
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            exit_code: Some(0),
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    pub fn failure(exit_code: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self {
            exit_code: Some(exit_code),
            stdout: Vec::new(),
            stderr: stderr.into(),
        }
    }

    fn succeeded(&self) -> bool {
        self.exit_code == Some(0)
    }
}

/// Injectable, no-shell process boundary.
pub trait CommandRunner {
    fn run(&mut self, program: &str, args: &[String]) -> io::Result<CommandOutput>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemCommandRunner;

impl CommandRunner for SystemCommandRunner {
    fn run(&mut self, program: &str, args: &[String]) -> io::Result<CommandOutput> {
        let output = Command::new(program).args(args).output()?;
        Ok(CommandOutput {
            exit_code: output.status.code(),
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

/// Injectable packet-device creation boundary.
pub trait TunDeviceFactory {
    type Device;

    fn create(&mut self, interface: &LinuxInterfaceName, mtu: u16) -> io::Result<Self::Device>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemTunDeviceFactory;

impl TunDeviceFactory for SystemTunDeviceFactory {
    type Device = tun_rs::AsyncDevice;

    fn create(&mut self, interface: &LinuxInterfaceName, mtu: u16) -> io::Result<Self::Device> {
        let fd = open_exclusive_tun(interface)?;
        // SAFETY: `open_exclusive_tun` returns an owned descriptor after a
        // successful TUNSETIFF. Ownership is transferred exactly once.
        let device = unsafe { tun_rs::AsyncDevice::from_fd(fd.into_raw_fd()) }?;
        device.set_mtu(mtu)?;
        device.enabled(false)?;
        Ok(device)
    }
}

/// Applies the native-egress mark to an already-created Linux socket.
///
/// The caller must invoke this before `connect` or the first destination-bound
/// send so Linux performs route selection under the marked-native RPDB rule.
/// This function borrows the descriptor and never closes it.
pub fn apply_linux_socket_mark(
    socket_fd: RawFd,
    mark: LinuxSocketMark,
) -> Result<(), LinuxSocketMarkApplyError> {
    apply_linux_socket_mark_with(socket_fd, mark, |fd, value| {
        let value_size = std::mem::size_of_val(&value) as libc::socklen_t;
        // SAFETY: `value` is live for the call, its size is exact, and
        // setsockopt only borrows the caller-owned descriptor.
        let result = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                libc::SO_MARK,
                std::ptr::from_ref(&value).cast(),
                value_size,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    })
}

fn apply_linux_socket_mark_with(
    socket_fd: RawFd,
    mark: LinuxSocketMark,
    setter: impl FnOnce(RawFd, u32) -> io::Result<()>,
) -> Result<(), LinuxSocketMarkApplyError> {
    setter(socket_fd, mark.get()).map_err(|source| {
        if source
            .raw_os_error()
            .is_some_and(|code| code == libc::EPERM || code == libc::EACCES)
        {
            LinuxSocketMarkApplyError::PermissionDenied { mark, source }
        } else {
            LinuxSocketMarkApplyError::System { mark, source }
        }
    })
}

#[derive(Debug)]
pub enum LinuxSocketMarkApplyError {
    PermissionDenied {
        mark: LinuxSocketMark,
        source: io::Error,
    },
    System {
        mark: LinuxSocketMark,
        source: io::Error,
    },
}

impl fmt::Display for LinuxSocketMarkApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PermissionDenied { mark, source } => write!(
                formatter,
                "permission denied applying Linux SO_MARK 0x{:x}; CAP_NET_RAW or CAP_NET_ADMIN is required: {source}",
                mark.get()
            ),
            Self::System { mark, source } => write!(
                formatter,
                "failed to apply Linux SO_MARK 0x{:x}: {source}",
                mark.get()
            ),
        }
    }
}

impl std::error::Error for LinuxSocketMarkApplyError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::PermissionDenied { source, .. } | Self::System { source, .. } => Some(source),
        }
    }
}

fn open_exclusive_tun(interface: &LinuxInterfaceName) -> io::Result<OwnedFd> {
    // SAFETY: the path is a static NUL-terminated string and the returned
    // descriptor is checked before being wrapped as owned.
    let raw_fd = unsafe { libc::open(c"/dev/net/tun".as_ptr(), libc::O_RDWR | libc::O_CLOEXEC, 0) };
    if raw_fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `raw_fd` is non-negative and newly returned by `open`.
    let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
    // SAFETY: zero is a valid initial representation for `ifreq`; all fields
    // read by TUNSETIFF are initialized below.
    let mut request = unsafe { std::mem::zeroed::<libc::ifreq>() };
    for (destination, source) in request.ifr_name.iter_mut().zip(interface.as_str().bytes()) {
        *destination = source as libc::c_char;
    }
    // The exclusive flag makes a persistent same-named TUN fail with EBUSY
    // rather than attaching to and mutating foreign state.
    request.ifr_ifru.ifru_flags =
        (libc::IFF_TUN | libc::IFF_NO_PI | libc::IFF_TUN_EXCL) as libc::c_short;
    // SAFETY: `fd` refers to /dev/net/tun and `request` is a live, initialized
    // ifreq for the duration of the ioctl.
    if unsafe { libc::ioctl(raw_fd, libc::TUNSETIFF as _, &mut request) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

pub type SystemLinuxHostNetworkBackend =
    LinuxHostNetworkBackend<SystemCommandRunner, SystemTunDeviceFactory>;

/// Concrete Linux host backend with injectable command and TUN boundaries.
///
/// The backend never invokes a shell. It uses `ip` only with `add`, `del`, or
/// link-state operations and uses `resolvectl` only for the newly created TUN
/// link. It never issues `replace`, `flush`, or a broad delete.
pub struct LinuxHostNetworkBackend<Runner, Factory>
where
    Runner: CommandRunner,
    Factory: TunDeviceFactory,
{
    runner: Runner,
    factory: Factory,
    prepared_device: Option<Factory::Device>,
    owned_tun: Option<(LinuxInterfaceName, u32)>,
    pending_dns_revert: Option<(LinuxInterfaceName, u32)>,
    checked_route_tables: BTreeSet<(AddressFamily, u32)>,
    active_native_rules: BTreeMap<AddressFamily, (LinuxSocketMark, u32)>,
    active_capture_rules: BTreeMap<AddressFamily, (u32, u32)>,
    resolved_checked: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyRuleSpec {
    NativeEgress {
        family: AddressFamily,
        mark: LinuxSocketMark,
        priority: u32,
    },
    Capture {
        family: AddressFamily,
        table: u32,
        priority: u32,
    },
}

impl PolicyRuleSpec {
    const fn family(self) -> AddressFamily {
        match self {
            Self::NativeEgress { family, .. } | Self::Capture { family, .. } => family,
        }
    }

    const fn priority(self) -> u32 {
        match self {
            Self::NativeEgress { priority, .. } | Self::Capture { priority, .. } => priority,
        }
    }

    const fn table(self) -> u32 {
        match self {
            Self::NativeEgress { .. } => LINUX_MAIN_ROUTE_TABLE,
            Self::Capture { table, .. } => table,
        }
    }
}

impl<Runner, Factory> LinuxHostNetworkBackend<Runner, Factory>
where
    Runner: CommandRunner,
    Factory: TunDeviceFactory,
{
    pub fn new(runner: Runner, factory: Factory) -> Self {
        Self {
            runner,
            factory,
            prepared_device: None,
            owned_tun: None,
            pending_dns_revert: None,
            checked_route_tables: BTreeSet::new(),
            active_native_rules: BTreeMap::new(),
            active_capture_rules: BTreeMap::new(),
            resolved_checked: false,
        }
    }

    pub fn runner(&self) -> &Runner {
        &self.runner
    }

    fn check_resolved(&mut self) -> Result<(), LinuxBackendError> {
        if self.resolved_checked {
            return Ok(());
        }
        let output = run_checked(
            &mut self.runner,
            RESOLVECTL_TOOL,
            string_args(["status", "--no-pager"]),
        )
        .map_err(|error| LinuxBackendError::ResolvedUnavailable(Box::new(error)))?;
        if !resolved_stub_is_active(&output.stdout) {
            return Err(LinuxBackendError::ResolvedUnavailable(Box::new(
                LinuxBackendError::ResolvedStubInactive {
                    status: bounded_diagnostic(&output.stdout),
                },
            )));
        }
        self.resolved_checked = true;
        Ok(())
    }

    fn create_tun(
        &mut self,
        interface: &LinuxInterfaceName,
        mtu: u16,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        if self.prepared_device.is_some() || self.owned_tun.is_some() {
            return Err(LinuxBackendError::PacketDeviceAlreadyPrepared);
        }
        if find_link(&mut self.runner, interface)?.is_some() {
            return Err(LinuxBackendError::InterfaceAlreadyExists(interface.clone()));
        }
        let device =
            self.factory
                .create(interface, mtu)
                .map_err(|source| LinuxBackendError::TunCreate {
                    interface: interface.clone(),
                    source,
                })?;
        self.prepared_device = Some(device);
        let created_link = find_link(&mut self.runner, interface);
        let Some(ifindex) = (match created_link {
            Ok(link) => link.map(|link| link.ifindex),
            Err(error) => {
                // Dropping the just-created tun-rs handle closes the packet
                // device and prevents a failed link snapshot from leaking it.
                self.prepared_device.take();
                return Err(error);
            }
        }) else {
            self.prepared_device.take();
            return Err(LinuxBackendError::CreatedInterfaceMissing(
                interface.clone(),
            ));
        };
        self.owned_tun = Some((interface.clone(), ifindex));
        self.checked_route_tables.clear();
        self.active_native_rules.clear();
        self.active_capture_rules.clear();
        Ok(LinuxRollbackToken::Tun {
            interface: interface.clone(),
            ifindex,
        })
    }

    fn add_address(
        &mut self,
        interface: &LinuxInterfaceName,
        address: IpNet,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        self.retry_pending_dns_revert()?;
        let ifindex = self.owned_tun_ifindex(interface)?;
        let args = address_args("add", interface, address);
        run_checked(&mut self.runner, IP_TOOL, args)?;
        Ok(LinuxRollbackToken::Address {
            interface: interface.clone(),
            ifindex,
            address,
        })
    }

    fn set_link_up(
        &mut self,
        interface: &LinuxInterfaceName,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        let ifindex = self.owned_tun_ifindex(interface)?;
        run_checked(
            &mut self.runner,
            IP_TOOL,
            string_args(["link", "set", "dev", interface.as_str(), "up"]),
        )?;
        Ok(LinuxRollbackToken::LinkUp {
            interface: interface.clone(),
            ifindex,
        })
    }

    fn add_bypass_route(
        &mut self,
        table: u32,
        destination: IpNet,
        native: &LinuxNativeRoute,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        self.ensure_route_table_empty(AddressFamily::of(destination.addr()), table)?;
        let args = bypass_route_args("add", table, destination, native);
        run_checked(&mut self.runner, IP_TOOL, args)?;
        Ok(LinuxRollbackToken::BypassRoute {
            table,
            destination,
            native: native.clone(),
        })
    }

    fn add_capture_route(
        &mut self,
        route: &LinuxCaptureRoute,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        let ifindex = self.owned_tun_ifindex(&route.interface)?;
        self.ensure_route_table_empty(AddressFamily::of(route.destination.addr()), route.table)?;
        let args = capture_route_args("add", route);
        run_checked(&mut self.runner, IP_TOOL, args)?;
        Ok(LinuxRollbackToken::LinuxCaptureRoute {
            route: route.clone(),
            ifindex,
        })
    }

    fn ensure_route_table_empty(
        &mut self,
        family: AddressFamily,
        table: u32,
    ) -> Result<(), LinuxBackendError> {
        if self.checked_route_tables.contains(&(family, table)) {
            return Ok(());
        }
        let output = run_checked(
            &mut self.runner,
            IP_TOOL,
            vec![
                "-json".to_string(),
                "-N".to_string(),
                family_flag(family).to_string(),
                "route".to_string(),
                "show".to_string(),
                "table".to_string(),
                "all".to_string(),
            ],
        )?;
        if route_table_has_entries(&output.stdout, table)? {
            return Err(LinuxBackendError::RouteTableNotEmpty { family, table });
        }
        if read_rules(&mut self.runner, family)?
            .iter()
            .any(|rule| numeric_value_matches(&rule.table, table))
        {
            return Err(LinuxBackendError::RouteTableReferenced { family, table });
        }
        self.checked_route_tables.insert((family, table));
        Ok(())
    }

    fn activate_policy_rule(
        &mut self,
        spec: PolicyRuleSpec,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        let family = spec.family();
        let priority = spec.priority();
        let rules = read_rules(&mut self.runner, family)?;
        if rules.iter().any(|rule| rule.priority == Some(priority)) {
            return Err(LinuxBackendError::RulePriorityInUse { family, priority });
        }
        if let PolicyRuleSpec::NativeEgress { mark, .. } = spec {
            for rule in &rules {
                if rule_matches_socket_mark(rule, mark)? {
                    return Err(LinuxBackendError::SocketMarkRuleInUse {
                        family,
                        mark,
                        priority: rule.priority,
                    });
                }
            }
        }
        run_checked(&mut self.runner, IP_TOOL, policy_rule_args("add", spec))?;
        let postcondition = read_rules(&mut self.runner, family);
        let verified = postcondition.as_ref().is_ok_and(|rules| {
            let at_priority = rules
                .iter()
                .filter(|rule| rule.priority == Some(priority))
                .collect::<Vec<_>>();
            at_priority.len() == 1 && policy_rule_is_owned(at_priority[0], spec)
        });
        if !verified {
            let cleanup =
                run_exact_delete(&mut self.runner, IP_TOOL, policy_rule_args("del", spec))
                    .err()
                    .map(Box::new);
            return Err(LinuxBackendError::RulePostcondition {
                family,
                priority,
                cause: postcondition.err().map(Box::new),
                cleanup,
            });
        }
        Ok(match spec {
            PolicyRuleSpec::NativeEgress {
                family,
                mark,
                priority,
            } => LinuxRollbackToken::NativeEgressRule {
                family,
                mark,
                priority,
            },
            PolicyRuleSpec::Capture {
                family,
                table,
                priority,
            } => LinuxRollbackToken::CaptureRule {
                family,
                table,
                priority,
            },
        })
    }

    fn activate_native_egress_rule(
        &mut self,
        family: AddressFamily,
        mark: LinuxSocketMark,
        priority: u32,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        if self.active_native_rules.contains_key(&family) {
            return Err(LinuxBackendError::ManagedRuleAlreadyActive {
                family,
                kind: "native-egress",
            });
        }
        let token = self.activate_policy_rule(PolicyRuleSpec::NativeEgress {
            family,
            mark,
            priority,
        })?;
        self.active_native_rules.insert(family, (mark, priority));
        Ok(token)
    }

    fn activate_capture_rule(
        &mut self,
        family: AddressFamily,
        table: u32,
        priority: u32,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        if self.active_capture_rules.contains_key(&family) {
            return Err(LinuxBackendError::ManagedRuleAlreadyActive {
                family,
                kind: "capture",
            });
        }
        let Some((_, native_priority)) = self.active_native_rules.get(&family).copied() else {
            return Err(LinuxBackendError::NativeEgressRuleRequired { family });
        };
        if native_priority >= priority {
            return Err(LinuxBackendError::ManagedRuleOrderInvalid {
                family,
                native_priority,
                capture_priority: priority,
            });
        }
        let token = self.activate_policy_rule(PolicyRuleSpec::Capture {
            family,
            table,
            priority,
        })?;
        self.active_capture_rules.insert(family, (table, priority));
        Ok(token)
    }

    fn rollback_policy_rule(&mut self, spec: PolicyRuleSpec) -> Result<(), LinuxBackendError> {
        let family = spec.family();
        let priority = spec.priority();
        let matching = read_rules(&mut self.runner, family)?
            .into_iter()
            .filter(|rule| rule.priority == Some(priority))
            .collect::<Vec<_>>();
        if matching.is_empty() {
            return Ok(());
        }
        if matching.len() != 1 || !policy_rule_is_owned(&matching[0], spec) {
            return Err(LinuxBackendError::RuleOwnershipChanged { family, priority });
        }
        run_exact_delete(&mut self.runner, IP_TOOL, policy_rule_args("del", spec))
    }

    fn configure_dns(
        &mut self,
        interface: &LinuxInterfaceName,
        servers: &[IpAddr],
        route_all: bool,
    ) -> Result<LinuxRollbackToken, LinuxBackendError> {
        let ifindex = self.owned_tun_ifindex(interface)?;
        self.check_resolved()?;
        if servers.is_empty() {
            return Err(LinuxBackendError::DnsServerRequired);
        }
        let mut dns_args = vec!["dns".to_string(), interface.as_str().to_string()];
        dns_args.extend(servers.iter().map(ToString::to_string));
        let mut steps = vec![("dns", dns_args)];
        if route_all {
            steps.push(("domain", string_args(["domain", interface.as_str(), "~."])));
        }
        steps.push((
            "default-route",
            string_args([
                "default-route",
                interface.as_str(),
                if route_all { "yes" } else { "no" },
            ]),
        ));
        self.pending_dns_revert = Some((interface.clone(), ifindex));
        for (step, args) in steps {
            if let Err(cause) = run_checked(&mut self.runner, RESOLVECTL_TOOL, args) {
                let revert = run_checked(
                    &mut self.runner,
                    RESOLVECTL_TOOL,
                    string_args(["revert", interface.as_str()]),
                )
                .err()
                .map(Box::new);
                if revert.is_none() {
                    self.pending_dns_revert = None;
                }
                return Err(LinuxBackendError::DnsPublish {
                    step,
                    cause: Box::new(cause),
                    revert,
                });
            }
        }
        self.pending_dns_revert = None;
        Ok(LinuxRollbackToken::Dns {
            interface: interface.clone(),
            ifindex,
        })
    }

    fn owned_tun_ifindex(
        &mut self,
        interface: &LinuxInterfaceName,
    ) -> Result<u32, LinuxBackendError> {
        let expected = self
            .owned_tun
            .as_ref()
            .filter(|(owned_interface, _)| owned_interface == interface)
            .map(|(_, ifindex)| *ifindex)
            .ok_or_else(|| LinuxBackendError::InterfaceNotOwned(interface.clone()))?;
        let actual = find_link(&mut self.runner, interface)?.map(|link| link.ifindex);
        if actual != Some(expected) {
            return Err(LinuxBackendError::InterfaceOwnershipChanged {
                interface: interface.clone(),
                expected,
                actual,
            });
        }
        Ok(expected)
    }

    fn original_link_still_exists(
        &mut self,
        interface: &LinuxInterfaceName,
        ifindex: u32,
    ) -> Result<bool, LinuxBackendError> {
        Ok(find_link(&mut self.runner, interface)?.is_some_and(|link| link.ifindex == ifindex))
    }

    fn retry_pending_dns_revert(&mut self) -> Result<(), LinuxBackendError> {
        let Some((interface, ifindex)) = self.pending_dns_revert.clone() else {
            return Ok(());
        };
        if !self.original_link_still_exists(&interface, ifindex)? {
            self.pending_dns_revert = None;
            return Ok(());
        }
        run_checked(
            &mut self.runner,
            RESOLVECTL_TOOL,
            string_args(["revert", interface.as_str()]),
        )?;
        self.pending_dns_revert = None;
        Ok(())
    }

    fn rollback_token(&mut self, token: &LinuxRollbackToken) -> Result<(), LinuxBackendError> {
        self.retry_pending_dns_revert()?;
        match token {
            LinuxRollbackToken::Noop => Ok(()),
            LinuxRollbackToken::Tun { interface, ifindex } => {
                self.prepared_device.take();
                let Some(current) = find_link(&mut self.runner, interface)? else {
                    self.owned_tun = None;
                    return Ok(());
                };
                if current.ifindex != *ifindex {
                    // The transaction's link is already gone and the name was
                    // reused. Never delete the replacement.
                    self.owned_tun = None;
                    return Ok(());
                }
                run_exact_delete(
                    &mut self.runner,
                    IP_TOOL,
                    string_args(["link", "delete", "dev", interface.as_str()]),
                )?;
                self.owned_tun = None;
                Ok(())
            }
            LinuxRollbackToken::Address {
                interface,
                ifindex,
                address,
            } => {
                if !self.original_link_still_exists(interface, *ifindex)? {
                    return Ok(());
                }
                run_exact_delete(
                    &mut self.runner,
                    IP_TOOL,
                    address_args("del", interface, *address),
                )?;
                Ok(())
            }
            LinuxRollbackToken::LinkUp { interface, ifindex } => {
                if !self.original_link_still_exists(interface, *ifindex)? {
                    return Ok(());
                }
                run_checked(
                    &mut self.runner,
                    IP_TOOL,
                    string_args(["link", "set", "dev", interface.as_str(), "down"]),
                )?;
                Ok(())
            }
            LinuxRollbackToken::BypassRoute {
                table,
                destination,
                native,
            } => {
                run_exact_delete(
                    &mut self.runner,
                    IP_TOOL,
                    bypass_route_args("del", *table, *destination, native),
                )?;
                Ok(())
            }
            LinuxRollbackToken::LinuxCaptureRoute { route, ifindex } => {
                if !self.original_link_still_exists(&route.interface, *ifindex)? {
                    return Ok(());
                }
                run_exact_delete(&mut self.runner, IP_TOOL, capture_route_args("del", route))?;
                Ok(())
            }
            LinuxRollbackToken::NativeEgressRule {
                family,
                mark,
                priority,
            } => {
                if self.active_capture_rules.contains_key(family) {
                    return Err(LinuxBackendError::CaptureRuleStillActive { family: *family });
                }
                self.rollback_policy_rule(PolicyRuleSpec::NativeEgress {
                    family: *family,
                    mark: *mark,
                    priority: *priority,
                })?;
                self.active_native_rules.remove(family);
                Ok(())
            }
            LinuxRollbackToken::CaptureRule {
                family,
                table,
                priority,
            } => {
                self.rollback_policy_rule(PolicyRuleSpec::Capture {
                    family: *family,
                    table: *table,
                    priority: *priority,
                })?;
                self.active_capture_rules.remove(family);
                Ok(())
            }
            LinuxRollbackToken::Dns { interface, ifindex } => {
                if !self.original_link_still_exists(interface, *ifindex)? {
                    return Ok(());
                }
                run_checked(
                    &mut self.runner,
                    RESOLVECTL_TOOL,
                    string_args(["revert", interface.as_str()]),
                )?;
                Ok(())
            }
        }
    }
}

impl<Runner, Factory> LinuxHostMutationBackend for LinuxHostNetworkBackend<Runner, Factory>
where
    Runner: CommandRunner,
    Factory: TunDeviceFactory,
{
    type RollbackToken = LinuxRollbackToken;
    type PreparedDevice = Factory::Device;
    type Error = LinuxBackendError;

    fn apply(
        &mut self,
        operation: &LinuxHostOperation,
    ) -> Result<Self::RollbackToken, Self::Error> {
        match operation {
            LinuxHostOperation::CheckResolvedSupport => {
                self.check_resolved()?;
                Ok(LinuxRollbackToken::Noop)
            }
            LinuxHostOperation::CreateTun { interface, mtu } => self.create_tun(interface, *mtu),
            LinuxHostOperation::AddAddress { interface, address } => {
                self.add_address(interface, *address)
            }
            LinuxHostOperation::SetLinkUp { interface } => self.set_link_up(interface),
            LinuxHostOperation::AddBypassRoute {
                table,
                destination,
                native,
                reasons: _,
            } => self.add_bypass_route(*table, *destination, native),
            LinuxHostOperation::AddCaptureRoute(route) => self.add_capture_route(route),
            LinuxHostOperation::ActivateNativeEgressRule {
                family,
                mark,
                priority,
            } => self.activate_native_egress_rule(*family, *mark, *priority),
            LinuxHostOperation::ActivateCaptureRule {
                family,
                table,
                priority,
            } => self.activate_capture_rule(*family, *table, *priority),
            LinuxHostOperation::ConfigureDns {
                interface,
                servers,
                route_all,
            } => self.configure_dns(interface, servers, *route_all),
        }
    }

    fn rollback(
        &mut self,
        _operation: &LinuxHostOperation,
        token: &Self::RollbackToken,
    ) -> Result<(), Self::Error> {
        self.rollback_token(token)
    }

    fn take_prepared_device(&mut self) -> Result<Self::PreparedDevice, Self::Error> {
        self.prepared_device
            .take()
            .ok_or(LinuxBackendError::PacketDeviceUnavailable)
    }
}

/// Opaque exact rollback ownership emitted by the Linux backend.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinuxRollbackToken {
    Noop,
    Tun {
        interface: LinuxInterfaceName,
        ifindex: u32,
    },
    Address {
        interface: LinuxInterfaceName,
        ifindex: u32,
        address: IpNet,
    },
    LinkUp {
        interface: LinuxInterfaceName,
        ifindex: u32,
    },
    BypassRoute {
        table: u32,
        destination: IpNet,
        native: LinuxNativeRoute,
    },
    LinuxCaptureRoute {
        route: LinuxCaptureRoute,
        ifindex: u32,
    },
    NativeEgressRule {
        family: AddressFamily,
        mark: LinuxSocketMark,
        priority: u32,
    },
    CaptureRule {
        family: AddressFamily,
        table: u32,
        priority: u32,
    },
    Dns {
        interface: LinuxInterfaceName,
        ifindex: u32,
    },
}

fn address_args(action: &str, interface: &LinuxInterfaceName, address: IpNet) -> Vec<String> {
    vec![
        family_flag(AddressFamily::of(address.addr())).to_string(),
        "address".to_string(),
        action.to_string(),
        address.to_string(),
        "dev".to_string(),
        interface.as_str().to_string(),
    ]
}

fn bypass_route_args(
    action: &str,
    table: u32,
    destination: IpNet,
    native: &LinuxNativeRoute,
) -> Vec<String> {
    let mut args = vec![
        family_flag(AddressFamily::of(destination.addr())).to_string(),
        "route".to_string(),
        action.to_string(),
        destination.to_string(),
        "table".to_string(),
        table.to_string(),
        "proto".to_string(),
        MPTUNNEL_ROUTE_PROTOCOL.to_string(),
    ];
    if let Some(gateway) = native.gateway() {
        args.push("via".to_string());
        args.push(gateway.to_string());
    }
    args.push("dev".to_string());
    args.push(native.interface().as_str().to_string());
    if native.onlink() {
        args.push("onlink".to_string());
    }
    if let Some(source) = native.preferred_source() {
        args.push("src".to_string());
        args.push(source.to_string());
    }
    args.push("metric".to_string());
    args.push(native.metric().to_string());
    args
}

fn capture_route_args(action: &str, route: &LinuxCaptureRoute) -> Vec<String> {
    vec![
        family_flag(AddressFamily::of(route.destination.addr())).to_string(),
        "route".to_string(),
        action.to_string(),
        route.destination.to_string(),
        "table".to_string(),
        route.table.to_string(),
        "proto".to_string(),
        MPTUNNEL_ROUTE_PROTOCOL.to_string(),
        "dev".to_string(),
        route.interface.as_str().to_string(),
    ]
}

fn policy_rule_args(action: &str, spec: PolicyRuleSpec) -> Vec<String> {
    let mut args = vec![
        family_flag(spec.family()).to_string(),
        "rule".to_string(),
        action.to_string(),
        "priority".to_string(),
        spec.priority().to_string(),
    ];
    if let PolicyRuleSpec::NativeEgress { mark, .. } = spec {
        args.push("fwmark".to_string());
        args.push(format!("0x{:x}/0x{LINUX_FULL_FWMASK:x}", mark.get()));
    }
    args.extend([
        "lookup".to_string(),
        spec.table().to_string(),
        "protocol".to_string(),
        MPTUNNEL_ROUTE_PROTOCOL.to_string(),
    ]);
    args
}

fn family_flag(family: AddressFamily) -> &'static str {
    match family {
        AddressFamily::Ipv4 => "-4",
        AddressFamily::Ipv6 => "-6",
    }
}

fn string_args<const N: usize>(args: [&str; N]) -> Vec<String> {
    args.into_iter().map(str::to_string).collect()
}

fn run_checked(
    runner: &mut impl CommandRunner,
    program: &'static str,
    args: Vec<String>,
) -> Result<CommandOutput, LinuxBackendError> {
    let output = runner.run(program, &args).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            LinuxBackendError::ToolMissing { program }
        } else if source.kind() == io::ErrorKind::PermissionDenied {
            LinuxBackendError::PermissionDenied {
                program,
                args: args.clone(),
                detail: source.to_string(),
            }
        } else {
            LinuxBackendError::CommandSpawn {
                program,
                args: args.clone(),
                source,
            }
        }
    })?;
    if output.succeeded() {
        return Ok(output);
    }
    let detail = bounded_diagnostic(&output.stderr);
    if detail.contains("Operation not permitted") || detail.contains("Permission denied") {
        return Err(LinuxBackendError::PermissionDenied {
            program,
            args,
            detail,
        });
    }
    Err(LinuxBackendError::CommandFailed {
        program,
        args,
        exit_code: output.exit_code,
        detail,
    })
}

fn run_exact_delete(
    runner: &mut impl CommandRunner,
    program: &'static str,
    args: Vec<String>,
) -> Result<(), LinuxBackendError> {
    match run_checked(runner, program, args) {
        Ok(_) => Ok(()),
        Err(LinuxBackendError::CommandFailed { detail, .. })
            if exact_delete_target_is_absent(&detail) =>
        {
            Ok(())
        }
        Err(error) => Err(error),
    }
}

fn exact_delete_target_is_absent(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    [
        "cannot assign requested address",
        "no such process",
        "cannot find device",
        "no such device",
        "does not exist",
    ]
    .iter()
    .any(|marker| detail.contains(marker))
}

fn bounded_diagnostic(bytes: &[u8]) -> String {
    let end = bytes.len().min(MAX_DIAGNOSTIC_BYTES);
    String::from_utf8_lossy(&bytes[..end]).trim().to_string()
}

fn resolved_stub_is_active(bytes: &[u8]) -> bool {
    String::from_utf8_lossy(bytes).lines().any(|line| {
        line.trim().split_once(':').is_some_and(|(key, value)| {
            key.trim().eq_ignore_ascii_case("resolv.conf mode")
                && value.trim().eq_ignore_ascii_case("stub")
        })
    })
}

#[derive(Debug, Deserialize)]
struct JsonLink {
    ifindex: u32,
    ifname: String,
}

fn read_links(runner: &mut impl CommandRunner) -> Result<Vec<JsonLink>, LinuxBackendError> {
    let output = run_checked(runner, IP_TOOL, string_args(["-json", "link", "show"]))?;
    let links = serde_json::from_slice::<Vec<JsonLink>>(&output.stdout)
        .map_err(|error| LinuxBackendError::LinkSnapshot(error.to_string()))?;
    if links.iter().any(|link| link.ifindex == 0) {
        return Err(LinuxBackendError::LinkSnapshot(
            "link snapshot contains ifindex zero".to_string(),
        ));
    }
    Ok(links)
}

fn find_link(
    runner: &mut impl CommandRunner,
    interface: &LinuxInterfaceName,
) -> Result<Option<JsonLink>, LinuxBackendError> {
    let mut matching = read_links(runner)?
        .into_iter()
        .filter(|link| link.ifname == interface.as_str())
        .collect::<Vec<_>>();
    if matching.len() > 1 {
        return Err(LinuxBackendError::LinkSnapshot(format!(
            "duplicate interface name {}",
            interface
        )));
    }
    Ok(matching.pop())
}

#[derive(Debug, Deserialize)]
struct JsonTableRoute {
    #[serde(default)]
    table: Option<Value>,
}

fn route_table_has_entries(bytes: &[u8], table: u32) -> Result<bool, LinuxBackendError> {
    let routes = serde_json::from_slice::<Vec<JsonTableRoute>>(bytes)
        .map_err(|error| LinuxBackendError::RouteSnapshot(error.to_string()))?;
    Ok(routes
        .iter()
        .any(|route| numeric_value_matches(&route.table, table)))
}

#[derive(Debug, Deserialize)]
struct JsonRule {
    #[serde(default)]
    priority: Option<u32>,
    #[serde(default)]
    table: Option<Value>,
    #[serde(default)]
    protocol: Option<Value>,
    #[serde(default)]
    src: Option<String>,
    #[serde(default)]
    fwmark: Option<Value>,
    #[serde(default)]
    fwmask: Option<Value>,
    #[serde(flatten)]
    selectors: BTreeMap<String, Value>,
}

fn read_rules(
    runner: &mut impl CommandRunner,
    family: AddressFamily,
) -> Result<Vec<JsonRule>, LinuxBackendError> {
    let output = run_checked(
        runner,
        IP_TOOL,
        vec![
            "-json".to_string(),
            "-N".to_string(),
            family_flag(family).to_string(),
            "rule".to_string(),
            "show".to_string(),
        ],
    )?;
    serde_json::from_slice(&output.stdout)
        .map_err(|error| LinuxBackendError::RuleSnapshot(error.to_string()))
}

fn numeric_value_matches(value: &Option<Value>, expected: u32) -> bool {
    value.as_ref().and_then(parse_numeric_value) == Some(expected)
}

fn parse_numeric_value(value: &Value) -> Option<u32> {
    match value {
        Value::Number(value) => value.as_u64().and_then(|value| value.try_into().ok()),
        Value::String(value) => parse_u32_text(value),
        _ => None,
    }
}

fn parse_u32_text(value: &str) -> Option<u32> {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .map_or_else(
            || value.parse::<u32>().ok(),
            |hex| u32::from_str_radix(hex, 16).ok(),
        )
}

fn parse_rule_mark(rule: &JsonRule) -> Result<Option<(u32, u32)>, LinuxBackendError> {
    let Some(raw_mark) = &rule.fwmark else {
        if rule.fwmask.is_some() {
            return Err(LinuxBackendError::RuleSnapshot(
                "rule has fwmask without fwmark".to_string(),
            ));
        }
        return Ok(None);
    };
    let (mark, embedded_mask) = match raw_mark {
        Value::String(value) => {
            if let Some((mark, mask)) = value.split_once('/') {
                (
                    parse_u32_text(mark),
                    Some(parse_u32_text(mask).ok_or_else(|| {
                        LinuxBackendError::RuleSnapshot(format!("invalid fwmark mask {mask:?}"))
                    })?),
                )
            } else {
                (parse_u32_text(value), None)
            }
        }
        value => (parse_numeric_value(value), None),
    };
    let mark = mark.ok_or_else(|| {
        LinuxBackendError::RuleSnapshot(format!("invalid fwmark value {raw_mark}"))
    })?;
    let separate_mask = match &rule.fwmask {
        Some(value) => Some(parse_numeric_value(value).ok_or_else(|| {
            LinuxBackendError::RuleSnapshot(format!("invalid fwmask value {value}"))
        })?),
        None => None,
    };
    if embedded_mask.is_some() && separate_mask.is_some() && embedded_mask != separate_mask {
        return Err(LinuxBackendError::RuleSnapshot(
            "rule has conflicting embedded and separate fwmark masks".to_string(),
        ));
    }
    Ok(Some((
        mark,
        embedded_mask.or(separate_mask).unwrap_or(LINUX_FULL_FWMASK),
    )))
}

fn rule_matches_socket_mark(
    rule: &JsonRule,
    socket_mark: LinuxSocketMark,
) -> Result<bool, LinuxBackendError> {
    Ok(
        parse_rule_mark(rule)?
            .is_some_and(|(value, mask)| socket_mark.get() & mask == value & mask),
    )
}

fn policy_rule_is_owned(rule: &JsonRule, spec: PolicyRuleSpec) -> bool {
    let selector_matches = match spec {
        PolicyRuleSpec::NativeEgress { mark, .. } => matches!(
            parse_rule_mark(rule),
            Ok(Some((value, mask)))
                if value == mark.get() && mask == LINUX_FULL_FWMASK
        ),
        PolicyRuleSpec::Capture { .. } => rule.fwmark.is_none() && rule.fwmask.is_none(),
    };
    rule.priority == Some(spec.priority())
        && numeric_value_matches(&rule.table, spec.table())
        && numeric_value_matches(&rule.protocol, MPTUNNEL_ROUTE_PROTOCOL)
        && rule.src.as_deref() == Some("all")
        && selector_matches
        && rule.selectors.is_empty()
}

/// Strictly parsed result of one address-family main-table snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpRouteSnapshot {
    pub default_route: Option<LinuxNativeRoute>,
    pub local_networks: Vec<LinuxNativeNetwork>,
}

#[derive(Debug, Deserialize)]
struct JsonRoute {
    #[serde(default, rename = "type")]
    route_type: Option<String>,
    #[serde(default)]
    dst: Option<String>,
    #[serde(default)]
    gateway: Option<String>,
    #[serde(default)]
    dev: Option<String>,
    #[serde(default)]
    protocol: Option<Value>,
    #[serde(default)]
    scope: Option<Value>,
    #[serde(default)]
    prefsrc: Option<String>,
    #[serde(default)]
    metric: Option<u32>,
    #[serde(default)]
    flags: Vec<String>,
    #[serde(default)]
    nexthops: Vec<Value>,
    #[serde(default)]
    nhid: Option<Value>,
}

/// Parses `ip -json -4/-6 route show table main`.
pub fn parse_ip_route_snapshot(
    family: AddressFamily,
    bytes: &[u8],
) -> Result<IpRouteSnapshot, LinuxBackendError> {
    let records = serde_json::from_slice::<Vec<JsonRoute>>(bytes)
        .map_err(|error| LinuxBackendError::RouteSnapshot(error.to_string()))?;
    let mut defaults = Vec::<LinuxNativeRoute>::new();
    let mut local_networks = Vec::<LinuxNativeNetwork>::new();
    for record in records {
        if record
            .route_type
            .as_deref()
            .is_some_and(|route_type| route_type != "unicast")
        {
            continue;
        }
        let destination = record
            .dst
            .as_deref()
            .ok_or_else(|| LinuxBackendError::RouteSnapshot("route is missing dst".to_string()))?;
        if record.flags.iter().any(|flag| flag == "linkdown") {
            continue;
        }
        if destination == "default" {
            if !record.nexthops.is_empty() || record.nhid.is_some() {
                return Err(LinuxBackendError::MultipathDefaultUnsupported(family));
            }
            defaults.push(native_route_from_record(family, &record)?);
            continue;
        }
        let prefix = parse_route_prefix(family, destination)?;
        if network_is_link_local(prefix) {
            // Link-local destinations require an interface zone. Treating the
            // same prefix from several links as a global bypass is ambiguous.
            continue;
        }
        let directly_connected = record.gateway.is_none()
            && record.dev.as_deref() != Some("lo")
            && (scope_is_link(&record.scope)
                || (record.scope.is_none() && protocol_is_kernel(&record.protocol)));
        if directly_connected {
            let route = native_route_from_record(family, &record)?;
            local_networks
                .push(LinuxNativeNetwork::new(prefix, route).map_err(native_route_error)?);
        }
    }

    defaults.sort();
    defaults.dedup();
    let minimum_metric = defaults.iter().map(LinuxNativeRoute::metric).min();
    let mut best = defaults
        .into_iter()
        .filter(|route| Some(route.metric()) == minimum_metric)
        .collect::<Vec<_>>();
    if best.len() > 1 {
        return Err(LinuxBackendError::AmbiguousDefaultRoute(family));
    }
    local_networks.sort_by_key(LinuxNativeNetwork::prefix);
    local_networks.dedup();
    Ok(IpRouteSnapshot {
        default_route: best.pop(),
        local_networks,
    })
}

fn native_route_from_record(
    family: AddressFamily,
    record: &JsonRoute,
) -> Result<LinuxNativeRoute, LinuxBackendError> {
    let interface = record
        .dev
        .as_ref()
        .ok_or_else(|| LinuxBackendError::RouteSnapshot("route is missing dev".to_string()))
        .and_then(|interface| {
            LinuxInterfaceName::parse(interface.clone())
                .map_err(|error| LinuxBackendError::RouteSnapshot(error.to_string()))
        })?;
    let gateway = parse_optional_ip(family, record.gateway.as_deref(), "gateway")?;
    let source = parse_optional_ip(family, record.prefsrc.as_deref(), "prefsrc")?;
    LinuxNativeRoute::new(
        family,
        interface,
        gateway,
        source,
        record.metric.unwrap_or(0),
    )
    .and_then(|route| route.with_onlink(record.flags.iter().any(|flag| flag == "onlink")))
    .map_err(native_route_error)
}

fn parse_optional_ip(
    family: AddressFamily,
    value: Option<&str>,
    field: &'static str,
) -> Result<Option<IpAddr>, LinuxBackendError> {
    value
        .map(|value| {
            let address = value.parse::<IpAddr>().map_err(|_| {
                LinuxBackendError::RouteSnapshot(format!("invalid {field} address {value:?}"))
            })?;
            if AddressFamily::of(address) != family {
                return Err(LinuxBackendError::RouteSnapshot(format!(
                    "{field} address {address} has the wrong family"
                )));
            }
            Ok(address)
        })
        .transpose()
}

fn parse_route_prefix(family: AddressFamily, value: &str) -> Result<IpNet, LinuxBackendError> {
    let network = if value.contains('/') {
        value.to_string()
    } else {
        format!(
            "{value}/{}",
            match family {
                AddressFamily::Ipv4 => 32,
                AddressFamily::Ipv6 => 128,
            }
        )
    };
    let network = network.parse::<IpNet>().map_err(|_| {
        LinuxBackendError::RouteSnapshot(format!("invalid route destination {value:?}"))
    })?;
    if AddressFamily::of(network.addr()) != family {
        return Err(LinuxBackendError::RouteSnapshot(format!(
            "route destination {network} has the wrong family"
        )));
    }
    Ok(match network {
        IpNet::V4(network) => {
            IpNet::V4(Ipv4Net::new(network.network(), network.prefix_len()).expect("valid prefix"))
        }
        IpNet::V6(network) => {
            IpNet::V6(Ipv6Net::new(network.network(), network.prefix_len()).expect("valid prefix"))
        }
    })
}

fn network_is_link_local(network: IpNet) -> bool {
    match network.network() {
        IpAddr::V4(address) => address.is_link_local(),
        IpAddr::V6(address) => address.is_unicast_link_local(),
    }
}

fn protocol_is_kernel(protocol: &Option<Value>) -> bool {
    matches!(protocol, Some(Value::String(protocol)) if protocol == "kernel" || protocol == "2")
        || matches!(protocol, Some(Value::Number(protocol)) if protocol.as_u64() == Some(2))
}

fn scope_is_link(scope: &Option<Value>) -> bool {
    matches!(scope, Some(Value::String(scope)) if scope == "link" || scope == "253")
        || matches!(scope, Some(Value::Number(scope)) if scope.as_u64() == Some(253))
}

fn native_route_error(error: LinuxNativeRouteError) -> LinuxBackendError {
    LinuxBackendError::RouteSnapshot(error.to_string())
}

/// Captures both main route tables without changing host state.
pub fn snapshot_linux_environment(
    runner: &mut impl CommandRunner,
) -> Result<LinuxVpnEnvironment, LinuxBackendError> {
    let mut defaults = Vec::new();
    let mut local_networks = Vec::new();
    for family in [AddressFamily::Ipv4, AddressFamily::Ipv6] {
        let output = run_checked(
            runner,
            IP_TOOL,
            vec![
                "-json".to_string(),
                family_flag(family).to_string(),
                "route".to_string(),
                "show".to_string(),
                "table".to_string(),
                "main".to_string(),
            ],
        )?;
        let snapshot = parse_ip_route_snapshot(family, &output.stdout)?;
        if let Some(default) = snapshot.default_route {
            defaults.push(default);
        }
        local_networks.extend(snapshot.local_networks);
    }
    LinuxVpnEnvironment::new(defaults, local_networks).map_err(native_route_error)
}

#[derive(Debug)]
pub enum LinuxBackendError {
    ToolMissing {
        program: &'static str,
    },
    PermissionDenied {
        program: &'static str,
        args: Vec<String>,
        detail: String,
    },
    CommandSpawn {
        program: &'static str,
        args: Vec<String>,
        source: io::Error,
    },
    CommandFailed {
        program: &'static str,
        args: Vec<String>,
        exit_code: Option<i32>,
        detail: String,
    },
    TunCreate {
        interface: LinuxInterfaceName,
        source: io::Error,
    },
    InterfaceAlreadyExists(LinuxInterfaceName),
    InterfaceNotOwned(LinuxInterfaceName),
    InterfaceOwnershipChanged {
        interface: LinuxInterfaceName,
        expected: u32,
        actual: Option<u32>,
    },
    CreatedInterfaceMissing(LinuxInterfaceName),
    PacketDeviceAlreadyPrepared,
    PacketDeviceUnavailable,
    DnsServerRequired,
    ResolvedUnavailable(Box<LinuxBackendError>),
    ResolvedStubInactive {
        status: String,
    },
    DnsPublish {
        step: &'static str,
        cause: Box<LinuxBackendError>,
        revert: Option<Box<LinuxBackendError>>,
    },
    LinkSnapshot(String),
    RouteSnapshot(String),
    RuleSnapshot(String),
    MultipathDefaultUnsupported(AddressFamily),
    AmbiguousDefaultRoute(AddressFamily),
    RouteTableNotEmpty {
        family: AddressFamily,
        table: u32,
    },
    RouteTableReferenced {
        family: AddressFamily,
        table: u32,
    },
    RulePriorityInUse {
        family: AddressFamily,
        priority: u32,
    },
    SocketMarkRuleInUse {
        family: AddressFamily,
        mark: LinuxSocketMark,
        priority: Option<u32>,
    },
    ManagedRuleAlreadyActive {
        family: AddressFamily,
        kind: &'static str,
    },
    NativeEgressRuleRequired {
        family: AddressFamily,
    },
    ManagedRuleOrderInvalid {
        family: AddressFamily,
        native_priority: u32,
        capture_priority: u32,
    },
    CaptureRuleStillActive {
        family: AddressFamily,
    },
    RulePostcondition {
        family: AddressFamily,
        priority: u32,
        cause: Option<Box<LinuxBackendError>>,
        cleanup: Option<Box<LinuxBackendError>>,
    },
    RuleOwnershipChanged {
        family: AddressFamily,
        priority: u32,
    },
}

impl fmt::Display for LinuxBackendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ToolMissing { program } => write!(
                formatter,
                "required Linux networking tool {program:?} was not found"
            ),
            Self::PermissionDenied {
                program,
                args,
                detail,
            } => write!(
                formatter,
                "permission denied running {}; CAP_NET_ADMIN and resolved access are required ({detail})",
                render_command(program, args)
            ),
            Self::CommandSpawn {
                program,
                args,
                source,
            } => write!(
                formatter,
                "failed to start {}: {source}",
                render_command(program, args)
            ),
            Self::CommandFailed {
                program,
                args,
                exit_code,
                detail,
            } => write!(
                formatter,
                "{} failed with status {exit_code:?}: {detail}",
                render_command(program, args)
            ),
            Self::TunCreate { interface, source } => write!(
                formatter,
                "failed to create Linux TUN {interface}: {source}; CAP_NET_ADMIN and /dev/net/tun are required"
            ),
            Self::InterfaceAlreadyExists(interface) => write!(
                formatter,
                "refusing to reuse existing interface {interface}; choose an unused VPN name"
            ),
            Self::InterfaceNotOwned(interface) => write!(
                formatter,
                "refusing to mutate interface {interface}; it is not the TUN owned by this transaction"
            ),
            Self::InterfaceOwnershipChanged {
                interface,
                expected,
                actual,
            } => write!(
                formatter,
                "refusing to mutate interface {interface}; expected link index {expected}, found {actual:?}"
            ),
            Self::CreatedInterfaceMissing(interface) => write!(
                formatter,
                "tun-rs created {interface} but it was absent from the Linux link snapshot"
            ),
            Self::PacketDeviceAlreadyPrepared => {
                formatter.write_str("a Linux packet device is already prepared")
            }
            Self::PacketDeviceUnavailable => {
                formatter.write_str("the prepared Linux packet device is unavailable")
            }
            Self::DnsServerRequired => {
                formatter.write_str("resolved DNS publication requires at least one server")
            }
            Self::ResolvedUnavailable(error) => {
                write!(
                    formatter,
                    "systemd-resolved/resolvectl is unavailable: {error}"
                )
            }
            Self::ResolvedStubInactive { status } => write!(
                formatter,
                "system resolver is not using systemd-resolved stub mode; refusing DNS publication to prevent bypass (status: {status})"
            ),
            Self::DnsPublish {
                step,
                cause,
                revert,
            } => {
                write!(
                    formatter,
                    "resolved DNS publish step {step:?} failed: {cause}"
                )?;
                if let Some(revert) = revert {
                    write!(formatter, "; per-link revert also failed: {revert}")?;
                }
                Ok(())
            }
            Self::LinkSnapshot(error) => {
                write!(formatter, "invalid `ip -json link` output: {error}")
            }
            Self::RouteSnapshot(error) => {
                write!(formatter, "invalid `ip -json route` output: {error}")
            }
            Self::RuleSnapshot(error) => {
                write!(formatter, "invalid `ip -json rule` output: {error}")
            }
            Self::MultipathDefaultUnsupported(family) => write!(
                formatter,
                "multipath native default route for {family:?} is unsupported"
            ),
            Self::AmbiguousDefaultRoute(family) => write!(
                formatter,
                "native {family:?} defaults have equal best metrics; select one before VPN activation"
            ),
            Self::RouteTableNotEmpty { family, table } => write!(
                formatter,
                "refusing to use non-empty {family:?} route table {table}"
            ),
            Self::RouteTableReferenced { family, table } => write!(
                formatter,
                "refusing to populate {family:?} route table {table}; an existing policy rule already references it"
            ),
            Self::RulePriorityInUse { family, priority } => write!(
                formatter,
                "refusing to reuse {family:?} policy-rule priority {priority}"
            ),
            Self::SocketMarkRuleInUse {
                family,
                mark,
                priority,
            } => write!(
                formatter,
                "refusing {family:?} native-egress mark 0x{:x}; existing rule {priority:?} already matches it",
                mark.get()
            ),
            Self::ManagedRuleAlreadyActive { family, kind } => {
                write!(
                    formatter,
                    "managed {family:?} {kind} rule is already active"
                )
            }
            Self::NativeEgressRuleRequired { family } => write!(
                formatter,
                "{family:?} marked native-egress rule must be active before capture"
            ),
            Self::ManagedRuleOrderInvalid {
                family,
                native_priority,
                capture_priority,
            } => write!(
                formatter,
                "{family:?} native-egress priority {native_priority} must precede capture priority {capture_priority}"
            ),
            Self::CaptureRuleStillActive { family } => write!(
                formatter,
                "refusing to remove {family:?} native-egress rule while capture is active"
            ),
            Self::RulePostcondition {
                family,
                priority,
                cause,
                cleanup,
            } => {
                write!(
                    formatter,
                    "{family:?} policy rule {priority} was not uniquely owned after creation"
                )?;
                if let Some(cause) = cause {
                    write!(formatter, ": verification failed: {cause}")?;
                }
                if let Some(cleanup) = cleanup {
                    write!(formatter, "; exact cleanup failed: {cleanup}")?;
                }
                Ok(())
            }
            Self::RuleOwnershipChanged { family, priority } => write!(
                formatter,
                "{family:?} policy rule {priority} no longer matches this transaction; it was not deleted"
            ),
        }
    }
}

impl std::error::Error for LinuxBackendError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CommandSpawn { source, .. } | Self::TunCreate { source, .. } => Some(source),
            Self::ResolvedUnavailable(error) => Some(error),
            Self::DnsPublish { cause, .. } => Some(cause),
            Self::RulePostcondition {
                cause: Some(cause), ..
            } => Some(cause),
            _ => None,
        }
    }
}

fn render_command(program: &str, args: &[String]) -> String {
    let mut rendered = program.to_string();
    for arg in args {
        rendered.push(' ');
        rendered.push_str(arg);
    }
    rendered
}

#[cfg(test)]
mod tests {
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
                result: ScriptResult::Output(CommandOutput::failure(
                    code,
                    stderr.as_bytes().to_vec(),
                )),
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

    fn backend(
        calls: Vec<ExpectedCall>,
    ) -> LinuxHostNetworkBackend<ScriptedRunner, FakeTunFactory> {
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
                        "-4", "rule", "add", "priority", "10000", "lookup", "51820", "protocol",
                        "242",
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
                        "-4", "rule", "del", "priority", "10000", "lookup", "51820", "protocol",
                        "242",
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
                        "-6", "rule", "add", "priority", "10000", "lookup", "51820", "protocol",
                        "242",
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
                        "-4", "rule", "add", "priority", "10000", "lookup", "51820", "protocol",
                        "242",
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
                        "-4", "rule", "del", "priority", "10000", "lookup", "51820", "protocol",
                        "242",
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
}

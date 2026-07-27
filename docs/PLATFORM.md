# MPTUNNEL platform lifecycle

This module keeps managed-VPN host mutation outside the protocol, scheduler,
packet, stream, and relay paths. `ManagedVpnConfig` is portable desired state.
`VpnPlatformCapabilities` and `VpnLifecycleAdapter` describe who owns each OS
lifecycle operation and whether prepare can be separated safely from traffic
publication.

`platform::PacketDeviceProvider` is the host/device boundary used by the
generic runtime. Packet-device construction, managed-device guards, target
selection, and process-managed VPN generation lifecycles remain under
`src/platform/`; `src/runtime/` only consumes the prepared providers.

| Platform | Ownership | Activation | Status |
| --- | --- | --- | --- |
| Linux | Process managed | Transactional prepare/publish | Implemented with exclusive TUN ownership, RPDB policy, exact route/DNS rollback, and `SO_MARK` native bypass |
| Android | Host owned | `VpnService.establish()` returns an already-published device | Rust contract implemented; Android host integration required |
| Windows | Process managed | Transactional prepare/publish | Built-in generation bridge: Wintun packet ownership, strict native-route snapshot, protected native sockets, IP Helper route/DNS publication, and exact reverse cleanup |
| macOS | Host owned product VPN | Network Extension settings publication | Privileged utun factory, strict route snapshot, route-socket backend, and exact two-phase helper transaction exist; the supported packet-flow/DNS product adapter still requires Network Extension |

The API reports `HostIntegrationRequired`, `AdapterRequired`, `Unsupported`,
or `TargetMismatch` precisely. It never silently downgrades managed operation
to a partially configured tunnel.

The capability report is deliberately limited to one live runtime generation:
device/address ownership, route/DNS publication, native socket bypass,
two-phase activation, and orderly transactional cleanup. It does not claim a
kill switch, crash/reboot restoration, service installation, code signing, or
Windows NRPT policy. Those are separate product/deployment gaps.

## Remaining OS integration

Android requires an application-owned `VpnService` that:

- obtains user VPN consent and owns foreground-service/revocation lifecycle;
- configures addresses, capture/exclude routes, DNS, and MTU with
  `VpnService.Builder`, calls `establish()`, and transfers the resulting file
  descriptor to the Rust packet-device host;
- calls `VpnService.protect(fd)` for every MPTUNNEL carrier, bootstrap DNS,
  local-proxy, and direct-egress socket before connect or first send; and
- closes/revokes the host session after the Rust worker has stopped.

The catch-all host entry rejects operating-system DNS before opening the packet
device or any egress socket. The OS resolver owns hidden sockets that cannot be
passed to `VpnService.protect` or the equivalent Apple callback, so an embedded
VPN must configure literal-bootstrap or outbound-backed DNS.

Android does not offer MPTUNNEL's Linux-style inert prepare phase: establishing
the device publishes capture policy. The lifecycle contract therefore requires
an already-published prepared result and rejects a false two-phase claim.
The host passes that established device through `PacketDevice::from_owned_fd`
and calls `runtime::run_with_vpn_host_providers` with TUN host mode `external`.
All public low-level runtime entry points reject process-managed TUN mode:
built-in Linux/Windows management is owned by the application generation
lifecycle, while Android/macOS hosts publish their own OS policy.

Windows now has native primitives for:

- explicit signed `wintun.dll` lookup, generation-GUID identity validation,
  adapter/session creation, MTU/address configuration, and async device handoff;
- strict pre-VPN route snapshots using Windows' complete
  route-plus-interface preference, endpoint/exclude/local-LAN bypass planning,
  exact IP Helper route ownership, per-interface DNS, two-phase publication,
  postcondition checks, and retryable reverse-order cleanup;
- one executable runtime-generation bridge that resolves carrier and proxy
  inventory before publication, binds every carrier/target/proxy/DNS socket to
  its pre-VPN native interface, publishes only after packet-worker readiness,
  unpublishes before worker stop, and then removes inert preparation.

The Windows package must keep the signed architecture-matched `wintun.dll`
beside `mptunnel.exe`, and the process needs rights to create the adapter and
change routes/DNS. GitHub MSVC lanes are authoritative for native compilation;
native clean-machine tests must still cover privilege failure, interface loss,
suspend/resume, and crash recovery. Those deployment/evidence requirements do
not cause a wired capability to be reported as `AdapterRequired`.

macOS now has a privileged-process slice for:

- utun allocation, MTU/address configuration, and async device handoff without
  implicit routes;
- strict pre-VPN route snapshots, endpoint/exclude/local-LAN bypass planning,
  exact route-socket ownership, two-phase publication, postcondition checks,
  and retryable reverse-order cleanup.

That slice intentionally rejects DNS publication. Apple’s supported custom VPN
boundary is an entitled `NEPacketTunnelProvider`, whose
`setTunnelNetworkSettings` owns addresses, included/excluded routes, DNS, MTU,
and packet flow. A first-party Network Extension host, signing/entitlements,
runtime bridge, and native lifecycle tests remain required.

macOS remains `AdapterRequired`: a restricted no-DNS privileged bridge would
not provide the daily-use product contract and would blur the supported
Network Extension ownership boundary. Android remains host-owned. The presence
of lower-level primitives never causes either platform to silently return an
unconfigured packet device.

`PacketDevice::from_owned_fd` is the explicit Android descriptor bridge. It is
not an Apple packet-flow bridge: the public `NEPacketTunnelFlow` API does not
hand ownership of a TUN descriptor to Rust. A first-party macOS host therefore
still needs a packet-flow adapter in addition to the existing lifecycle and
socket-protection seams. Until that adapter, signing, entitlements, and native
lifecycle evidence exist, the CLI reports every macOS managed-VPN capability
as `adapter-required`.

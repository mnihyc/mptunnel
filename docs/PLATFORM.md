# Platform support

Proxy operation uses the portable runtime on every release target. Managed TUN
uses native host facilities only where they improve lifecycle safety; Android
and macOS keep VPN policy in their application host.

| Platform | Owner | Activation | TUN |
| --- | --- | --- | --- |
| Linux | Process | Two-phase | Native |
| Android | Host | Published | Adapter |
| Windows | Process | Two-phase | Wintun |
| macOS | Host | Published | Adapter |

Run `mptunnel platform` for the exact current-host report. Unsupported managed
operation fails explicitly; it never starts a partially configured tunnel.

Linux owns device, route, DNS, and cleanup as one process generation. Windows
does the same through Wintun and native route/DNS APIs; the matching signed
`wintun.dll` must remain beside `mptunnel.exe`.

Android requires an application-owned `VpnService` for consent, capture policy,
DNS, protected sockets, and packet-device handoff. macOS requires a signed,
entitled Network Extension host for routes, DNS, packet flow, and protected
sockets. In both cases MPTUNNEL supplies the portable protocol/runtime boundary
but does not replace the operating-system application lifecycle.

Managed VPN does not claim a kill switch, crash/reboot restoration, service
installation, code signing, or Windows NRPT policy.

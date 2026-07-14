use std::fmt::Write as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReleaseTarget {
    pub triple: &'static str,
    pub os: &'static str,
    pub arch: &'static str,
    pub artifact_ext: &'static str,
}

pub const RELEASE_TARGETS: &[ReleaseTarget] = &[
    ReleaseTarget {
        triple: "x86_64-unknown-linux-musl",
        os: "linux",
        arch: "amd64",
        artifact_ext: "tar.gz",
    },
    ReleaseTarget {
        triple: "aarch64-unknown-linux-musl",
        os: "linux",
        arch: "aarch64",
        artifact_ext: "tar.gz",
    },
    ReleaseTarget {
        triple: "x86_64-apple-darwin",
        os: "macos",
        arch: "amd64",
        artifact_ext: "tar.gz",
    },
    ReleaseTarget {
        triple: "aarch64-apple-darwin",
        os: "macos",
        arch: "aarch64",
        artifact_ext: "tar.gz",
    },
    ReleaseTarget {
        triple: "x86_64-pc-windows-msvc",
        os: "windows",
        arch: "amd64",
        artifact_ext: "zip",
    },
    ReleaseTarget {
        triple: "aarch64-pc-windows-msvc",
        os: "windows",
        arch: "aarch64",
        artifact_ext: "zip",
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformReport {
    pub os: &'static str,
    pub arch: &'static str,
    pub tun_backend: &'static str,
    pub tun_privilege: &'static str,
    pub tun_device_status: String,
    pub service_manager: &'static str,
}

impl PlatformReport {
    pub fn current() -> Self {
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            tun_backend: tun_backend(),
            tun_privilege: tun_privilege_hint(),
            tun_device_status: tun_device_status(),
            service_manager: service_manager(),
        }
    }

    pub fn render_text(&self) -> String {
        let mut output = String::new();
        let _ = writeln!(output, "platform:");
        let _ = writeln!(output, "  os: {}", self.os);
        let _ = writeln!(output, "  arch: {}", self.arch);
        let _ = writeln!(output, "  tun_backend: {}", self.tun_backend);
        let _ = writeln!(output, "  tun_privilege: {}", self.tun_privilege);
        let _ = writeln!(output, "  tun_device_status: {}", self.tun_device_status);
        let _ = writeln!(output, "  service_manager: {}", self.service_manager);
        let _ = writeln!(output, "release_targets:");
        for target in RELEASE_TARGETS {
            let _ = writeln!(
                output,
                "  - {} ({}, {}, .{})",
                target.triple, target.os, target.arch, target.artifact_ext
            );
        }
        output
    }
}

pub fn tun_privilege_hint() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "requires CAP_NET_ADMIN or equivalent privilege to create/configure TUN"
    }
    #[cfg(target_os = "macos")]
    {
        "requires permission to create utun interfaces and configure routes/DNS"
    }
    #[cfg(target_os = "windows")]
    {
        "requires Administrator rights and the Wintun driver for TUN mode"
    }
    #[cfg(target_os = "android")]
    {
        "requires VpnService consent and a host-provided owned TUN descriptor"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "TUN privilege requirements are platform-specific"
    }
}

fn tun_backend() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "Linux /dev/net/tun via tun-rs"
    }
    #[cfg(target_os = "macos")]
    {
        "macOS utun via tun-rs"
    }
    #[cfg(target_os = "windows")]
    {
        "Windows Wintun via tun-rs"
    }
    #[cfg(target_os = "android")]
    {
        "Android VpnService descriptor via host PacketDeviceProvider"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "platform TUN via tun-rs"
    }
}

fn service_manager() -> &'static str {
    #[cfg(target_os = "linux")]
    {
        "systemd"
    }
    #[cfg(target_os = "macos")]
    {
        "launchd"
    }
    #[cfg(target_os = "windows")]
    {
        "Windows Service Control Manager"
    }
    #[cfg(target_os = "android")]
    {
        "Android VpnService host lifecycle"
    }
    #[cfg(not(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "windows",
        target_os = "android"
    )))]
    {
        "external supervisor"
    }
}

#[cfg(target_os = "linux")]
fn tun_device_status() -> String {
    let path = std::path::Path::new("/dev/net/tun");
    if !path.exists() {
        return "/dev/net/tun missing".to_string();
    }
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(_) => "/dev/net/tun present and openable".to_string(),
        Err(err) => format!("/dev/net/tun present but not openable: {err}"),
    }
}

#[cfg(target_os = "macos")]
fn tun_device_status() -> String {
    "utun devices are allocated dynamically at runtime".to_string()
}

#[cfg(target_os = "windows")]
fn tun_device_status() -> String {
    "Wintun availability is checked when the TUN device is created".to_string()
}

#[cfg(target_os = "android")]
fn tun_device_status() -> String {
    "requires a configured TUN descriptor from the embedding VpnService host".to_string()
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "windows",
    target_os = "android"
)))]
fn tun_device_status() -> String {
    "TUN device status is not checked on this platform".to_string()
}

#[cfg(test)]
#[path = "platform_test.rs"]
mod tests;

use crate::config::{
    AppConfig, ClientConfig, CommandConfig, DEFAULT_PATH_PROBE_INTERVAL_MS,
    DEFAULT_PATH_PROBE_TIMEOUT_MS, ResourceLimits, SecurityConfig, ServerConfig, SharedSecret,
};
use crate::ingress::IngressConfig;
use crate::outbound::OutboundConfig;
use crate::transport::{Endpoint, PathSpec};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(name = "mptunnel")]
#[command(about = "Encrypted multipath proxy and tunnel")]
#[command(version)]
pub struct Cli {
    #[arg(long, global = true, env = "MPTUNNEL_LOG", default_value = "info")]
    pub log_level: String,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_SECURITY",
        value_enum,
        default_value_t = SecurityArg::Encrypted
    )]
    pub security: SecurityArg,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_I_UNDERSTAND_THIS_IS_INSECURE",
        default_value_t = false
    )]
    pub i_understand_this_is_insecure: bool,

    #[arg(long, global = true, env = "MPTUNNEL_SECRET")]
    pub secret: Option<String>,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_CHECK_CONFIG",
        default_value_t = false
    )]
    pub check_config: bool,

    #[command(flatten)]
    pub resources: ResourceArgs,

    #[command(subcommand)]
    pub command: Command,
}

impl Cli {
    pub fn into_config(self) -> Result<AppConfig, CliConfigError> {
        let security = self
            .security
            .into_config(self.i_understand_this_is_insecure, self.secret)?;
        let command = match self.command {
            Command::Client(args) => CommandConfig::Client(args.into_config()?),
            Command::Server(args) => CommandConfig::Server(args.into_config()?),
        };
        let config = AppConfig {
            log_level: self.log_level,
            check_config: self.check_config,
            resources: self.resources.into_limits(),
            security,
            command,
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Debug, Args)]
pub struct ResourceArgs {
    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_FRAME_BYTES",
        default_value_t = 1_048_576
    )]
    pub max_frame_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_PAYLOAD_BYTES",
        default_value_t = 1_048_512
    )]
    pub max_payload_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_ACK_RANGES",
        default_value_t = 256
    )]
    pub max_ack_ranges: usize,

    #[arg(long, global = true, env = "MPTUNNEL_MAX_PATHS", default_value_t = 64)]
    pub max_paths: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_STREAMS",
        default_value_t = 65_536
    )]
    pub max_streams: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_STREAM_WINDOW_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    pub max_stream_window_bytes: u64,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_REPAIR_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    pub max_repair_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_REORDER_BYTES",
        default_value_t = 16 * 1024 * 1024
    )]
    pub max_reorder_bytes: usize,

    #[arg(
        long,
        global = true,
        env = "MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES",
        default_value_t = 4 * 1024 * 1024
    )]
    pub max_datagram_queue_bytes: usize,
}

impl ResourceArgs {
    fn into_limits(self) -> ResourceLimits {
        ResourceLimits {
            max_frame_bytes: self.max_frame_bytes,
            max_payload_bytes: self.max_payload_bytes,
            max_ack_ranges: self.max_ack_ranges,
            max_paths: self.max_paths,
            max_streams: self.max_streams,
            max_stream_window_bytes: self.max_stream_window_bytes,
            max_repair_bytes: self.max_repair_bytes,
            max_reorder_bytes: self.max_reorder_bytes,
            max_datagram_queue_bytes: self.max_datagram_queue_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum SecurityArg {
    Encrypted,
    PlaintextLab,
}

impl SecurityArg {
    fn into_config(
        self,
        acknowledged: bool,
        secret: Option<String>,
    ) -> Result<SecurityConfig, CliConfigError> {
        if matches!(self, Self::PlaintextLab) && !acknowledged {
            return Err(CliConfigError::PlaintextNotAcknowledged);
        }
        let secret = SharedSecret::new(
            secret
                .ok_or(CliConfigError::Security(
                    crate::config::SecurityPolicyError::MissingSecret,
                ))?
                .into_bytes(),
        )?;
        match self {
            Self::Encrypted => Ok(SecurityConfig::encrypted(secret)),
            Self::PlaintextLab => Ok(SecurityConfig::plaintext_lab(secret)),
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run the local proxy/TUN ingress side.
    Client(ClientArgs),
    /// Run the remote path listener and outbound connector side.
    Server(ServerArgs),
}

#[derive(Debug, Args)]
pub struct ClientArgs {
    #[arg(long, env = "MPTUNNEL_INGRESS", value_enum, default_value_t = IngressArg::Socks5)]
    pub ingress: IngressArg,

    #[arg(long, env = "MPTUNNEL_LISTEN")]
    pub listen: Option<SocketAddr>,

    #[arg(
        long = "path",
        env = "MPTUNNEL_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub paths: Vec<PathSpec>,

    #[arg(
        long,
        env = "MPTUNNEL_PATH_PROBE_INTERVAL_MS",
        default_value_t = DEFAULT_PATH_PROBE_INTERVAL_MS
    )]
    pub path_probe_interval_ms: u64,

    #[arg(
        long,
        env = "MPTUNNEL_PATH_PROBE_TIMEOUT_MS",
        default_value_t = DEFAULT_PATH_PROBE_TIMEOUT_MS
    )]
    pub path_probe_timeout_ms: u64,
}

impl ClientArgs {
    fn into_config(self) -> Result<ClientConfig, CliConfigError> {
        let ingress = match self.ingress {
            IngressArg::Socks5 => IngressConfig::Socks5 {
                listen: self
                    .listen
                    .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 1080))),
            },
            IngressArg::HttpConnect => IngressConfig::HttpConnect {
                listen: self
                    .listen
                    .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8080))),
            },
        };
        Ok(ClientConfig {
            ingress,
            paths: self.paths,
            path_probe_interval: Duration::from_millis(self.path_probe_interval_ms),
            path_probe_timeout: Duration::from_millis(self.path_probe_timeout_ms),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum IngressArg {
    Socks5,
    HttpConnect,
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(
        long = "bind-path",
        env = "MPTUNNEL_BIND_PATHS",
        value_delimiter = ',',
        required = true
    )]
    pub bind_paths: Vec<PathSpec>,

    #[arg(long, env = "MPTUNNEL_OUTBOUND", value_enum, default_value_t = OutboundArg::Direct)]
    pub outbound: OutboundArg,

    #[arg(long, env = "MPTUNNEL_OUTBOUND_BIND_IP")]
    pub outbound_bind_ip: Option<IpAddr>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_SOCKS5")]
    pub upstream_socks5: Option<Endpoint>,

    #[arg(long, env = "MPTUNNEL_UPSTREAM_HTTP")]
    pub upstream_http: Option<Endpoint>,
}

impl ServerArgs {
    fn into_config(self) -> Result<ServerConfig, CliConfigError> {
        let outbound = match self.outbound {
            OutboundArg::Direct => OutboundConfig::Direct,
            OutboundArg::Bind => OutboundConfig::BindSourceIp(
                self.outbound_bind_ip
                    .ok_or(CliConfigError::MissingOutboundBindIp)?,
            ),
            OutboundArg::Socks5 => OutboundConfig::Socks5 {
                proxy: self
                    .upstream_socks5
                    .ok_or(CliConfigError::MissingUpstreamSocks5)?,
            },
            OutboundArg::HttpConnect => OutboundConfig::HttpConnect {
                proxy: self
                    .upstream_http
                    .ok_or(CliConfigError::MissingUpstreamHttp)?,
            },
        };
        Ok(ServerConfig {
            bind_paths: self.bind_paths,
            outbound,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OutboundArg {
    Direct,
    Bind,
    Socks5,
    HttpConnect,
}

#[derive(Debug)]
pub enum CliConfigError {
    Config(crate::config::ConfigError),
    Security(crate::config::SecurityPolicyError),
    PlaintextNotAcknowledged,
    MissingOutboundBindIp,
    MissingUpstreamSocks5,
    MissingUpstreamHttp,
}

impl From<crate::config::ConfigError> for CliConfigError {
    fn from(value: crate::config::ConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<crate::config::SecurityPolicyError> for CliConfigError {
    fn from(value: crate::config::SecurityPolicyError) -> Self {
        Self::Security(value)
    }
}

impl std::fmt::Display for CliConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::Security(err) => write!(f, "{err}"),
            Self::PlaintextNotAcknowledged => write!(
                f,
                "plaintext lab mode requires --i-understand-this-is-insecure"
            ),
            Self::MissingOutboundBindIp => {
                write!(f, "--outbound bind requires --outbound-bind-ip")
            }
            Self::MissingUpstreamSocks5 => {
                write!(f, "--outbound socks5 requires --upstream-socks5")
            }
            Self::MissingUpstreamHttp => {
                write!(f, "--outbound http-connect requires --upstream-http")
            }
        }
    }
}

impl std::error::Error for CliConfigError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CommandConfig, TransportSecurity};
    use clap::Parser;

    #[test]
    fn client_cli_builds_default_socks_config() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--check-config",
            "--secret",
            "0123456789abcdef",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
            "--path",
            "udp://127.0.0.1:443",
        ])
        .expect("parse cli");
        let config = cli.into_config().expect("config");

        assert_eq!(config.security.transport, TransportSecurity::Encrypted);
        assert!(config.check_config);
        assert_eq!(config.resources, ResourceLimits::default());
        match config.command {
            CommandConfig::Client(client) => {
                assert_eq!(client.paths.len(), 2);
                assert!(matches!(client.ingress, IngressConfig::Socks5 { .. }));
                assert_eq!(
                    client.path_probe_interval,
                    crate::config::DEFAULT_PATH_PROBE_INTERVAL
                );
                assert_eq!(
                    client.path_probe_timeout,
                    crate::config::DEFAULT_PATH_PROBE_TIMEOUT
                );
            }
            CommandConfig::Server(_) => panic!("expected client config"),
        }
    }

    #[test]
    fn plaintext_lab_mode_requires_acknowledgement() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--security",
            "plaintext-lab",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::PlaintextNotAcknowledged)
        ));
    }

    #[test]
    fn secret_is_required() {
        let cli = Cli::try_parse_from(["mptunnel", "client", "--path", "tcp://127.0.0.1:443"])
            .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Security(
                crate::config::SecurityPolicyError::MissingSecret
            ))
        ));
    }

    #[test]
    fn server_outbound_requires_matching_parameters() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef",
            "server",
            "--bind-path",
            "tcp://0.0.0.0:443",
            "--outbound",
            "socks5",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::MissingUpstreamSocks5)
        ));
    }

    #[test]
    fn resource_limits_are_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef",
            "--max-frame-bytes",
            "128",
            "--max-payload-bytes",
            "128",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PayloadLimitExceedsFrameLimit
            ))
        ));
    }

    #[test]
    fn mux_memory_limits_are_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef",
            "--max-payload-bytes",
            "1024",
            "--max-repair-bytes",
            "512",
            "client",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::RepairLimitTooSmall
            ))
        ));
    }

    #[test]
    fn path_probe_timing_is_validated() {
        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef",
            "client",
            "--path-probe-interval-ms",
            "0",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PathProbeIntervalZero
            ))
        ));

        let cli = Cli::try_parse_from([
            "mptunnel",
            "--secret",
            "0123456789abcdef",
            "client",
            "--path-probe-timeout-ms",
            "0",
            "--path",
            "tcp://127.0.0.1:443",
        ])
        .expect("parse cli");

        assert!(matches!(
            cli.into_config(),
            Err(CliConfigError::Config(
                crate::config::ConfigError::PathProbeTimeoutZero
            ))
        ));
    }
}

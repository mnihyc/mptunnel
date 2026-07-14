use crate::cli::{Cli, CliConfigError, Command};
use crate::config::{AppConfig, ConfigFileError, DEFAULT_CONFIG_PATH};
use clap::Parser;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::Duration;

pub fn build_config(cli: Cli) -> Result<AppConfig, CliConfigError> {
    cli.into_config()
}

pub fn run_from_env() -> Result<(), AppError> {
    let args = std::env::args_os().collect::<Vec<_>>();
    if let Some(config_file) = config_file_from_args(&args)? {
        return run_config_file(config_file);
    }
    run(Cli::parse_from(args))
}

pub fn run(cli: Cli) -> Result<(), AppError> {
    if let Command::Platform(_) = &cli.command {
        print!(
            "{}",
            crate::platform::PlatformReport::current().render_text()
        );
        return Ok(());
    }
    let config = build_config(cli)?;
    run_config(config)
}

pub fn run_config_file(invocation: ConfigFileInvocation) -> Result<(), AppError> {
    let mut config =
        crate::config::load_config_toml(&invocation.path).map_err(AppError::ConfigFile)?;
    if let Some(check_config) = invocation.check_config {
        config.check_config = check_config;
    }
    run_config(config)
}

fn run_config(config: AppConfig) -> Result<(), AppError> {
    if let Some(warning) = config.security.warning() {
        eprintln!("warning: {warning}");
    }
    if config.check_config {
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_worker_threads())
        .enable_all()
        .build()
        .map_err(AppError::BuildRuntime)?;
    if config.service.service_mode {
        eprintln!("mptunnel service intent enabled; process registration remains host-owned");
    }
    if config.service.supervise {
        runtime.block_on(run_supervised(config))?;
    } else {
        runtime.block_on(crate::runtime::run(config))?;
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigFileInvocation {
    pub path: PathBuf,
    pub check_config: Option<bool>,
}

fn config_file_from_args(args: &[OsString]) -> Result<Option<ConfigFileInvocation>, AppError> {
    if args.len() == 1 {
        return Ok(Some(ConfigFileInvocation {
            path: PathBuf::from(DEFAULT_CONFIG_PATH),
            check_config: None,
        }));
    }
    let mut config_path = None;
    let mut check_config = None;
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == OsStr::new("--config") || arg == OsStr::new("-c") {
            let Some(path) = args.get(index + 1) else {
                return Err(AppError::ConfigFileArgumentMissing);
            };
            config_path = Some(PathBuf::from(path));
            index += 2;
            continue;
        }
        if arg == OsStr::new("--check-config") {
            check_config = Some(true);
            index += 1;
            continue;
        }
        if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--config="))
        {
            config_path = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--check-config="))
        {
            check_config = Some(parse_bool_flag(value).ok_or(AppError::InvalidCheckConfigFlag)?);
            index += 1;
            continue;
        }
        index += 1;
    }
    Ok(config_path.map(|path| ConfigFileInvocation { path, check_config }))
}

fn parse_bool_flag(value: &str) -> Option<bool> {
    match value {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn runtime_worker_threads() -> usize {
    std::env::var("MPTUNNEL_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|parallelism| parallelism.get())
                .unwrap_or(1)
        })
}

async fn run_supervised(config: AppConfig) -> Result<(), crate::runtime::RuntimeError> {
    let mut restarts = 0u32;
    let mut backoff = config.service.restart_backoff;
    loop {
        match crate::runtime::run(config.clone()).await {
            Ok(()) => return Ok(()),
            Err(err) => {
                if let Some(max_restarts) = config.service.max_restarts
                    && restarts >= max_restarts
                {
                    return Err(err);
                }
                restarts = restarts.saturating_add(1);
                eprintln!(
                    "warning: runtime exited: {err}; restarting attempt {restarts} in {} ms",
                    backoff.as_millis()
                );
                tokio::time::sleep(backoff).await;
                backoff = next_restart_backoff(backoff, config.service.restart_max_backoff);
            }
        }
    }
}

fn next_restart_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

#[derive(Debug)]
pub enum AppError {
    Config(CliConfigError),
    ConfigFile(ConfigFileError),
    ConfigFileArgumentMissing,
    InvalidCheckConfigFlag,
    Runtime(crate::runtime::RuntimeError),
    BuildRuntime(std::io::Error),
}

impl From<CliConfigError> for AppError {
    fn from(value: CliConfigError) -> Self {
        Self::Config(value)
    }
}

impl From<crate::runtime::RuntimeError> for AppError {
    fn from(value: crate::runtime::RuntimeError) -> Self {
        Self::Runtime(value)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::ConfigFile(err) => write!(f, "{err}"),
            Self::ConfigFileArgumentMissing => write!(f, "--config requires a file path"),
            Self::InvalidCheckConfigFlag => write!(f, "--check-config value must be true or false"),
            Self::Runtime(err) => write!(f, "{err}"),
            Self::BuildRuntime(err) => write!(f, "failed to build async runtime: {err}"),
        }
    }
}

impl std::error::Error for AppError {}

#[cfg(test)]
#[path = "app_test.rs"]
mod tests;

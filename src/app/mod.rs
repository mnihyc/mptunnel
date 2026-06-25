use crate::cli::{Cli, CliConfigError};
use crate::config::AppConfig;

pub fn build_config(cli: Cli) -> Result<AppConfig, CliConfigError> {
    cli.into_config()
}

pub fn run(cli: Cli) -> Result<(), AppError> {
    let config = build_config(cli)?;
    if let Some(warning) = config.security.warning() {
        eprintln!("warning: {warning}");
    }
    if config.check_config {
        return Ok(());
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(AppError::BuildRuntime)?;
    runtime.block_on(crate::runtime::run(config))?;
    Ok(())
}

#[derive(Debug)]
pub enum AppError {
    Config(CliConfigError),
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
            Self::Runtime(err) => write!(f, "{err}"),
            Self::BuildRuntime(err) => write!(f, "failed to build async runtime: {err}"),
        }
    }
}

impl std::error::Error for AppError {}

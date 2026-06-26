use crate::benchmarks::{BenchmarkError, BenchmarkOptions};
use crate::cli::{BenchmarkFormatArg, Cli, CliConfigError, Command};
use crate::config::AppConfig;
use std::time::Duration;

pub fn build_config(cli: Cli) -> Result<AppConfig, CliConfigError> {
    cli.into_config()
}

pub fn run(cli: Cli) -> Result<(), AppError> {
    if let Command::Platform(_) = &cli.command {
        print!(
            "{}",
            crate::platform::PlatformReport::current().render_text()
        );
        return Ok(());
    }
    if let Command::Bench(args) = &cli.command {
        let options = BenchmarkOptions::new(args.resource_sample_mib)?;
        let report = crate::benchmarks::run_benchmarks(options)?;
        match args.format {
            BenchmarkFormatArg::Text => print!("{}", report.render_text()),
            BenchmarkFormatArg::Json => println!("{}", report.render_json()?),
        }
        if args.strict && !report.passed {
            return Err(AppError::Benchmark(BenchmarkError::GateFailures(
                report.failure_count(),
            )));
        }
        return Ok(());
    }
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
    if config.service.service_mode {
        eprintln!("mptunnel service mode enabled");
    }
    if config.service.supervise {
        runtime.block_on(run_supervised(config))?;
    } else {
        runtime.block_on(crate::runtime::run(config))?;
    }
    Ok(())
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
    Runtime(crate::runtime::RuntimeError),
    BuildRuntime(std::io::Error),
    Benchmark(BenchmarkError),
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

impl From<BenchmarkError> for AppError {
    fn from(value: BenchmarkError) -> Self {
        Self::Benchmark(value)
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(err) => write!(f, "{err}"),
            Self::Runtime(err) => write!(f, "{err}"),
            Self::BuildRuntime(err) => write!(f, "failed to build async runtime: {err}"),
            Self::Benchmark(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for AppError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_backoff_doubles_until_max() {
        assert_eq!(
            next_restart_backoff(Duration::from_millis(100), Duration::from_millis(1_000)),
            Duration::from_millis(200)
        );
        assert_eq!(
            next_restart_backoff(Duration::from_millis(800), Duration::from_millis(1_000)),
            Duration::from_millis(1_000)
        );
    }
}

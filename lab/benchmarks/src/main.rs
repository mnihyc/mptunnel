mod benchmarks;

use benchmarks::{BenchmarkError, BenchmarkOptions};
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(name = "mptunnel-bench")]
#[command(about = "Developer benchmark and ablation tool for mptunnel")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run deterministic regression gates outside the release mptunnel binary.
    Gates(GatesArgs),
    /// Run deterministic scheduler ablation comparisons.
    Ablation(AblationArgs),
}

#[derive(Debug, Args)]
struct GatesArgs {
    #[arg(long, env = "MPTUNNEL_BENCH_STRICT", default_value_t = false)]
    strict: bool,

    #[arg(
        long,
        env = "MPTUNNEL_BENCH_FORMAT",
        value_enum,
        default_value_t = OutputFormat::Text
    )]
    format: OutputFormat,

    #[arg(
        long,
        env = "MPTUNNEL_BENCH_RESOURCE_SAMPLE_MIB",
        default_value_t = BenchmarkOptions::default().resource_sample_mib
    )]
    resource_sample_mib: u32,
}

#[derive(Debug, Args)]
struct AblationArgs {
    #[arg(
        long,
        env = "MPTUNNEL_BENCH_FORMAT",
        value_enum,
        default_value_t = OutputFormat::Text
    )]
    format: OutputFormat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum OutputFormat {
    Text,
    Json,
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> Result<(), BenchmarkError> {
    match cli.command {
        Command::Gates(args) => run_gates(args),
        Command::Ablation(args) => run_ablation(args),
    }
}

fn run_gates(args: GatesArgs) -> Result<(), BenchmarkError> {
    let options = BenchmarkOptions::new(args.resource_sample_mib)?;
    let report = benchmarks::run_benchmarks(options)?;
    match args.format {
        OutputFormat::Text => print!("{}", report.render_text()),
        OutputFormat::Json => println!("{}", report.render_json()?),
    }
    if args.strict && !report.passed {
        return Err(BenchmarkError::GateFailures(report.failure_count()));
    }
    Ok(())
}

fn run_ablation(args: AblationArgs) -> Result<(), BenchmarkError> {
    let report = benchmarks::run_ablation_study();
    match args.format {
        OutputFormat::Text => print!("{}", report.render_text()),
        OutputFormat::Json => println!("{}", report.render_json()?),
    }
    Ok(())
}

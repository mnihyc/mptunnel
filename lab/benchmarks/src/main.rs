mod benchmarks;
mod replay;

use benchmarks::BenchmarkError;
use clap::{Args, Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
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
    /// Run deterministic path-profile ablation comparisons.
    Ablation(AblationArgs),
    /// Replay a versioned scheduling/recovery observation trace.
    Replay(ReplayArgs),
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

#[derive(Debug, Args)]
struct ReplayArgs {
    /// Versioned JSON observation trace.
    #[arg(long)]
    input: PathBuf,
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
        Command::Replay(args) => run_replay(args),
    }
}

fn run_gates(args: GatesArgs) -> Result<(), BenchmarkError> {
    let report = benchmarks::run_benchmarks();
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

fn run_replay(args: ReplayArgs) -> Result<(), BenchmarkError> {
    let report = replay::replay_file(&args.input).map_err(BenchmarkError::Replay)?;
    println!(
        "{}",
        replay::render_json(&report).map_err(BenchmarkError::Replay)?
    );
    Ok(())
}

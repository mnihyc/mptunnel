use std::process::ExitCode;

fn main() -> ExitCode {
    match mptunnel::app::run_from_env() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

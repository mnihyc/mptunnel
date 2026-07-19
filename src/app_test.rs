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

#[test]
fn config_file_invocation_defaults_to_config_toml_without_args() {
    let args = vec![OsString::from("mptunnel")];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from(DEFAULT_CONFIG_PATH),
            check_config: None,
        })
    );
}

#[test]
fn config_file_invocation_preserves_check_config_override() {
    let args = vec![
        OsString::from("mptunnel"),
        OsString::from("--config"),
        OsString::from("edge.toml"),
        OsString::from("--check-config"),
    ];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from("edge.toml"),
            check_config: Some(true),
        })
    );
}

#[test]
fn config_file_invocation_accepts_false_check_config_override() {
    let args = vec![
        OsString::from("mptunnel"),
        OsString::from("--check-config=false"),
        OsString::from("--config=client.toml"),
    ];
    assert_eq!(
        config_file_from_args(&args).expect("args"),
        Some(ConfigFileInvocation {
            path: PathBuf::from("client.toml"),
            check_config: Some(false),
        })
    );
}

#[test]
fn config_file_version_and_help_are_cli_meta_actions() {
    for flag in ["--version", "-V", "--help", "-h"] {
        let args = vec![
            OsString::from("mptunnel"),
            OsString::from("--config"),
            OsString::from("edge.toml"),
            OsString::from(flag),
        ];
        assert!(config_file_meta_action_requested(&args), "flag={flag}");
    }

    let runtime_args = vec![
        OsString::from("mptunnel"),
        OsString::from("--config=edge.toml"),
    ];
    assert!(!config_file_meta_action_requested(&runtime_args));
}

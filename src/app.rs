use crate::cli::{Cli, CliConfigError, Command};
use crate::config::{
    AppConfig, CanonicalConfigStore, CommandConfig, ConfigFileError, ConfigStoreError,
    DEFAULT_CONFIG_PATH, OutboundLeafConfig, canonical_config_owned_paths,
};
use clap::Parser;
use clap::error::ErrorKind;
use std::ffi::{OsStr, OsString};
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

const PROCESS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_VPN_READY_TIMEOUT: Duration = Duration::from_secs(10);
const MANAGED_VPN_SHUTDOWN_ATTEMPTS: usize = 8;
const MANAGED_VPN_SHUTDOWN_RETRY_DELAY: Duration = Duration::from_millis(250);

pub fn build_config(cli: Cli) -> Result<AppConfig, CliConfigError> {
    cli.into_config()
}

pub fn run_from_env() -> Result<(), AppError> {
    let args = std::env::args_os().collect::<Vec<_>>();
    if config_file_meta_action_requested(&args) || operational_command_requested(&args) {
        return match Cli::try_parse_from(args) {
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
                ) =>
            {
                error.print().map_err(AppError::CliOutput)
            }
            Err(error) => Err(AppError::Cli(error)),
            Ok(cli) => run(cli),
        };
    }
    if let Some(config_file) = config_file_from_args(&args)? {
        return run_config_file(config_file);
    }
    run(Cli::parse_from(args))
}

pub fn report_fatal_error(error: &AppError) {
    crate::observability::process_event!(
        Error,
        "process",
        "fatal",
        "MPTUNNEL {} failed: {error}",
        env!("CARGO_PKG_VERSION")
    );
}

pub fn run(cli: Cli) -> Result<(), AppError> {
    if cli.command.is_operational() {
        let logging = cli.logging_config();
        logging.validate().map_err(CliConfigError::from)?;
        let config_path = cli
            .config_file
            .clone()
            .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
        let config_path =
            std::path::absolute(&config_path).map_err(|source| AppError::ConfigPathResolution {
                path: config_path,
                source,
            })?;
        let owned_paths = canonical_config_owned_paths(config_path)?;
        crate::observability::configure_for_owned_paths(&logging, &owned_paths)?;
    }
    if let Command::Platform(_) = &cli.command {
        print!(
            "{}",
            crate::platform::PlatformReport::current().render_text()
        );
        return Ok(());
    }
    if cli.command.is_operational() {
        let stdout = std::io::stdout();
        let mut output = stdout.lock();
        crate::operations::execute(&cli, &mut output)?;
        return Ok(());
    }
    let config = build_config(cli)?;
    run_config(config)
}

pub fn run_config_file(mut invocation: ConfigFileInvocation) -> Result<(), AppError> {
    invocation.path =
        std::path::absolute(&invocation.path).map_err(|source| AppError::ConfigPathResolution {
            path: invocation.path.clone(),
            source,
        })?;
    let OpenedConfigFileGeneration {
        config,
        config_control,
    } = open_config_file_generation(&invocation)?;
    crate::observability::validate_store_path(&config.logging, config_control.store())?;
    if config.check_config {
        return Ok(());
    }

    crate::observability::configure_for_store(&config.logging, config_control.store())?;
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "process",
        "starting",
        format_args!("MPTUNNEL {} starting", env!("CARGO_PKG_VERSION")),
    );
    log_file_configuration(&config, config_control.store());
    let runtime = build_runtime()?;
    let update_check = crate::update::spawn(&runtime);
    let shutdown = ProcessShutdown::new();
    let result = runtime.block_on(run_process_until_shutdown(
        run_config_file_generations(config, config_control, invocation, shutdown.clone()),
        shutdown,
    ));
    update_check.abort();
    let _ = runtime.block_on(update_check);
    result?;
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "process",
        "stopped",
        format_args!("MPTUNNEL stopped cleanly"),
    );
    Ok(())
}

fn run_config(config: AppConfig) -> Result<(), AppError> {
    validate_host_generation(&config)?;
    if config.check_config {
        return Ok(());
    }

    crate::observability::configure(&config.logging)?;
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "process",
        "starting",
        format_args!("MPTUNNEL {} starting", env!("CARGO_PKG_VERSION")),
    );
    log_command_line_configuration(&config);
    let runtime = build_runtime()?;
    let update_check = crate::update::spawn(&runtime);
    let shutdown = ProcessShutdown::new();
    let result = runtime.block_on(run_process_until_shutdown(
        run_standalone_generations(config, shutdown.clone()),
        shutdown,
    ));
    update_check.abort();
    let _ = runtime.block_on(update_check);
    result?;
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "process",
        "stopped",
        format_args!("MPTUNNEL stopped cleanly"),
    );
    Ok(())
}

fn log_file_configuration(config: &AppConfig, store: &CanonicalConfigStore) {
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "configuration",
        "loaded",
        format_args!(
            "Loaded {} (revision {})",
            store.path().display(),
            store.revision()
        ),
    );
    log_configuration_inventory(config);
}

fn log_command_line_configuration(config: &AppConfig) {
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "configuration",
        "loaded",
        format_args!("Loaded command-line configuration"),
    );
    log_configuration_inventory(config);
}

fn log_configuration_inventory(config: &AppConfig) {
    let CommandConfig::Node(node) = &config.command;
    let forwarding_mode = match node.forwarding_mode {
        crate::config::ForwardingMode::L4 => "L4",
        crate::config::ForwardingMode::L3 => "L3 (experimental)",
    };
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "configuration",
        "forwarding_mode",
        format_args!("Forwarding mode {forwarding_mode}"),
    );
    let route_count = node
        .product_policy
        .as_ref()
        .map_or(0, |policy| policy.routes.len());
    let inbound_count =
        node.local_ingresses.len() + node.tun_l3_ingresses.len() + node.servers.len();
    crate::observability::emit_lifecycle(
        crate::config::LogLevel::Info,
        "configuration",
        "summary",
        format_args!(
            "Configured {inbound_count} {}, {} {}, {} {}, {route_count} {}, and {} {}",
            plural_noun(inbound_count, "inbound", "inbounds"),
            node.outbounds.len(),
            plural_noun(node.outbounds.len(), "outbound", "outbounds"),
            node.gateway_balancers.len(),
            plural_noun(node.gateway_balancers.len(), "balancer", "balancers"),
            plural_noun(route_count, "route", "routes"),
            node.dns_policy.spec.plans.len(),
            plural_noun(
                node.dns_policy.spec.plans.len(),
                "DNS policy",
                "DNS policies",
            ),
        ),
    );

    for outbound in &node.outbounds {
        match outbound {
            OutboundLeafConfig::Mpp { id, config } => {
                crate::observability::emit_lifecycle(
                    crate::config::LogLevel::Info,
                    "outbound",
                    "configured",
                    format_args!(
                        "{}: MPP over {} configured {}, TCP and UDP targets",
                        id.as_str(),
                        config.paths.len(),
                        plural_noun(config.paths.len(), "path", "paths"),
                    ),
                );
                for path in &config.paths {
                    log_mpp_path(
                        "outbound",
                        id.as_str(),
                        &path.name,
                        &path.spec,
                        path.tls.shared_transport_secret_configured(),
                    );
                }
            }
            OutboundLeafConfig::Local { id, config, .. } => {
                log_native_outbound(id.as_str(), config);
            }
        }
    }
}

fn log_mpp_path(
    owner_kind: &str,
    owner: &str,
    name: &str,
    path: &crate::transport::PathSpec,
    shared_transport_secret: bool,
) {
    let transport = crate::transport::encrypted::carrier_security_description(
        path.underlay,
        shared_transport_secret,
    );
    if let Some(carriers) = path.tcp_carrier_range() {
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "path",
            "configured",
            format_args!(
                "{owner_kind} {owner}, path {name}: {transport} to {}, {} {}",
                path.endpoint.authority(),
                carriers.max(),
                plural_noun(usize::from(carriers.max()), "carrier", "carriers"),
            ),
        );
    } else {
        crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "path",
            "configured",
            format_args!(
                "{owner_kind} {owner}, path {name}: {transport} to {}",
                path.endpoint.authority(),
            ),
        );
    }
}

fn log_native_outbound(name: &str, outbound: &crate::outbound::OutboundConfig) {
    let (kind, detail, authentication) = match outbound {
        crate::outbound::OutboundConfig::Direct => ("direct", None, None),
        crate::outbound::OutboundConfig::BindSourceIp(address) => {
            ("direct", Some(address.to_string()), None)
        }
        crate::outbound::OutboundConfig::BindSourceIps { ipv4, ipv6 } => {
            let bindings = [
                ipv4.map(|address| format!("IPv4 {address}")),
                ipv6.map(|address| format!("IPv6 {address}")),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(", ");
            ("direct", Some(bindings), None)
        }
        crate::outbound::OutboundConfig::Socks5(proxy) => (
            "SOCKS5 proxy",
            Some(proxy.endpoint().authority()),
            Some(proxy.credentials().is_some()),
        ),
        crate::outbound::OutboundConfig::HttpConnect(proxy) => (
            "HTTP CONNECT proxy",
            Some(proxy.endpoint().authority()),
            Some(proxy.credentials().is_some()),
        ),
        crate::outbound::OutboundConfig::HttpsConnect(proxy) => (
            "HTTPS CONNECT proxy",
            Some(proxy.proxy().endpoint().authority()),
            Some(proxy.proxy().credentials().is_some()),
        ),
    };
    let networks = if outbound.supports_udp_targets() {
        "TCP and UDP targets"
    } else {
        "TCP targets"
    };
    match (detail, authentication) {
        (Some(detail), Some(authentication)) => crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "outbound",
            "configured",
            format_args!(
                "{name}: {kind} via {detail}, {networks}; authentication {}",
                if authentication {
                    "configured"
                } else {
                    "disabled"
                },
            ),
        ),
        (Some(detail), None) => crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "outbound",
            "configured",
            format_args!("{name}: {kind} using source {detail}, {networks}"),
        ),
        (None, None) => crate::observability::emit_lifecycle(
            crate::config::LogLevel::Info,
            "outbound",
            "configured",
            format_args!("{name}: {kind}, {networks}"),
        ),
        (None, Some(_)) => unreachable!("proxy outbounds always have an endpoint"),
    }
}

fn plural_noun<'a>(count: usize, singular: &'a str, plural: &'a str) -> &'a str {
    if count == 1 { singular } else { plural }
}

fn build_runtime() -> Result<tokio::runtime::Runtime, AppError> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_worker_threads())
        .enable_all()
        .build()
        .map_err(AppError::BuildRuntime)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigFileInvocation {
    pub path: PathBuf,
    pub check_config: Option<bool>,
}

#[derive(Debug)]
struct OpenedConfigFileGeneration {
    config: AppConfig,
    config_control: crate::runtime::RuntimeConfigControl,
}

fn open_config_file_generation(
    invocation: &ConfigFileInvocation,
) -> Result<OpenedConfigFileGeneration, AppError> {
    let (store, mut config) = CanonicalConfigStore::open(invocation.path.clone())?;
    if let Some(check_config) = invocation.check_config {
        config.check_config = check_config;
    }
    validate_host_generation(&config)?;
    Ok(OpenedConfigFileGeneration {
        config,
        config_control: crate::runtime::RuntimeConfigControl::new(Arc::new(store)),
    })
}

fn reopen_supervised_config_file_generation(
    invocation: &ConfigFileInvocation,
) -> Result<OpenedConfigFileGeneration, AppError> {
    open_config_file_generation(invocation).map_err(|source| AppError::SupervisedConfigReopen {
        path: invocation.path.clone(),
        source: Box::new(source),
    })
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
    let mut unsupported_argument = false;
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == OsStr::new("--config") || arg == OsStr::new("-c") {
            if config_path.is_some() {
                return Err(AppError::DuplicateConfigFileArgument);
            }
            let Some(path) = args.get(index + 1) else {
                return Err(AppError::ConfigFileArgumentMissing);
            };
            if path.is_empty() {
                return Err(AppError::ConfigFileArgumentMissing);
            }
            config_path = Some(PathBuf::from(path));
            index += 2;
            continue;
        }
        if arg == OsStr::new("--check-config") {
            if check_config.is_some() {
                return Err(AppError::DuplicateCheckConfigArgument);
            }
            check_config = Some(true);
            index += 1;
            continue;
        }
        if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--config="))
        {
            if config_path.is_some() || value.is_empty() {
                return if value.is_empty() {
                    Err(AppError::ConfigFileArgumentMissing)
                } else {
                    Err(AppError::DuplicateConfigFileArgument)
                };
            }
            config_path = Some(PathBuf::from(value));
            index += 1;
            continue;
        }
        if let Some(value) = arg
            .to_str()
            .and_then(|value| value.strip_prefix("--check-config="))
        {
            if check_config.is_some() {
                return Err(AppError::DuplicateCheckConfigArgument);
            }
            check_config = Some(parse_bool_flag(value).ok_or(AppError::InvalidCheckConfigFlag)?);
            index += 1;
            continue;
        }
        unsupported_argument = true;
        index += 1;
    }
    if config_path.is_some() && unsupported_argument {
        return Err(AppError::UnsupportedConfigFileArgument);
    }
    Ok(config_path.map(|path| ConfigFileInvocation { path, check_config }))
}

fn config_file_meta_action_requested(args: &[OsString]) -> bool {
    let has_config = args.iter().skip(1).any(|arg| {
        arg == OsStr::new("--config")
            || arg == OsStr::new("-c")
            || arg.to_str().is_some_and(|arg| arg.starts_with("--config="))
    });
    has_config
        && args.iter().skip(1).any(|arg| {
            arg == OsStr::new("--help")
                || arg == OsStr::new("-h")
                || arg == OsStr::new("--version")
                || arg == OsStr::new("-V")
        })
}

fn operational_command_requested(args: &[OsString]) -> bool {
    let mut index = 1;
    while index < args.len() {
        let arg = &args[index];
        if arg == OsStr::new("--config") || arg == OsStr::new("-c") {
            index = index.saturating_add(2);
            continue;
        }
        if matches!(
            arg.to_str(),
            Some("platform" | "status" | "doctor" | "route" | "dns")
        ) {
            return true;
        }
        index += 1;
    }
    false
}

fn parse_bool_flag(value: &str) -> Option<bool> {
    match value {
        "true" => Some(true),
        "false" => Some(false),
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

fn validate_host_generation(config: &AppConfig) -> Result<(), AppError> {
    crate::platform::validate_vpn_generation(config)
        .map_err(|error| AppError::VpnGeneration(Box::new(error)))
}

async fn prepare_managed_vpn(
    config: &AppConfig,
) -> Result<Option<crate::platform::PreparedVpnGeneration>, AppError> {
    crate::platform::prepare_vpn_generation(config)
        .await
        .map_err(|error| AppError::VpnGeneration(Box::new(error)))
}

#[derive(Debug, Clone)]
struct ProcessShutdown {
    requested: watch::Sender<bool>,
    host_publication_may_be_active: watch::Sender<bool>,
}

impl ProcessShutdown {
    fn new() -> Self {
        let (requested, _) = watch::channel(false);
        let (host_publication_may_be_active, _) = watch::channel(false);
        Self {
            requested,
            host_publication_may_be_active,
        }
    }

    fn request(&self) {
        self.requested.send_replace(true);
    }

    fn is_requested(&self) -> bool {
        *self.requested.borrow()
    }

    fn protect_published_vpn_runtime(&self) {
        self.host_publication_may_be_active.send_replace(true);
    }

    fn release_published_vpn_runtime(&self) {
        self.host_publication_may_be_active.send_replace(false);
    }

    fn must_preserve_published_vpn_runtime(&self) -> bool {
        *self.host_publication_may_be_active.borrow()
    }

    async fn wait_until_published_vpn_runtime_is_releasable(&self) {
        let mut active = self.host_publication_may_be_active.subscribe();
        loop {
            if !*active.borrow_and_update() {
                return;
            }
            active
                .changed()
                .await
                .expect("process shutdown retains its publication sender");
        }
    }

    async fn wait(&self) {
        let mut requested = self.requested.subscribe();
        loop {
            if *requested.borrow_and_update() {
                return;
            }
            requested
                .changed()
                .await
                .expect("process shutdown retains its sender");
        }
    }
}

async fn run_process_until_shutdown<E>(
    operation: impl Future<Output = Result<(), E>> + Send + 'static,
    shutdown: ProcessShutdown,
) -> Result<(), ProcessExitError<E>>
where
    E: Send + 'static,
{
    run_process_with_signal(
        operation,
        shutdown,
        process_shutdown_signal(),
        PROCESS_SHUTDOWN_TIMEOUT,
    )
    .await
}

async fn run_process_with_signal<E>(
    operation: impl Future<Output = Result<(), E>> + Send + 'static,
    shutdown: ProcessShutdown,
    signal: impl Future<Output = io::Result<()>> + Send,
    teardown_timeout: Duration,
) -> Result<(), ProcessExitError<E>>
where
    E: Send + 'static,
{
    let mut operation = tokio::spawn(operation);
    tokio::select! {
        joined = &mut operation => map_process_join(joined),
        signal = signal => {
            let signal_error = signal.err();
            if signal_error.is_none() {
                crate::observability::emit_lifecycle(
                    crate::config::LogLevel::Info,
                    "process",
                    "shutdown_requested",
                    format_args!("Shutdown signal received; draining runtime services"),
                );
            }
            shutdown.request();
            let joined = match tokio::time::timeout(teardown_timeout, &mut operation).await {
                Ok(joined) => joined,
                Err(_) if shutdown.must_preserve_published_vpn_runtime() => {
                    crate::observability::process_event!(
                        Warn,
                        "vpn",
                        "retirement_waiting",
                        "managed VPN host publication is still active; continuing safe retirement instead of aborting its packet runtime"
                    );
                    tokio::select! {
                        joined = &mut operation => joined,
                        () = shutdown.wait_until_published_vpn_runtime_is_releasable() => {
                            operation.abort();
                            let _ = operation.await;
                            return Err(ProcessExitError::ShutdownTimeout(teardown_timeout));
                        }
                    }
                }
                Err(_) => {
                    operation.abort();
                    let _ = operation.await;
                    return Err(ProcessExitError::ShutdownTimeout(teardown_timeout));
                }
            };
            if let Some(error) = signal_error {
                return Err(ProcessExitError::Signal(error));
            }
            map_process_join(joined)
        }
    }
}

fn map_process_join<E>(
    joined: Result<Result<(), E>, tokio::task::JoinError>,
) -> Result<(), ProcessExitError<E>> {
    match joined {
        Ok(result) => result.map_err(ProcessExitError::Operation),
        Err(error) => Err(ProcessExitError::Task(error)),
    }
}

async fn process_shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

#[derive(Debug)]
enum ProcessExitError<E> {
    Operation(E),
    Signal(io::Error),
    Task(tokio::task::JoinError),
    ShutdownTimeout(Duration),
}

async fn run_standalone_generations(
    config: AppConfig,
    shutdown: ProcessShutdown,
) -> Result<(), AppError> {
    let mut restarts = 0u32;
    let mut backoff = config.service.restart_backoff;
    loop {
        if shutdown.is_requested() {
            return Ok(());
        }
        let generation = crate::runtime::RuntimeGenerationControl::new();
        let outcome =
            run_standalone_generation(config.clone(), generation, shutdown.clone()).await?;
        let error = match outcome {
            crate::runtime::RuntimeGenerationOutcome::ShutdownRequested => return Ok(()),
            crate::runtime::RuntimeGenerationOutcome::ReloadRequested => {
                crate::runtime::RuntimeError::Protocol(
                    "standalone runtime received a configuration reload request",
                )
            }
            crate::runtime::RuntimeGenerationOutcome::Failed(error) => error,
        };
        if shutdown.is_requested() {
            return Ok(());
        }
        if !config.service.supervise
            || config
                .service
                .max_restarts
                .is_some_and(|maximum| restarts >= maximum)
        {
            return Err(AppError::Runtime(error));
        }
        restarts = restarts.saturating_add(1);
        crate::observability::process_event!(
            Warn,
            "process",
            "generation_restart",
            "runtime exited: {error}; restarting attempt {restarts} in {} ms",
            backoff.as_millis()
        );
        if wait_for_restart_or_shutdown(backoff, &shutdown).await {
            return Ok(());
        }
        backoff = next_restart_backoff(backoff, config.service.restart_max_backoff);
    }
}

async fn run_standalone_generation(
    config: AppConfig,
    generation: crate::runtime::RuntimeGenerationControl,
    shutdown: ProcessShutdown,
) -> Result<crate::runtime::RuntimeGenerationOutcome, AppError> {
    // Preparation may eventually be host-published on embedded platforms.
    // Fence destructive process timeout before entering the cancellable
    // prepare future; a `None` or error result certifies no publication.
    shutdown.protect_published_vpn_runtime();
    let prepared = match prepare_managed_vpn(&config).await {
        Ok(prepared) => prepared,
        Err(error) => {
            shutdown.release_published_vpn_runtime();
            return Err(error);
        }
    };
    if let Some(prepared) = prepared {
        generation.defer_retirement();
        let packet_devices = prepared.packet_device_provider();
        let carrier_network = prepared.carrier_network_provider();
        let native_sockets = prepared.native_socket_configurator();
        let runtime = crate::runtime::run_with_all_host_providers_and_generation_control(
            config,
            packet_devices,
            carrier_network,
            native_sockets,
            generation.clone(),
        );
        return drive_managed_runtime_generation(runtime, generation, None, shutdown, prepared)
            .await
            .map(|terminal| terminal.outcome);
    }
    shutdown.release_published_vpn_runtime();
    Ok(drive_runtime_generation(
        crate::runtime::run_with_generation_control(config, generation.clone()),
        generation,
        shutdown,
    )
    .await)
}

async fn drive_runtime_generation(
    runtime: impl Future<Output = crate::runtime::RuntimeGenerationOutcome>,
    generation: crate::runtime::RuntimeGenerationControl,
    shutdown: ProcessShutdown,
) -> crate::runtime::RuntimeGenerationOutcome {
    tokio::pin!(runtime);
    let mut readiness_pending = true;
    if shutdown.is_requested() {
        generation.mark_stopping();
        generation.request_shutdown();
        return runtime.await;
    }
    loop {
        tokio::select! {
            outcome = &mut runtime => return outcome,
            () = shutdown.wait() => {
                generation.mark_stopping();
                generation.request_shutdown();
                return runtime.await;
            }
            ready = generation.wait_until_ready(), if readiness_pending => {
                readiness_pending = false;
                if ready.is_ok() && generation.is_ready() && !shutdown.is_requested() {
                    crate::observability::emit_lifecycle(
                        crate::config::LogLevel::Info,
                        "process",
                        "generation_ready",
                        format_args!("Runtime generation is ready"),
                    );
                }
            }
        }
    }
}

async fn run_config_file_generations(
    mut config: AppConfig,
    mut config_control: crate::runtime::RuntimeConfigControl,
    invocation: ConfigFileInvocation,
    shutdown: ProcessShutdown,
) -> Result<(), AppError> {
    let mut restarts = 0u32;
    let mut backoff = config.service.restart_backoff;
    loop {
        if shutdown.is_requested() {
            rollback_unactivated_desired(&config_control)?;
            return Ok(());
        }
        let runtime_revision = config_control.runtime_revision();
        let pending_generation =
            config_control.store().pending_revision() == Some(runtime_revision);
        let terminal = match run_canonical_generation(
            config.clone(),
            config_control.clone(),
            shutdown.clone(),
        )
        .await
        {
            Ok(terminal) => terminal,
            Err(error) => {
                if pending_generation
                    && let Some(restored) =
                        rollback_failed_candidate(&config_control, runtime_revision)?
                {
                    crate::observability::process_event!(
                        Warn,
                        "configuration",
                        "generation_rejected",
                        "rejected configuration generation {runtime_revision} before activation: {error}; restored {}",
                        restored.revision
                    );
                    config = config_with_inherited_transport_state(&config, restored.config);
                    config_control = config_control.next_generation();
                    restarts = 0;
                    backoff = config.service.restart_backoff;
                    continue;
                }
                return Err(error);
            }
        };

        match terminal {
            CanonicalGenerationTerminal::ActivationFailed(error) => {
                if pending_generation
                    && let Some(restored) =
                        rollback_failed_candidate(&config_control, runtime_revision)?
                {
                    crate::observability::process_event!(
                        Warn,
                        "configuration",
                        "activation_failed",
                        "configuration generation {runtime_revision} reached readiness but could not be activated: {error}; restored {}",
                        restored.revision
                    );
                    config = config_with_inherited_transport_state(&config, restored.config);
                    config_control = config_control.next_generation();
                    restarts = 0;
                    backoff = config.service.restart_backoff;
                    continue;
                }
                return Err(*error);
            }
            CanonicalGenerationTerminal::Runtime {
                outcome: crate::runtime::RuntimeGenerationOutcome::ShutdownRequested,
                ..
            } => {
                rollback_unactivated_desired(&config_control)?;
                return Ok(());
            }
            CanonicalGenerationTerminal::Runtime {
                outcome: crate::runtime::RuntimeGenerationOutcome::ReloadRequested,
                ..
            } => {
                if shutdown.is_requested() {
                    rollback_unactivated_desired(&config_control)?;
                    return Ok(());
                }
                config = config_with_inherited_transport_state(
                    &config,
                    config_control.store().current_config(),
                );
                crate::observability::emit_lifecycle(
                    crate::config::LogLevel::Info,
                    "configuration",
                    "reload_requested",
                    format_args!(
                        "Preparing configuration generation {} from {}",
                        config_control.store().revision(),
                        config_control.store().path().display(),
                    ),
                );
                config_control = config_control.next_generation();
                restarts = 0;
                backoff = config.service.restart_backoff;
            }
            CanonicalGenerationTerminal::Runtime {
                outcome: crate::runtime::RuntimeGenerationOutcome::Failed(error),
                activated,
            } => {
                if pending_generation
                    && !activated
                    && let Some(restored) =
                        rollback_failed_candidate(&config_control, runtime_revision)?
                {
                    crate::observability::process_event!(
                        Warn,
                        "configuration",
                        "generation_rejected",
                        "rejected configuration generation {runtime_revision}: {error}; restored {}",
                        restored.revision
                    );
                    config = config_with_inherited_transport_state(&config, restored.config);
                    config_control = config_control.next_generation();
                    restarts = 0;
                    backoff = config.service.restart_backoff;
                    continue;
                }
                if shutdown.is_requested() {
                    rollback_unactivated_desired(&config_control)?;
                    return Ok(());
                }
                if config_control.store().pending_revision().is_some() {
                    config = config_with_inherited_transport_state(
                        &config,
                        config_control.store().current_config(),
                    );
                    config_control = config_control.next_generation();
                    restarts = 0;
                    backoff = config.service.restart_backoff;
                    continue;
                }
                if !config.service.supervise
                    || config
                        .service
                        .max_restarts
                        .is_some_and(|maximum| restarts >= maximum)
                {
                    return Err(AppError::Runtime(error));
                }
                restarts = restarts.saturating_add(1);
                crate::observability::process_event!(
                    Warn,
                    "process",
                    "generation_restart",
                    "runtime exited: {error}; restarting attempt {restarts} in {} ms",
                    backoff.as_millis()
                );
                if wait_for_restart_or_shutdown(backoff, &shutdown).await {
                    rollback_unactivated_desired(&config_control)?;
                    return Ok(());
                }
                let previous_revision = runtime_revision;
                let next_backoff =
                    next_restart_backoff(backoff, config.service.restart_max_backoff);
                let installed_logging = config_control.store().active_config().logging;
                let reopened = reopen_supervised_config_file_generation(&invocation)?;
                if reopened.config.logging != installed_logging {
                    crate::observability::configure_for_store(
                        &reopened.config.logging,
                        reopened.config_control.store(),
                    )?;
                }
                config = config_with_inherited_transport_state(&config, reopened.config);
                config_control = reopened.config_control;
                if config_control.runtime_revision() == previous_revision {
                    backoff = next_backoff.min(config.service.restart_max_backoff);
                } else {
                    restarts = 0;
                    backoff = config.service.restart_backoff;
                    crate::observability::process_event!(
                        Info,
                        "configuration",
                        "supervised_reload",
                        "supervised restart loaded changed configuration {} from {}",
                        config_control.runtime_revision(),
                        config_control.store().path().display()
                    );
                    log_configuration_inventory(&config);
                }
            }
        }
    }
}

fn config_with_inherited_transport_state(previous: &AppConfig, mut next: AppConfig) -> AppConfig {
    let CommandConfig::Node(previous) = &previous.command;
    let CommandConfig::Node(next_node) = &mut next.command;
    for next_inbound in &mut next_node.servers {
        let Some(previous_inbound) = previous
            .servers
            .iter()
            .find(|inbound| inbound.name == next_inbound.name)
        else {
            continue;
        };
        let _ = next_inbound
            .tls
            .inherit_transport_replay_state(&previous_inbound.tls);
    }
    next
}

async fn run_canonical_generation(
    config: AppConfig,
    config_control: crate::runtime::RuntimeConfigControl,
    shutdown: ProcessShutdown,
) -> Result<CanonicalGenerationTerminal, AppError> {
    shutdown.protect_published_vpn_runtime();
    let prepared = match prepare_managed_vpn(&config).await {
        Ok(prepared) => prepared,
        Err(error) => {
            shutdown.release_published_vpn_runtime();
            return Err(error);
        }
    };
    if let Some(prepared) = prepared {
        config_control.defer_retirement();
        let packet_devices = prepared.packet_device_provider();
        let carrier_network = prepared.carrier_network_provider();
        let native_sockets = prepared.native_socket_configurator();
        let runtime = crate::runtime::run_with_all_host_providers_and_config_control(
            config,
            packet_devices,
            carrier_network,
            native_sockets,
            config_control.clone(),
        );
        let terminal = drive_managed_runtime_generation(
            runtime,
            config_control.generation(),
            Some(config_control),
            shutdown,
            prepared,
        )
        .await?;
        return Ok(CanonicalGenerationTerminal::Runtime {
            outcome: terminal.outcome,
            activated: terminal.activated,
        });
    }
    shutdown.release_published_vpn_runtime();
    Ok(drive_canonical_generation(
        crate::runtime::run_with_config_control(config, config_control.clone()),
        config_control,
        shutdown,
    )
    .await)
}

#[derive(Debug)]
struct ManagedGenerationTerminal {
    outcome: crate::runtime::RuntimeGenerationOutcome,
    activated: bool,
}

#[derive(Debug, Clone, Copy)]
struct ManagedVpnLifecyclePolicy {
    ready_timeout: Duration,
    shutdown_attempts: NonZeroUsize,
    retry_delay: Duration,
}

impl ManagedVpnLifecyclePolicy {
    fn production() -> Self {
        Self {
            ready_timeout: MANAGED_VPN_READY_TIMEOUT,
            shutdown_attempts: managed_vpn_shutdown_attempts(),
            retry_delay: MANAGED_VPN_SHUTDOWN_RETRY_DELAY,
        }
    }
}

async fn drive_managed_runtime_generation<Runtime, Lifecycle>(
    runtime: Runtime,
    generation: crate::runtime::RuntimeGenerationControl,
    config_control: Option<crate::runtime::RuntimeConfigControl>,
    shutdown: ProcessShutdown,
    prepared: Lifecycle,
) -> Result<ManagedGenerationTerminal, AppError>
where
    Runtime: Future<Output = crate::runtime::RuntimeGenerationOutcome>,
    Lifecycle: crate::platform::VpnGenerationLifecycle,
{
    drive_managed_runtime_generation_with_policy(
        runtime,
        generation,
        config_control,
        shutdown,
        prepared,
        ManagedVpnLifecyclePolicy::production(),
    )
    .await
}

async fn drive_managed_runtime_generation_with_policy<Runtime, Lifecycle>(
    runtime: Runtime,
    generation: crate::runtime::RuntimeGenerationControl,
    config_control: Option<crate::runtime::RuntimeConfigControl>,
    shutdown: ProcessShutdown,
    mut prepared: Lifecycle,
    policy: ManagedVpnLifecyclePolicy,
) -> Result<ManagedGenerationTerminal, AppError>
where
    Runtime: Future<Output = crate::runtime::RuntimeGenerationOutcome>,
    Lifecycle: crate::platform::VpnGenerationLifecycle,
{
    // Conservatively protect the entire prepared generation. Linux prepare is
    // inert, while host-owned adapters may already be published.
    shutdown.protect_published_vpn_runtime();
    tokio::pin!(runtime);
    let mut readiness_pending = true;
    let mut activated = false;
    loop {
        if shutdown.is_requested() {
            generation.mark_stopping();
            generation.request_shutdown();
            let outcome = retire_managed_runtime(
                runtime.as_mut(),
                &generation,
                &mut prepared,
                &shutdown,
                policy,
            )
            .await?;
            return Ok(ManagedGenerationTerminal { outcome, activated });
        }
        tokio::select! {
            biased;
            outcome = &mut runtime => {
                let outcome = cleanup_completed_managed_runtime(
                    outcome,
                    &mut prepared,
                    &shutdown,
                    policy,
                )
                .await?;
                return Ok(ManagedGenerationTerminal { outcome, activated });
            }
            () = shutdown.wait() => {
                generation.mark_stopping();
                generation.request_shutdown();
                let outcome = retire_managed_runtime(
                    runtime.as_mut(),
                    &generation,
                    &mut prepared,
                    &shutdown,
                    policy,
                )
                .await?;
                return Ok(ManagedGenerationTerminal { outcome, activated });
            }
            _ = generation.wait_for_stop() => {
                let outcome = retire_managed_runtime(
                    runtime.as_mut(),
                    &generation,
                    &mut prepared,
                    &shutdown,
                    policy,
                )
                .await?;
                return Ok(ManagedGenerationTerminal { outcome, activated });
            }
            ready = generation.wait_until_ready(), if readiness_pending => {
                readiness_pending = false;
                if ready.is_err() {
                    generation.request_shutdown();
                    let outcome = retire_managed_runtime(
                        runtime.as_mut(),
                        &generation,
                        &mut prepared,
                        &shutdown,
                        policy,
                    )
                    .await?;
                    return Ok(ManagedGenerationTerminal { outcome, activated });
                }
                if generation.is_ready()
                    && generation.stop_reason().is_none()
                    && !shutdown.is_requested()
                {
                    if let Err(error) = prepared
                        .publish_when_worker_ready(policy.ready_timeout)
                        .await
                    {
                        generation.request_shutdown();
                        let publish_error = AppError::VpnGeneration(Box::new(error));
                        retire_managed_runtime(
                            runtime.as_mut(),
                            &generation,
                            &mut prepared,
                            &shutdown,
                            policy,
                        )
                        .await?;
                        return Err(publish_error);
                    }

                    // Publication is synchronous but a stop request can race
                    // with it. Never make that generation active afterward.
                    if shutdown.is_requested()
                        || generation.stop_reason().is_some()
                        || !generation.is_ready()
                    {
                        if shutdown.is_requested() {
                            generation.request_shutdown();
                        }
                        let outcome = retire_managed_runtime(
                            runtime.as_mut(),
                            &generation,
                            &mut prepared,
                            &shutdown,
                            policy,
                        )
                        .await?;
                        return Ok(ManagedGenerationTerminal { outcome, activated });
                    }

                    if let Some(control) = config_control.as_ref() {
                        match activate_ready_generation(control) {
                            Ok(activation) => {
                                activated = true;
                                match activation {
                                    Some(activation) if activation.changed => {
                                        crate::observability::process_event!(
                                            Info,
                                            "configuration",
                                            "generation_activated",
                                            "activated configuration generation {} from {} after managed VPN publication",
                                            activation.revision,
                                            control.store().path().display()
                                        );
                                        log_configuration_inventory(&activation.config);
                                    }
                                    Some(activation) => {
                                        crate::observability::process_event!(
                                            Info,
                                            "vpn",
                                            "generation_ready",
                                            "runtime generation {} and managed VPN are ready",
                                            activation.revision
                                        );
                                    }
                                    None => {
                                        crate::observability::process_event!(
                                            Info,
                                            "vpn",
                                            "generation_ready",
                                            "runtime generation {} and managed VPN are ready",
                                            control.runtime_revision()
                                        );
                                    }
                                }
                            }
                            Err(error) => {
                                generation.request_shutdown();
                                retire_managed_runtime(
                                    runtime.as_mut(),
                                    &generation,
                                    &mut prepared,
                                    &shutdown,
                                    policy,
                                )
                                .await?;
                                return Err(error);
                            }
                        }
                    } else {
                        crate::observability::process_event!(
                            Info,
                            "vpn",
                            "generation_ready",
                            "managed VPN generation is ready"
                        );
                    }
                }
            }
        }
    }
}

async fn retire_managed_runtime<Runtime, Lifecycle>(
    runtime: std::pin::Pin<&mut Runtime>,
    generation: &crate::runtime::RuntimeGenerationControl,
    prepared: &mut Lifecycle,
    shutdown: &ProcessShutdown,
    policy: ManagedVpnLifecyclePolicy,
) -> Result<crate::runtime::RuntimeGenerationOutcome, AppError>
where
    Runtime: Future<Output = crate::runtime::RuntimeGenerationOutcome>,
    Lifecycle: crate::platform::VpnGenerationLifecycle,
{
    unpublish_managed_vpn(prepared, shutdown, policy).await;
    generation.authorize_retirement();
    let outcome = runtime.await;
    prepared
        .cleanup_after_worker_stopped(policy.shutdown_attempts, policy.retry_delay)
        .await
        .map_err(|error| AppError::VpnGeneration(Box::new(error)))?;
    Ok(outcome)
}

async fn cleanup_completed_managed_runtime<Lifecycle>(
    outcome: crate::runtime::RuntimeGenerationOutcome,
    prepared: &mut Lifecycle,
    shutdown: &ProcessShutdown,
    policy: ManagedVpnLifecyclePolicy,
) -> Result<crate::runtime::RuntimeGenerationOutcome, AppError>
where
    Lifecycle: crate::platform::VpnGenerationLifecycle,
{
    unpublish_managed_vpn(prepared, shutdown, policy).await;
    prepared
        .cleanup_after_worker_stopped(policy.shutdown_attempts, policy.retry_delay)
        .await
        .map_err(|error| AppError::VpnGeneration(Box::new(error)))?;
    Ok(outcome)
}

async fn unpublish_managed_vpn<Lifecycle>(
    prepared: &mut Lifecycle,
    shutdown: &ProcessShutdown,
    policy: ManagedVpnLifecyclePolicy,
) where
    Lifecycle: crate::platform::VpnGenerationLifecycle,
{
    loop {
        match prepared
            .unpublish(policy.shutdown_attempts, policy.retry_delay)
            .await
        {
            Ok(()) => {
                shutdown.release_published_vpn_runtime();
                return;
            }
            Err(error) => {
                crate::observability::process_event!(
                    Warn,
                    "vpn",
                    "unpublish_retry",
                    "{error}; retaining the packet runtime and retrying unpublication"
                );
                tokio::time::sleep(policy.retry_delay).await;
            }
        }
    }
}

fn managed_vpn_shutdown_attempts() -> NonZeroUsize {
    NonZeroUsize::new(MANAGED_VPN_SHUTDOWN_ATTEMPTS)
        .expect("managed VPN shutdown attempts are non-zero")
}

#[derive(Debug)]
enum CanonicalGenerationTerminal {
    Runtime {
        outcome: crate::runtime::RuntimeGenerationOutcome,
        activated: bool,
    },
    ActivationFailed(Box<AppError>),
}

async fn drive_canonical_generation(
    runtime: impl Future<Output = crate::runtime::RuntimeGenerationOutcome>,
    config_control: crate::runtime::RuntimeConfigControl,
    shutdown: ProcessShutdown,
) -> CanonicalGenerationTerminal {
    tokio::pin!(runtime);
    let mut readiness_pending = true;
    let mut activated = false;
    loop {
        if shutdown.is_requested() {
            config_control.mark_stopping();
            config_control.request_shutdown();
            return CanonicalGenerationTerminal::Runtime {
                outcome: runtime.await,
                activated,
            };
        }
        tokio::select! {
            biased;
            outcome = &mut runtime => {
                return CanonicalGenerationTerminal::Runtime { outcome, activated };
            }
            () = shutdown.wait() => {
                config_control.mark_stopping();
                config_control.request_shutdown();
                return CanonicalGenerationTerminal::Runtime {
                    outcome: runtime.await,
                    activated,
                };
            }
            ready = config_control.wait_until_ready(), if readiness_pending => {
                readiness_pending = false;
                if ready.is_ok()
                    && config_control.is_ready()
                    && !shutdown.is_requested()
                {
                    match activate_ready_generation(&config_control) {
                        Ok(activation) => {
                            activated = true;
                            match activation {
                                Some(activation) if activation.changed => {
                                    crate::observability::process_event!(
                                        Info,
                                        "configuration",
                                        "generation_activated",
                                        "activated configuration generation {} from {}",
                                        activation.revision,
                                        config_control.store().path().display()
                                    );
                                    log_configuration_inventory(&activation.config);
                                }
                                Some(activation) => {
                                    crate::observability::process_event!(
                                        Info,
                                        "process",
                                        "generation_ready",
                                        "runtime generation {} is ready",
                                        activation.revision
                                    );
                                }
                                None => {
                                    crate::observability::process_event!(
                                        Info,
                                        "process",
                                        "generation_ready",
                                        "runtime generation {} is ready",
                                        config_control.runtime_revision()
                                    );
                                }
                            }
                        }
                        Err(error) => {
                            config_control.mark_stopping();
                            config_control.request_shutdown();
                            let _ = runtime.await;
                            return CanonicalGenerationTerminal::ActivationFailed(Box::new(error));
                        }
                    }
                }
            }
        }
    }
}

fn activate_ready_generation(
    config_control: &crate::runtime::RuntimeConfigControl,
) -> Result<Option<crate::config::CommittedConfig>, AppError> {
    let store = config_control.store();
    let _mutation = store.lock_mutation();
    let runtime_revision = config_control.runtime_revision();
    if store.active_revision() == runtime_revision {
        return Ok(None);
    }
    let config = store.current_config();
    let active = store.active_config();
    let prepared = if config.logging != active.logging {
        Some(crate::observability::prepare_for_store(
            &config.logging,
            store,
        )?)
    } else {
        None
    };
    let activated = store.activate_desired(runtime_revision)?;
    if let Some(prepared) = prepared {
        crate::observability::install(prepared);
    }
    Ok(Some(activated))
}

fn rollback_unactivated_desired(
    config_control: &crate::runtime::RuntimeConfigControl,
) -> Result<(), AppError> {
    let store = config_control.store();
    let _mutation = store.lock_mutation();
    if store.pending_revision().is_some() {
        store.rollback_pending()?;
    }
    Ok(())
}

fn rollback_failed_candidate(
    config_control: &crate::runtime::RuntimeConfigControl,
    runtime_revision: crate::config::ConfigRevision,
) -> Result<Option<crate::config::CommittedConfig>, AppError> {
    let store = config_control.store();
    let _mutation = store.lock_mutation();
    if store.pending_revision() != Some(runtime_revision) {
        return Ok(None);
    }
    store.rollback_pending().map(Some).map_err(AppError::from)
}

async fn wait_for_restart_or_shutdown(delay: Duration, shutdown: &ProcessShutdown) -> bool {
    tokio::select! {
        () = tokio::time::sleep(delay) => false,
        () = shutdown.wait() => true,
    }
}

fn next_restart_backoff(current: Duration, max: Duration) -> Duration {
    current.saturating_mul(2).min(max)
}

#[derive(Debug)]
pub enum AppError {
    Cli(clap::Error),
    CliOutput(std::io::Error),
    Config(CliConfigError),
    ConfigFile(ConfigFileError),
    ConfigStore(Box<ConfigStoreError>),
    ConfigFileArgumentMissing,
    DuplicateConfigFileArgument,
    DuplicateCheckConfigArgument,
    UnsupportedConfigFileArgument,
    InvalidCheckConfigFlag,
    ConfigPathResolution {
        path: PathBuf,
        source: io::Error,
    },
    SupervisedConfigReopen {
        path: PathBuf,
        source: Box<AppError>,
    },
    Runtime(crate::runtime::RuntimeError),
    Operation(crate::operations::OperationError),
    Logging(crate::observability::ConfigureError),
    VpnGeneration(Box<crate::platform::VpnGenerationError>),
    ShutdownSignal(std::io::Error),
    ShutdownTimeout(Duration),
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

impl From<crate::operations::OperationError> for AppError {
    fn from(value: crate::operations::OperationError) -> Self {
        Self::Operation(value)
    }
}

impl From<crate::observability::ConfigureError> for AppError {
    fn from(value: crate::observability::ConfigureError) -> Self {
        Self::Logging(value)
    }
}

impl From<ConfigStoreError> for AppError {
    fn from(value: ConfigStoreError) -> Self {
        Self::ConfigStore(Box::new(value))
    }
}

impl std::fmt::Display for AppError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Cli(err) => write!(f, "{err}"),
            Self::CliOutput(err) => write!(f, "failed to write CLI output: {err}"),
            Self::Config(err) => write!(f, "{err}"),
            Self::ConfigFile(err) => write!(f, "{err}"),
            Self::ConfigStore(err) => write!(f, "{err}"),
            Self::ConfigFileArgumentMissing => write!(f, "--config requires a file path"),
            Self::DuplicateConfigFileArgument => {
                write!(f, "--config may be specified only once")
            }
            Self::DuplicateCheckConfigArgument => {
                write!(f, "--check-config may be specified only once")
            }
            Self::UnsupportedConfigFileArgument => {
                write!(f, "unsupported process argument used with --config")
            }
            Self::InvalidCheckConfigFlag => write!(f, "--check-config value must be true or false"),
            Self::ConfigPathResolution { path, source } => write!(
                f,
                "failed to resolve configuration path {}: {source}",
                path.display()
            ),
            Self::SupervisedConfigReopen { path, source } => write!(
                f,
                "failed to reopen supervised configuration {}: {source}",
                path.display()
            ),
            Self::Runtime(err) => write!(f, "{err}"),
            Self::Operation(err) => write!(f, "{err}"),
            Self::Logging(err) => write!(f, "{err}"),
            Self::VpnGeneration(err) => write!(f, "{err}"),
            Self::ShutdownSignal(err) => {
                write!(f, "failed to register process shutdown signal: {err}")
            }
            Self::ShutdownTimeout(timeout) => {
                write!(f, "runtime generation did not retire within {timeout:?}")
            }
            Self::BuildRuntime(err) => write!(f, "failed to build async runtime: {err}"),
        }
    }
}

impl From<ProcessExitError<AppError>> for AppError {
    fn from(value: ProcessExitError<AppError>) -> Self {
        match value {
            ProcessExitError::Operation(err) => err,
            ProcessExitError::Signal(err) => Self::ShutdownSignal(err),
            ProcessExitError::Task(err) => {
                Self::Runtime(crate::runtime::RuntimeError::TaskJoin(err))
            }
            ProcessExitError::ShutdownTimeout(timeout) => Self::ShutdownTimeout(timeout),
        }
    }
}

impl std::error::Error for AppError {}

#[cfg(test)]
#[path = "tests_app.rs"]
mod tests;

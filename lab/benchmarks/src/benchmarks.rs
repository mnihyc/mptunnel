use aes_gcm::aead::{AeadInPlace, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce as AesNonce};
use chacha20poly1305::{ChaCha20Poly1305, Nonce as ChaChaNonce};
use mptunnel::config::ResourceLimits;
use mptunnel::protocol::{PathId, UnderlayProtocol};
use mptunnel::scheduler::{PathSnapshot, TrafficClass};
use mptunnel::simulator::{Simulator, VirtualPath};
use serde::Serialize;
use std::hint::black_box;
use std::time::Instant;

const PROFILE: &str = "developer-gates-v1";
const MIB: usize = 1024 * 1024;
const GIB_BYTES: f64 = 1024.0 * 1024.0 * 1024.0;
const IDEAL_LAB_TARGET_MIB: f64 = 256.0;
const IDEAL_LAB_TARGET_MBPS: f64 = 950.0;
pub const MAX_RESOURCE_SAMPLE_MIB: u32 = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BenchmarkOptions {
    pub resource_sample_mib: u32,
}

impl BenchmarkOptions {
    pub fn new(resource_sample_mib: u32) -> Result<Self, BenchmarkError> {
        if resource_sample_mib == 0 {
            return Err(BenchmarkError::InvalidResourceSample);
        }
        if resource_sample_mib > MAX_RESOURCE_SAMPLE_MIB {
            return Err(BenchmarkError::ResourceSampleTooLarge {
                actual: resource_sample_mib,
                limit: MAX_RESOURCE_SAMPLE_MIB,
            });
        }
        Ok(Self {
            resource_sample_mib,
        })
    }
}

impl Default for BenchmarkOptions {
    fn default() -> Self {
        Self {
            resource_sample_mib: 8,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkReport {
    pub profile: String,
    pub passed: bool,
    pub gates: Vec<BenchmarkGate>,
}

impl BenchmarkReport {
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        out.push_str("mptunnel benchmark report\n");
        out.push_str(&format!("profile: {}\n", self.profile));
        out.push_str(&format!(
            "status: {}\n",
            if self.passed { "pass" } else { "fail" }
        ));
        out.push_str("gates:\n");
        for gate in &self.gates {
            out.push_str(&format!(
                "  [{}] {} {} {:.3} {} (value {:.3})\n",
                if gate.passed { "pass" } else { "fail" },
                gate.name,
                gate.comparator.symbol(),
                gate.threshold,
                gate.unit,
                gate.value
            ));
        }
        out
    }

    pub fn render_json(&self) -> Result<String, BenchmarkError> {
        serde_json::to_string_pretty(self).map_err(BenchmarkError::Serialize)
    }

    pub fn failure_count(&self) -> usize {
        self.gates.iter().filter(|gate| !gate.passed).count()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BenchmarkGate {
    pub name: String,
    pub metric: String,
    pub value: f64,
    pub threshold: f64,
    pub unit: String,
    pub comparator: GateComparator,
    pub passed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GateComparator {
    AtMost,
    AtLeast,
}

impl GateComparator {
    fn symbol(self) -> &'static str {
        match self {
            Self::AtMost => "<=",
            Self::AtLeast => ">=",
        }
    }
}

#[derive(Debug)]
pub enum BenchmarkError {
    GateFailures(usize),
    InvalidResourceSample,
    ResourceSampleTooLarge { actual: u32, limit: u32 },
    Crypto,
    Serialize(serde_json::Error),
}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GateFailures(count) => write!(f, "{count} benchmark gate(s) failed"),
            Self::InvalidResourceSample => write!(f, "benchmark resource sample must be nonzero"),
            Self::ResourceSampleTooLarge { actual, limit } => {
                write!(
                    f,
                    "benchmark resource sample {actual} MiB exceeds limit {limit} MiB"
                )
            }
            Self::Crypto => write!(f, "benchmark crypto operation failed"),
            Self::Serialize(err) => write!(f, "failed to serialize benchmark report: {err}"),
        }
    }
}

impl std::error::Error for BenchmarkError {}

pub fn run_benchmarks(options: BenchmarkOptions) -> Result<BenchmarkReport, BenchmarkError> {
    let mut gates = Vec::new();

    let page = page_load_benchmark();
    gates.push(at_most(
        "page_load_complete",
        "page_load_complete_ms",
        page.complete_ms,
        1_200.0,
        "ms",
    ));
    gates.push(at_most(
        "page_load_interactive_p95",
        "interactive_p95_latency_ms",
        page.interactive_p95_ms,
        60.0,
        "ms",
    ));

    let video = video_streaming_benchmark();
    gates.push(at_most(
        "video_startup",
        "video_startup_ms",
        video.startup_ms,
        1_500.0,
        "ms",
    ));
    gates.push(at_most(
        "video_rebuffer",
        "video_rebuffer_events",
        f64::from(video.rebuffer_events),
        0.0,
        "events",
    ));

    let download = file_download_benchmark();
    gates.push(at_least(
        "file_download_goodput",
        "goodput_mbps",
        download.goodput_mbps,
        240.0,
        "Mbps",
    ));
    gates.push(at_least(
        "aggregation_efficiency",
        "aggregation_efficiency_ratio",
        download.aggregation_efficiency,
        0.70,
        "ratio",
    ));
    let ideal = ideal_lab_benchmark();
    gates.push(at_least(
        "ideal_lab_goodput",
        "ideal_lab_goodput_mbps",
        ideal.goodput_mbps,
        IDEAL_LAB_TARGET_MBPS,
        "Mbps",
    ));

    let failover = failover_benchmark();
    gates.push(at_most(
        "failover_gap",
        "failover_gap_ms",
        failover.gap_ms,
        500.0,
        "ms",
    ));
    gates.push(at_least(
        "failover_reinjection",
        "reinjected_chunks",
        failover.reinjected_chunks as f64,
        1.0,
        "chunks",
    ));

    let resource = resource_benchmark(options.resource_sample_mib)?;
    gates.push(at_most(
        "chacha20poly1305_cpu",
        "cpu_core_seconds_per_gib",
        resource.chacha_core_seconds_per_gib,
        300.0,
        "core-s/GiB",
    ));
    gates.push(at_most(
        "aes256gcm_cpu",
        "cpu_core_seconds_per_gib",
        resource.aes_core_seconds_per_gib,
        300.0,
        "core-s/GiB",
    ));
    gates.push(at_most(
        "stream_ram_budget",
        "stream_memory_budget_mib",
        resource.stream_memory_budget_mib,
        192.0,
        "MiB",
    ));
    gates.push(at_most(
        "datagram_ram_budget",
        "datagram_queue_budget_mib",
        resource.datagram_queue_budget_mib,
        16.0,
        "MiB",
    ));
    gates.push(at_most(
        "path_flight_budget",
        "path_flight_budget_mib",
        resource.path_flight_budget_mib,
        64.0,
        "MiB",
    ));
    gates.push(at_most(
        "lab_hot_path_ram_budget",
        "lab_hot_path_ram_budget_mib",
        resource.lab_hot_path_ram_budget_mib,
        IDEAL_LAB_TARGET_MIB,
        "MiB",
    ));

    let passed = gates.iter().all(|gate| gate.passed);
    Ok(BenchmarkReport {
        profile: PROFILE.to_string(),
        passed,
        gates,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct AblationReport {
    pub profile: String,
    pub rows: Vec<AblationRow>,
}

impl AblationReport {
    pub fn render_text(&self) -> String {
        let mut out = String::new();
        out.push_str("mptunnel deterministic ablation report\n");
        out.push_str(&format!("profile: {}\n", self.profile));
        out.push_str("rows:\n");
        for row in &self.rows {
            out.push_str(&format!(
                "  {}: page_p95={:.3} ms, download={:.3} Mbps, aggregation={:.3}, failover_gap={:.3} ms, reinjected_chunks={}\n",
                row.name,
                row.page_interactive_p95_ms,
                row.download_goodput_mbps,
                row.aggregation_efficiency,
                row.failover_gap_ms,
                row.reinjected_chunks
            ));
        }
        out
    }

    pub fn render_json(&self) -> Result<String, BenchmarkError> {
        serde_json::to_string_pretty(self).map_err(BenchmarkError::Serialize)
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct AblationRow {
    pub name: String,
    pub path_profile: String,
    pub page_interactive_p95_ms: f64,
    pub download_goodput_mbps: f64,
    pub aggregation_efficiency: f64,
    pub failover_gap_ms: f64,
    pub reinjected_chunks: usize,
}

pub fn run_ablation_study() -> AblationReport {
    let rows = vec![
        ablation_row(
            "single_low_latency",
            "low_latency_only",
            vec![download_paths()[0]],
        ),
        ablation_row(
            "single_high_bandwidth",
            "high_bandwidth_only",
            vec![download_paths()[1]],
        ),
        ablation_row(
            "single_poor_internet",
            "poor_internet_only",
            vec![download_paths()[2]],
        ),
        ablation_row(
            "multipath_full",
            "heterogeneous_all_links",
            download_paths(),
        ),
        ablation_row(
            "multipath_good_links",
            "heterogeneous_good_links",
            download_paths().into_iter().take(2).collect(),
        ),
        ablation_row(
            "multipath_unstable_mix",
            "low_latency_plus_unstable",
            vec![download_paths()[0], download_paths()[2]],
        ),
        ablation_row(
            "ideal_lab_same_protocol_group",
            "ideal_udp_udp_tcp_group",
            ideal_lab_paths(),
        ),
    ];
    AblationReport {
        profile: "deterministic-path-ablation-v2".to_string(),
        rows,
    }
}

fn ablation_row(name: &str, path_profile: &str, paths: Vec<VirtualPath>) -> AblationRow {
    let mut page_simulator = Simulator::new(paths.clone());
    page_simulator
        .schedule_transfer(TrafficClass::Throughput, 128 * MIB, MIB)
        .expect("ablation paths schedule background bulk");
    let page_interactive_p95_ms = page_simulator
        .route_interactive_burst(1024, 20, 5.0)
        .expect("ablation paths schedule interactive burst")
        .p95_latency_ms()
        .expect("interactive burst is nonempty");

    let mut download_simulator = Simulator::new(paths.clone());
    let download = download_simulator
        .schedule_transfer(TrafficClass::Throughput, 512 * MIB, MIB)
        .expect("ablation paths schedule file download");

    let mut failover_simulator = Simulator::new(failover_paths_for(paths));
    let failover = failover_simulator
        .schedule_transfer_with_reinjection(TrafficClass::Throughput, 8 * MIB, 256 * 1024, 10.0)
        .expect("ablation paths schedule transfer with reinjection");

    AblationRow {
        name: name.to_string(),
        path_profile: path_profile.to_string(),
        page_interactive_p95_ms,
        download_goodput_mbps: download.achieved_goodput_bps() / 1_000_000.0,
        aggregation_efficiency: download.aggregation_efficiency(mbps(1_000.0)),
        failover_gap_ms: failover.failover_gap_ms.unwrap_or(f64::INFINITY),
        reinjected_chunks: failover.reinjected_chunks,
    }
}

fn failover_paths_for(paths: Vec<VirtualPath>) -> Vec<VirtualPath> {
    let mut snapshots = paths
        .into_iter()
        .map(|path| path.snapshot)
        .collect::<Vec<_>>();
    if snapshots.len() < 2 {
        snapshots.push(PathSnapshot::new(
            PathId(99),
            UnderlayProtocol::Udp,
            80.0,
            mbps(160.0),
        ));
    }
    vec![
        VirtualPath::new(snapshots[0]).with_failure_at(70.0),
        VirtualPath::new(snapshots[1]),
    ]
}

fn at_most(name: &str, metric: &str, value: f64, threshold: f64, unit: &str) -> BenchmarkGate {
    BenchmarkGate {
        name: name.to_string(),
        metric: metric.to_string(),
        value,
        threshold,
        unit: unit.to_string(),
        comparator: GateComparator::AtMost,
        passed: value <= threshold,
    }
}

fn at_least(name: &str, metric: &str, value: f64, threshold: f64, unit: &str) -> BenchmarkGate {
    BenchmarkGate {
        name: name.to_string(),
        metric: metric.to_string(),
        value,
        threshold,
        unit: unit.to_string(),
        comparator: GateComparator::AtLeast,
        passed: value >= threshold,
    }
}

#[derive(Debug, Clone, Copy)]
struct PageLoadMetrics {
    complete_ms: f64,
    interactive_p95_ms: f64,
}

fn page_load_benchmark() -> PageLoadMetrics {
    let mut simulator = Simulator::new(browser_paths());
    simulator
        .schedule_transfer(TrafficClass::Throughput, 128 * MIB, MIB)
        .expect("benchmark paths schedule bulk warmup");

    let start_ms = simulator.now_ms();
    let mut completions = Vec::new();
    for _ in 0..4 {
        completions.push(
            simulator
                .route(TrafficClass::Control, 768)
                .expect("benchmark paths schedule control")
                .estimated_completion_ms
                - start_ms,
        );
    }
    completions.push(
        simulator
            .route(TrafficClass::Latency, 32 * 1024)
            .expect("benchmark paths schedule html")
            .estimated_completion_ms
            - start_ms,
    );
    for _ in 0..72 {
        completions.push(
            simulator
                .route(TrafficClass::Latency, 24 * 1024)
                .expect("benchmark paths schedule page object")
                .estimated_completion_ms
                - start_ms,
        );
    }
    completions.sort_by(f64::total_cmp);
    let complete_ms = completions.last().copied().unwrap_or_default();

    let mut interactive_simulator = Simulator::new(browser_paths());
    interactive_simulator
        .schedule_transfer(TrafficClass::Throughput, 128 * MIB, MIB)
        .expect("benchmark paths schedule background bulk");
    let interactive_p95_ms = interactive_simulator
        .route_interactive_burst(1024, 20, 5.0)
        .expect("benchmark paths schedule interactive burst")
        .p95_latency_ms()
        .expect("interactive burst is nonempty");
    PageLoadMetrics {
        complete_ms,
        interactive_p95_ms,
    }
}

#[derive(Debug, Clone, Copy)]
struct VideoMetrics {
    startup_ms: f64,
    rebuffer_events: u32,
}

fn video_streaming_benchmark() -> VideoMetrics {
    let mut simulator = Simulator::new(browser_paths());
    let first_segment = simulator
        .schedule_transfer(TrafficClass::Throughput, 3 * MIB, 512 * 1024)
        .expect("benchmark paths schedule first video segment");
    let startup_ms = first_segment.duration_ms();
    let playback_origin_ms = first_segment.completion_ms;
    let mut rebuffer_events = 0u32;

    for segment_index in 1..=10 {
        let request_at_ms = segment_index as f64 * 1_500.0;
        simulator.advance_to(request_at_ms);
        let segment = simulator
            .schedule_transfer(TrafficClass::Throughput, 3 * MIB, 512 * 1024)
            .expect("benchmark paths schedule video segment");
        let playback_deadline_ms = playback_origin_ms + segment_index as f64 * 2_000.0;
        if segment.completion_ms > playback_deadline_ms {
            rebuffer_events = rebuffer_events.saturating_add(1);
        }
    }

    VideoMetrics {
        startup_ms,
        rebuffer_events,
    }
}

#[derive(Debug, Clone, Copy)]
struct DownloadMetrics {
    goodput_mbps: f64,
    aggregation_efficiency: f64,
}

fn file_download_benchmark() -> DownloadMetrics {
    let mut simulator = Simulator::new(download_paths());
    let transfer = simulator
        .schedule_transfer(TrafficClass::Throughput, 512 * MIB, MIB)
        .expect("benchmark paths schedule file download");
    DownloadMetrics {
        goodput_mbps: transfer.achieved_goodput_bps() / 1_000_000.0,
        aggregation_efficiency: transfer.aggregation_efficiency(mbps(1_000.0)),
    }
}

fn ideal_lab_benchmark() -> DownloadMetrics {
    let mut simulator = Simulator::new(ideal_lab_paths());
    let transfer = simulator
        .schedule_transfer(TrafficClass::Throughput, 1024 * MIB, MIB)
        .expect("ideal lab paths schedule file download");
    DownloadMetrics {
        goodput_mbps: transfer.achieved_goodput_bps() / 1_000_000.0,
        aggregation_efficiency: transfer.aggregation_efficiency(mbps(1_000.0)),
    }
}

#[derive(Debug, Clone, Copy)]
struct FailoverMetrics {
    gap_ms: f64,
    reinjected_chunks: usize,
}

fn failover_benchmark() -> FailoverMetrics {
    let primary = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(120.0));
    let survivor = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 80.0, mbps(160.0));
    let mut simulator = Simulator::new(vec![
        VirtualPath::new(primary).with_failure_at(70.0),
        VirtualPath::new(survivor),
    ]);
    let transfer = simulator
        .schedule_transfer_with_reinjection(TrafficClass::Throughput, 8 * MIB, 256 * 1024, 10.0)
        .expect("benchmark paths schedule transfer with reinjection");
    FailoverMetrics {
        gap_ms: transfer.failover_gap_ms.unwrap_or(f64::INFINITY),
        reinjected_chunks: transfer.reinjected_chunks,
    }
}

#[derive(Debug, Clone, Copy)]
struct ResourceMetrics {
    chacha_core_seconds_per_gib: f64,
    aes_core_seconds_per_gib: f64,
    stream_memory_budget_mib: f64,
    datagram_queue_budget_mib: f64,
    path_flight_budget_mib: f64,
    lab_hot_path_ram_budget_mib: f64,
}

fn resource_benchmark(sample_mib: u32) -> Result<ResourceMetrics, BenchmarkError> {
    let sample_bytes = sample_mib as usize * MIB;
    let chacha_core_seconds_per_gib = measure_chacha20poly1305(sample_bytes)?;
    let aes_core_seconds_per_gib = measure_aes256gcm(sample_bytes)?;
    let limits = ResourceLimits::default();
    let stream_memory_budget_mib = bytes_to_mib(
        limits
            .max_stream_window_bytes
            .saturating_add(limits.max_repair_bytes as u64)
            .saturating_add(limits.max_reorder_bytes as u64),
    );
    Ok(ResourceMetrics {
        chacha_core_seconds_per_gib,
        aes_core_seconds_per_gib,
        stream_memory_budget_mib,
        datagram_queue_budget_mib: bytes_to_mib(limits.max_datagram_queue_bytes as u64),
        path_flight_budget_mib: bytes_to_mib(limits.max_path_flight_bytes as u64),
        lab_hot_path_ram_budget_mib: bytes_to_mib(lab_hot_path_ram_budget_bytes(limits)),
    })
}

fn lab_hot_path_ram_budget_bytes(limits: ResourceLimits) -> u64 {
    limits.max_repair_bytes as u64
        + limits.max_reorder_bytes as u64
        + limits.max_path_flight_bytes as u64
        + limits.max_datagram_queue_bytes as u64
}

fn measure_chacha20poly1305(sample_bytes: usize) -> Result<f64, BenchmarkError> {
    let cipher = ChaCha20Poly1305::new_from_slice(&[0x41; 32]).expect("valid chacha key");
    measure_aead(sample_bytes, |counter, aad, payload| {
        let nonce = nonce_bytes(counter);
        let tag = cipher
            .encrypt_in_place_detached(ChaChaNonce::from_slice(&nonce), aad, payload)
            .map_err(|_| BenchmarkError::Crypto)?;
        cipher
            .decrypt_in_place_detached(ChaChaNonce::from_slice(&nonce), aad, payload, &tag)
            .map_err(|_| BenchmarkError::Crypto)
    })
}

fn measure_aes256gcm(sample_bytes: usize) -> Result<f64, BenchmarkError> {
    let cipher = Aes256Gcm::new_from_slice(&[0x23; 32]).expect("valid AES key");
    measure_aead(sample_bytes, |counter, aad, payload| {
        let nonce = nonce_bytes(counter);
        let tag = cipher
            .encrypt_in_place_detached(AesNonce::from_slice(&nonce), aad, payload)
            .map_err(|_| BenchmarkError::Crypto)?;
        cipher
            .decrypt_in_place_detached(AesNonce::from_slice(&nonce), aad, payload, &tag)
            .map_err(|_| BenchmarkError::Crypto)
    })
}

fn measure_aead<F>(sample_bytes: usize, mut round_trip: F) -> Result<f64, BenchmarkError>
where
    F: FnMut(u64, &[u8], &mut [u8]) -> Result<(), BenchmarkError>,
{
    let chunk_bytes = 16 * 1024;
    let iterations = (sample_bytes / chunk_bytes).max(1);
    let aad = b"mptunnel benchmark aead v1";
    let mut payload = vec![0xA5; chunk_bytes];
    let started = Instant::now();
    for counter in 0..iterations {
        round_trip(counter as u64, aad, &mut payload)?;
        black_box(payload[0]);
    }
    let elapsed = started.elapsed().as_secs_f64();
    let processed_bytes = (iterations * chunk_bytes * 2) as f64;
    Ok(elapsed / (processed_bytes / GIB_BYTES).max(f64::EPSILON))
}

fn browser_paths() -> Vec<VirtualPath> {
    let mut low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    low_latency.policy.bulk_allowed = false;
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.jitter_ms = 20.0;
    unstable.loss_rate = 0.02;
    vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
        VirtualPath::new(unstable),
    ]
}

fn download_paths() -> Vec<VirtualPath> {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 20.0, mbps(30.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 180.0, mbps(300.0));
    let mut unstable = PathSnapshot::new(PathId(2), UnderlayProtocol::Udp, 80.0, mbps(100.0));
    unstable.jitter_ms = 20.0;
    unstable.loss_rate = 0.02;
    vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
        VirtualPath::new(unstable),
    ]
}

fn ideal_lab_paths() -> Vec<VirtualPath> {
    let low_latency = PathSnapshot::new(PathId(0), UnderlayProtocol::Udp, 4.0, mbps(500.0));
    let high_bandwidth = PathSnapshot::new(PathId(1), UnderlayProtocol::Udp, 8.0, mbps(500.0));
    let mut tcp_like = PathSnapshot::new(PathId(2), UnderlayProtocol::Tcp, 6.0, mbps(250.0));
    tcp_like.jitter_ms = 0.5;
    vec![
        VirtualPath::new(low_latency),
        VirtualPath::new(high_bandwidth),
        VirtualPath::new(tcp_like),
    ]
}

fn nonce_bytes(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..12].copy_from_slice(&counter.to_be_bytes());
    nonce
}

fn mbps(value: f64) -> f64 {
    value * 1_000_000.0
}

fn bytes_to_mib(bytes: u64) -> f64 {
    bytes as f64 / MIB as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn developer_benchmark_gates_pass() {
        let report = run_benchmarks(BenchmarkOptions {
            resource_sample_mib: 1,
        })
        .expect("benchmark report");

        assert!(
            report.passed,
            "failed gates: {:?}",
            report
                .gates
                .iter()
                .filter(|gate| !gate.passed)
                .collect::<Vec<_>>()
        );
        assert!(
            report
                .gates
                .iter()
                .any(|gate| gate.name == "page_load_complete")
        );
        assert!(
            report
                .gates
                .iter()
                .any(|gate| gate.name == "chacha20poly1305_cpu")
        );
    }

    #[test]
    fn benchmark_json_contains_profile_and_gates() {
        let report = run_benchmarks(BenchmarkOptions {
            resource_sample_mib: 1,
        })
        .expect("benchmark report");
        let json = report.render_json().expect("json");

        assert!(json.contains("\"profile\""));
        assert!(json.contains("\"developer-gates-v1\""));
        assert!(json.contains("\"gates\""));
    }

    #[test]
    fn ablation_report_compares_single_and_multipath_profiles() {
        let report = run_ablation_study();

        assert!(
            report
                .rows
                .iter()
                .any(|row| row.name == "single_low_latency")
        );
        assert!(report.rows.iter().any(|row| row.name == "multipath_full"));
        let json = report.render_json().expect("json");
        assert!(json.contains("deterministic-path-ablation-v2"));
    }

    #[test]
    fn invalid_resource_sample_is_rejected() {
        assert!(matches!(
            BenchmarkOptions::new(0),
            Err(BenchmarkError::InvalidResourceSample)
        ));
        assert!(matches!(
            BenchmarkOptions::new(MAX_RESOURCE_SAMPLE_MIB + 1),
            Err(BenchmarkError::ResourceSampleTooLarge { .. })
        ));
    }
}

# Developer Benchmarks

`mptunnel-bench` is a developer/lab tool under `lab/benchmarks/`. It is
outside the root crate, is intentionally not part of the release `mptunnel`
binary, and is not packaged in release archives. Hosted source-quality CI
builds its test harness and runs deterministic model and observation-trace
replay tests. It does not run Docker experiments or use host timing as
performance-acceptance evidence.

The benchmark goals are diagnostic. They are used to identify performance or resource regressions in the lab. Production `mptunnel` does not contain these thresholds, does not self-limit to them, and does not terminate because a benchmark goal is missed.

## Test Goal

The test goal is to measure whether the adaptive transport behaves well under controlled network pressure:

- small interactive flows should keep low latency while bulk traffic is active
- bulk flows should aggregate useful bandwidth across healthy links
- failed or blackholed links should recover through survivor paths without ending the user flow
- CPU and memory use should stay within the manual lab target on ideal hardware

These are observations for regression triage and scheduler tuning. They are not production limits, exit conditions, or fixed runtime caps.

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict
```

JSON output is available for saved lab reports and dashboards:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict --format json
```

The gate command is safe to run on a normal host. It uses deterministic models,
including a simulator-private virtual queue that shares production path-scoring
primitives, plus production resource-limit arithmetic. It does not exercise
deployed sender queues or carrier recovery, create TUN devices, alter routes,
change DNS, bind privileged service state, or modify host networking.

Deterministic ablation output is also available:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation
cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation --format json
```

The ablation report compares single low-latency, single high-bandwidth, single poor-Internet, and heterogeneous multipath profiles. These are simulator path-profile comparisons only; matched Docker/runtime labs are required before making external performance claims.

## Gates

The current developer profile is `developer-gates-v1`.

| Gate | Metric | Requirement |
| --- | --- | --- |
| `page_load_complete` | modeled page-load completion | <= 1200 ms |
| `page_load_interactive_p95` | interactive request p95 under concurrent bulk load | <= 60 ms |
| `video_startup` | first video segment startup | <= 1500 ms |
| `video_rebuffer` | modeled rebuffer events | 0 |
| `file_download_goodput` | large-download goodput | >= 240 Mbps |
| `aggregation_efficiency` | achieved goodput divided by usable healthy-path capacity | >= 0.70 |
| `ideal_lab_goodput` | modeled clean-lab aggregate goodput | >= 950 Mbps |
| `failover_gap` | first survivor delivery gap after path failure | <= 500 ms |
| `failover_reinjection` | MPP chunks reinjected after path failure | >= 1 |
| `stream_ram_budget` | default stream window+retained-data+reorder envelope | <= 192 MiB |
| `datagram_ram_budget` | default datagram queue budget | <= 16 MiB |
| `path_flight_budget` | default path flight budget | <= 64 MiB |
| `lab_hot_path_ram_budget` | modeled hot-path RAM budget for manual lab target | <= 256 MiB |

## Options

`--strict`
: Exit nonzero when any benchmark gate fails. This applies only to the manual lab binary, never to production `mptunnel`.

`--format text|json`
: Print a human-readable report or machine-readable JSON.

Environment variables:

- `MPTUNNEL_BENCH_STRICT`
- `MPTUNNEL_BENCH_FORMAT`

## Build Policy

Normal root-level `cargo build`, `cargo test`, `cargo clippy`, target checks,
and release packaging build only the product crate and `--bin mptunnel`. The
source-quality gate separately runs:

```bash
cargo test --locked --manifest-path lab/benchmarks/Cargo.toml
```

That command checks deterministic model gates, versioned trace replay, output
contracts, and production resource-limit arithmetic. CPU, carrier-crypto,
runtime A/B, Docker shaping, and competitor comparisons must be measured
end-to-end through the dedicated performance process described in
[`LAB.md`](LAB.md); disconnected primitive timings are not release evidence.

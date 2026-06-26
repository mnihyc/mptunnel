# Developer Benchmarks

`mptunnel-bench` is a manual developer/lab tool under `lab/benchmarks/`. It is outside the root crate, is intentionally not part of the release `mptunnel` binary, is not packaged in release archives, and is not built or run by CI/release workflows.

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict
```

JSON output is available for saved lab reports and dashboards:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- gates --strict --format json
```

The gate command is safe to run on a normal host. It uses deterministic scheduler/runtime models and a bounded local AEAD hot-path sample. It does not create TUN devices, alter routes, change DNS, bind privileged service state, or modify host networking.

Deterministic ablation output is also available:

```bash
cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation
cargo run --manifest-path lab/benchmarks/Cargo.toml -- ablation --format json
```

The ablation report compares single low-latency, single high-bandwidth, single poor-Internet, full multipath, and scheduler-ablation profiles. These are model comparisons only; Docker lab tests are required before making external performance claims.

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
| `failover_gap` | first repaired delivery gap after path failure | <= 500 ms |
| `failover_repair` | repaired chunks after path failure | >= 1 |
| `chacha20poly1305_cpu` | local AEAD encrypt+decrypt cost | <= 300 core-s/GiB |
| `aes256gcm_cpu` | local AEAD encrypt+decrypt cost | <= 300 core-s/GiB |
| `stream_ram_budget` | default per-stream window+repair+reorder budget | <= 64 MiB |
| `datagram_ram_budget` | default datagram queue budget | <= 8 MiB |
| `tcp_path_inflight_budget` | default TCP path inflight budget | <= 8 MiB |

The CPU gates benchmark both ChaCha20-Poly1305 and AES-256-GCM because supported machines vary by architecture and hardware acceleration. The running transport currently uses ChaCha20-Poly1305; the AES gate prevents regressions in the alternative crypto profile the design evaluates for amd64 and aarch64.

## Options

`--strict`
: Exit nonzero when any benchmark gate fails.

`--format text|json`
: Print a human-readable report or machine-readable JSON.

`--resource-sample-mib <N>`
: Set the bounded local crypto sample size in MiB, from 1 through 1024. Manual quick checks can use a small sample for speed; manual local validation can use the default larger sample.

Environment variables:

- `MPTUNNEL_BENCH_STRICT`
- `MPTUNNEL_BENCH_FORMAT`
- `MPTUNNEL_BENCH_RESOURCE_SAMPLE_MIB`

## Build Policy

CI and release workflows do not build or run this benchmark crate. Benchmarks are manual lab checks only. Normal root-level `cargo build`, `cargo test`, `cargo clippy`, target checks, and release packaging build only the product crate and `--bin mptunnel`.

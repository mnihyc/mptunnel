# Benchmarks

`mptunnel bench` runs the release benchmark gates used to protect browsing smoothness, video startup, file-download aggregation, failover recovery, CPU cost, and memory budgets.

```bash
cargo run -- bench --strict
```

JSON output is available for CI artifacts and dashboards:

```bash
cargo run -- bench --strict --format json
```

The command is safe to run on a normal host. It uses deterministic scheduler/runtime models and a bounded local AEAD hot-path sample. It does not create TUN devices, alter routes, change DNS, bind privileged service state, or modify host networking.

## Gates

The current release profile is `release-gates-v1`.

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
: Set the bounded local crypto sample size in MiB, from 1 through 1024. CI uses a small sample for speed; release and local validation can use the default larger sample.

Environment variables:

- `MPTUNNEL_BENCH_STRICT`
- `MPTUNNEL_BENCH_FORMAT`
- `MPTUNNEL_BENCH_RESOURCE_SAMPLE_MIB`

## CI

CI runs:

```bash
cargo run -- bench --strict --resource-sample-mib 1
```

Release packaging depends on the same gate, so archives are produced only after the benchmark profile passes.

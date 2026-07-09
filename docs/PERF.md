# Performance Diagnostics

This project keeps production binaries free of lab-only diagnostics. Internal timing is compiled only with the `lab-diagnostics` feature and is emitted only when the process receives explicit lab environment variables.

Use this workflow when end-to-end Mbps, latency, or failover rows are not enough to explain where time and resources are spent.

## Quick Diagnostic Run

Run a normal daily-use diagnostic first. The wrapper builds an optimized diagnostic binary, runs selected Docker lab cases for a fixed duration, samples container CPU/RAM, and extracts per-component timing from client/server logs.

```bash
CASE_FILTER='mptunnel_mixed_single_balanced,mptunnel_udp_stream_single_balanced,mptunnel_mixed_multipath_all,mptunnel_udp_stream_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=20 \
BUILD_PRODUCT=1 \
BUILD_LAB_IMAGES=0 \
lab/run-perf-diagnostics.sh
```

The script prints:

- `results`: normal JSONL lab rows from `lab/run-heterogeneous-ablation.sh`.
- `component_summary`: parsed `mptunnel_lab_perf` component timing rows.
- `docker_stats`: sampled Docker CPU/RAM lines for client, server, and target containers.

Use a longer interval when investigating steady-state file-download behavior:

```bash
CASE_FILTER='mptunnel_mixed_multipath_all,mptunnel_udp_stream_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=60 \
MPTUNNEL_LAB_PERF_INTERVAL_MS=1000 \
lab/run-perf-diagnostics.sh
```

## What Is Measured

Internal component timing is interval and cumulative. Each line starts with `mptunnel_lab_perf` and includes count, bytes, total microseconds, average microseconds, max microseconds, and PID.

Important component groups:

- `transport.tcp.*`: encrypted TCP frame encode/decode, AEAD encrypt/decrypt, socket read/write wait, and flush wait.
- `runtime.path_queue.*`: queue send time before frames reach path-session writers.
- `runtime.tcp_reader.queue_send`: time to route encrypted TCP reader output into runtime queues.
- `runtime.server_stream.route_frame` and `runtime.tcp_stream.route_frame`: per-stream frame routing queue time.
- `relay.local_read_wait`, `relay.local_write_wait`, `relay.local_flush_wait`: local ingress/egress I/O wait.
- `relay.copy_local_chunk`: payload copy cost from local socket buffers into frame payloads.
- `relay.path_recv_frame_wait`: relay wait for a TCP path frame or QUIC UDP path stream frame to arrive.
- `mux.send_data`, `mux.receive_data`, `mux.apply_ack`, `mux.ack_frames`, `mux.retransmit_*`: reliable-stream bookkeeping, ACK generation, and repair-frame generation.

Interpretation:

- High `transport.tcp.*.socket_wait` with low CPU components points at TCP carrier/network limits.
- High `transport.*.encrypt`, `decrypt`, `encode_frame`, or `decode_frame` points at per-frame CPU or allocation cost.
- High `relay.path_recv_frame_wait` on QUIC UDP path rows now points at QUIC/network wait, remote-side backpressure, or QUIC scheduling/congestion behavior rather than mptunnel overlay pacing.
- High `runtime.path_queue.*` or route-frame time means internal queues/backpressure are limiting throughput.
- High `mux.receive_data` or `mux.retransmit_*` under loss means reorder/repair work is the hot path.

## Per-Component Ablation

Run one scenario at a time when isolating a regression. Keep the workload duration fixed and compare component summaries before and after each core change.

Normal daily-use rows:

```bash
CASE_FILTER='mptunnel_mixed_single_balanced,mptunnel_reliable_mixed_single_balanced,mptunnel_udp_stream_single_balanced' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
lab/run-perf-diagnostics.sh
```

Multipath aggregation rows:

```bash
CASE_FILTER='mptunnel_tcp_multipath_all,mptunnel_reliable_mixed_multipath_all,mptunnel_mixed_multipath_all,mptunnel_udp_stream_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
lab/run-perf-diagnostics.sh
```

Failure and unstable-link rows:

```bash
CASE_FILTER='mptunnel_mixed_multipath_failover_blackhole_fat,mptunnel_mixed_multipath_latency_spike_fat,mptunnel_mixed_multipath_flapping_links' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
MPTUNNEL_LAB_FLAP_SEED=20260710 \
lab/run-perf-diagnostics.sh
```

Keep `MPTUNNEL_LAB_FLAP_SEED`, the ordered flap modes, hold bounds, and effective
netem overrides identical for flapping A/B runs. Confirm the embedded
`flapping.trace_complete` and schedule digest fields before comparing them; seed
reuse does not fix netem's packet-level random loss, jitter, or Docker command
latency, so strict comparisons must also inspect the trace's actual application
offsets.

Matrix rows:

```bash
CASE_FILTER='mptunnel_matrix_*' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
lab/run-perf-diagnostics.sh
```

TUN rows:

```bash
CASE_FILTER='mptunnel_tun_tcp_single_balanced,mptunnel_tun_udp_stream_single_balanced,mptunnel_tun_mixed_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
lab/run-perf-diagnostics.sh
```

## Process-Level Monitoring

The wrapper samples `docker stats` once per second by default. Adjust the interval if the sampling overhead is too high:

```bash
MPTUNNEL_PERF_STATS_INTERVAL_SECONDS=2 lab/run-perf-diagnostics.sh
```

Inspect live process state while a run is active:

```bash
docker compose -f lab/docker-compose.yml exec client ps -o pid,comm,%cpu,%mem,rss,vsz,etime,args
docker compose -f lab/docker-compose.yml exec server ps -o pid,comm,%cpu,%mem,rss,vsz,etime,args
```

For container-level snapshots:

```bash
docker stats --no-stream $(docker compose -f lab/docker-compose.yml ps -q client server target)
```

## Linux `perf` / Flamegraph

For symbolized CPU profiles, build the optimized diagnostic binary with frame pointers and debug info. This remains a lab build and is not part of release packaging.

```bash
RUSTFLAGS='-C force-frame-pointers=yes -C debuginfo=1' \
MPTUNNEL_LAB_DIAGNOSTICS=1 \
MPTUNNEL_LAB_PERF=1 \
CASE_FILTER='mptunnel_mixed_multipath_all' \
KEEP_LAB=1 \
lab/run-heterogeneous-ablation.sh
```

Find the host PID of the mptunnel process:

```bash
container_id="$(docker compose -f lab/docker-compose.yml ps -q client)"
docker top "$container_id" -eo pid,comm,args | grep mptunnel
```

Record a CPU profile from the host:

```bash
perf record -F 99 -g -p <host-pid> -- sleep 20
perf report
```

If `perf` is unavailable or the host denies access to performance counters, keep using `mptunnel_lab_perf` plus Docker stats; those are sufficient to distinguish network wait, internal queue wait, carrier wait, transport CPU, and mux/repair CPU without privileged host changes.

If `cargo flamegraph` is already installed on the machine, it can be used with the same optimized diagnostic build settings. Do not add flamegraph generation to release CI or build scripts.

## Methodology

Use one experiment, one reflection, and one core improvement at a time:

1. Run the fixed-duration release or diagnostic row.
2. Compare mptunnel against direct, VMess, Hysteria2, and MPTCP baselines where the case supports them.
3. Inspect `component_summary` and Docker stats.
4. Decide whether the limiting component is carrier wait, queueing, transport CPU, mux repair/reorder, or local I/O.
5. Make one essential implementation change guided by that bottleneck.
6. Re-run the same case and then a broader normal/mixed/matrix/failover set to prove the fix did not overfit one condition.

This mirrors the useful lessons from mature transports: MPTCP-style reinjection must be checked against head-of-line cost, Hysteria2-style UDP control must be checked against pacing and loss recovery cost, and BBR/BBRv3-style model changes must be checked against delivered rate, queue growth, and latency instead of final throughput alone.

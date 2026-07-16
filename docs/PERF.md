# Performance diagnostics

Lab-only timing is compiled with the `lab-diagnostics` feature and emitted only
when explicitly enabled. Release binaries and packages do not include these
call sites.

Use diagnostics to explain a matched performance row, then rerun the same case
without instrumentation for the accepted measurement.

## Quick run

```bash
CASE_FILTER='mptunnel_tcp_single_balanced,mptunnel_tcp_multipath_all,mptunnel_udp_stream_single_balanced,mptunnel_reliable_mixed_multipath_all' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=20 \
BUILD_PRODUCT=1 \
BUILD_LAB_IMAGES=0 \
lab/run-perf-diagnostics.sh
```

The wrapper records normal lab JSONL rows, `mptunnel_lab_perf` component
summaries, container CPU/RAM samples, and bounded client/server logs.

Run one case at a time when isolating a regression:

```bash
CASE_FILTER='mptunnel_tcp_multipath_all_upload' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=20 \
lab/run-perf-diagnostics.sh
```

## Component boundaries

Interpret timings by architectural owner rather than by an obsolete event
name:

- **Transport CPU**: frame encode/decode, AEAD, copying, and Quinn/TCP adapter
  work.
- **Socket/carrier wait**: TCP read/write/flush or QUIC stream/datagram wait.
- **Carrier command queue**: work waiting before a concrete TCP/QUIC writer.
- **Sender queue**: MPP work waiting for available-first path admission and
  commit.
- **MPP range work**: offset allocation, ACK-range application, retained
  range lookup, reinjection generation, and receive reassembly.
- **Relay local I/O**: ingress reads, target writes/flushes, and target reads.
- **Runtime routing**: frame dispatch between carrier, stream registry, sender,
  relay, and datagram services.

Diagnostic identifiers may change when an owner is reorganized. The durable
correlation keys are direction, stream/flow ID, logical path, physical path
instance, MPP range, usage sequence, and monotonic timestamp.

## Reading a bottleneck

- High socket wait with low CPU and low internal queue delay points to network
  or native carrier control.
- High transport encode/encrypt/decode CPU points to frame size, copies, AEAD,
  or allocation behavior.
- High carrier-command wait points to writer credit, native flow control, or a
  blocked carrier actor.
- High sender wait with idle eligible capacity points to usage filtering,
  metric provenance, path admission, or stale generation rejection.
- High receive range/reassembly cost with growing holes points to harmful
  cross-path ordering or excessive reinjection.
- High local I/O wait means the ingress/target application may be the limit;
  carrier throughput is not then the only variable.
- Traffic growth without receiver-delivery growth points to native
  retransmission, MPP reinjection, measurement traffic, or accounting error.

TCP and QUIC diagnostics remain separate below the MPP range ledger. Do not
compare a TCP ACK clock directly to a QUIC packet ACK or use either as proof of
MPP data-level delivery.

## Causal workflow

1. Reproduce one bounded release row and its adjacent control.
2. State the expected protocol transition: usage selection, data commit,
   `STREAM_ACK`, window release, reinjection, or carrier recovery.
3. Run the same case with diagnostics and container stats.
4. Locate the first missing or delayed transition at its owner.
5. Make one general code/model correction; do not add a case-specific constant.
6. Re-run the diagnostic case to prove causality.
7. Re-run the instrumentation-free matched pair.
8. Run representative upload/download, single/multipath, TCP/QUIC/mixed,
   latency, aggregation, failover, and overhead guards.

Stop when evidence answers the hypothesis. Longer duration is useful only when
the issue is explicitly steady-state, periodic, or tail-distribution behavior.

## Fault diagnostics

```bash
CASE_FILTER='mptunnel_tcp_multipath_failover_blackhole_fat,mptunnel_tcp_multipath_failover_blackhole_fat_upload,mptunnel_mixed_multipath_flapping_links' \
MPTUNNEL_LAB_LOAD_DURATION_SECONDS=30 \
MPTUNNEL_LAB_FLAP_SEED=20260710 \
lab/run-perf-diagnostics.sh
```

Anchor failover to the recorded trigger, not process start. Verify that the
failed physical path instance stops receiving commits, missing ranges are
reconciled exactly, an eligible survivor receives bounded reinjection, and
application delivery resumes. Native TCP/QUIC recovery may continue for copies
already inside those transports.

For flapping A/B runs, compare the actual trace and transition offsets as well
as the configured seed.

## Process monitoring

The wrapper samples Docker CPU/RAM. Increase its interval if collection affects
the case:

```bash
MPTUNNEL_LAB_CONTAINER_STATS_INTERVAL_SECONDS=2 \
lab/run-perf-diagnostics.sh
```

Live inspection:

```bash
docker compose -f lab/docker-compose.yml exec client \
  ps -o pid,comm,%cpu,%mem,rss,vsz,etime,args
docker compose -f lab/docker-compose.yml exec server \
  ps -o pid,comm,%cpu,%mem,rss,vsz,etime,args
docker stats --no-stream \
  $(docker compose -f lab/docker-compose.yml ps -q client server target)
```

## Linux perf

Linux `perf` is an optional profiler, not a product dependency. Build an
optimized diagnostic binary with symbols and frame pointers:

```bash
RUSTFLAGS='-C force-frame-pointers=yes -C debuginfo=1' \
MPTUNNEL_LAB_DIAGNOSTICS=1 \
MPTUNNEL_LAB_PERF=1 \
CASE_FILTER='mptunnel_tcp_multipath_all' \
KEEP_LAB=1 \
lab/run-heterogeneous-ablation.sh
```

Find the host PID and record a bounded profile:

```bash
container_id="$(docker compose -f lab/docker-compose.yml ps -q client)"
docker top "$container_id" -eo pid,comm,args
perf record -F 99 -g -p <host-pid> -- sleep 20
perf report
```

If counters are unavailable, use component timing and Docker stats. A Linux
profile cannot prove Windows/macOS/Android CPU behavior, and Linux `TCP_INFO`
availability cannot be assumed on other targets.

## Acceptance

An optimization is accepted only when the intended owner gets measurably
better and no material guard regresses. Report receiver-delivered goodput,
latency/gaps, traffic overhead, CPU, memory, and path use together. A failed
attempt is evidence about the model; revert or correct it rather than stacking
another heuristic over it.

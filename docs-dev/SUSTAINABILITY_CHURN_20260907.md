# Bounded ownership and post-churn observation

Date: 2026-09-07 01:48 UTC. Scope: existing SEEN restart/RAM sustainability
gate only. No production changes, new instrumentation, limit changes or
long stress run. This is not a release acceptance or attribution of the
uncaptured deployed RAM incident.

## Verdict

The eleven existing targeted ownership tests pass. A two-cycle ordinary
browser-style connection churn observation nevertheless leaves **1,571 live
server Product/admission owners after all 1,932 client requests completed**.
The client has zero active flows. The server retains these owners for the
remaining 67 seconds after the second probe finishes, while its reported
queue, Product flight and native flight are zero. This is live retained
logical/admission state, not an inference from allocator RSS alone.

The executed binary is `./.tmp/reflection/bin/current-ack-rtt/mptunnel`, the
explicitly held composition selected by the main worker, on both endpoints.
This result does not prove the same outcome on pristine HEAD or v0.4.8 and
does not identify which held or accepted change owns the terminal defect.
Do not reopen the already proved journal fix solely from this observation.

## Targeted checks

All checks used the existing `./target` release cache and `-j 3`; no additional
dev build tree was created. The first release lib-test build took 3m15s.
Tests completed before traffic started. The following commands passed:

```sh
CARGO_TARGET_DIR=./target cargo test --release --locked --lib send_buffer::tests -j 3 -- --nocapture
for test_name in control_close_discards_stale_stream_data_and_releases_queue_bytes cancelling_waiting_terminal_reset_releases_queue_debt frame_reservation_owns_byte_charge_from_reserve_through_writer_release restart_reset_terminates_during_blocked_product_write native_flow_registry_bounds_live_state_without_exhausting_on_churn; do
  CARGO_TARGET_DIR=./target cargo test --release --locked --lib "$test_name" -j 3 -- --nocapture || exit
done
CARGO_TARGET_DIR=./target cargo test --release --locked --manifest-path crates/quinn-proto/Cargo.toml --lib migration_rollback_does_not_restore_settled_loss_transaction -j 3 -- --nocapture
MPP_DIAG_LOSS_ROUNDS=8192 CARGO_TARGET_DIR=./target cargo test --release --locked --manifest-path crates/quinn-proto/Cargo.toml --lib loss_budget_journal_compacts_expired_prefix_with_live_suffix -j 3 -- --nocapture
```

The send-buffer filter covers four tests: shared unique-byte ownership and
Data ACK release, unused-permit/stream-drop release, waiter wake, and the
session limit's independence from native path flight. Together with five
single-filter runtime tests and two native tests, this is eleven passes.

The 8,192-round loss journal test ends with three records/two epochs for three
live transactions, record/epoch capacity four each. After the clean round,
both lengths and capacities are zero. This retains the prior journal proof;
it does not certify overall Product lifecycle reclamation.

## Live setup and reproducible commands

Reused owned `mptunnel-reflection-{client,server,router}-1` containers and
`./.tmp/reflection/routed.yml`, existing mixed/server TOML, origin HTTP server,
and `lab/mixed_workload_probe.py`. No outside target was contacted. The router
was reset with the existing shaping helper to 500/500 Mbit/s, 70/30 ms
directional delay, zero deliberate jitter/loss. This isolates ordinary close
and churn from the independently open adverse-link timing owner. Existing
qdisc queue parameters were unchanged.

```sh
env REFLECTION_ROUTED=1 REFLECTION_NO_JITTER=1 REFLECTION_NO_LOSS=1 python3 -c 'import sys; sys.path.insert(0,".tmp/reflection"); import run; run.shape("server","init",46,"500mbit",0); run.shape("client","init",46,"500mbit",0)'
docker exec mptunnel-reflection-server-1 /workspace/.tmp/reflection/bin/current-ack-rtt/mptunnel --config server.toml
docker exec mptunnel-reflection-client-1 /workspace/.tmp/reflection/bin/current-ack-rtt/mptunnel --config client-mixed.toml
docker exec mptunnel-reflection-client-1 python3 /workspace/lab/mixed_workload_probe.py --label sustainability-churn-cycle-1 --mode socks5 --proxy 127.0.0.1:1080 --http-target 127.0.0.1:8080 --browser-only --browser-full-load --small-batch-size 16 --load-duration 25 --timeout 35
# Leave both MPP processes running through quiescence; repeat with label cycle-2.
```

Sixteen overlapping requests are a cheap concurrency/churn discriminator,
not the configured concurrency-limit gate. Each cycle uses the existing
25-second ordinary probe interval, with a 35-second probe timeout rather than
any new Product timeout. The origin's existing small object is 100,000 bytes.
The probe uses `Connection: close`, reads the complete Content-Length body,
and closes each socket (`http_get`'s `with sock` scope); this is not a browser
keeping all 1,932 TCP connections intentionally alive.

The existing management and container samplers ran at one-second cadence
through both cycles and quiet periods, with `COMPOSE_PROJECT_NAME=mptunnel-reflection`:

```sh
python3 lab/management_snapshots.py --compose-file .tmp/reflection/routed.yml --case sustainability-churn-20260907 --output .tmp/reflection/results/sustainability-churn-20260907/management.jsonl --stop-file .tmp/reflection/results/sustainability-churn-20260907/STOP --services client server --interval 1 --token reflection-public-test
python3 lab/container_stats.py sample --compose-file .tmp/reflection/routed.yml --case sustainability-churn-20260907 --output .tmp/reflection/results/sustainability-churn-20260907/resources.jsonl --stop-file .tmp/reflection/results/sustainability-churn-20260907/STOP --services client server --interval 1
```

This deliberately does not use `run.py` to launch the probe: that runner stops
products immediately afterward and would erase the post-load observation.
No MPP restart occurred between cycles. Only the owned MPP processes were
terminated after the final snapshot; origin/echo/sink services and containers
were left intact. The STOP file ended both samplers. The router remains at
the documented clean profile; later timing runs must establish their profile.

## Exact results

| Observation | Cycle 1 | Cycle 2 |
| --- | ---: | ---: |
| Successful/accepted HTTP requests | 956/956 | 976/976 |
| Failed/incomplete requests | 0/0 | 0/0 |
| Probe elapsed | 25.385944 s | 25.358850 s |
| Response payload | 95,600,000 B | 97,600,000 B |
| Peak simultaneous requests | 16 | 16 |
| Request p95 | 454.502 ms | 468.069 ms |
| Maximum request time | 1,452.572 ms | 625.806 ms |
| Client quiet active/admission flows | 0 | 0 |
| Server quiet active/admission flows | 779 | 1,571 |
| Server cumulative completed flows | 177 | 361 |

Cycle 1 finished at 01:44:01.213 UTC. From the first post-probe sample at
01:44:02.450 through 01:44:58.378 the server remains at 779 live owners.
Cycle 2 finished at 01:45:23.686. The final 01:46:30.434 server sample still
has 1,571 active flows and 1,571 admission owners: 792 additional retained
owners after cycle 2. No rejection counter or application failure explains
the difference. The management detail capacity is 1,024, with 547 details
overflowing after cycle 2; the aggregate and admission counts agree on 1,571.

The first retained server flow shows 68 request bytes and 100,204 response
bytes and over 81 seconds idle by 01:44:57.389. Both server quiet windows
have zero reported queue, Product flight and native flight. The final quiet
window (01:45:36 through 01:46:30) also has zero for all three client fields.
During the earlier quiet window the client occasionally reports up to 49,232
native-flight bytes despite zero Product flow/queue/flight; do not relabel
every earlier native sample as zero.

Per-process `ps -C mptunnel -o pid,rss,pcpu,time,etime` captures (RSS in KiB):

| UTC / phase | Client RSS | Server RSS |
| --- | ---: | ---: |
| 01:43:35 / before cycle 1 | 31,316 | 30,404 |
| 01:44:22 / quiet after cycle 1 | 42,968 | 193,608 |
| 01:46:07 / quiet after cycle 2 | 44,196 | 360,724 |

PIDs remain unchanged through all three captures. RSS growth accompanies
retained server logical/admission owners, but these counters do not identify
which allocations dominate the 330,320 KiB server increase.

Container CPU samples include the existing origin and one-second observer
subprocesses; they are not isolated MPP CPU measurements. Baseline means are
10.096% client/11.840% server; the final quiet interval means are
10.728%/14.992%. Earlier quiet means are higher (28.994%/39.664%) and remain in
the raw record rather than being discarded. Peak sampled container CPU is
37.68%/51.88%. Per-process `ps %CPU` is a lifetime average, so it is not used
as an instantaneous quiet-period metric. Container memory is not RSS and is
kept separately in the raw record.

There are no runtime warnings during either load/quiet observation. The sole
server warning is at deliberate teardown (01:46:30.792), reporting peer
`ApplicationClose: H3_NO_ERROR` after stopping the owned client.

## Evidence and remaining boundary

Full records are preserved under
`./.tmp/reflection/results/sustainability-churn-20260907/`:
`cycle-1.json`, `cycle-2.json`, both `.err` files, `management.jsonl`,
`resources.jsonl`, and client/server logs. The management record is about
70.8 MB because it retains the entire existing active-flow detail, including
overflow information; it is not additional production instrumentation.

The next bounded question is why the server's logical terminal owner remains
after these completed, explicitly closed client requests. Do not answer that
by lowering concurrency, introducing an arbitrary expiry, or claiming that
raising a limit fixes it. The component tests prove release mechanics when
the right terminal transition occurs; this ordinary observation shows the
assembled lifecycle does not reach that transition for most requests.

Exact `SessionSendBuffer.used_bytes` is not exported in management. Native
buffers, retained command payloads, shared source charges, diagnostic records
and allocator-resident pages remain distinct owners. Therefore this run proves
retained logical/admission state in the selected composition, not exact heap
attribution, unlimited growth, the defect's introducing commit, or the sole
cause of the user's random deployed RAM incident.

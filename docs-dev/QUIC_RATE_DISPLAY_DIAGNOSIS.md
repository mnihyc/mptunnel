# QUIC rate display versus delivered application throughput

Date: 2026-09-05T16:20:00Z. Examined HEAD: `6636091`.
Status update (2026-09-05T19:23Z): D1 is implemented and verified. Rate shows
native ACK byte delta/time separately from E (retained model) and P (literal
pacing). Quality normalizes measured ACK rates, not model estimates or raw
bytes from unequal intervals; independent windows make multi-path shares
approximate. Direction, missing/zero/stale data, exact u64 deltas and counter
epochs remain explicit. Root2308 tests and live browser checks pass; the final
unequal-interval browser control is recorded in the batch closure report.

Historical diagnosis follows. A real asymmetric local QUIC run
reproduces an approximately 395-Mbit/s native controller estimate while
application delivery is approximately 9.5 Mbit/s under a 10-Mbit/s shaper.
The native component trace identifies a probe-drain/model feedback loop;
the user's external network cause is not established. No production behavior
changed in that initial audit. Later HTB dequeue evidence in
`QUIC_DRAIN_MODEL_DECISION.md` supersedes the earlier restoration inference.

## What the supplied observation establishes

The user reports about 1 MB/s while the peer QUIC row shows approximately
474 Mbit/s, marked stale, with application-limited state true. 1 MB/s is about
8 Mbit/s (8.39 Mbit/s if MiB was intended): roughly a 60-fold discrepancy.
A stale capacity estimate explains the displayed number, not the poor download.
Under sufficient sustained demand through an otherwise unconstrained path,
current delivered traffic should broadly approach usable capacity. A persistent
gap this large must remain an open diagnosis, not be waived as cosmetic.

The screenshot alone does not supply aligned byte deltas, actual source backlog,
per-stream carrier allocation or the blocking layer. Its default mixed-carrier
table also cannot establish that every application byte used QUIC.

## Confirmed implementation semantics

- `QuicPathMetricTracker::observe_shape_at` exports central native C0/Bop into
  `delivery_rate_bps`; this is a controller capacity model, not current Product
  goodput. `3a6d0ea` established one native scheduling authority; the legacy
  field's diagnostic fallback was refined in `b5b4b5a`.
- The server sidecar retains an immutable qualified epoch across application-
  limited/ACK-less polls. Expiry revokes freshness without erasing the last
  known value. This intentionally avoids polling an old sample into freshness.
- Dashboard Quality normalizes displayed rate values within a comparable group.
  For the reported peer numbers, 474 / (474 + 3.77 + 0.285 + 0.0486) is about
  99.1%. This is not measured traffic allocation. The tooltip's description as
  peer-observed delivery-rate share obscures the distinction.
- The peer Use direction is explicitly the opposite of its metric direction:
  requested path usage and sender measurements describe different directions.
  The actual rate direction is only in a tooltip. Identical visible C→S labels
  in local/peer tables therefore do not by themselves prove reversed counters.
- The legacy pacing projection takes `max(native_pacing, estimated_rate)`.
  In the diagnostic below it displays 474 Mbit/s for a native 469 Mbit/s value.
  That is another loss of literal native telemetry, not proof of the 60-fold
  speed cause. This max predates the recent model correction (`7131fc7`).

## Executed diagnostic

`diagnosis_quic_rate_projection_is_capacity_model_not_current_goodput` passes
through the actual observer with a 474 Mbit/s native model and an ACK interval
of 1,000,000 bytes in one second (8 Mbit/s). The exported rate remains 474.
This proves the observer's projection; it does not prove that the real BBR3
controller would produce that model from this single ACK interval.

The initial test expected literal native pacing 469 and failed at that
assertion: actual output was 474 because of the max above. The retained test
explicitly records that existing behavior rather than hiding the discrepancy
by changing the input or changing production code. The existing immutable-
sidecar test also passes and confirms that old high rates survive lower current
poll values while the present app-limited flag is retained.

```sh
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --lib diagnosis_quic_rate_projection -j 4 -- --nocapture
CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 \
  cargo test --locked --lib server_quic_sidecar_freezes_expiry -j 4
```

## Bounded correction and unresolved performance diagnosis

Keep native capacity, actual native pacing, measured carrier delivery, and
unique Product goodput distinct. A traffic-share percentage requires byte
deltas over the same interval and direction, not normalized model estimates.
Display unavailable measurements as unavailable; retain stale estimates with
their explicit age and meaning. Do not change scheduling authority merely to
make the dashboard number agree with one application's speed.

For the sustained gap, compare one synchronized interval across source reads,
MPP per-carrier admission, native ACK delivery, cumulative Product ACK progress,
and client socket writes. Include backlog, credit and oldest missing range at
each boundary. Low native ACK delivery with available source backlog differs
from high native delivery followed by low contiguous Product progress; a slow
target is a third, flow-scoped case. `app_limited=1` is only a clue: Quinn's
transport flag does not independently prove that MPP had no work to submit.

Blindly lowering the native model to current Product goodput could recreate
the previous starvation feedback loop or let one slow target poison a carrier.
Blindly making BBR more aggressive could increase queued data without fixing
the blocking layer. Neither is justified by this screenshot or these tests.
There is no new throughput, competitive-baseline or release acceptance here.

## Real asymmetric reproduction — 2026-09-05T16:47Z

Examined HEAD remains `6636091`; uncommitted code is diagnostic/test-only.
Built the real binary with `cargo build --locked --release --features
lab-diagnostics -j4` and release debug information disabled. Existing
`quic_carrier_ack_poll` logging only was enabled, avoiding per-frame traces.
The build completed in 2m13s. No initial-rate override or congestion parameter
change was made; the configured loss-compensation default remains 10%.

Two isolated Docker containers ran the actual MPP client/server and a local
HTTP source. One fixed SOCKS-proxied download stayed backlogged for 100 seconds.
No other carrier could receive this flow. The Docker network was internal,
with no published ports or host qdisc changes.

| Direction | Rate | One-way configured delay |
| --- | --- | --- |
| Server to client | 400 → 10 → 400 Mbit/s at t=20/70 s | 70 ms |
| Client to server | 300 Mbit/s throughout | 10 ms |

Each container's eth0 used `netem delay ... rate ... limit 8192`.
No random loss or jitter was injected. The finite packet-count queue was deep
enough that final qdisc statistics reported zero drops. GSO means its packet
limit must not be interpreted as 8192 MTU-sized packets or a small byte queue.

| Observation window | Application delivery | Live native model | Native median RTT |
| --- | --- | --- | --- |
| t=3–18 s | approximately 327–379 Mbit/s | median 394.9 Mbit/s | 82.6 ms |
| t=25–40 s | mostly 9.4–9.5 Mbit/s | median 335.7 Mbit/s | 1.888 s |
| t=45–65 s | mostly 9.4–9.5 Mbit/s | 394.95 Mbit/s | 7.631 s |

During t=45–65 s the native median pacing rate was 391.0 Mbit/s and median
congestion window was 19,435,901 bytes. Transport app-limited was false in
all sampled native polls. The interval is entirely before rate restoration,
so its discrepancy does not depend on post-change queue-drain interpretation.
Management also retained 394.95 Mbit/s. Thus the high number exists in the
actual native model, not just an observer fed an artificial model value.

The fixed request had no failed/replacement requests; first body arrived in
164 ms and maximum read gap was 487 ms. This run proves the rate mismatch and
large queued delay, not the user's separate >10-second read-gap incident.
The aggregate 82.65-Mbit/s average mixes high and throttled phases and is not
a useful acceptance score. Raw per-second data, not trimmed averages, is the
primary evidence. The final short bucket is not a link-capacity observation.

At t=70 s the shaper was configured back to 400 Mbit/s, but throughput did not
immediately rise. Netem can retain serialization deadlines assigned before a
rate change; queue dequeue service was not sampled through that transition.
Consequently this run is **not** conclusive evidence of an additional native
post-restoration recovery delay. A follow-up recovery check must measure the
actual bottleneck service/backlog, not assume `tc change` retimed queued data.

### What is measured, and what is not

`native_shape_snapshot` reads the active controller's `metrics()`; it does not
query VPS NIC speed. BBR3 derives an ACK delivery sample using the larger of
send elapsed and ACK elapsed, applies aligned observed-loss compensation,
retains a probe-cycle maximum, and exports the model bounded by its short-term
estimate. With the 10% setting, loss correction alone is at most 1/0.9 = 1.111
times the raw sample; clean samples are unchanged. It cannot alone explain
400 versus 300 Mbit/s, much less 400 versus 10 Mbit/s.

The bounded next causal check is max-filter epoch progress, ProbeBW/ProbeRTT
state, raw ACK sample and operational/min RTT during this same directional
service drop. Existing 10x-drop unit tests use a one-BDP finite tail-drop
queue and restore capacity after model convergence; they do not establish
behavior under this fixed-duration deep-queue case. Neither changing the
loss allowance nor clamping the model to Product goodput is justified yet.

### Artifacts and lifecycle

Local evidence: `.tmp/qos-rate-3k0MAU/` contains the compose/config fixtures,
`download.json` (raw series), `transitions.txt`, filtered native `server.log`,
`client.log`, and `management.jsonl`. Fixtures use known local test credentials,
not deployment secrets. The initial test certificate mistakenly carried CA
capability and was rejected; regenerating a CA:false serverAuth certificate
corrected the fixture. That failure was not an MPP regression.

The collector was stopped and both owned containers plus their isolated
network were removed after the run. No production deployment was contacted.
The source object was a sparse local file, not downloaded Internet content.
Its disposable 8-GiB logical source file (zero physical blocks) and generated
private key were removed after explicit preflight. Evidence logs remain.

## Native causal isolation — 2026-09-05T17:04Z

`diagnosis_deep_queue_rate_drop_retains_capacity_model` reuses the existing
BBR3 `Sim`, driving real send/ACK/end-ACK/pacing/window callbacks. It changes
one directional FIFO service from 400 to 10 Mbit/s at t=20 s, keeps 70/10-ms
forward/reverse propagation, and observes through t=80 s. The component FIFO
is lossless/deep, with no socket, MPP scheduler, dashboard, Docker or netem
involvement. The fixed probe RNG sequence is shared by both attribution arms.

The second arm disables only the added operational-RTT estimate between ACK
callbacks, leaving ordinary flight sizing on raw propagation RTT. It is an
attribution ablation, not a proposed production fix or an upstream binary.
Both arms reproduce: the final raw ACK sample is 10 Mbit/s, exported model
remains 400 Mbit/s, and recorded minimum RTT has risen above one second.
The test asserts these incorrect current outcomes explicitly. It passes in
55.96 seconds in the optimized test profile. The component default has zero
loss compensation and no losses; thus 10% compensation is not necessary for
this failure. The real MPP run used its normal 10% default.

| t (s), normal arm | State | Raw ACK / model (Mbit/s) | min RTT | Window | Max-filter epoch |
| --- | --- | --- | --- | --- | --- |
| 20.1 | ProbeBW Cruise | 391.4 / 400 | 80 ms | 8.0 MB | 5 |
| 25.1 | ProbeRTT | 10 / 400 | 80 ms | 2.0 MB | 5 |
| 30.1 | ProbeBW Refill | 4.59 / 400 | 786 ms | 11.6 MB | 5 |
| 40.1 | ProbeRTT | 10 / 400 | 4.95 s | 24.1 MB | 5 |
| 60.1 | ProbeRTT | 10 / 400 | 16.4 s | 49.1 MB | 5 |
| 75.1 | ProbeRTT | 10 / 400 | 23.9 s | 67.8 MB | 5 |

### Mechanism and calculation

1. A 400-Mbit/s, 80-ms path has 4 MB of BDP. The current ProbeRTT budget is
   half that estimate: 2 MB. After the rate falls to 10 Mbit/s, actual BDP is
   only 0.1 MB. The nominal drain budget still permits **20 times** actual BDP,
   or approximately 1.6 seconds of flight rather than 80 ms.
2. `handle_probe_rtt` arms the hold when flight reaches that stale-model
   budget, not when the bottleneck queue is actually drained. It deliberately
   marks probe traffic app-limited so intentional under-sending cannot lower
   ordinary capacity. This controller-internal flag differs from the native
   transport queue's app-limited flag reported in the live poll.
3. `update_min_rtt` can refresh an expired minimum from a still-queued sample.
   `probe_rtt_cwnd` recomputes its budget using retained high bandwidth times
   that larger RTT. `set_cwnd` may grow by newly ACKed bytes while under this
   inflated target. The probe therefore increases the allowed queue it was
   meant to drain. The operational-RTT extension can also learn queued probe
   observations, but disabling it does not remove the feedback loop.
4. The max-rate filter advances on eligible normal feedback following a
   completed ProbeUP. ProbeRTT-marked samples cannot lower it, and the trace
   never advances epoch 5 throughout the low-service phase. Retained high
   bandwidth and queued RTT consequently reinforce each other.

This is a demonstrated model-composition failure in the current MPP native
controller, not a proof that the remote user's ISP runs this particular
queue. The live run and independent native component establish that neither
dashboard retention nor MPP multipath scheduling is required to produce the
high model / low service discrepancy. They do not attribute every historical
speed symptom to it.

### Introduction, intent and correction boundary

The raw RTT expiry and BDP-derived probe code came with the BBR3 integration
(`d5a7413`); explicit probe-cycle filter boundaries are from `e4ad373`; the
operational-RTT sampling extension is from `95d00aa`. The latter is **not a
necessary cause** according to the ablation. The protected probe samples and
retained maximum intentionally avoid treating controlled under-sending as
evidence of low capacity. The missed composition is a severe actual service
drop without prompt loss feedback, where the nominal half-BDP probe no
longer drains the queue and then grows its own budget.

The [BBR draft-06 ProbeRTT/RTT logic](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html#section-5.3.4.3)
also describes probe sample protection and time-expiring RTT estimates;
its [probe window formula](https://www.ietf.org/archive/id/draft-ietf-ccwg-bbr-06.html#section-5.6.4.5)
is BDP-based. These similarities do not establish that an upstream
implementation reproduces the entire interaction. Do not call this an
upstream Quinn defect without that comparison. This repository's BBR3
integration and modifications are the code actually tested here.

The proposed invariant is confined to probe/drain evidence: the probe must
establish genuinely reduced flight, may not expand its active drain budget
using delay from its own undrained queue, and must allow subsequent normal
evidence to retire an obsolete bandwidth epoch. A bound based on a disproven
old BDP is not by itself proof of low flight. The exact mechanism still needs
a model decision and targeted counterexamples before implementation. Merely
freezing RTT forever would break genuine propagation-delay changes; merely
accepting all low samples would resurrect application-limited starvation.

```sh
CARGO_TARGET_DIR=target CARGO_PROFILE_RELEASE_DEBUG=0 \
  cargo test --manifest-path crates/quinn-proto/Cargo.toml --locked \
  --release --lib diagnosis_deep_queue_rate_drop -j4 -- --nocapture
```

The initial debug-profile run was deliberately stopped for speed and rerun
optimized. An initial workspace-package invocation was rejected because the
vendored crate is not a workspace member; using its manifest corrected the
invocation. Neither event indicates a product failure. Raw model/ablation
traces are preserved alongside the real-run evidence.

# Request cohort correction: incomplete ordinary upload pair

Recorded 2026-09-08 00:31 +08:00; observations 2026-09-07 07:47--07:50 UTC.
Category: practical acceptance withheld; bounded causal evidence, not release
or a measured causal performance gain. Applies the mandatory
[performance method](PERFORMANCE_METHOD_AND_LESSONS.md).

## Comparison and disposition

Baseline runtime `11d6f3a` versus its uncommitted request cohort clock correction,
whose exact scope and 23 focused GREEN checks are in
[the correction](REQUEST_COHORT_CLOCK_CORRECTION_20260907.md). Frozen ordinary
executables: `./.tmp/reflection/bin/late-startup-scope-20260907/mptunnel` and
`./.tmp/reflection/bin/request-cohort-20260907/mptunnel`. Native/event diagnostic
flags were off. The second run is a separate random realization, not the same
packet-loss sequence replayed or a causally conclusive one-pair regression.
The exact unaccepted source/test/RFC diff is preserved in
[the candidate patch](REQUEST_COHORT_UNACCEPTED_20260907.patch); retaining this
evidence is not promoting it into accepted runtime.

| Observation | Baseline | Candidate |
| --- | ---: | ---: |
| Target-confirmed bytes | 109,313,995 | 133,561,732 |
| Locally accepted bytes | 168,296,448 | 191,758,336 |
| Probe duration | 85.895780 s | 85.563214 s |
| First target confirmation | 0.441956 s | 0.491257 s |
| Greatest confirmation gap | 1.004911 s | 2.021790 s |
| Greatest local write gap | 7.147240 s | 2.042942 s |
| Transfer completed | no | no |

Both reach the existing runner's 85-second observation guard. Teardown closes
the tunnel before the sink's terminal acknowledgement, producing the recorded
`upload sink closed before terminal acknowledgement` error. This is not an
independent spontaneous Product reset or a proof of permanent noncompletion.
The roughly 59 MB remaining in each run is nevertheless a real observed
settlement backlog. A quotient of partial confirmed bytes over censored time
is not a completed-transfer rate or an accepted 22.7% improvement.

Stop promotion here: no subsequent QUIC/mixed candidate cells and no controller,
timer, resource limit or topology change. Correct numerical refresh is not
enough to accept composed timing.

## Unchanged experiment

TCP-only: three default native carriers to one server endpoint, not three
independent links. Same owned Docker routed profile and runner `./.tmp/reflection/run.py`;
whole-profile mirroring puts the main impairment on upload. Each direction has
a 500 Mbps configured link; main delay/jitter 70/20 ms, return 30/5 ms. Main
five-second loss epochs are `[3,8,5,6,10,3,5,8]` percent (mean 6%); return epochs
`[1,2,0.5,3,2,0.5,1,2]`. Main QoS is 10 Mbps at 15--25 s; UDP-only blackhole
30--33 s does not disable these TCP carriers. Source load is 40 s, not a claim
that intermediate queues finish at 40 s. FIFO override is off. No initial-rate
override, loss/jitter removal, host shaping, native trace or overlapping build.

Raw probes and all 86 management/socket/router samples per run are preserved
in [the archive](REQUEST_COHORT_ORDINARY_20260907.raw.tar.gz), with paths under
`results/tcp-combined-up-request-cohort-{control,candidate}-0907/`.
The upload probe discards its interval map whenever terminal accounting fails
(`lab/bulk_upload_probe.py`, `ack_accounting_valid` and `interval_metric_fields`).
Both preserved confirmation-bin arrays are therefore empty. Management counters
cannot reconstruct those missing confirmation times or locate the reported
2.021790-second maximum exactly. No fabricated curves are supplied.

## What the counters actually prove

Source inspection establishes these distinct boundaries:

- Client management `io.to_peer_bytes`: successful reads from the local source
  socket, via `ObservedProductIo::poll_read` and `relay/control.rs`. Not native
  transmission or exact carrier allocation.
- Server management `io.from_peer_bytes`: successful ordered writes to the
  target socket, via `ObservedProductIo::poll_write` and `relay/server.rs`.
  Not raw Product receipt/reassembly or target application consumption.
- Client `ss` ACK/Send-Q/notsent: native TCP carrier byte domain, including
  framing/control/copies. Not additive with Product flight/debt.
- Sink confirmation: cumulative target consumption observed back at the
  client. Return-path and scheduling delays remain included.

At the last service sample, control source/target counters are
168,296,448/108,986,315 B; candidate 191,758,336/133,496,196 B. Final actual
client Send-Q totals are 7,703,048 and 21,817,582 B respectively. Thus not all
unconfirmed work can be called native send-queue backlog. The source/target
difference repeatedly equals 64 MiB; that alone is not grounds to lower the
resource allowance or treat resource permission as required placement.

Candidate session `6493328813790544979` maps path/instance `0/3` to client
`:44630`, `1/2` to `:44628`, and `2/1` to `:44644`, all toward server `:7443`.

Two useful bounded observations survive independent read-only audit:

1. **Stale telemetry is not a stopped socket.** Path `0/3` management retains
   ACK 76,432,366, sample time 27,551,408 us and queue 32,807,912 B throughout
   samples 28.005252--50.007632 s. Actual `ss` ACKs rise 76,834,670--87,372,274 B
   and notsent falls 32,423,060--21,999,824 B. A large native queue is real;
   the apparent ACK freeze is not. At the final sample this socket still has
   Send-Q 20,085,116 B and Product debt 20,447,232 B, overlapping inventories.
2. **One carrier's ordered delivery pauses while siblings progress.** At
   samples 39.006448--40.006565 s, target writes remain 108,002,692 B. Server
   `:44628` received remains 23,260,439 B; its client's SACK count rises
   340--692 while cumulative ACK moves one MSS. Other server sockets receive
   another 341,728 and 251,904 B. By 41.006694 s the former socket and target
   writes both advance. This is consistent with a blocked native prefix, but
   does not establish which carrier owns the missing logical Product range.

## Next exact question and falsifier

Map one actually stalled upload's first missing logical range to its original
carrier, any repair, receiver frontier and target-write boundary. Distinguish
slow native ordered service from late/ineffective MPP placement or recovery,
and from an already-received prefix held by a Product actor or target writer.

If the putative owner supplies that exact prefix only when the frontier
advances, the native dependency is supported; if it was already available at
the receiver during the plateau, that explanation is falsified. A high rate
estimate, aggregate sibling ACK progress or finite total backlog cannot decide
this. First check whether existing diagnostic events provide the mapping;
collect only missing discriminating observations, not a new harness or another
broad matrix. This artifact does not assign the slower gap to the sampler.

## Follow-up: exact prefix discriminator, 2026-09-08

One unchanged-candidate, unchanged-profile TCP upload capture used existing
events, with no rebuild or instrumentation/probe edit. Its raw evidence is
[archived here](REQUEST_PREFIX_DIAGNOSTIC_20260908.raw.tar.gz), under
`results/tcp-combined-up-request-cohort-prefix-diag-0908/`.
Enabled events: `sender_service_decision`, `server_receive_hole`,
`server_receive_delivery_stall`, `stream_ack_received`, `tcp_sender_metrics`,
`client_sender_enqueue`, `request_path_stale`, `client_path_frame_error`.
Synchronous diagnostics make this causal evidence, not a throughput comparison.
Again censored at the existing guard: 97,189,623/116,588,544 confirmed/accepted
bytes in 85.616251s; confirmation gap 4.171412s. Products/probe are stopped;
three origin services remain. No subsequent experiment follows this capture.

### Exact observed episode

Logical stream 0; physical mappings remain stable throughout:
client path index 0/1/2 maps to instance 2/1/3 respectively.

| Boundary | Timestamp, Unix ms | Evidence |
| --- | ---: | --- |
| Original `[21364471,21430007)` published on TCP index 1 | 1788798709118 | `client.log:2051`, post-command-publication event |
| Last reassembly progress, frontier 21364471 | 1788798715759 | `server.log:3091` |
| Next reassembly progress, frontier 21430007 | 1788798719935 | `server.log:3619`, explicit 4,176,371 us gap |
| Covering repair | none | Complete client publication trace has none |

The original was published 10.817s before its reassembly release. During the
4.176371s gap, 318 ACK events release 6,946,816 B of other received work and
8,650,752 B of new originals are published across the three carriers. Every
ACK evaluation reports an alternative available, persistent repair not ready,
and zero queued gap repairs. Matching target writes remain 21,364,471 B in
service samples 26--29 s. The receiver's reordered bytes grow from 4,587,520 to
11,665,408 B. This disproves missing source assignment and a completely stopped
Product actor; it does not divide the original's downstream delay precisely.

The first hole in the same capture was different: original `[131072,196608)`
on index 0 already had a partial copy on index 1 published 21ms before server
hole observation. Later stalls must not be generalized to "repair never ran."
Persistent nonempty reorder state likewise does not mean 85s without delivery.

### Proven recovery-evidence mismatch

All 27 complete ACK events have greatest end at most **H = 11,009,783**;
the last is at 1788798692173. A larger complete observation cannot be hidden
by the idempotency early return: `AuthoritativeStreamAckSnapshot::subsumes`
explicitly rejects an incoming horizon greater than stored H. The synchronous
ACK update then reaches the recorded evaluator. Partial positives are correctly
clipped to H only in this negative-authority snapshot; actual cache/flight ACK
release and positive mux frontier advance independently.

Thus, at stalled positive frontier **F = 21,364,471 > H**:

1. Authoritative ACK-gap recovery cannot select the current missing range.
2. Retained tail recovery also requires stored ranges exactly `[0,F)`. Its
   stored prefix is bounded by H, so it returns before original age, eligible
   alternate or service admission. Source closure does not bypass that guard.

The negative-horizon rule is intentional and must remain: a partial ACK's
omission is not loss. `3353d7d` introduced retained negative authority and
`5e1ace6` corrected its horizon to receiver evidence rather than local sent
extent. The composition error is using this narrower snapshot as a prerequisite
for separately valid positive retained-owner fallback. RFC8.3 explicitly keeps
retained ownership above H valid for Product recovery, without declaring loss.
This is not a stateless packed-codec defect or proof that every copy is useful.

Symbolic case: complete `[0,1)`, then partial `[1,2)` and `[3,4)`, leaves H=1,
F=2 and exact retained `[2,3)` on A. Even after A's original recovery interval
and with qualified B available, the equality guard rejects. Correcting only
sparse publication cannot solve the general case: legal multi-frame snapshots
are all incomplete. A focused real-owner RED/control is the next stage before
changing fallback authority. Preserve native ownership, exact copy suppression,
ranking and service bounds; delayed ACKs/shared contention are the adverse
case because a speculative copy may arrive unnecessarily and cost latency.

### Focused retained-tail RED/control

2026-09-08 00:55 +08:00. No recovery implementation change.

```sh
CARGO_BUILD_JOBS=3 CARGO_TARGET_DIR=./target cargo test --release --locked --features lab-diagnostics --lib retained_completion_tail_ -- --nocapture
```

Optimized compilation: 3m09s; two tests: 0.20s, one expected RED and one GREEN,
no ignored cases. The initial compile found two usize/u64 comparisons in new
assertions; correcting those test-only types is not a Product failure.

Both cases commit three 4 KiB cache chunks through ordinary Product admission
and actual carrier command publication. A receiver accepts the first two and
produces legal ACK frames; validation and Product/cache release derive F=8192,
N=12288 and exactly 4096 B retained on the original owner. They wait until the
actual original assignment plus unchanged recovery interval. The alternate's
rate is explicitly fixture-seeded; the real fallback selector must verify
that exact target and sufficient service before the tested entry point.

| Case | H / F | Result |
| --- | --- | --- |
| `retained_completion_tail_survives_partial_ack_frontier_beyond_horizon` | 4096 / 8192 | RED: no recovery queued, no capacity/model-publication block |
| `retained_completion_tail_with_aligned_complete_ack_horizon_control` | 8192 / 8192 | GREEN: exact retained range and target queued |

The two setups differ only in whether the second receiver ACK is partial or
complete. Neither injects H/F, cache release or original flight ownership.
This proves the sender-contract eligibility defect, not actual copy delivery
or a timing improvement. Independent audit confirms the distinction: the
fixture directly uses a legal delta for contiguous receiver progress, whereas
the real TCP sparse producer uses deltas while reordering persists. The live
capture above supplies that producer reachability; the fixture alone does not.

The isolated test-only diff is
[preserved here](REQUEST_RETAINED_HORIZON_RED_20260908.patch). It remains an
intentionally failing test in the working tree, not an accepted runtime patch
or a CI/release pass. Next, correct the retained-owner fallback contract without
widening negative ACK authority, then preserve this RED/control and compare
ordinary timing with the sampler candidate kept independently attributable.

## Isolated retained-frontier correction — 2026-09-08 01:20 +08:00

The sampler source/RFC overlay is shelved; its exact patch and executable are
retained above. The recovery correction has no sampler dependency. Retained
recovery now reads F/N from the positive mux/cache state, while active source
and partial feedback cannot disable an aged exact original frontier. Negative
H and authoritative-gap/path-withdrawal consumers are unchanged.

The two completion fixtures now pass, along with an active-source test using
four ordinarily committed2KiB chunks and receiver ACKs for chunks0,1,3. H=2KiB,
F=4KiB; only the retained2KiB middle hole is eligible after its original clock.
Fresh-owner, queued duplicate, ACKed suffix and final release checks pass. This
uses the shared production actor predicate and sender method, not the full
actor select-loop integration. The prior live trace supplies reachability.

Before accepting the broadened branch, review requested a three-output control:
actual original A, actual accepted B copy, then measured vacant C while B's
immutable deadline D remains future. Without a shared-range guard, it fails:
`RequestCompletionTailEnqueueOutcome { queued: true,
blocked_for_carrier_capacity: false, waiting_for_path_model_publication: false }`.
Target avoidance excludes B but does not suppress another same-range copy on C.
The correction uses the existing exact-frame suppression accessor over all
scoring frames after owner age and before target modeling. It retains full
attachment membership and the existing D wake. The same fixture now blocks C
before D, permits it after D, and preserves B's outstanding Product ownership.

All21 distinct focused tests pass in1.74s. Exact invocation after the release
lib test build (features `lab-diagnostics`, events off):

```sh
target/release/deps/mptunnel-3a813700b0d8f97b \
  retained_completion_tail_ active_request_retained_hole_ \
  retained_frontier_suppresses_ completion_tail_ client_live_tail_ \
  ambiguous_prefix_ack committed_request_copy_deadline accepted_request_copy \
  exact_recovery_copy authoritative_ack_snapshot complete_ack_negative_authority \
  retained_authoritative_ack request_live_tail_uses accepted_copy_wake_ \
  --nocapture --test-threads=3
```

Current closure plan records original intent, tradeoff, ordinary pair and
unchanged global acceptance. The existing request wake may wait for the later
successor floor absent other events; this correction does not rewrite that
policy. No claim that eligibility alone removes the original measured stall.

## Isolated recovery ordinary pair: gate failed/incomplete, 2026-09-08

Recorded 2026-09-08 01:30 +08:00. Category: ordinary composition acceptance
withheld; exact model correction retained separately. The 21 focused GREEN
checks and isolated checkpoint `011aee9` above do not constitute a practical
performance pass. This pair compares frozen baseline `11d6f3a` at
`./.tmp/reflection/bin/late-startup-scope-20260907/mptunnel` with
`./.tmp/reflection/bin/retained-frontier-20260908/mptunnel`. Both endpoints
change together; the unaccepted sampler overlay remains shelved.

The unchanged ordinary `run.py tcp combined up` profile described above uses
`REFLECTION_ROUTED=1`, `REFLECTION_MANAGEMENT=1`,
`REFLECTION_MIRROR_IMPAIRMENT=1`, `REFLECTION_FIFO=0`,
`REFLECTION_RETURN_RATE=500mbit`, and
`REFLECTION_NO_JITTER/NO_LOSS/NO_QOS/NO_BLACKHOLE=0` (each name prefixed
`REFLECTION_`). `REFLECTION_DIAG=0` and `REFLECTION_NATIVE_TRACE=0`;
per-side binary overrides are unset. There is no overlapping build or added
interactive workload. The complete raw probes and 86 service samples per run
are [preserved here](REQUEST_RETAINED_FRONTIER_ORDINARY_20260908.raw.tar.gz),
under `results/tcp-combined-up-retained-frontier-{control,candidate}-0908/`.

| Observation | Control | Isolated recovery candidate |
| --- | ---: | ---: |
| Target-confirmed bytes | 105,447,159 | 122,326,512 |
| Locally accepted bytes | 139,591,680 | 155,058,176 |
| Probe duration | 85.634515 s | 85.815888 s |
| First target confirmation | 0.560093 s | 0.483701 s |
| Greatest confirmation gap | 0.743567 s | 4.033013 s |
| First local write | 0.151015 s | 0.115312 s |
| Greatest local write gap | 2.940043 s | 4.094871 s |
| Transfer completed / completed streams | no / 0 of 1 | no / 0 of 1 |
| Exact accounting / lower-bound accounting | no / yes | no / yes |

Both reach the existing runner's approximately 85-second observation guard,
before the upload probe's 90-second internal completion deadline. Teardown
produces `upload sink closed before terminal acknowledgement` in both runs;
neither proves permanent noncompletion or a spontaneous Product reset. Both
confirmation-bin arrays are empty after invalid terminal ACK accounting. A
partial-byte/time quotient is not completed-transfer goodput, and the larger
candidate partial total is not an accepted gain. Maximum confirmation and
local-write gaps both worsen in this pair. Independent random loss realizations
prevent a conclusive causal regression verdict, but do not waive the failed
ordinary gate. No mixed/QUIC/down acceptance expansion follows this result.

### Wire, native queues and sampled resource cost

Class counters are monotone in both captures. The measurement windows are
service samples `0.000060351--85.138338230 s` for control and
`0.000058550--85.255061149 s` for candidate. Router HTB class `1:10` on `eth1`
is upload toward the server; `eth0` is return toward the client. Raw upload
byte counters are `372245 -> 135434131` and `332305 -> 154506066`; raw return
counters are `16053 -> 4595747` and `13740 -> 4854982`, respectively.

| Sampled observation | Control | Candidate |
| --- | ---: | ---: |
| Upload class-byte delta | 135,061,886 B | 154,173,761 B |
| Return class-byte delta | 4,579,694 B | 4,841,242 B |
| Upload / return class-drop delta | 3,470 / 808 | 3,785 / 766 |
| Peak actual client TCP Send-Q | 8,608,572 B | 13,720,332 B |
| Final actual client TCP Send-Q / notsent | 8,265,656 / 8,051,472 B | 4,143,212 / 3,992,558 B |
| Client RSS first / peak / final | 35,444 / 96,268 / 93,320 KiB | 37,752 / 107,860 / 107,860 KiB |
| Server RSS first / peak / final | 17,364 / 36,100 / 34,972 KiB | 18,112 / 41,212 / 34,820 KiB |
| Final lifetime-average CPU, client / server | 3.0% / 1.5% | 4.0% / 1.4% |

These are directional class-byte proxies, not physical-link accounting or
repair-only amplification. Do not add parent HTB and child netem totals, which
count overlapping work. Normal Product traffic, framing/control, native TCP
retransmission and Product copies all contribute. Management exports no
cumulative recovery-copy counter. Its logical forwarded totals explicitly
exclude retransmission/reinjection; `data_level_bytes_in_flight` tracks unique
Product ownership, not copy volume. The producer in
`commit_enqueued_request_product_send` records `Data`/`OriginalData`, while
reinjection's `None` mutation does not reopen Product flight.

The higher wire totals and RSS coexist with different accepted/received byte
volumes and unfinished work. They cannot isolate recovery cost, and RSS does
not establish a leak. `ps %CPU` is a process-lifetime average, not interval
CPU. One-second sampling can miss peaks and includes no post-quiet reclamation
observation. Service `elapsed` is recorded before sequential endpoint/router
collection; management and socket counters are not simultaneous, and native
management samples can be older than the current status timestamp.

At the final management sample, source-read/target-socket-write counts are
`139591680 / 105250551 B` and `155058176 / 118590960 B`: stage differences
34,341,129 and 36,467,216 B. The later final sink confirmations in the first
table use a different boundary and observation time. Neither whole stage
difference can be assigned to the much smaller actual native Send-Q. Unique
Product flight and native queue inventories overlap and must not be summed.
Source reads first reach their final totals at `61.135889486 s` and
`68.253288234 s`; these are sampled local-consumption boundaries, not exact
source-EOF timestamps or measured EOF-to-terminal drain durations.

### Discriminating late ordered-write plateau

Candidate target-socket writes remain exactly **79,691,776 B** from samples
**49.251307966 through 52.251615069 s**. Source reads remain 146,800,640 B.
Server status timestamps advance `1788801860046 -> 1788801863046` Unix ms;
the whole management snapshot is not frozen. Target writes reach 79,888,384 B
by `53.251712207 s`. This establishes a 3.000307-second sampled ordered-write
plateau, not its exact start/end or an identification of the probe's separately
reported 4.033013-second confirmation gap.

During that window, all three server native TCP sockets make receive progress:
peer ports `34432`, `34446`, `34428` add 1,227,880, 891,720 and 975,888 B,
respectively. Their client cumulative native ACKs also advance. Unique Product
flight decreases `33496032 -> 31821296 B`, a net 1,674,736 B, while management
path `1`, instance `1`, retains 13,078,000 B. Candidate session
`15188167014112505753` and path/instance pairs `0/2`, `1/1`, `2/3` remain stable
in the capture. These facts do not identify which exact logical range or copy
is waiting; aggregate native receipt is not ordered Product receipt.

Router upload class bytes advance `108507366 -> 111978642 B` (3,471,276 B),
with backlog `96872 -> 87824 B`. The current class is 500 Mbps, with 8% loss
and unchanged 70/20 ms delay/jitter. The 10 Mbps phase ended more than 24 s
earlier, so an active 10 Mbps cap cannot directly explain this plateau.
Earlier queued work, ongoing loss/reordering, native ordered service and
recovery placement/cost remain competing causes. The control advances target
writes at every adjacent sample, including 655,360 B over its corresponding
49--52-second window; different random realizations still matter.

Candidate also has a `32.167679--34.249700 s` sampled target-write plateau at
51,970,048 B, followed by a 15,663,104 B target-write jump. Neither episode may
be substituted for the missing exact confirmation chronology. The ordinary
upload has no concurrent echo task or loaded-latency distribution.

The next discriminating observation is one exact stalled logical frontier's
original and covering-repair publication/first-receipt timeline, with stable
carrier-instance identity. It must distinguish no eligible/effective copy,
a published copy waiting behind native service, and an already-receipted
prefix held by Product or target service. Timely covering-copy publication
would refute the old complete-H gate as the sole remaining cause; a prefix
already receipted during flat output would refute transmission-only waiting.
Existing-event diagnostic capture is separately predeclared in
CURRENT_CLOSURE_PLAN; it is causal evidence, not another ordinary throughput
rerun. No claim that a newly eligible copy must arrive faster, no timer/gain
change, and no promotion follows from these incomplete ordinary totals.

## Post-correction exact range trace — 2026-09-08 01:44 +08:00

One unchanged candidate/profile trace is preserved in
[the raw archive](REQUEST_RETAINED_FRONTIER_DIAGNOSTIC_20260908.raw.tar.gz).
It confirms108,265,101/accepts135,856,128B before85.657787s censoring; maximum
probe confirmation gap2.247582s. Diagnostics are enabled only through existing
events; no new instrumentation or runtime change. These are causal observations,
not a replacement ordinary performance result.

| Event | File:line | Wall Unix ms | Exact work |
| --- | --- | ---: | --- |
| Original writer-command commitment | client.log:4668 |1788802168343|[73596928,73662464), TCP index0 / instance2 |
| Ordered mux frontier reaches hole | server.log:7961 |1788802191771|F=73596928 |
| Retained-frontier repair queued | client.log:7965 |1788802191797|F=73596928, N=106889216 |
| First repair writer-command commitment | client.log:7966 |1788802191797|[73596928,73611528), index1 / instance1 |
| Second repair writer-command commitment | client.log:7972 |1788802191998|same14,600B prefix, index2 / instance3 |
| Next ordered mux delivery | server.log:8161 |1788802193871|65,536B, F=73662464, gap2.100765s |

At the first repair H=73,531,392 (client.log:7957), below F=73,596,928. The
formerly rejected H<F case therefore now queues and commits a covering repair
about26ms after the frontier stops. It precedes the next ordered release by
2.074s. Do not subtract the per-process monotonic fields: their origins differ
by43ms. Cross-process wall timestamps give26ms, not69ms.

`sender_service_decision` proves accepted writer-command commitment, not a TCP
socket write or wire departure. The server stall hook runs immediately after
`recv_stream.receive_data`, before onward target delivery, and updates its
reference on EVERY positive result (including unlogged short intervals).
Thus no intervening14,600B ordered mux advance occurred during the2.100765s
gap. Which physical transmission closed the hole is not recorded.

Stable client identities in this session:

| Diagnostic index | Instance | Management wire path_id | Socket local port |
| ---: | ---: | ---: | ---: |
|0 original|2|1|52778|
|1 first repair|1|2|52796|
|2 second repair|3|0|52776|

Index/instance identity is explicit in native events; socket association uses
distinct cumulative-ACK histories, not an explicit fd join. Management wire
path_id is NOT the diagnostic configured index.

The original has a native ordered-receive stall over samples51.079996--54.080289s:
server received stays25,597,846B, client ACK advances only oneMSS, SACK grows
72->900, and server out-of-order memory grows29,888->1,459,696B despite Recv-Q0.
By55.080381s, received jumps1,310,184B and target writes resume. Original-index
telemetry at2193891,20ms after mux release, publishes1,395,304 newly ACKed bytes.
This is consistent with original native recovery but does not identify the
winning Product copy.

Both alternatives have substantial work already in service. Last native queue
publications before copies report2,868,601B (index1 at2191328) and2,828,631B
(index2 at2190643). Bracketing samples52.080096--55.080381s show their server
receive counters advance1,189,624/1,291,592B. Upload router class advances
3,922,758B, backlog90,840->109,008B, rate500Mbps. These sequential observations
do not place either repair at an exact socket byte position; neither a stale
queue estimate nor zero Recv-Q proves no preceding native work.

**Disposition:** the negative-horizon eligibility correction is exercised in
the live pipeline and repair is promptly published. Remaining delay lies after
that publication and before ordered mux processing. Current events do not
divide writer/socket/network service from server routing/actor service. This
joins the already-documented ordered-repair/irreversible-work family, not a
new timer defect. Do not increase recovery gain or reduce buffers from this
aggregate evidence. Full performance acceptance remains withheld.

## Bounded mixed-upload pair: no practical-benefit pass, 2026-09-08

Recorded 2026-09-08 01:55 +08:00. CURRENT_CLOSURE_PLAN explicitly predeclared
this single exception to the stopped matrix: compare baseline `11d6f3a` with
isolated `011aee9` using `run.py mixed combined up`, to test practical value
with the already-existing independent QUIC repair stream. Both endpoints
change together; the mirrored routed profile, 40-second source load, loss,
jitter, QoS and observation guards are unchanged. Diagnostics are off and
the sampler remains shelved. The 30--33-second UDP blackhole now affects the
QUIC carrier; this is not an equivalent TCP-only outage. No third repeat.

Full probes, logs and `service.jsonl` snapshots are preserved in
[the raw mixed pair archive](REQUEST_RETAINED_FRONTIER_MIXED_20260908.raw.tar.gz),
under `results/mixed-combined-up-retained-frontier-{control,candidate}-0908/`.

| Observation | Control | Candidate |
| --- | ---: | ---: |
| Confirmed = locally accepted bytes | 453,967,872 | 295,043,072 |
| Completed streams / failed streams | 1 / 0 | 1 / 0 |
| Exact accounting / valid ACK accounting | yes / yes | yes / yes |
| Probe duration | 51.442681 s | 47.237684 s |
| Completed-transfer goodput | 70.598 Mbps | 49.967 Mbps |
| Elapsed beyond planned 40-second source load | 11.442681 s | 7.237684 s |
| First target confirmation | 0.530640 s | 0.421549 s |
| Greatest confirmation gap | 3.242749 s | 3.400809 s |
| First local write | 0.141209 s | 0.105455 s |
| Greatest local write gap | 5.937819 s | 2.488303 s |

Both transfers complete without probe errors, so these whole-transfer rates
are valid, unlike the censored TCP quotients. Candidate runner elapsed
48.141624 s is not its probe measurement duration. Candidate accepts
158,924,800 fewer bytes; its shorter finish/post-load window is not an
equal-work drain improvement. First confirmation and local-write gaps improve,
but completed goodput falls and maximum confirmation gap worsens. This pair
therefore supplies no practical-benefit pass and does not waive the failed TCP
gate, loaded latency, browser experience or release acceptance. Different
random loss realizations prevent assigning this whole difference causally to
the correction or to a particular QUIC repair.

### Complete confirmation history and stage limits

The following are the untrimmed raw one-second confirmation bins in Mbps,
starting at probe time zero. Bin i covers [i,i+1); the last bin can be partial
but uses a one-second denominator. Values are rounded by the existing probe.
They measure confirmations observed at the client, not contemporaneous wire
service; the candidate's 527.242 Mbps bin is not a measured link-capacity claim.

```text
control, bins 0--51:
0.620,20.115,178.782,130.119,141.033,197.849,36.529,8.080,308.806,144.275,
220.105,138.999,243.303,170.253,144.083,72.809,6.291,24.878,0,0.620,
0,0,26.214,0,0,36.368,7.340,76.133,64.059,245.898,
1.617,0,7.820,0,0,1.145,2.118,0,0,195.153,
102.620,172.823,66.254,0,0.288,0.524,190.839,117.057,34.507,39.801,
36.176,19.438
candidate, bins 0--47:
1.573,6.290,4.719,7.864,4.719,5.767,4.719,3.670,3.670,2.621,
4.194,2.621,3.670,3.670,3.146,4.194,3.670,3.146,4.719,1.145,
0,0,0,0.096,0,527.242,35.799,112.486,227.253,136.219,
87.748,106.578,0,0,19.015,150.995,17.086,90.061,93.656,7.960,
63.675,4.311,2.601,433.251,52.195,0,0,112.332
```

Approximate confirmed MB by phase, summed from those rounded raw bins:

| Probe-time phase | Control | Candidate |
| --- | ---: | ---: |
| 0--15 s, before QoS | 260.369 | 7.864 |
| 15--25 s, 10 Mbps phase | 16.352 | 2.121 |
| 25--30 s, restored rate | 53.725 | 129.875 |
| 30--33 s, UDP outage | 1.180 | 24.291 |
| 33--40 s, post-outage load | 24.802 | 47.347 |
| 40 s through completion | 97.541 | 83.546 |

The dominant early deficit predates QoS. Candidate sample 5.000582 s already
has upload class bytes 83,370,239, source reads 70,319,969 and target-socket
writes only 3,211,105; at 15.001646 s target writes are 7,929,697 B. This
establishes substantial work without corresponding ordered target service,
not the original/copy ownership or precise waiting stage. Phase coincidence
with QoS/outage does not prove a particular repair caused or cleared a hole.

Candidate target writes stay 166,515,745 B across samples
31.137191--34.140121 s, spanning the UDP outage/recovery. Later, every source
byte has been read by sample 44.141154 s; every target byte has been written
and unique Product debt is zero by 45.141255 s. Nevertheless confirmation
bins 45 and 46 are zero and bin 47 acknowledges about 14.042 MB before probe
completion at 47.237684 s. Target-socket acceptance, sink consumption and
returned confirmation are distinct boundaries; this late confirmation tail
cannot be assigned to a still-missing forward Product prefix. Control source
reads reach their final total at sample 42.087222 s. None of these sampled
boundaries is an exact source-EOF or sink-consumption timestamp, and the full
individual confirmation timestamps needed to locate each maximum are absent.

### Wire and resource comparison

Control has 52 service samples, 0.000051520--51.088144554 s; candidate has 48,
0.000046800--47.141465661 s. Monotone router upload class counters are
`327294 -> 608108903` and `534567 -> 413647900`; return counters are
`26185 -> 23175746` and `30230 -> 15863488`, respectively.

| Sampled observation | Control | Candidate |
| --- | ---: | ---: |
| Upload class-byte delta | 607,781,609 B | 413,113,333 B |
| Return class-byte delta | 23,149,561 B | 15,833,258 B |
| Upload / return class-drop delta | 8,218 / 1,586 | 5,612 / 1,205 |
| Client RSS first / peak / final | 49,964 / 320,244 / 289,832 KiB | 55,988 / 300,892 / 264,424 KiB |
| Server RSS first / peak / final | 30,024 / 111,868 / 111,868 KiB | 30,496 / 133,436 / 133,436 KiB |
| Final lifetime-average CPU, client / server | 43.6% / 15.8% | 33.4% / 12.1% |

Wire totals cover different durations and different completed byte volumes;
they include ordinary data, control, native retransmission and Product copies.
No repair-only counter or exact native-copy cost is exposed, and nested
qdiscs must not be summed. Lower aggregate wire/CPU or client RSS cannot be
called efficiency improvements from this unequal-work pair; higher server RSS
does not establish a leak. One-second snapshots are sequential, may miss peaks
and have no post-quiet reclamation observation. CPU is process-lifetime average,
not interval utilization; no concurrent loaded-latency probe ran.

**Disposition:** completion is established in this mixed cell, but overall
timing does not support practical promotion of the isolated correction. The
21 focused GREEN checks and prior prompt H<F repair remain valid mechanism
evidence. Early ordered-work attribution is unresolved; this ordinary pair
does not identify a QUIC repair-bypass episode. Keep the adverse phase history,
stop the matrix, and do not seek a favorable third mean or tune a gain from
aggregate counters.

### Mixed prefix trace: the next missing owner boundary (02:14 +08:00)

The unchanged diagnostic cell `mixed-combined-up-retained-frontier-mixed-prefix-diag-0908`
is archived in REQUEST_RETAINED_FRONTIER_MIXED_DIAGNOSTIC_20260908.raw.tar.gz.
It completes 387,317,760 B in 44.085606 s, with maximum confirmation/write gaps
2.213740/4.151510 s. Logging perturbs scheduling; these are not ordinary
acceptance or a favourable replacement for the preceding pair.

Use the shared Unix clock, not differences between process-monotonic clocks:
client original TCP0/instance1 `[327627,393057)` is committed at
1788803911291; the retained correction commits QUIC0/instance4 repair
`[327627,342227)` at 1788803911911. Server ordered delivery releases the
whole 65,430 B at 1788803912039, 128 ms after repair-command commitment.
The winning original/copy and native write boundary remain unobserved.
This disproves a blanket claim that QUIC is never selected for recovery.

At 1788803913866 another QUIC repair covers `[2686923,2701523)`.
TCP0 goes request-stale at 3874 ms; the next 113 ms publishes 9,488,120 B
of its following retained work onto QUIC through stale-path reinjection.
The server releases the preceding 14,600 B at 3897 ms: that following
bulk handoff did not block this preceding repair. Its effect on subsequent
service is a separate, presently unproven explanation.

QUIC request attachment 4/3 becomes stale at 1788803914227 (client
monotonic 3.055 s). Its next original publication is at 12.610 s,
9.555 s later. Native ACK progress during this interval is not necessarily
current-epoch, unique Product progress. The existing trace lacks final
Product evidence eligibility and the exact requalification probe lifecycle,
so it cannot distinguish a clock inconsistency from local FIFO, forward,
reverse or actor delay. This is the next explicitly bounded question.

The temporary overlay is preserved as REQUEST_REQUALIFICATION_TRACE_20260908.patch.
It records those missing boundaries only; it changes no recovery policy and
will be reversed after freezing its diagnostic binary, before the single
declared follow-up. Publication/H3 acceptance still must not be called wire
departure. No automatic repeat if the exclusion does not recur.

### Requalification trace: false silence localized to ACK attribution

Raw evidence: REQUEST_REQUALIFICATION_DIAGNOSTIC_20260908.raw.tar.gz;
temporary source patch and frozen diagnostic binary are identified above and
in CURRENT_CLOSURE_PLAN. Build1m31s, source overlay reversed before execution.
This independent realization completes500236288B/42.715124s with a4.662451s
maximum confirmation gap. Diagnostic rate93.688Mbps is not an ordinary pass.

The exact partial-copy evidence is independently checked against **all**
intersecting original/copy publication intervals in the client log:

| Exact QUIC original | Only overlapping TCP copy | Full original ACK | Discarded unique bytes |
| --- | --- | --- | ---: |
| `[64028513,64094049)`; line1365 | `[64028513,64043113)`; line5713,3489ms | line5962,3535ms |50936 |
| `[64487265,64552801)`; line1372 | `[64487265,64501865)`; line6319,3606ms | line6375,3620ms |50936 |

There is no other covering publication in either suffix. Both whole-original
releases have `path_proving=false` and `product_evidence_eligible=false`.
The last eligible QUIC progress/reset is lines5550/5551 at3373/3374ms with
249643us persistence. Its stale clock expires at3623ms and withdrawal occurs
at3624ms, only4ms after the second original-only suffix was acknowledged.
These are one client clock domain; the250ms-scale deadline is observed, not
a proposed parameter. Late old-epoch receipts remain legitimately ineligible
after withdrawal, which makes preventing the false entry important.

Source cause: request/flight.rs computes the exact multiply-owned intervals,
then applies `any overlap` to each entire ACKed-original intersection.
This erases adjacent unique progress. Response delivery has the same coarse
path-progress bit despite its separately precise qualification ranges.
The old response partial-overlap test intentionally preserved zero rate bytes
while repairing qualification; it did not validate byte-level stale progress.
It must be explicitly revised, not claimed as an unchanged regression test.

The correct invariant for the pre-release ownership snapshot is:
`proving = eligible_original intersect newly_ACKed minus multiply_owned`.
Partition these sets into nonoverlapping release atoms. Their settlement sum
is unchanged; only the unique atoms may contribute to progress and samples.
The current exact epoch and per-ACK sample aggregation still apply. Frame or
ACK coalescing alone cannot change that attributable union. No controller,
expiry, copy limit, stale persistence or native ownership changes follow.

There is also a separate downstream receipt-delay observation, not bundled
into this fix: QUIC probe3 is published/H3-accepted atUnix1788805128280;
server decodes and queues all return ACKs at8382. Client UDP mailbox receives
at8705 and completes at8706, before expiry8814; actor handles it at8864,
after the pending probe has expired. Probe5 later succeeds within120ms.
Thus local probe FIFO starvation is disproved for these probes; the actor
delay is real in this instrumented execution but does not justify loosening
probe epochs. Its ordinary magnitude and needed correction remain separate.

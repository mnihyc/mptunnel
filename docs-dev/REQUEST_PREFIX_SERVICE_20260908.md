# Post-outage request-prefix service discriminator

Started: 2026-09-08 07:09 +08:00. Base runtime445011f; diagnosis only.
The active scope and acceptance gates remain CURRENT_CLOSURE_PLAN. This is
not a controller change, performance claim or new issue inventory.

## Question and contrary possibilities

The ordinary frontier-scope candidate completes420610048B but has a4.627694s
confirmation gap and6.914621s local-write gap. At33.128065--37.128495s,
source345211123B and ordered target278102259B stay flat,64MiB apart; both reply
counters stay1080B. Native TCP ACKs progress while QUIC ACKs resume late.
Those counters do not identify the missing Product range or its receipt.

Locate the actual first missing range and distinguish: no alternate published,
native work not delivered, server input service delayed, mux awaiting a prefix,
or target socket service delayed after ordered release. Prompt mux release
excludes mux-prefix starvation; timely alternate publication excludes an
eligibility-only explanation. A received suffix cannot settle the prefix.

## One declared observation, not a policy experiment

Reuse the same asymmetric mixed-combined-up profile and existing runner. One
temporary overlay joins exact request commitment, client writer begin/completion,
server decode/admission/mux,
and native timer/probe snapshots. No source-rate, controller, PTO, queue,
quantum, ownership, admission, or scheduling change. Freeze the diagnostic
executable and archive/reverse the hooks before its single capture.

The native-only part of NATIVE_PTO_AND_ATTACHMENT_DIAGNOSTIC_20260907.patch
applies exactly to the current Quinn source. Its unrelated historical
late-STARTUP registry hook is deliberately excluded. Existing ACK_ATOMS and
RESPONSE_QUIC_HANDOFF observers supply suitable Product seams, not a new
harness. Independent review and successful build precede the capture.

## Interpretation constraints

- Successful command commitment is publication, not native transmission.
  Accepted-copy D must come from that actual publication, not be recomputed.
  Its signed remaining microseconds are sampled before the log timestamp, so
  their sum is only an approximate wall-clock deadline. Without the writer
  boundary, publication-to-decode would conflate local and native withholding.
- Exact path identity exists at server registry input, but is discarded before
  mux. Equal-range copies can race: do not invent a carrier for the mux winner.
- Decoded ordinary QUIC and independent repair input are distinct readers.
  A prior awaited mailbox send can delay the next decode. Preserve its interval
  and count/total/max; await includes downstream/executor service, not just
  time when a channel is Pending. Aggregates cannot be assigned wholesale to
  narrower frontier plateaus.
- Native snapshots are at most once per second on existing transmit/ACK work;
  quiet connections create no new wake. Actual PTO/probe events are separate.
  A time-threshold loss timer can precede PTO; exponential PTO code alone does
  not establish the cause of the observed gap.
- Encoded native probes are protocol Transmit production, not OS submission,
  wire delivery or peer receipt. Native ACK totals do not prove exact Product
  receipt; controller ACK counts exclude obsolete-controller ownership.
- Timestamp precision and independent cached management observations prevent
  treating sequential snapshots as an atomic barrier. Match session, stream,
  range and incarnation where actually available.
- Client runtime/physical identifiers and server wire/physical identifiers are
  different namespaces. Likewise Quinn controller generation is not the opaque
  management native epoch. Native CID/side/remote scopes native observations;
  establish a connection join separately rather than equating numeric values.
- Synchronous diagnostics can perturb timing. This run localizes events and
  competing causes; its throughput is not ordinary performance acceptance.
  Existing performance scopes containing these prints include observer cost.

No fix is authorized merely by a high estimated rate, an Active label, a long
PTO, or another average. Preserve negative/ambiguous results and stop promotion
until an actual mechanism is attributable. Broader ordinary/baseline/browser
gates remain outstanding.

## Observer review and capture identity

07:19 +08:00: root and independent reviewers checked the complete temporary
overlay; production send/receive/flush/route awaits, error suppression,
interlocked Full/Closed outcomes, writer barriers and native finalization
arguments remain unchanged. REQUEST_PREFIX_SERVICE_TRACE_20260908.patch
preserves all hooks, including explicit cfg-disabled unrelated call sites.

`request_publication` records successful command commitment and actual D.
`request_writer` records TCP protected transaction begin/successful flush,
or QUIC frame/batch write begin/completion with result. Shared batch rows
bracket one transaction, not independent per-frame service. QUIC failure can
follow partial acceptance; TCP begin-only can mean error or cancellation.
QUIC H3 stream IDs are connection-local, not carrier incarnations.

`request_reader` records TCP, quic_ordinary and quic_repair decode/handoff;
`request_registry` distinguishes begin/admitted/try_admitted/pending/closed.
Interlocked pending completion itself is not observed before mux. `request_mux`
records actual per-item receive_data before target write, including batching.
No observer fabricates the mux winner's source identity.

Request rejection events supplement successful publication, but some early
ServiceBlocked, missing target/snapshot and queue-reservation exits remain
silent. Absence of rejection events is not proof that an eligible path existed.
Native observation uses its separate opt-in flag, activity-driven snapshots,
and actual timeout/probe events. No per-ACK event printing is enabled.

Optimized `cargo build --release --locked -j2 --features lab-diagnostics
--bin mptunnel` completed in2m14s without warnings. Frozen executable:
`./.tmp/reflection/bin/request-prefix-diag-20260908/mptunnel`. Root compared the
complete source overlay with the archived patch, then reversed it with
apply_patch; src/crates are clean before the capture. target/release currently
holds the diagnostic binary, not an ordinary comparator. No lab overlapped
compilation. This is a diagnostic executable, not another performance candidate.

The single `mixed-combined-up-request-prefix-diag-0908` capture started07:21
+08:00 using that frozen binary on both endpoints, DIAG/NATIVE_TRACE enabled,
the same routed/mirrored profile, and exactly these Product events:

`request_publication,request_writer,request_retained_frontier_reinjection,
reinjection,request_path_stale,data_ack_loss_timer,request_path_proof,
request_path_apply,request_product_admission,request_reinjection_admission,
request_native_authority,request_reader,request_registry,request_mux,
server_receive_hole,server_receive_delivery_stall,server_stream_unknown_frame_drop`.

Capture results are pending; no new runtime fix has been made.

## Completed diagnostic: probe and management stages

07:31 +08:00 analysis checkpoint. This supersedes the pending-capture status
above. The single capture completed; it was not censored by the85s guard.
Evidence is the original `probe.json`, `service.jsonl`, `client.log` and
`server.log` in
`./.tmp/reflection/results/mixed-combined-up-request-prefix-diag-0908/`.
Full raw cell: [REQUEST_PREFIX_SERVICE_20260908.raw.tar.gz](REQUEST_PREFIX_SERVICE_20260908.raw.tar.gz).
No additional run, runtime change or model proposal follows from this section.

| Probe outcome | Actual result |
| --- | ---: |
| Complete streams / failed streams / exit code |1 /0 /0 |
| Confirmed bytes = locally accepted bytes |310640640 =310640640 |
| Exact ACK accounting / lower bound / probe errors |true /false /none |
| Elapsed seconds |52.591374 |
| Whole confirmed Mbps, diagnostic only |47.253 |
| First confirmation / maximum confirmation gap seconds |.452167 /10.010333 |
| First local write / maximum local write gap seconds |.146407 /2.200550 |
| Nominal load duration / elapsed beyond40s seconds |40 /12.591374 |

The last duration is end-to-end residual completion time, not exclusively
target-socket drain. Client consumption of buffered local input, native work,
ordered target service and returned confirmation remain distinct stages.
This diagnostic is not a packet-identical ordinary comparator and cannot
promote the prior adverse ordinary result.

### Full receiver-confirmation series

All53 `interval_goodput_raw_mbps` values are retained below, without the
probe's three-bin trim at each end. Zero-based index i identifies the probe's
[i,i+1) one-second observation bin; the terminal bin is partial in elapsed
time but the reported rate retains the probe's1s denominator. These are
increments of receiver-confirmed bytes observed at the source, not router
transmission rates. In particular664.465 and523.764Mbps can represent
buffered ordered/confirmation release, not physical service above500Mbps.
Bins42--50 are all zero; the continuous maximum gap is10.010333s and must
not be replaced by counting empty bins.

| Index | Mbps | Index | Mbps | Index | Mbps |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 1.573 | 18 | 3.146 | 36 | 82.978 |
| 1 | 9.961 | 19 | 2.193 | 37 | 168.717 |
| 2 | 7.340 | 20 | 2.214 | 38 | 7.594 |
| 3 | 7.864 | 21 | 3.553 | 39 | 0.096 |
| 4 | 6.816 | 22 | 1.573 | 40 | 0.270 |
| 5 | 4.290 | 23 | 1.573 | 41 | 12.915 |
| 6 | 4.719 | 24 | 2.001 | 42 | 0.000 |
| 7 | 5.147 | 25 | 664.465 | 43 | 0.000 |
| 8 | 2.717 | 26 | 226.396 | 44 | 0.000 |
| 9 | 3.146 | 27 | 90.702 | 45 | 0.000 |
| 10 | 3.670 | 28 | 43.324 | 46 | 0.000 |
| 11 | 3.574 | 29 | 56.815 | 47 | 0.000 |
| 12 | 2.834 | 30 | 38.273 | 48 | 0.000 |
| 13 | 3.982 | 31 | 64.347 | 49 | 0.000 |
| 14 | 5.339 | 32 | 168.392 | 50 | 0.000 |
| 15 | 3.050 | 33 | 0.000 | 51 | 523.764 |
| 16 | 4.815 | 34 | 106.475 | 52 | 116.356 |
| 17 | 3.050 | 35 | 13.107 | — | — |

### Source, ordered target and both reply boundaries

S = client reliable IO `to_peer_bytes`, local input consumed by the relay;
T = server reliable IO `from_peer_bytes`, ordered bytes accepted by the
target socket; Rs = server reliable IO `to_peer_bytes`, replies read from
the target; Rc = client reliable IO `from_peer_bytes`, replies written to
the local client. S is not the probe's local-accepted counter; T is not raw
native receipt or mux reassembly. All values below are bytes.

| service.jsonl lines; elapsed seconds | S | T | Rs / Rc | Bounded interpretation |
| --- | ---: | ---: | --- | --- |
|3--25;2.000300--24.002590 |68485067→79444179 |1441739→12335315 |78/66→1063/1063 |Only10893576 target bytes advance over22s; S−T stays near64MiB. Early slow forward service predates the15s QoS phase. |
|25--26;24.002590--25.002688 |79444179→146669515 |12335315→79650187 |1063/1063→1102/1089 |67314872 target bytes release between samples; coincidence with QoS ending is not a causal attribution. |
|30--35;29.003102--34.209524 |209083769→218567097 |168796665→196619929 |1323/1239→1519/1351 |Forward target service continues; returned work is also held after server reply-read, with Rs−Rc84→168B. |
|35--37;34.209524--36.209736 |218567097→273952409 |196619929→206843545 |1519/1351→1519/1519 |The168B return backlog clears while target service has its own shorter pause. |
|41--52;40.210134--51.211250 |296089049→300814777 |233705913→233705913 |1687/1645→1687/1687 |Target flat for11 sampled seconds; Rc reaches1687 by L43 and then stays equal to Rs. This is not a produced reply held in the return pipeline. |
|53;52.211366 |310640640 |296103649 |1715/1701 |Last cached sample precedes exact probe settlement;14536991 target bytes remain at this observation. |

The dominant late window has server timestamps1788823313789--1788823324789.
From L45 through L52, S300814777−T233705913 is exactly67108864B.
The corresponding earlier L43--44 separation is62390272B: do not describe
every sample in the plateau as a full64MiB gap. Source consumption after40s
does not prove that the probe generated new work after its load window.

Use each endpoint's `generated_unix_ms` rather than treating nominal elapsed
time as an atomic barrier. L34 records server1788823306790 but
client1788823307788,998ms later; L35 repeats that exact client snapshot.
Socket snapshots likewise have no individual per-socket timestamp. The
exact missing frontier and its winning receive event require the trace join,
not an assumption that T is always identical to mux F.

### Native progress and retained ownership during the late window

Session8246511402058504553 has stable client path identities:

| Client writer index | Wire PathId / physical instance | Management native epoch |
| --- | --- | --- |
|TCP0 |1 /4 |6833557844859355867 |
|TCP1 |2 /2 |4099839052147457605 |
|TCP2 |0 /3 |15247622118075293584 |
|QUIC |0 /1 |8473231463543338147 |

Writer events establish TCP index/PathId/instance joins; these are not assumed
ordinal equalities. The native epochs above are management opaque identities,
not Quinn's internal controller_epoch0.

At L44/t43.210420, QUIC Original Product debt and native flight are both0,
with cumulative native ACKs287588959B. T nevertheless remains233705913B.
Through L51, remaining Original debt is TCP physical4=524288B and
physical3=1572864B; physical2=0. This excludes explaining the entire late
pause as continued outstanding QUIC Original debt. It does not identify the
missing range or establish spare repair permission.

Direct `ss` observations show continued TCP service during L41--L52 despite
several repeated cached management native snapshots:

| Client local port | Client ACK delta B | Client sent delta B | Server received delta B | Client Send-Q B, endpoints |
| --- | ---: | ---: | ---: | --- |
|58584 |4944388 |5942000 |4973208 |2980204→3444996 |
|58614 |2852220 |3551596 |2853392 |3891552→3777312 |
|58600 |3992640 |4723792 |4104132 |3516080→3629780 |

Repeated exact cumulative-ACK fingerprints map58584 to PathId0/physical3 and
58614 to PathId2/physical2;58600 is the remaining PathId1/physical4 by
three-carrier elimination. This socket mapping is not an independently logged
attachment/port binding. Server Recv-Q is0 throughout these sampled late rows.
Client queue endpoints are mostly `notsent`: respectively2834244→3381916,
3887284→3644096 and3407712→3554564B. Thus whole-native silence is falsified,
but these encrypted-carrier counters cannot assign a queued payload range,
distinguish originals from copies, or prove exact prefix delivery.

### Bounded resource and wire context

| Sampled process | First RSS KiB | Peak RSS KiB; elapsed s | Last RSS KiB | Maximum observed ps %CPU / last |
| --- | ---: | --- | ---: | --- |
|Client PID255053 |53588 |407540;38.209941 |339204 |49.3 /41.7 |
|Server PID261175 |30584 |159868;52.211366 |159868 |45.2 /21.1 |

The CPU fields are lifetime `ps` percentages, not interval CPU utilization or
CPU-seconds. There is no quiet post-completion RSS/backlog reclamation series;
the last service sample precedes final completion. Logging overhead is included.

| Router class1:10 direction | First→last bytes | Delta bytes | Drop delta | Packet delta |
| --- | --- | ---: | ---: | ---: |
|Upload, router eth1 |558619→404462356 |403903737 |5241 |332310 |
|Return, router eth0 |29018→13834855 |13805837 |993 |81869 |

These are first-to-last sampled whole-class counters, including framing,
control, retransmission and any recovery traffic—not repair-only overhead or
a complete physical capture through teardown. They do not support normalized
cost improvement against an unequal-work, differently realized ordinary run.

Disposition: exact diagnostic completion and a concrete late forward-stage
stall are established; practical performance promotion remains stopped.
The native/exact-frontier joins below must determine the narrower causal
boundary. No controller, placement or queue correction is inferred here.

## Exact prefix joins and native recovery

07:37 +08:00. Root and independent agents joined the complete logs; all8575
successful publications were scanned for overlap, not merely equal offsets.
The following row references are one-based original log lines. Timestamps
below are Unix milliseconds with the common prefix178882 omitted.

| Exact missing F | Publication and native writer | First covering server input | Actual mux effect |
| --- | --- | --- | --- |
|206843545 |QUIC Original[206843545,206855545), client32059/32063/32064 at3302404; batch locally accepted |ordinaryQUIC server93617 at3310534 |server93973 at3310546;12ms after decode |
|233705913 |TCP Original[233705913,233771449), client73221 at3307963; writer/flush254330/254332 at3317116 |this TCP arrives only atserver137998/3325622 and loses |QUIC repair below has already advanced F |
|233705913 repair |TCP1 copy client257130 at3324669,Dremaining199997us; then QUIC copy257620 at3324884,Dremaining171071us; H3stream8 accepted257626/257627 same ms |QUICrepair[233705913,233717913), server136799 at3325367 |137220 at3325383;16ms after decode |

F206843545 is held3307792--3310546,2.754s. There is no earlier covering
repair publication. All51 TCP copies published during that hold address
earlier188046713--188585601 or194784921--196631929 ranges. The preceding
ordinaryQUIC reader handoff is3304168; the6.366s interval before decode has
zero intervening nondata send waits. Original local acceptance->decode8.130s
is before this server reader, not an input-mailbox residence claim.

F233705913 is held3313539--3325383,11.844s. Its Original spent9.153s before
the local TCP writer transaction, then8.506s until server decode. The first
covering copy is not published until11.130s after the frontier hold begins;
there is no earlier accepted-copy deadline to explain that interval. Its
200ms suppression is consistent with the later QUIC publication215ms afterward;
it does not explain the preceding wait. The winning QUIC repair takes499ms
from publication to mux; server input accounts only16ms of that.

All28499 server registry transactions are normal begin->admitted; no pending
interlocked ambiguity occurred in this capture. During the long hold,
1295 QUIC-repair frames decode, with96.916ms summed route-await/max4.071ms;
ordinaryQUIC reader-send awaits sum256.170ms/max149.507ms; TCP waits sum372us/
max27us. These aggregates do not establish exclusive CPU or channel-Pending
time. T equals the actual stalled F, excluding a post-mux target hold.
Another TCP frame[195358937,195424473) closes F195394937 at3304803, with
decode/registry/mux in the same millisecond. Not every frontier waits on QUIC.

One native connection joins both sides by CID
6bd9b086757afea4190a4f0d13fa3b6badc43663 and peer socket context. Its controller
generation remains0; this is not a management native-epoch equality join.
Client native PTO queues occur at connection-relative seconds31.409624,
31.600536,31.982524,32.742118,34.259876 and37.296086, with PTOcount1--6.
At those expirations loss_time=None and the armed timer matches the computed
PTO. SRTT42.411ms/variance6.845ms produce69.791ms base plus25ms ACK delay
before exponential backoff. Each grants two probes; emission events observe
encoded probe-granted datagrams, not wire receipt. No earlier time-threshold
loss timer explains those particular expirations.

Native ACKcount26937/controller-ACKbytes197793745 remain unchanged through
the37.295325s snapshot despite later authenticated incoming packets. By38.295382s,
ACK progress resumes and PTOcount0. Later QUIC native flight is0 at42.627838s
and47.088327s; controller-ACKbytes287588959 stayflat until new work near52s.
Thus native backoff is visible during outage recovery, but cannot explain the
whole later11.844s missing-prefix pause. Exact outage removal time cannot be
reconstructed at microsecond precision from cached management timestamps.
There is no basis here for shortening native PTO or changing BBR.

## Next bounded model question, with attribution limits

TCP owner0 appears in stale-owner queued=false passes at3313672,3314539,
3315539,3316539,3321308,3321552 and3324655 while F233705913 is held.
Such events specifically mean overlap with already-queued repair, after
target/range selection. They do not expose the queued offset or sender F.
The last successful retained-frontier enqueue logs F228921785 at3311941.
Later TCP copy commits require non-stale target eligibility; earlier stale
events do not prove all TCP stayed stale for the whole interval.

Source audit identifies a separate testable composition: retained frontier
fallback excludes stale Original owners, while stale/detached recovery assigns
finite target service in historical qualification-entry order. A suffix owner
visited first can charge all available service before the earlier prefix owner.
The useful existing per-target accounting, exact stale qualification and
no-unbound fallback are not defects to undo. A real-producer, order-reversed
RED/control is declared in CURRENT_CLOSURE_PLAN before any correction.

This is not yet full causal attribution of the11.844s capture: queued offsets,
sender F and silent no-target branches were not observed. Keep that distinction
even if the source counterexample passes. The232219 reinjection rows also
demonstrate substantial diagnostic volume; do not reuse this filter casually
or promote its durations as ordinary regressions. No new runtime fix or
public performance claim is accepted at this checkpoint.

## Actual-producer recovery-order RED and control

2026-09-08 07:52 +08:00. Test-only transaction on unchanged runtime445011f;
root coordinated compilation and execution. No production correction, native
capacity change or additional network experiment was made.

The first fixture was invalid: all three new cases stopped at
`tests_request.rs:900`, asserting that an empty startup target's current repair
credit was smaller than `max_repair_bytes`. Its actual model grants the full
configured repair envelope. That compile took1m32s; two existing controls
passed, but none of the new cases reached dispatch. This was a fixture failure,
**not Product RED**. We removed the unsupported premise instead of inventing a
measured rate, reducing a resource limit or forcing native credit to fit it.

The corrected shared fixture uses default three-TCP context, actual
`ReliableSendStream::send_data` cache production and the existing exact
OriginalData-flight recorder. A and B retain disjoint ranges; C is the real
fresh alternate. Both Original owners are independently recovery-eligible,
the positive frontier is0, and no accepted-copy suppression deadline exists.
Only the qualification-entry insertion order is reversed between the first
two cases. The interleaved case retains A/B/A ownership in byte order.

The quantum is derived from unchanged configuration:
`q = min(reliable_relay_buffer_len(limits), max_repair_bytes / 3) =524288B`.
The real target snapshot is checked against
`K_C = min(max_repair_bytes, total_retained_bytes)`:1048576B for A/B and
1572864B for A/B/A. **All retained ranges fit this startup credit.** Thus this
test proves ordering of first native command handoffs, not finite-credit
exhaustion. Before checking offsets, it runs the real
`drive_request_path_recovery` and `dispatch_client_queued_work`, observes the
actual targetC command receiver, and checks exact target-scoped queued bytes.

| Test | Geometry / entry order | Actual result |
| --- | --- | --- |
|`request_recovery_prefix_first_owner_order_control` |A[0,q), B[q,2q); entriesA,B |PASS: first repair offset0 |
|`request_recovery_suffix_first_owner_order_preserves_lowest_prefix` |Same bytes/credit; entriesB,A |Intended RED at final offset assertion: expected0, actual524288 |
|`request_recovery_interleaved_owners_preserve_global_range_order` |A[0,q), B[q,2q), A[2q,3q); entriesA,B |First offset0 passes; second intended RED: expected524288, actual1048576 |

The coordinated `request_recovery` filter ran five tests: these three plus
`request_recovery_skips_exhausted_fast_target_for_free_second_target` and
`committed_request_recovery_copy_survives_target_drain_until_retry_deadline`.
The prefix-first and both existing controls passed; only the two intended
ordering assertions failed at `tests_request.rs:1008`.

This demonstrates a real historical-owner-order dependency at recovery
publication. The interleaved counterexample rules out merely sorting owners
by their first retained offset: that still sends A's later range before B's
earlier range. Original ownership is established by the existing component
flight fixture, not by transmitting Original TCP packets over a network;
repair command receipt is likewise local native handoff, not wire delivery or
receiver completion. These results do not attribute the entire captured
11.844s stall, establish a wall-clock gain, or prove credit starvation in this
fixture. A coherent byte-obligation-first correction must preserve exact target
admission, accepted-copy ownership/deadlines and independent-target progress;
focused GREEN and ordinary timing still remain required.

## Candidate model, declared before implementation

2026-09-08 08:03 +08:00. Independent reviews confirm reachable local ordering,
not a universal advantage: if an earlier Original is about to arrive, repairing
a later missing range first can finish the whole stream sooner. Without that
future knowledge, historical qualification insertion order is not service
evidence. For a serial target serving C bytes/s, an unnecessary q-byte suffix
placed first adds q/C seconds before the lower repair, absent other arrivals.
This is a conditional prefix-service benefit, not a predicted Mbps gain.

Chosen candidate: remove bulk stale/failure payload queue materialization.
Keep obligations in existing retained cache/flight ownership. At each existing
bounded Dispatch batch, collect due range metadata once, merge by offset and
consume a transient cursor. A candidate keeps its exact Original owner/cause;
target selection and Apply retain current eligibility, accepted-copy exclusion,
immutable D, exact K/J, regular-before-backup order and native reservation.
After commitment, another range may immediately use remaining service within
that same batch. No ACK-per-frame wait, smaller structural window or new timer.

Unpublished suffix bindings disappear from this branch, rather than adding
another persistent queue planner. Direct recovery includes all existing queued
live repair in target accounting and overlap exclusion, but must not subtract
the unrelated queue front as if it were the direct candidate. Recovery traffic
accounting records successful direct commitment once, not every observation or
failed reservation. Accepted/native work remains irreversible and untouched.

Readiness/deadline/capacity state already owned by the relay must schedule a
recovery batch even with no ordinary queue item. Carry forward existing copy
deadlines, capacity/model wakes, error and terminal handling; a batch stopped by
the existing cooperative item/byte budget remains ready. A target blocked at
Apply must not force a later range onto that same blocked output or hide an
independently usable target. No target leaves debt in the ledger, not an unbound
repair or spurious session-close error. Do not rebuild the full ledger for
every frame: collect once per batch and discard its metadata afterward.

Required discriminators before promotion: order reversal; interleaved owners;
newly due lower range after an unpublished later plan; same-range queued/accepted
copy suppression; unavailable target with an independent usable target; filling
the unchanged structural service allowance within one dispatch batch; existing
qualification/removal/copy-deadline and terminal controls. Then one affected
ordinary comparison with full completion, first service, gaps and cost. Any
adverse result stops promotion. This proposal does not close every captured
pause or authorize new controller/profile/threshold changes.

### Reasoning obligations for the candidate

Within a serialized batch, the ordered range cursor is monotone in Product
offset. For a currently usable exact target, the first attempted eligible
range is therefore no later than another eligible retained range that could
use that same service. Committing advances only the emitted range slice;
accepted-copy flight establishes its existing D/J before native publication.
Subsequent slices can use remaining K immediately. A target that rejects
reservation is excluded while attempting the current uniform range, not
requalified or assigned synthetic capacity. The exclusion resets when that
range is consumed or skipped: a different range may have different ownership
or admission. Different usable targets remain eligible. Rejected attempts can
therefore cost ranges times targets, not targets once per batch; the published
item budget alone does not bound those attempts. Metadata is discarded at the
batch boundary. No persistent future suffix owns K.

This argument assumes actual target selection/Apply enforce current ownership
and the cursor does not skip a lower eligible slice because of a later slice's
ownership boundary. The implementation and independent audit must check that
premise rather than declare the model proved by sorting alone. Existing queued
live repairs and accepted copies still constrain eligibility. No guarantee is
claimed for already accepted native work or unknown future Original arrivals.

Discovery work is once per existing batch, not per emitted frame. Frame lookup
must reuse the non-overlapping cache tree's existing predecessor/next search,
then slice at most one chunk; otherwise calling the old bulk cache collector
per frame would recreate full-ledger work. A direct equivalence check uses
actual send/ACK clipping and the old bulk collector's first result as oracle,
including chunk boundaries, ACK holes, empty credit, empty cache and off-end
ranges. This accessor adds no capacity, timer, persistent state or new byte
geometry. The unchanged cooperative dispatch budget bounds publications, not
all failed attempts or discovery work; ordinary CPU/service timing must
determine whether batches are cheap enough in practice.

### Unpublished suffix discriminator

2026-09-08 08:01 +08:00: functional rebuild1m07s on unchanged runtime445011f.
The new `request_recovery_prequeued_suffix_yields_to_newly_due_prefix` also
fails at its intended native-command offset assertion:524288 instead of0.
The six-test filter has three passing controls and three intended ordering
failures. Initially only B is stale; the lower owner A is explicitly not a
structural candidate. With identical default path snapshots, C is placed first
in attached iteration to resolve an actual equal-score tie, and selection is
asserted to choose C. The first drive queues only B's suffix; no repair command
or accepted-copy deadline exists. After A becomes stale, another drive still
leaves that unpublished suffix ahead of A. This is why sorting only newly
generated ranges would be incomplete. It is not an expired-copy, native
capacity exhaustion or timed-network test.

### Implementation review before the first candidate build

2026-09-08 08:28 +08:00. Source review is complete; compilation and tests are
still pending. This is not performance acceptance.

The transient plan splits at attached accepted-copy coverage and queued-live
repair boundaries, in addition to original ownership. It does not split on
retired-copy storage history. This prevents a queued middle copy from hiding
uncovered prefix/suffix bytes, and preserves exact copy identity when a cache
chunk is sliced internally. The shared legacy exact-start accessor is not
changed. Cache lookup emits one predecessor/next-tree slice, not a bulk suffix.

Review removed a redundant per-frame suppression scan that walked old flights.
Collection already excludes current live-copy deadlines. The batch is consumed
inside serialized Dispatch; no external ACK, qualification or membership event
interleaves. Its own successful copies cover monotonically advancing disjoint
slices. Queued-copy dispatch follows completion of that cursor; the batch is
discarded at the cooperative boundary. Expiry can add future eligibility but
cannot invalidate a currently due slice. Exact final native admission, queued
debt and target exclusions remain. This removes duplicate work without relaxing
copy authority or introducing another deadline.

Direct publication also changes the owner of readiness. Capacity/model waits
are armed before reservation and carried across a blocked Dispatch, so a release
before the next select cannot be lost. A partial batch remains dirty. Review
caught a second exclusion-lifetime gap: cancellation/defer of a queued copy,
or preparation pruning of one, must make the retained obligation discoverable
again. Both Dispatch removal outcomes now mark recovery changed; actual pruning
sets dirty and same-pass requested, clearing the existing retry. No guessed
fallback timer or unrelated future event is required.

Repairs retain the existing item-only dispatch charge (ordinary Data alone
charges the payload-byte budget). Direct work includes the unrelated queued
front in target accounting. FIN may declare final_offset before retained repair
arrives, but final retirement still requires retained bytes zero and both
terminal halves. Producerless structural-queue cancellation code is removed;
live/persistent queued recovery and direct target-bound causes remain.

Independent source review found no remaining blocker in this mechanism after
these corrections. Required controls include actual queued exclusion removal,
native capacity release before first wait poll, partial overlap, independently
serviceable later ranges, direct queue accounting, and cache oracle equivalence.
These guard code paths, not a promise that the full recorded stall is solved.

### Focused candidate verification

2026-09-08 08:36 +08:00. The first build stopped on a missing RuntimeError
test import and the removed lifecycle helper's unused queue import. No test
ran and this was not Product RED. Those import-only corrections were applied;
the coordinated functional rebuild completed in1m08s without warnings.

The existing library test binary ran485 distinct checks in1.26s: request sender
(including multipath and all three former ordering REDs), sender queue, relay,
request flight, mux stream, plus native TCP in-flight receive after FIN and
QUIC bidirectional repair-companion half-close controls. All485 pass; none are
ignored. New blocked-target/independent-range, queued partial overlap, actual
queued cancellation, prearmed capacity wake, unrelated queued-front admission,
full structural service and cache-slice oracle controls pass within this set.
The cancellation test uses the real queued removal then fresh collection;
actual actor dirty propagation additionally has source review, not a claim
that this component fixture executed the complete actor loop.

Commands (functional optimizer override is not used for performance):

```sh
cargo test --release --locked -j1 --config 'profile.release.package.mptunnel.opt-level=0' --lib --no-run
target/release/deps/mptunnel-29ba8ebb1d3edd33 runtime::sender::request runtime::sender::queue runtime::relay runtime::stream::request mux::stream::tests client_tcp_path_routes_inflight_receive_frames_to_live_stream repair_companion_is_bidirectional_survives_half_close_and_fails_only_attachment --quiet
```

Independent scoped source review and focused GREEN establish this mechanism's
disposition only. The ordinary optimized binary is building for the predeclared
comparison; no runtime performance, release or whole-stall attribution yet.

### Pre-lab rejection: preserve the earlier queued-overlap work correction

2026-09-08 08:43 +08:00. Ordinary build completed3m36s but the binary is not
being measured or promoted. New unused-helper warnings caused root to inspect
introducing history614dc73 before deleting its obsolete interface/tests.
REQUEST_RECOVERY_OVERLAP_WORK_MODEL records a real old2,098,176 versus2,048
operation-count RED and34.143s overlap/enqueue profile. The candidate's per-frame
`has_queued_reinjection_overlap` would reintroduce Q-times-R overlap traversal.
Passing485 tests missed it because the old count test still exercised the now
unused helper. Removing those tests silently would hide, not resolve, this
candidate regression. Both independent reviewers agree with this correction.

Exact model: collect and normalize the existing queued union U once. Split due
metadata at U's endpoints; each resulting range lies wholly inside or outside U.
Record that boolean with the range. Every subsequent cache/credit-sized slice
inherits it. The queue does not mutate during the direct cursor's lifetime;
queued dispatch begins only after exhaustion, and a cooperative boundary or
ACK/prune/cancel requires fresh collection. Thus whole queued ranges can be
skipped before cache lookup without per-frame rediscovery. No capacity, traffic
allowance or exclusion lifetime changes. Exact target-byte accounting still
reads queued debt; this restores overlap discovery only, not globally linear
total Dispatch cost.

Before implementing, the test agent adds a small actual collector/direct-path
overlap-visit discriminator to the existing partial-copy fixture. It must fail
on this candidate's repeated overlap walk while preserving partial-overlap
semantics. After correction, migrate the old helper-specific controls instead
of retaining a producerless algorithm to claim GREEN. This is the same candidate
transaction and a previously solved cost boundary, not another transport fix.
No ordinary comparison, parameter change or additional global audit is started.

The actual-path counter RED completed at08:46 +08:00. Its first compile stopped
on a wrong relative module path in the test-only counter call; root corrected
it to the existing crate-qualified module. That is not Product RED. Rebuild
1m07s then ran five direct-recovery checks: four pass; only the partial-middle
case fails at the intended work-count assertion, after exact prefix/suffix
native commands, queued-middle integrity and accounting assertions all pass.
Collection visits one queued extent once: `(snapshot, scalar)=(1,0)`.
Subsequent direct Dispatch visits are `(0,3)`, expected `(0,0)`. This proves the
duplicate work on the real caller, not only an unused helper. Required target
debt/accounting scans are deliberately not instrumented. Test-only counters
are thread-local and the fixture uses Tokio's current-thread runtime.

Approved correction now replaces that scalar query with the captured uniform
range's queued bit and skips the whole covered range. The obsolete helper and
its two algorithm-only tests can then be removed; actual-path count/semantics
remain. Test-fixture-only enqueue/query wrappers become cfg(test), with enqueue
delegating to its existing production priority method. Earlier unrelated
dead-code warnings remain outside this patch. Renewed focused GREEN and an
ordinary optimized rebuild are required before the declared network pair.

Renewed verification: functional rebuild1m08s;483 current focused checks pass
in1.26s with none ignored. The actual direct-path counter is now `(1,0)` during
collection and `(0,0)` during dispatch, with both uncovered native commands and
queued-middle/accounting assertions intact. The count is two below the previous
set because the abandoned helper's two tests were removed, not waived. This
does not claim the old64-shift oracle was duplicated; normalization's existing
contract, uniform-range proof and actual consumer controls support equivalence.
Independent scoped review passes. An ordinary optimized rebuild is running;
the prior repeated-scan binary was never used for a performance claim.

## Ordinary comparison: timing mixed, promotion withheld

2026-09-08 08:58 +08:00. The declared control then candidate both completed
without errors, exact local-accepted and target-confirmed byte equality and
valid ACK accounting. Control uses445011f; candidate uses this request-ordered
dispatch change at both endpoints. Ordinary optimized rebuild3m23s, then no
build/lab overlap. No diagnostics, native trace, sampler overlay, profile or
congestion change. Raw probes, all96 confirmation bins,51/45 management/process/
shaping samples and logs are in REQUEST_PREFIX_ORDINARY_20260908.raw.tar.gz.
Live result directories are
`./.tmp/reflection/results/mixed-combined-up-request-prefix-{control,candidate}-0908/`.

| Outcome | Control | Candidate |
| --- | ---: | ---: |
| Exact confirmed/accepted bytes |475529216 |427360256 |
| Completion seconds |50.507429 |44.919042 |
| Whole confirmed Mbps |75.320 |76.112 |
| First confirmation seconds |.493971 |.435791 |
| Maximum confirmation gap seconds |6.834457 |2.829451 |
| First local write seconds |.094643 |.116729 |
| Maximum local-write gap seconds |10.603097 |5.414096 |
| Client sampled first/peak/last RSS KiB |53968/1125964/1100556 |55404/801244/639628 |
| Server sampled first/peak/last RSS KiB |30608/142732/129352 |30372/143296/137016 |
| Upload class byte delta |653130165 |552789090 |
| Return class byte delta |28704142 |25833457 |
| Upload packet/drop delta |539006/9506 |458163/8670 |
| Return packet/drop delta |157891/1928 |135426/1774 |

Sample intervals0.000045--50.222449s and0.000055--44.115547s have stable process
IDs, monotone class counters and no management errors. Client RSS peaks at
49.222345/26.003928s; server at10.001086/34.114541s. Client sampled maximum/last
psCPU123.0/92.2 versus70.0/62.6; server37.6/19.4 versus26.9/18.9. These are
lifetime-average percentages, not interval CPU. Candidate transfers10.13% fewer
useful bytes; lower total wire/RSS therefore is not normalized efficiency proof.
Class bytes include framing, control, native retries and Product copies, not
repair-only traffic. Fixed configured shaping does not make random realizations
packet-identical. The same HTB quantum warning appears in both cells; no tuning.

Full one-second raw confirmation series (index0 is the first interval; final
interval can be partial). Values above500Mbps release previously buffered
confirmations and are not physical link-rate claims:

```text
control:
0.620,2.118,3.026,537.352,107.213,102.524,118.105,66.016,85.075,87.032,
399.559,156.238,120.106,163.438,120.446,0.234,0,0,0.117,0,
0,0,0,0,0,0.270,293.121,64.155,58.100,64.295,
41.803,78.739,0,162.625,0,107.235,0,0,99.903,0,
0,66.540,22.064,0,0,0,0,114.250,72.780,77.927,411.207
candidate:
2.097,7.863,8.098,18.737,15.108,6.933,8.892,7.244,9.650,6.079,
540.017,115.535,75.785,42.083,39.181,60.958,89.417,43.656,73.452,110.529,
0,0,38.701,0,9.437,26.214,27.691,134.454,0.384,0.428,
0.096,0,0.096,5.575,531.670,35.182,0,81.981,368.811,90.004,
29.874,67.921,258.998,154.045,276.006
```

### Contrary phases prevent acceptance from improved maximum gaps

Candidate early delivery is substantially worse over intervals3--9. At about
10s control ordered target T=149132841B, candidate T=11402999B. In candidate
samples4--10, source S minus target T is approximately64MiB and server reply
reads Rs equal client reply writes Rc. Thus this early delay is on the forward
ordered-service side, not solely held return confirmations. Candidate T jumps
11402999→78905079 between samples10 and11; native QUIC ACKed bytes had already
reached60475573 by sample4 and68917544 by sample10. Native receipts include
framing/copies and are not exact mux-prefix delivery, but they refute a completely
idle QUIC transport. The first TCP owner's Product debt falls11468588B at1s
to458752B at10s and0 at11s while native ACKed bytes increase. No exact blocking
range or repair eligibility is present in these ordinary snapshots.

Candidate target T also stays166199031B across server timestamps
1788828813716--1788828818717 (about20--25s, at least5.001s sampled unchanged).
Rc meanwhile catches732→760 reply bytes. Therefore maximum confirmation
gap2.829451s does NOT bound ordered forward target stalls. Completion minus
the nominal40s source phase improves10.507429→4.919042s, but it is not exact
EOF-to-confirm drain: source reads continue after40s in both cells.

Disposition: exact ordering and overlap-work defects fixed at component level;
ordinary practical performance remains phase-mixed/ambiguous. No promotion,
third favourable run, public README claim or release. Independent extraction
and root agree. This pair does not establish that the new structural ordering
caused the early slowdown. Earlier initial-membership evidence already showed
12.19MB assigned while only TCP was attached; QUIC then attached91ms and was
used2ms later. Do not reopen the disproved claim that an already usable QUIC
was ignored or prescribe a protocol preference from these rows.

Next exact question: which retained prefix blocks early ordered upload despite
substantial native QUIC receipt, and what live-owner, copy and exact target
service evidence prevents earlier recovery? Read existing live-prefix capture
and model first; it already proves14.6KiB ACK-clocked winning chains during
10Mbps QoS, not during500Mbps spare service. No new parameter or implementation
is authorized until that evidence gap is resolved.

## Reused pre-QoS evidence — live-prefix service, not native inactivity

Recorded 2026-09-08 09:25 +08:00. Category: existing-capture discriminator;
no new experiment, runtime edit, or practical acceptance. Independent audit
and root checked exact publications, receiver advances and competing Originals.
Use Unix timestamps and event/range identity below: concurrent log sequence
numbers are not physical line numbers or a global execution order.

The older765683b capture in ACK_ATOMS_SERVICE_DIAGNOSTIC_20260908.raw.tar.gz
already contains this pair before the15s QoS transition. Its client zero is
1788808128204ms. Both successful causes are persistent ACK-gap repair;
concurrent retained-frontier selection is eligibility evidence, not proof that
the retained-only producer committed these copies.

| Exact half-open range | QUIC Apply / writer, Unix ms | Actual receiver F reaches end, Unix ms | Relevant feedback |
| --- | --- | --- | --- |
| `[8912684,8927284)` |1788808141862 /1862 |1788808141947 |Client F reaches8927284 at1788808142047 |
| `[8927284,8941884)` |1788808142047 /2047 |1788808142107 |Later ACK also includes winning Original bytes; do not isolate a second-copy ACK delay |

The second publication occurs in the same logged millisecond as the first
copy's positive frontier ACK. Each extent is14600B; selected L is3342336 /
3306336B. The only covering Original is TCP0/client physical instance2,
attachment0, `[8912684,8978220)`, published1788808128219 and received only at
1788808142162, after both copies. No competing TCP repair supplies either
advance. Surrounding actual TCP Original receives advance the frontier through
8912684 before the pair and past8941884 afterwards: the owner progresses.
The capture does not expose its exact qualifying-ACK epoch; native ACK or
released debt alone must not be substituted for that proof.

At these copies' Apply, previous queued/accepted repair debt is0. QUIC
Original debt is65536/44136B, native flight47502/16434B, native limit
4275882/4265193B, Product limit67108864B. The logged14600B queue includes the
current reservation. These distinguish resource eligibility and prompt small
copy service, not spare500Mbps capacity. Copies reach the receiver in85/60ms;
the first waits another100ms for client ACK application. In between, QUIC
continues accepting new OriginalData above75MB while the missing prefix is
below9MB. Neither native inactivity nor a delayed repair writer explains this
specific chain.

The newer445011f capture in REQUEST_PREFIX_SERVICE_20260908.raw.tar.gz supplies
an independent earlier pair at4.837/4.958s (zero1788823273955ms):

| Exact half-open range | Apply / writer, Unix ms | Actual receiver F reaches end, Unix ms |
| --- | --- | --- |
| `[4259787,4274387)` |1788823278792 /8793 |1788823278866 |
| `[4274387,4288987)` |1788823278913 /8913 |1788823278967 |

Both are persistent ACK-gap copies on QUIC0/client instance1/attachment2,
native repair stream8. TCP0/client instance4/attachment0's covering Original
`[4259787,4325323)` was published1788823273967, written1788823275620, and
decoded only1788823279027; it loses both prefixes. TCP Original arrivals at
1788823278430--8433/8628/8681/8762 advance surrounding lower bytes. QUIC
decoder-to-mux is at most1ms, with no preceding non-data or reader-send wait
at the copies. This newer capture lacks exact client post-ACK F and selected
L hooks, so it does not independently prove the second publication's ACK
trigger. Both captures finish these pairs before configured QoS begins.

### Model question and proof boundary

For a long missing span, if Original arrivals do not cover its next quantum,
only one repair quantum Q may be outstanding at the sender's cumulative F,
and the feedback cycle is tau, repair-only ordered service cannot exceed
approximately8Q/tau. With Q14600B and tau100ms this is1.168Mbps; this is a
conditional countermodel, not a measured global tunnel ceiling. T06 explicitly
defines a bounded live-owner latency hedge, not sustained migration. The
serialization therefore does not by itself violate its implementation contract.

The useful intention must survive: T06 stopped one14600B ranking from exposing
10667416B of unranked service and reduced measured repair amplification.
Neither enlarging Q, declaring a progressing owner stale, nor repeatedly
appending independently checked quanta proves that cumulative service is safe.
Current K/native enqueue readiness is permission, not spare service or a
physical completion prediction. A speculative continuation can lose to an
Original and delay new work at a shared bottleneck.

Next bounded decision: can existing exact range ownership and native service
boundaries define ordered allocation between due lower repair and new suffix
without an ACK-per-quantum bottleneck or the prior suffix-amplification defect?
Independent model review must supply a concrete counterexample, conditional
benefit and adverse case before any RED or implementation. If that requires
unavailable comparable native predecessor work, reject that claimed guarantee
instead of fabricating capacity. These captures answer reachability/geometry;
they do not attribute1436ff4's exact ordinary early hold or prove a pipeline's
counterfactual benefit. No additional capture is required merely to repeat
this already demonstrated geometry.

### Independent disposition: reject a superficially bounded repair loop

Both independent reviews reject proceeding with repeated independently ranked
Q-byte live repairs until K is full. Each decision would preserve T06's local
score/Apply extent but the loop could recreate its harmful bulk duplicate load.
Strict substitution for an otherwise-admissible new Original is narrower,
yet cannot serve a receive-window-blocked stream with no such Original action.
Calling a free writer slot an opportunity does not remove that distinction.

The adverse worlds are indistinguishable until new feedback: in one, the old
TCP prefix remains slow and ahead copies help; in another, its Original has
already arrived or arrives first and its ACK is delayed. Copies then displace
unique work. For illustration,1.8MB ahead of useful work consumes1.44s on a
shared10Mbps cut. This is a serialization calculation, not a new limit. The
recorded Original-winning hedge and target-complete/unreleased-Product-debt
case support the adverse premises. No implementation or tuning follows.

### More precise existing-stage evidence: initial pre-native commitment

Recorded 2026-09-08 09:30 +08:00. The same445011f capture distinguishes work
still waiting in MPP from work already inside an irreversible native write.
These counts/times belong to this capture, not the older765683b187-frame,
91ms-attachment capture. This capture has no attachment hook; actual QUIC
publication/write/receipt establishes readiness without guessing its start.

All192 initial TCP0/client-instance4 Originals,12517323B, were published
within23ms of zero1788823273955. First QUIC0/client-instance1/attachment2
Original `[12517323,12582859)` commits at1788823274072 (+117ms), starts H3
ordinary stream4 writing then, and completes its local write at4073. Its first
fragment is decoded/routed by the server at4173,101ms after publication.
At the first QUIC publication the exact initial TCP partition is:

| Stage | Half-open range | Bytes |
| --- | --- | ---: |
| Local write/flush completed | `[0,3866571)` |3866571 |
| Protected native transaction already begun | `[3866571,3932107)` |65536 |
| No writer-begin event yet,131 commands | `[3932107,12517323)` |8585216 |

No boundary depends on ambiguous same-millisecond event ordering. The begun
frame starts1788823273967 and flushes only1788823275619. Its successor
`[3932107,3997643)` was published3967 but cannot begin until5619:1.652s before
writer entry. The previously joined `[4259787,4325323)` waits similarly.
A later Original `[8257483,8323019)` is published1788823273972 (+17ms), begins
and completes local writing1788823280570 (+6.615s), and reaches the server at
1788823287378 (+13.423s). Its6.598s pre-writer delay is distinct from subsequent
native/transport delay. At4/10/15s, initial commands still without writer begin
contain5898240/1966080/458752B respectively.

The observer is immediately before `writer.write_frames` and after successful
flush in the archived TRACE patch. Therefore unstarted commands have not
entered that protected encode/write transaction. Once it begins, even a pending
future may have partially advanced encoding or the socket: it is not revocable
from a missing completion event. Flush completion means local acceptance,
never wire transmission or remote receipt.

This proves material pre-native queuing, not safe cancellation in current code.
Current SendFrame has no exact claim/revocation ticket. Product assignment,
qualification and debt are recorded before MPSC publication, and ordinary
writer commands retain that path choice until service or terminal cleanup.
The queue was deliberately enlarged for high-BDP and concurrent-actor
pipelining; its existence is not a bug. T03_ADVISORY_SCORE and T04a already
reject shrinking it or inserting a one-action writer-idle gate. A proposed
single writer-ready offer would restore that rejected gate and is not pursued.

The narrower next model question is whether an **unstarted queued Original**
can change exact ownership without a second physical copy: retain full queues
and protected writes, but require an atomic writer-claim versus actor-revocation
boundary before encoding. A transfer must have a valid alternate reservation
and move exact Original ownership, not release shared O, mint receive credit,
reset remaining-byte ages, erase earlier-copy ambiguity, or borrow another
incarnation's qualification. Claimed/writing work remains unchanged. This is
a candidate ownership-contract revision, not an implemented fix or a promise
that a different native path will finish earlier. Independent review must
resolve ACK/copy races, cancellation, close/FIN, refund and wake ownership
before any producer RED or implementation. No new queue limit, timer, controller
or protocol preference is authorized.

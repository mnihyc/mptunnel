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

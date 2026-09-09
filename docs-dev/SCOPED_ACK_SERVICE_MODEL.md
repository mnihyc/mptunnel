# Independently scoped receive feedback

Updated: 2026-09-09 02:27 +08:00. Category: mixed/stall owner redesign and
pre-implementation proof. Ordinary comparator remains `4c7e232`. This is not
performance acceptance. The active global ledger remains CURRENT_CLOSURE_PLAN.

## Transaction and expected value

Issue: mixed TCP+QUIC loses useful downstream service when the asymmetric
return link cannot serialize repeated cumulative MPP feedback. In the ordinary
500Mbps DOWN / temporarily 10Mbps UP case, QUIC/raw/H2 keep approximately
441/476/470Mbps while mixed falls125Mbps and an echo times out. The codec
observer attributes47.375MB to396286 mixed ACK frames containing6.563million
range entries; Q-only3.607MB contains134221 single-range frames. A separate
queue shadow finds roughly30% replaceable pending work, insufficient evidence
for curing ACK peaks of33.207Mbps against10Mbps. Queue-only replacement is
deferred, not silently combined with this change.

Competing causes remain native overhead, mixed reordering, feedback fanout,
local service and source allocation. The observed repeated range support is a
real dominant serialization cost; reducing it does not prove all stalls vanish.
The original model deliberately offered full receive state independently to
every attachment (5e1ace67), preserving delivery when one carrier was blocked;
pre-local-write feedback (444fb38) deliberately avoided application backpressure.
Both intentions remain. The defective premise is that every new fact needs
the full cumulative range history to carry truthful omission evidence.

Exact question: can every feedback record stand alone, transmit primarily new
facts, and preserve positive release and explicit gap recovery across loss,
reordering, partial publication and replacement without a codec dictionary?

Conditional fixed-frame-count forecast: a conservative35B one-range update
would reduce47.375MB ACK to13.870MB (~71%), and unchanged ACK+MAX54.688MB to
21.183MB (~61%). This is an illustrative upper-value case, not a promised wire
ratio or goodput multiplier. Multi-range updates, catch-up and native overhead
remain. Q-only single-range frames gain a scope field and can cost more. Faster
forward service also produces more feedback: the collapsed-run ratio is NOT a
steady-state proof that all feedback fits10Mbps. Using the existing minimal
varuint64 representation for the independent scope offset typically costs less
than the conservative8B assumption; no new compression history is introduced.
Implementation simplification09:19: a scope wholly covered by its positives
asserts an empty negative set and is omitted. This is the identity
`G union empty = G`, not weakened authority or a timing threshold. For sorted
normalized chunks it is exactly `last_positive.start <= proposed_scope_start`.
Contiguous Q updates therefore need no additional scope bytes; the ordinary
Q control remains required because CPU/service composition can still change.

Value: this targets an observed hundreds-of-Mbps collapse and multi-second
stall in an environment where matched baselines remain fast. It is not a
small low-capacity optimization. Falsifiers are invalid omission evidence,
weakened recovery, excessive producer/consumer work, unchanged practical stalls,
or worse affected Q-only/healthy timing. No performance promotion on byte-count
savings alone. Smallest sequence: exact set/producer/cursor counterexamples,
independent source audit, ordinary mixed asymmetric case and Q/healthy controls.
Retain every timing bin, failed echo, recovery phase and resource/wire cost.

## Standalone evidence, no generation dependency

For one directional Product stream, a frame carries normalized positive ranges
`R_f` and optional `scope_start=L`. If present its scope is
`S_f=[L,max_end(R_f))`; otherwise it has no negative authority. It asserts:

- every byte in `R_f` has been received;
- every byte in `S_f \\ R_f` was not received when this frame was formed;
- omission outside `S_f` means unknown, not missing.

Require nonempty positive ranges and `L < max_end(R_f)` for a present scope.
Empty positive reports have no scope. Validate every original range and scope
before mutating any live Product state, against the frozen assigned extent.
The wire version changes explicitly; there is no silent old-complete-bit
reinterpretation, compatibility dictionary or cross-record decode state.

Let `P` be all positively acknowledged bytes, and `U` the sender's existing
exact retained unacknowledged send-cache coverage. The authoritative missing
set evolves as `G' = (G union (S_f \\ R_f)) intersection U'`, where positives
have already removed `R_f` from `U` to obtain `U'`. Thus positives always win,
including those received on another attachment before a delayed older scope.
No second lifetime-sized positive ledger is necessary: over committed offsets,
the exact send cache is the complement of all applied positive evidence.

Boundedness: each proven gap's right endpoint abuts positive coverage removed
from the send cache. Within each normalized retained unacknowledged component,
G can occupy only a suffix. New scopes cannot split it without a positive
anchor that also splits/removes retained coverage; later positives intersect
both. Thus normalized gap components are bounded by current retained cache
components, not lifetime feedback count. Uncommitted/rolled-back offsets have
no valid receiver proof under the frozen assigned-extent transaction.

Example: a new positive fills[300,400), then an older record with positive
[400,500) and scope[300,500) arrives. Intersection with retained unacknowledged
coverage prevents resurrecting[300,400). Disjoint scopes never authorize the
unknown region between them. Keep `G` as explicit missing intervals, not a
compatibility maximum horizon that downstream callers mistake for whole-prefix
authority. Prune acknowledged coverage; retain only current unacknowledged
Product support. Local exact-owner recovery outside `G` remains independent.

For a complete cumulative receive set `R` through H, a single scope[0,H)
gives precisely the old complete snapshot's omissions. Partitioning sorted R
into chunks with scopes[0,end(chunk0)),[end(chunk0),end(chunk1)),... gives the
same union of facts. Every chunk is truthful independently, so interruption,
reordering or duplication cannot manufacture gaps from absent chunks. This
also removes the old all-chunks-incomplete loss of gap authority for large R.

## Producer and publication proof obligations

The receive stream remains the sole cumulative range owner. At a publication
boundary a dirty flag on each surviving merged range node records new evidence;
one materialization gathers and clears flags. No revision counters or second
delta ledger are needed because the Product owner serializes this boundary.
At that
boundary retain all successfully admitted positives since the prior boundary,
not just the last frame in a batch. Let H0 be the prior boundary's highest
received offset and delta be those new positives (a truthful positive superset
is also valid). If the high-water extends, delta contains all receive coverage
above H0, so scope[H0,new_H) is complete within that region. Previously known
gaps persist at the sender until positives fill them. Non-extending updates
need only positives. Duplicate/rejected input adds no new fact.

Chunking an update: chunks ending at or below H0 are positive-only. For each
chunk that extends the current covered high-water, its scope starts there and
ends at that chunk's highest end; advance the local chunk high-water afterward.
Earlier chunk positives are outside the next scope. A batch's early positives
inside a claimed scope may not disappear when later receives are merged.

Existing per-attachment generation/chunk cursors remain efficiency fences,
not remote decoding prerequisites. An attachment that accepted the immediately
preceding full update may receive the current incremental update. A fresh,
replaced, missed-generation or partially published attachment receives truthful
cumulative catch-up. Generation advance requires all update chunks accepted.
Failed or cancelled publication cannot let a successor inherit an unjustified
baseline. Already accepted frames are immutable and independently valid.

First feedback remains immediate at the existing receive-publication boundary;
no ACK delay, rate cap, new timer, control-path preference or fanout reduction.
MAX credit, Native congestion/recovery, original/copy identities, byte-exact
ACK release, actual receipt clocks, ambiguous attribution and terminal no-op
validation remain unchanged. ACK batching may change observation granularity;
do not claim identical sample counts or estimator history from set equivalence.

## Required focused controls

1. Actual receive producer retains every new fact across a batched sequence;
   duplicates and rejected input do not publish new evidence.
2. Chunked full catch-up and incremental updates are independently truthful;
   reordered, duplicated and missing chunks never widen negative authority.
3. Positive fill before delayed negative scope cannot resurrect a gap; scopes
   separated by unknown coverage do not couple it to recovery.
4. Exact existing send-cache and original/copy release remain equal for equal
   positive coverage, including terminal, malformed and beyond-assigned input.
5. Actual two-attachment publisher handles immediate progress, blocked queue,
   partial generation and replacement without waiting for the other carrier.
6. Retained-owner recovery above/outside observed scope stays available; ACK
   gaps, staleness and prepared-source synchronization consume explicit G.

Disposition09:54: the coherent wire-v14 model and affected1241checks now pass;
the [first ordinary pair](SCOPED_ACK_ORDINARY_20260909.md) shows only partial
benefit, with lower restored service and higher observed costs. Its performance
promotion remains held. The proof above preserves the pre-change forecast and
its assumptions; it is not a substitute for practical closure. RFC8.3 now
describes the same independently scoped authority model.

## Publication alternatives reviewed — 2026-09-09 12:03 +08:00

The scoped representation remains the current runtime. This section is a
source/model review, not an accepted extension. Receipt batching was tested
and rejected after both healthy test orders worsened latency. The subsequent
queue observation finds newer work at only about20% of restricted-phase takes;
that is neither safe scoped-fact substitution nor a wire-saving estimate.

### Independent deferred publication: possible, but not latency-neutral

The earlier all-TCP-feedback withholding ablation removes a material cost:
restricted mixed service266→399Mbps and worst restricted echo2.405s→274ms.
It deliberately abandons the independent return protection introduced by
5e1ace67 after actual7–14s blackhole stalls. It cannot be a shipped policy.

A coherent prompt-ingress/deferred-sibling model would need one retained
current receipt/grant state plus an exact-incarnation obligation per attachment:
last covered generation/grant, a nonrenewing pending deadline, and at most one
finite in-progress publication job. New facts must not restart that job's
chunk cursor or postpone its outstanding deadline. Accepted queue work is not
proof that the peer received it. ACK catch-up and repeated publication are
different operations; current retry_cumulative returns immediately for an
already-covered generation. Pinning a coherent multi-chunk job adds bounded
snapshot retention; it is not a free cursor-only extension.

For a healthy sibling b, a conditional feedback bound would be
`D_b <= interval_b + residual_job_service + next_job_service + native_return`.
The terms require executor, queue-capacity and native progress. With roughly
constant interval Delta, ordinary sibling jobs can be bounded by approximately
`1 + T/Delta`; sparse cumulative jobs still have multiple chunks and may be
expensive. Neither formula proves that a given asymmetric return cut suffices.

The decisive counterexample is an ingress whose reverse direction blackholes
while another return path works. Immediate fanout can deliver after T_b;
deferring b adds its eligibility interval and service. No reduction of unknown
return-path copies can universally preserve the earliest available feedback
time. At500Mbps,100ms extra feedback delay requires about6.25MB additional
pipeline authority; enlarging windows to conceal that cost is not a fix.
This model is therefore not selected as a latency-neutral optimization. No
new timer, PTO multiplier, threshold, native policy or RFC rule is installed.

Actual integration constraints, not speculative new release obligations:

- Server routing knows exact ingress but enqueues Frame without that identity;
  its shared ingress hint can be overwritten by the next queued arrival before
  the first is applied. A prompt-ingress policy cannot use that hint as proof.
- Global last-feedback timestamps can be renewed by primary activity and
  cannot own independent sibling deadlines.
- Current cumulative cursor resets on every new generation. A one-slot
  attachment facing continuous generations can restart a multi-chunk job.
  Existing immediate fanout must not be silently replaced with that deferred
  service pattern and then patched with another timeout.
- Client retains one application-write future while servicing some controls;
  server awaits write before ACK/MAX. Neither branch already implements the
  complete proposed per-attachment deadline/credit service contract.
- New attachment, partial catch-up, duplicate receipt, FIN, RESET and ordered
  exact-incarnation detach need explicit obligations. No obligation creates
  received bytes or grants capacity before application consumption.

These are source-audited prerequisites for that alternative, not attribution
of all current stalls or independent implementation tasks. It remains deferred.

### Same-publication ACK/credit pairing: insufficient reachability evidence

Server enqueue_tcp_recv_progress can produce ACK and MAX after one receive/
write boundary. Dominant client download deliberately emits ACK before local
write and MAX afterward. Those are not one publication event. Combining them
would require delaying the ACK or advertising unconsumed capacity; neither is
authorized by a packet-count argument. The eligible same-call fraction is not
measured, so total ACK/MAX counts do not forecast a material gain here.

One protected record also does not imply atomic Product application. Received
MAX bypasses the ACK FIFO through the latest-credit ingress introduced by
83734b2. A composite must not restore that old coupling or discard newer credit
because its ACK is already subsumed. Conversely, extracting MAX before ACK
assigned-extent validation is not an atomic combined transaction. Same-event
pairing is distinct from both rejected receipt batching and writer-ready
packetization, but that distinction is not enough to select it. No wire or
command variant is added.

### Next discriminator: separate ACK fanout from credit fanout

The combined ablation cannot identify whether repeated receipt copies, repeated
credit copies, or their interaction dominates the observed collapse. This
question can change the owner/model decision without implementing either
alternative above. Reuse its feature-only local intervention with separate
ACK and MAX selectors, retaining all data paths and normal server behavior.
Compare one env-unset control, TCP-MAX withheld only, and TCP-ACK withheld only
using the same frozen binary and unchanged40s500/10 return-restriction profile.
Withholding either kind is deliberately unsafe under failure of the chosen
return path and is diagnostic only. Do not treat these as candidate policies.

Information forecast: a large benefit from MAX suppression alone focuses the
next proof on shared-credit publication, not ACK timing. A benefit from ACK
suppression alone focuses receipt publication. Similar gains from either or
poor individual gains leave interaction/saturation rather than a unique owner;
retain that uncertainty instead of declaring one cause. Lower return bytes
without better gaps/loaded latency is not a useful performance result. Keep
all phases, failed/censored work and CPU/RSS/queue cost. Existing total fanout
result is context, not a fourth matched cell. No guessed numerical pass gate,
new controller or repeat-until-green. Freeze and reverse the observer source
before runs; ordinary runtime and all prior independent recovery remain intact.

## Independent-service proof outcome — 2026-09-09

The [three-kind experiment](FEEDBACK_KIND_ABLATION_20260909.md) gives restricted
169.929Mbps control,243.267Mbps without TCP MAX copies and296.144Mbps without
TCP ACK copies. Both kinds contribute. This supports publication-service work,
not a one-kind-only cure or immediate implementation of deferred copies.

### Finite-horizon cursor: truthful, but insufficient service bound

For a proposed job `(goal_generation,H,c)`, freeze H at its start and read the
monotone current receive coverage intersecting `[c,H)`. Include any predecessor
range that has since merged across c. Choose a bounded prefix of those ranges,
clip at H, and include every currently received byte inside the emitted scope
`[c,last_end)`. Advance c only when the exact attachment queue accepts it.
New global generations do not reset c or extend H. Since receive coverage only
adds/merges, induction over admitted cursor intervals covers every job-start
positive by c=H. The byte before nonzero H remains received, so a noncompleted
job cannot legitimately reach an empty terminal suffix. Newly received fills
behind c belong to a later generation; finishing the old job cannot cover them.

This avoids an incorrect merge lookup: publishing[0,10), then merging into
[0,30), must still find the overlap when resuming at10. Ignoring that predecessor
could produce false negative evidence. Temporary work can be bounded per step
by the existing256-range frame and an indexed lookup, with scalar retained
state per attachment. These are useful truth/ownership facts, not a wall-clock
service guarantee.

Counterexample to the stronger work claim: start with K small one-byte islands
and a distant positive ending at H. After each accepted chunk, merge already
published islands into a prefix and add K islands just ahead of c. Current
node count stays O(K), but the distant original positive can require O(H/K)
chunks. The actual65,536-range ceiling therefore does not imply the frozen
snapshot's256-frame job bound. With K=256 and a64MiB unconsumed span, only a
much looser roughly131,000-full-chunk byte-span bound follows. The sequence is
legal under the receive model; it is not an invented malformed-input test.
No useful worst-case recovery-latency claim follows from that bound.

### The alternatives have explicit costs

A frozen full snapshot has at most65,536 ranges in256 frames, about1MiB of
range payload. Distinct retained jobs on64 attachment slots can add about64MiB
per logical stream; the shipped four-path case can add4MiB, excluding frame/
allocation overhead and already-queued copies. Sharing an Arc only helps when
jobs actually pin the same version; staggered or blocked jobs need not do so.
Do not call this negligible or silently add it to a sustainability-sensitive
runtime. These are verified configured/default bounds, not new limits.

Periodically skipping current publication generations also falls back to
cumulative catch-up under the existing cursor. Repeating full sparse history
could restore the cost the scoped model removed. A traffic forecast must count
those catch-up ranges, not merely jobs per second or the current one-range lab
trace. This source consequence needs resolution before a delayed-fanout model
can be selected as a general Internet fix.

ACK/MAX fairness is independent: always ACK-first can hold credit behind a
long catch-up; always latest-MAX-first can starve ACK under continuous grants.
Persistent alternation when both are eligible is a possible admission rule,
advancing only on success, not a selected implementation. Freezing credit to
an entire ACK job instead explicitly delays newer independent credit.

There is also no automatic joint rate/delay bound for successor jobs. Keeping
all newer facts on an expired predecessor deadline permits continuous jobs;
resetting the deadline delays previously uncovered facts. Thus one cannot
claim both `jobs <= 1+T/Delta` and an oldest-fact latency bound from a single
last-publication timestamp. Exact semantics must precede implementation.

### Actual integration scope, if a replacement is later selected

Server Frame events must preserve authenticated ingress through the existing
FIFO and mailbox continuation. PendingMailboxFrame currently accepts a function
pointer wrapper; a captured Send wrapper can preserve identity without a new
channel. The client already has exact ingress per item. Record every successfully
applied ingress, including valid duplicates, in the unchanged bounded batch;
retain it through the same write future for later credit publication. A last
frame or mutable global hint is insufficient.

Existing attachment entries can own eligible ACK/MAX obligations. Capacity
retry must honor eligibility rather than immediately bypass any deferred
service. Client retained-write service needs the relevant deadline/MAX wakes;
server must separate its existing synchronous apply and pinned write helpers,
without advancing credit before consumption. Shutdown eligibility/state commit
must surround the same retained shutdown future. No partial write is replayed.
Preserving input FIFO also means queued data/reset/detach cannot all be claimed
universally prompt during application blockage; that broader claim is excluded.

Disposition: this second proof pass rules out promoting the scalar cursor as
a bounded-latency fix or snapshot retention as a free cleanup. No timer,
snapshot/job framework, per-attachment delta ledger, RFC change or runtime
implementation is selected. The next model decision must jointly resolve
independent return recovery, retained/sent fact cost, bounded work/memory,
first/sparse service and explicit asymmetric-failure timing. Preserve the
existing scoped encoding and independently published protection meanwhile.

## Conditional immediate-write pairing: bounded discriminator, not code selection

Origin correction:444fb38 introduced pre-write ACK/startup service after a real
one-byte-then-Pending sink prevented ACK, OPEN and FINAL progress. 6e2504b
introduced the ACK-only timer API; moved-line blame at59fbd22 was not the origin.
The existing blocked-delivery regression remains mandatory, not a removable
piece of performance overhead.

A narrower alternative may avoid delayed sibling timers and repeated sparse
catch-up: poll the same retained application write/flush future once without
waiting. Ready success makes both receipt and consumed-capacity facts available
in one turn; Pending preserves the original ACK/startup-before-yield path and
the same partial-write future; Ready error requires receipt ACK before cleanup
and no MAX. No extra Product input or ACK generations are merged. Known prepared
errors and existing nonblocking startup/capacity/FIN work must not be bypassed.

Ready-success publication needs force_ack=true, publish_max_data=true,
force_max_data=false. Existing new(false)/new(true) are not equivalent. A typed
internal pair could carry at most one original ACK and one independently due
MAX through existing transport write_frames, with no wire/receiver change.
ACK-only, MAX-only, partial ACK catch-up and failed admission remain independently
retryable. Pair acceptance certifies its chunk and grant, not an incomplete
whole ACK generation. Preserve two logical frames and two pressure units;
one queue envelope instead of two is an explicit admission-granularity change.
Terminal filtering, all TCP/QUIC writers including deferred-input QUIC drain,
partial native failure and successful sent accounting need the same ownership.
Do not use a new aggregate codec bound to reject two individually valid frames.

This is approximately15 production-file integration, not a tiny helper tweak.
One poll also is not one syscall: gap filling can release a large retained
receive span, and the current helper loops through writes and flush. Therefore
ACK timing can shift even without awaiting a Pending future. Concrete sinks
are accepted Tokio TcpStream for ordinary proxy/forwarding,64KiB DuplexStream
for routed DNS, and netstack-smoltcp's stream for TUN. Telemetry/activity wrappers
delegate polls. An arbitrary slow always-ready mock is an API counterexample,
not a proven production defect; a large gap-fill is real, but its first-poll
work and latency still need observation.

Next information-only action: observe the FIRST poll of the existing complete
write/flush future without moving its invocation or ACK publication. For nonempty
delivered batches, aggregate Ready-success, Ready-error and Pending counts,
offered bytes and first-poll duration using existing feature perf summaries.
Keep empty delivery separate so out-of-order receipt/flush-only turns cannot
inflate apparent pairing opportunities. No per-frame logging, data decisions,
extra polling, timer, threshold or frame/queue change. Root freezes and reverses
the observer before one unchanged mixed return-restriction run.

Information forecast: a high actual Ready-success fraction with small observed
first-poll work would justify only the next coherent candidate/RED decision;
rare ready completion or material synchronous work can defer it without a
15-file patch. Observation occurs at the OLD post-ACK invocation and is not
proof that an earlier pre-ACK poll has identical readiness. It is neither a
speed forecast nor candidate acceptance. Existing MAX-withheld170→243Mbps is
only a material-cost reference, not an upper bound or promised gain from
pairing. Any selected candidate still needs the original blocked-write/FIN/
partial-error controls and ordinary timing comparisons, not sample-count proof.

### Measured opportunity and next bounded optimization transaction

2026-09-09 13:10 +08:00. One unchanged mixed capture is complete; the feature
observer is fully removed. Client nonempty first polls:73,914 Ready-success,
3 Pending,1 Ready-error at shutdown. Ready-success covers99.9946% of calls
but97.7146% of offered bytes; Pending contains42.845MB of large gap-filled
batches. Mean Ready-success poll is34.329us, maximum16.195ms, including existing
feature instrumentation and possible descheduling. Empty42,638 successes are
excluded. This supports a common opportunity, not universally cheap writes.
Full counts/timing/profile/cost are retained in WRITE_FIRST_POLL_20260909.

Select a bounded conditional-publication CANDIDATE transaction, not acceptance.
Issue: mixed feedback saturates the restricted return cut and harms ordered
delivery/echo tails. Competing residual causes include native packet ACKs,
same-cut data contention and allocation/recovery timing. Current suppression
cells prove both Product feedback kinds matter; they do not predict a safe
pair's gain. This candidate retains every existing ACK generation, independent
grant and return attachment while removing one separate queue/native record
transaction when the exact ACK and due MAX are already jointly publishable.

Benefit forecast: at most one separate record and associated packet overhead
can be removed per eligible pair; QUIC's existing batching and native socket
packetization can reduce this saving to zero. The measured ready fraction is
not the exact paired-frame fraction and is not a Mbps forecast. Cost reference
is the observed170→243Mbps credit-withholding restriction result, not a bound
or promised gain. Material current feedback pressure justifies one coherent
prototype plus affected ordinary comparison; no gain/regression remains a
credible outcome. Do not enlarge pairing, defer sparse feedback or add a knob
if its ordinary timing result is weak/adverse.

The current private TCP transport adds a2-byte record length and16-byte tag.
For illustration only, pairing every observed73,914 ready success on all three
TCP attachments could remove at most3,991,356 such record bytes over40s, about
0.798Mbps of return service. This assumes every call has a due grant, all three
attachments, and an otherwise separate record; actual opportunity can be lower.
Saved TCP/IP packets, syscalls and arbitration may matter more, but existing
offload/packetization does not justify assigning them a fixed per-pair saving.
Thus record bytes alone do not predict recovery of the whole throughput gap;
the ordinary comparison must establish the composed gain and latency cost.

Smallest next action: existing real receive-progress publisher/queue regression
must first establish that an eligible ACK and due MAX become separate commands,
after validating both original frame contents and exact pressure. This is an
optimization work-bound RED, not a claimed corruption or sufficient speed proof.
Then implement the typed same-publication pair through all consumers, retaining
standalone cases, pressure2, exact terminal/drop/native commit ownership and
unchanged wire decoding/latest-credit ingress. No generic ready-head batching.

Actor sequencing: preserving every prior relative order is impossible because
the candidate moves current ACK behind one local poll. Keep the existing
nonblocking startup/prepared-error/notices/capacity prelude before that poll.
On a prelude error offer receipt ACK before returning, without local I/O.
Cache the first result; Ready-success permits independently due MAX, Pending
grants no new credit and re-arms newly pending ACK capacity before parking,
Ready-error preserves ACK then error cleanup. Never repoll a Ready future.
Previous feedback/startup can therefore precede the current ACK; record and
test this temporal change rather than pretending the event trace is identical.
Final ACK admission still precedes shutdown.

The first-poll wrapper must itself return Ready(inner_poll) on the same outer
poll. A select-based helper with a ready branch is insufficient as a proof:
[Tokio's select expansion](https://github.com/tokio-rs/tokio/blob/master/tokio/src/macros/select.rs)
checks cooperative budget before polling branches. The candidate therefore
uses an explicit standard poll_fn around the same borrowed pinned future;
inner Pending still permits synchronous receipt publication before parking.
This changes no existing cooperative policy or opportunistic-read helper and
does not classify that helper's other uses as shipped defects.

Candidate cross-review caught an ordering dependency at a newly freed queue:
the first-write prelude must not retry the previous desired ACK generation
before offering the newly received facts. Otherwise old G1 can occupy the
last slot intended for G2's joint publication. Guard that retry with
receipt_offered; preserve pre-poll notification arming and the first-Pending
continuation so newly blocked G2 is retried without a lost wake. This restores
latest-fact priority within the trial, not a new deployed defect or timer.

Gates: existing blocked-write/startup, partial-write/error, FIN, independent
ACK/MAX catch-up/new attachment/backpressure, all writer and queue/drop checks;
independent source audit; then ordinary control/candidate on the same return
restriction with full phases/body gaps/echo failures and CPU/RSS/traffic costs.
Only a material composed benefit permits healthy mixed and QUIC controls plus
UP. Any adverse/ambiguous timing stops promotion for one attribution decision;
no favorable-average acceptance or compensating threshold/controller changes.
Global asymmetric failure/recovery, aggregation, browser and baseline gates
remain unchanged. No release/README follows this local candidate selection.

### Ordinary disposition: REJECTED,2026-09-09 14:03 +08:00

Both1267-check batches pass, including original blocked startup, pair/credit
fences and encrypted transport. The first ordinary restriction pair improves
162.871→237.654Mbps and echo p95/max582/1283→398/614ms, with26.98%less
UP class bytes, but median echo and maximum body gap worsen. Promotion stops.
One predeclared healthy candidate→control discriminator retains the cost
reduction yet produces413.444→402.332Mbps and echo p95/max468/606→562/814ms.
Median324→291ms and bodygap.390→.331s improve; the adverse tails still reject
the trial. All four histories/costs and limitations are in JOINT_FEEDBACK_20260909.

The mechanism forecast held at the return-work boundary, but failed to establish
overall timing improvement. Shared queue/allocation and moved feedback timing
remain competing causes; measured queues do not identify the individual echo's
blocking byte. No controller tweak or broader batch is justified. Exact trial
source/RFC is fully reversed; typed pair, actor move and associated tests are
not retained obligations. The original ordinary scoped implementation remains.

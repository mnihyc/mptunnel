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
pre-local-write feedback (6e2504b) deliberately avoided application backpressure.
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

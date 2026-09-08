# Prepared Original ownership — candidate model and integration proof

2026-09-08 13:01 +08:00. Category: bounded architectural candidate for the
existing early ordered-upload failure. **Not accepted runtime, performance or
RFC policy.** Read PERFORMANCE_METHOD_AND_LESSONS and CURRENT_CLOSURE_PLAN.
REQUEST_PREFIX_SERVICE_20260908 contains the exact existing capture; this file
defines the new ownership question rather than duplicating its evidence.

## Why this model, and why not another repair adjustment

In the445011f capture,12.517MB was assigned to the first TCP output in23ms.
When QUIC actually published a64KiB Original at117ms,8.585MB of that initial
TCP assignment had not entered the writer. One later frame waited6.598s
before encoding/write began. Some other bytes were already in native service;
they are a different case. The current implementation obeys its contract:
Product Apply irrevocably binds a frame before publishing it to a private
writer queue. Queue capacity was intentionally retained for high-BDP and
concurrent-actor pipelines. The undesirable coupling is early path ownership,
not the existence or size of a prepared-work resource queue.

RFC15.1 explicitly requires the bounded command reservation followed by
Product ownership before publication, and describes that queue as the staging
linearization point. The wording is present at3a6d0ea; the dispatch function's
current source boundary was named in4139704. This candidate changes that
particular placement contract while retaining the RFC's separate resource,
qualification, no-native-ACK-credit and protected-write intentions. It must
not be described as merely bringing current code into RFC compliance.

T06's one-quantum live hedge cannot provide sustained migration by itself;
conditional repair service is8Q/tau when each succeeding quantum waits for
frontier feedback. Repeating independently scored quanta until K fills is not
a sufficient correction: it preserves local score/Apply identity while
recreating speculative duplicate cost. A single writer-idle offer instead
restores the previously rejected per-action pipeline gate. A blocking
writer-to-Product-actor transfer request can deadlock behind the actor's
pending local response write. These alternatives are rejected, not dependencies
of the candidate below.

## Candidate boundary

Keep prepared payload owned by the logical stream direction until an eligible
native writer claims the next atomic action, synchronously, immediately before
its existing protected encode/write transaction. The prepared resource queue
is shared by that direction's current eligible outputs, not irrevocably bound
to a future writer. It retains configured memory/pipeline capacity. This is
not a smaller queue, a native ACK window, or a request/response exchange with
the application actor before every frame.

Once a writer claims bytes they remain exact claimed Original ownership until
positive Data ACK or existing terminal cleanup. Encoding, partial writes,
native acceptance and recovery cannot return them to prepared state. A claim
is before native exposure, so its debt includes the protected local write;
calling it only physical flight would leave a false accounting gap.

The smallest scope is reliable OriginalData. Control, attachment/open/return
plan, datagrams, experimental L3 and existing exact recovery contracts are
not silently migrated. Request direction supplies the first real producer
counterexample. A direction-neutral model is not proof that the response
implementation has been changed or validated.

## Resource and evidence invariants

For one stream direction, let U be retained prepared bytes not yet claimed,
O_i be un-DataACKed claimed Original bytes on exact output i, and B the total
retained unique source-byte obligation. With disjoint intervals:

    B = U + sum_i O_i
    B <= configured shared W and applicable retained/reorder resources

W is a ceiling, not necessarily equal to B. Choose **unnumbered preparation**:
ordered source bytes have source end A, but the wire send/cache/ACK-validation
horizon C advances only at claim. U=A−C; prepared bytes have no wire Original
assignment. Existing claimed cache/recovery and ACK validation stop at C, not
A. Preparation uses its configured source/retained resource allowance and
does not advertise source delivery or consume target P_i. Claim assigns exact
wire offsets starting at C and validates receiver offset credit there. The
source and retained resource owner must account U once, not maintain two copies
of the same source buffer. Source EOF fixes A; ordinary FIN eligibility waits
for U=0, without waiting for all claimed Originals to be ACKed.

A claim of exact q-byte prefix r performs one synchronous transaction:

1. Validate current stream/source frontier, return-plan/attachment eligibility,
   exact carrier and attachment identities, lane and terminal state.
2. Validate current target Original authority P_i/E_i/q_i and shared receive
   credit, plus actual native/writer admission and its generation/fence.
3. Move r from U to O_i, without enlarging B or manufacturing credit. Install
   its exact Original provenance and evidence clock before native exposure.
4. Commit the native transaction with r; any competing claim observes it absent
   from U. Failed pre-claim validation changes no ownership or source frontier.

In particular, do not apply a fresh `B+q<=W` test to this conversion: a full
but legitimate prepared window must still permit its first claim. Only
preparation increases B; claim newly consumes target authority and wire-offset
credit. Native and Product capacity remain distinct. The unnumbered source
staging may wait for future receiver credit, under its unchanged bounded
staging resource; it does not pretend that credit was already advertised.

Prepared bytes cannot qualify a path, form a path delivery sample, hold an
old path's Original flight, or become native-loss/recovery evidence. Claim
installs the exact owner's generation/assignment clock. Existing claimed
Originals retain their old clocks; preparation or a new producer cannot reset
them. Positive Data ACK clips claimed ownership exactly once; native ACK or
writer completion does not release Product B. A claimed write failure leaves
the existing retained/recovery obligation, rather than silently re-preparing
possibly transmitted bytes. Existing accepted-copy ambiguity remains intact.

The first-owner test uses `sum_i O_i=0`, not `B=0`: preparing source bytes must
not silently classify the first native claim as an unqualified additional
output and restore the old startup throttle. Later claims retain the existing
exact frontier/qualification rule. Prepared storage chunks do not define
atomic service geometry: the normal positive action quantum can span or split
them before ranking, with the same captured extent at native Apply.

## Required coordination and lifetime model

- The claim state must be directly available to the writer in a bounded
  synchronous transaction. No network/local application write may hold its
  lock; no guard crosses await. A mailbox request to a temporarily blocked
  Product actor is not an implementation of this condition.
- Eligibility, qualification, original debt and source-frontier changes used
  in a claim need one coherent owner or an explicitly proved atomic protocol.
  Mirrored counters and an eventually delivered bookkeeping message are not
  enough to make a frame safe to transmit before its ownership exists.
- Native writers continue feeding their native transport without waiting for
  packet ACK or a per-frame application-actor reply. Existing protected-write
  and inbound-feedback interlocks remain active. Shared QUIC connection credit
  is not duplicated across its logical stream writers.
- Preparing data publishes the actual ready-work wake. ACK/credit, membership,
  writer/native readiness and terminal changes retain their own wakes. A zero-
  claim pass arms/rechecks/parks; a successful claim with successor work retains
  bounded cooperative service. No same-state self-wake or polling loop.
- Multiple streams require bounded arbitration at the existing carrier writer
  boundary; one stream's unavailable prepared head cannot block independent
  ready work. Multiple writers cannot claim the same bytes. Claim only tests
  admission; a backup action must not bypass an eligible regular action.
- Prepared work has explicit live demand independent of claimed Original debt.
  An attachment must not retire merely because O_i=0 while its direction has
  eligible prepared work. This demand is not fake path proof or delivered bytes.
  It registers ready-source subscriptions, not active-path demand on every
  attachment: active Product path demand remains tied to that path's O_i.
- Source EOF fixes the final source extent but does not erase U. FIN/final
  offset, half-close, reset, detach, restart and terminal reclamation must
  account for both prepared and claimed work. No finalization reopens a closed
  stream or loses unclaimed data; cancelling preparation releases it once.

### Claim arbitration is part of the model, not executor luck

A writer's imminent transaction boundary is local readiness, not evidence of
spare network capacity. The shared owner must retain exact current readiness
and perform finite regular-before-backup/lane/eligibility arbitration; it cannot
silently replace the existing policy with whichever thread acquires a lock
first. A writer still inside a protected transaction has no new claim boundary.
If the preferred output is not at a claim boundary, it cannot reserve an
arbitrary future prepared suffix or make independent ready work wait for it.
For the request candidate, retain the existing full-membership ordinary
observation, advisory ordering and successful-claim cursor. Add an exact
writer-boundary predicate separately from structural membership. The current
FirstPath/frontier/AdditionalPath classification must retain actual claimed
ownership rather than filtering the member set to ready writers. Regular-before-
backup follows the RFC's finite exact-attempt rule: try every current regular
candidate in the applicable freshness/policy class first. Backup is eligible
only after that pass fails. Revalidate membership and authority, and repeat
selection against current sampled writer opportunities before commit. Fence
the selected writer's exact ready epoch; do not freeze every unselected writer's
ready/occupied generation. A busy regular is unavailable for this imminent
claim, not stale, dead or permanently demoted. A regular observed newly ready
by final selection participates and displaces a backup when eligible. This
does not promise an instantaneous globally best choice under arbitrary
concurrent readiness changes. Never requalify a stale owner
or classify a filtered survivor as a fresh FirstPath merely to claim bytes.

Pre-code cross-check rejected this draft's earlier blanket ban on backup while
regular writers are busy. Current latency selection is queue-aware and can use
backup after all regular choices fail; blindly reusing the bulk Apply helper
`current_request_original_data_tier` would instead impose its structural-
membership veto and narrow that behaviour. The existing blocked-regular test
still has an enqueueable regular QUIC reference and does not justify an all-
regular-blocked ban. The new claim arbiter must implement the full failed-pass
and generation rule above, not treat that helper as the whole policy. This is
a model-migration obligation found before writer code, not a separate new
runtime fix or evidence that an arbitrary busy instant grants backup priority.

A caller claims only when the coherent finite decision selects that caller's
exact ready writer. If another currently ready writer wins, leave U untouched
and coalescing-wake the selected writer; install no sticky payload grant. A
later attempt revalidates full membership, current readiness and authority.
The losing writer arms/rechecks the corresponding generations and parks rather
than spinning. Withdraw readiness before every operation that can occupy the
writer, including control writes and awaited input forwarding. This rule
must be checked against existing persistence/hysteresis and tier controls;
those tests, not task wake order, determine whether migration preserved policy.

The executed integration counterexample below supersedes the earlier blanket
whole-Ready generation rule: it admitted a zero-progress retry cycle. Readiness
is scheduling evidence except for the selected epoch consumed by the claim;
an unselected withdrawal does not revoke source or target authority.

This readiness representation is not a per-frame actor handshake. A singleton
writer must synchronously claim its next prepared action immediately after its
previous native transaction, with no actor roundtrip or Product ACK. A successful
claim may cover the existing imminent encoder transaction geometry, but not
prefetch another fixed-target private suffix waiting behind it. No guard may
span native I/O. The lock/fence order and cancellation of ready claims remain
explicit implementation-proof obligations; their absence would block code.

## Concrete request migration boundary

Existing `dispatch_client_data_work` calls `ReliableSendStream::send_data`,
then installs per-target ownership through `RequestSenderService` and publishes
the carrier command. Failed publication rolls the mux commit back. The new
boundary must preserve that transaction's effects while moving it to claim;
do not keep early `send_data` and merely relabel the command as prepared.

The smallest coherent shared aggregate includes:

| Existing owner | Why it belongs to the claim transaction |
| --- | --- |
| `ReliableSendStream` |C, receiver credit, retained claimed cache, positive ACK validation/release |
| `ReliableRelaySenderQueue` |Unnumbered Data and its joint bounded accounting with repair/control queues; splitting Data alone would need another reservation protocol |
| `RequestMultipathController` |Exact flights, qualification/rate/requalification, ACK-clock operation and successful-send cursor |
| Exact attachment admission records |Identity/generation, output/lane/proof and claimed-load ownership; no stale mirror may authorize native exposure |

The concrete source review simplifies this partition: retain the whole
RequestSenderService, send mux, sender queue and RemoteSet's synchronous
membership/publication metadata as one actor-owned aggregate initially. Keep
its existing feedback cursors, optional epochs, output handles, load/proof/lane
authority and forwarder abort tokens; splitting each into a separate registry
would add coordination without a demonstrated benefit. Forwarder tasks already
own their executing receiver/sender independently; their JoinHandle only
aborts synchronously on exact removal/Drop. Move the merged `frames_rx` into
actor-owned input, with its existing receive/ready-count/try-receive methods.
The set retains `frames_tx`, so empty live membership does not manufacture EOF.
Already merged input is neither filtered nor erased on path removal.

Local pending response writes and actual open/close I/O remain outside the
aggregate. Membership invalidation updates authoritative claim eligibility
before asynchronous teardown, retaining existing claimed debt for recovery.
An owned close future may be created only at the selected terminal action:
synchronous withdrawal at construction must not accidentally execute discarded
select alternatives. No source or ownership guard may cross its later await.
This first extraction stays actor-owned; adding a mutex before separating
planning from Native-fenced Apply remains forbidden.

The used aggregate/input extraction passes483 existing controls in1.28s after
a1m06s build, with only the original128KiB proposed-contract RED remaining.
Independent review confirms queue/sender/cache/membership drop order, accepted
merged-frame preservation and eager withdrawal only in selected terminal branches.
It creates neither a shared lock nor a new scheduling boundary. That equivalence
checkpoint permits the next claim transaction work, not performance acceptance.

Do not wrap today's whole sender/ACK/planner methods in a mutex. Normal request
observation reads Native shapes before health observations; making that call
under Product ownership would invert the existing Native-to-Product direction.
Advisory observation obtains native inputs outside the Product lock. Final
QUIC claim reuses `commit_with_current_scheduling_shape`: activation fence,
coordinator, shape, then Product ownership and applicable health bookkeeping.
Under that fence use the supplied target shape and suppress other Native reads,
as the current native-override Apply already does. Release every guard before
encoding/native I/O. Actor ACK/recovery must either operate only on captured
non-Native inputs under Product ownership or follow the same Native-first
transaction; no Product-to-Native callback is permitted.

The next used seam captures only ordered exact attachment/proof identities,
membership generation and cloned UDP Native authority handles. Resolve Native
outside Product, then reject a changed receipt before projecting current path
fields. Re-read lane/admission/load/qualification/flights there; do not cache
them as another authority. The serialized actor wrapper may expect its unchanged
receipt; a future concurrent caller must reobserve on rejection. TCP fenced
Apply must also consume explicit resolved inputs: the old optional QUIC override's
`None` means normal Native capture, not no Native read. This is a migration
obligation, not evidence of a present actor lock deadlock.

The writer also needs the actual authoritative ACK snapshot currently owned by
the actor's `last_send_ack`, not a default Live flag inferred from positive F.
Move that authority into the same Product transaction when claims are shared.
The narrow ACK transaction already settles mux/flight/qualification and health
sampling without Native reads; the outer handler's subsequent staleness and
recovery observations do read Native and must stay split. Existing retry/watch
timing does not become a second ACK authority.

Detached capture and explicit TCP-normal/QUIC-fenced Apply input are now used
by the current actor:485 focused controls pass1.26s after a warning-free1m09s
rebuild, including unchanged observation/choice and rejection after exact
replacement, explicit order change, or actual proof renewal. The128KiB producer
RED is unchanged. Independent review confirms read count/order and current
commit/rollback semantics. Capture adds bounded per-path metadata/Arc work;
its cost is not a claimed optimization and ordinary migration cost remains a
gate. A first test read the fixture's undecorated command clone rather than its
attached Native output and failed setup; correcting that fixture is not a
Product fix or a waived invariant.

The used extraction keeps three responsibilities explicit: the existing
Original chooser consumes its observation without Native reads; the fenced
Product commit records flight, qualification, load and cursor; the selected
reserved publication follows synchronously under the same fence. Legacy repair
selection must not blindly reuse the Original observation: its bulk-evidence
flag is gated differently and would wrongly exclude persistent repairs.
Neither helper alone authorizes concurrent claims. Source C/prepared head,
ACK/qualification changes, readiness and current exact admission must be
revalidated by the final shared transaction before it calls the commit helper.
The sole ACK snapshot is also now a Product field, still passed by actual borrow
through current actor ACK/recovery orchestration. Independent equivalence audits
pass; warning-free1m08s build and485 GREEN1.27s, unchanged128KiB boundary RED.
No mutex, weak writer subscription or native-claim boundary is active yet.

Existing request structural-recovery batches assume one serialized Dispatch.
After this migration, a writer may append a higher-offset Original while the
actor retains that batch. Such an append cannot silently invalidate the old
range's ownership/copy exclusions. Membership, ACK/requalification and queued
copy mutations still require batch invalidation; each final Apply rechecks
current target P/K and membership. Preserve the once-per-batch overlap work
property without retaining its obsolete no-concurrent-mutation premise.

Writer integration starts after the existing retirement/control/priority/
repair/data lane arbitration. TCP claims immediately before its protected
ordinary transaction. QUIC claims the imminent bounded transaction only after
any optional coalescing yield, not when filling a private `pending_frames`
prefetch. A coalesced weak source notification can occupy the existing Original
lane without claiming payload bytes. Exact writer readiness is a new owner,
not authentication readiness, an empty queue, or zero sampled pending bytes.
TCP shares that writer across streams; QUIC's attachment writers still share
connection-native capacity. Notifications must neither obstruct independent
ready streams nor retain cancelled sources through reference cycles.

## Conditional benefit and limits

If lower bytes r are still prepared, an eligible alternate actually claims
them before the first writer becomes able to do so, and all admission checks
pass, r no longer waits in that first writer's private future-work queue.
Exactly one native Original transaction owns r. The claimed local benefit is
removal of that pre-native path-binding wait without Product copy traffic.

This is not a promise of earlier remote receipt: the alternate can suffer a
later outage or share a constrained cut. Already-claimed slow prefixes still
require existing recovery. A native writer can accept a large native backlog;
native acceptance is not physical service. This candidate therefore neither
proves a sustained allocator nor closes T03's unknown-path discovery/shared-
bottleneck questions. It must not be sold as a complete500Mbps or ideality fix.

Unlike queue shrinking, the sum of prepared work need not be one quantum or
one frame per carrier. Unlike adding repair permission, changing which writer
claims unclaimed bytes creates no second native copy. The price is a real
shared ownership/claim implementation and its contention/lifecycle cost, not
a new configuration constant. If that implementation needs a per-frame actor
roundtrip or locks across awaits, reject it before performance experiments.

## Pre-implementation discriminator and stop conditions

Before migrating production, map the unnumbered preparation representation
and actual source/cache/claim/native/ACK lifecycle in both directions, and close
the remaining readiness/arbitration/fence ownership obligations above.
The first boundary fixture uses actual request source dispatch into the
unchanged default command queue, with no data-command consumption or writer.
Two normal source quanta must remain conserved without a claimed wire horizon
or exact target Original ownership under the proposed contract. This identifies
the current early-binding boundary; it is not a current RFC-mismatch test and
does not prove alternate service or a future timing benefit.

The subsequent integration discriminator must hold TCP before its next
protected transaction, prepare multiple legal quanta, then make QUIC genuinely
able to claim an Original under unchanged authority. The lower unclaimed prefix
must enter that writer with no old-writer submission and no duplicate bytes.
A fixture that simply lowers queue capacity or cannot satisfy P/E is not that
intended migration proof. Keep these two claims separate.

Necessary adverse controls are: old writer claims first; no alternate; current
P/E/receive credit exhausted; shared QUIC credit exhausted; carrier replacement;
claim cancellation/partial native write; prior-copy ACK ambiguity; multiple
streams; EOF/FIN/reset; blocked application writer with functioning native
feedback; and sustained singleton high-BDP feeding. Preserve earlier failures
instead of declaring the new architecture exempt from their tests.

Independent model review comes before implementation. Focused real-producer
RED/GREEN follows; then ordinary affected first-body, ordered forward progress,
read gaps, loaded latency, complete bytes and CPU/RSS/wire costs under the
unchanged profile. Stop promotion on an adverse or ambiguous pair. No new rate
prior, timer, percentage, queue size or favourable third run may make it pass.

## Producer boundary result — 2026-09-08 10:06 +08:00

`prepared_request_data_keeps_wire_horizon_unclaimed_until_writer_start` uses
the real request sender, default carrier queue, retained live attachment input
and two normal64KiB source quanta. Only the attachment proof is consumed;
no data command reaches a writer. Source/retained conservation, ordinary target
eligibility, credit, absence of repair and non-full queue controls all pass.
The final proposed-contract assertion alone fails: wire horizon131072 and
exact TCP Original[0,131072), versus an unclaimed horizon0 and no Original.
The existing483 focused sender/queue/relay/request/mux/interlock controls pass
in1.27s. Functional build1m11s; no optimized build or lab for this boundary test.

This is a real producer counterexample to the proposed ownership contract,
not proof that current code violates its current early-publication RFC. The
live pre-native waiting evidence supplies the practical reason to consider
changing that contract. Alternate claiming, singleton feeding, migration cost
and ordinary timing remain unproved. Do not waive them after this RED.

## Used preparation refactor — 2026-09-08 10:24 +08:00

All10 request sender async methods were transitively non-yielding. Their
wrappers and immediate caller awaits are removed; the two deferred timeout
fixtures retain lazy async blocks. Actual open, receive, native I/O and close
awaits remain. Native capture now supplies an owned, consumed value seam with
the same eager read order and target-only override; it is not yet a concurrent
membership receipt. Both attachment wrappers now take owned advisory inputs
and an optional immutable FIN offset captured before opening. All9 actor
callers retain their prior lane, mode and FIN flag. Current actor serialization
makes this offset equal to the previous post-open read.

Independent source reviews passed; functional rebuild1m06s and the existing483
controls pass1.27s. The intended128KiB boundary RED remains unchanged. This
preparation installs no shared mutex, changes no Original publication boundary
and claims no speed gain. The next owner extraction must make captured
membership validation real and keep native I/O outside the shared owner.

The next extraction question is ownership, not a new network hypothesis:
can the existing actor retain one authoritative send/queue/flight/membership
aggregate while merged reception and teardown own no borrow of it? Existing
field/method audit permits the smaller partition above. Falsifiers are changed
accepted-input drain/drop behaviour, changed cleanup order, a duplicate mutable
admission view or a hidden send-state borrow across I/O. Use the existing
terminal/half-close/default producer controls, keep the intended boundary RED,
and add no unrelated metric/queue/rate changes. This refactor is preparation
for the same observed pre-writer waiting defect, not practical acceptance.

## Integration question — bounded lock acquisition, not another controller

2026-09-08 11:39 +08:00. Both independent source reviews conditionally accept
the following smaller lock protocol for implementation. It supersedes this
draft's universal Native-before-Product requirement, not its byte or admission
invariants. No shared writer or performance acceptance follows from review.

The strict Native-before-Product draft above would require splitting every
existing synchronous actor recovery/ACK observer and publication into detached
intents. The actual source now has separate I/O boundaries and fenced source
commit. A smaller candidate preserves synchronous actor Product-to-Native
calls but makes the writer's Native-fenced Product acquisition **nonblocking**.
No writer may wait for Product while retaining any Native guard.

For locks P (the single request Product owner) and N (the existing Native
activation/coordinator/shape chain), an actor may hold P while waiting for N.
A writer may acquire N, then try P. If P is busy it immediately leaves N,
retains no source grant, and parks on a previously armed owner-release wake.
The wait-for graph then has P→N but no blocking N→P edge, so this pair cannot
form a lock cycle. Once P is acquired in the fenced writer callback, only
current Product state and the supplied target Native shape may be consumed;
entering another Native authority there would invalidate this argument.

Busy is a synchronization outcome, not failed admission, stale path evidence
or a reason to try a backup tier. Arm and enable the release wait before the
try-acquisition. The owner guard unlocks before notifying; reader guards also
release any registered waiters. A losing writer must not manufacture a Product
generation or self-wake while nothing has changed. Existing source, ACK,
membership, Native and writer-readiness wake owners remain separate. Successful
singleton claims still need no actor reply or packet ACK.

This alternative changes the implementation lock protocol, not source/flight
invariants, claim policy, protected writes, resource limits or congestion
control. Required falsifiers are a Native-held blocking Product acquisition,
hidden Native call after a writer acquires P, a missed release wake, spinning
under contention, an actor guard across real await, or inability to preserve
exact current source/ACK/attachment revalidation. Until those are reviewed and
exercised, no successful shared-claim or liveness claim is accepted. The concrete
owner uses a non-Send standard mutex guard so production Send futures also reject
a guard retained across await at compile time. Neither this type property nor
the acyclic lock graph establishes bounded wall-clock service under arbitrary
contention; preserve actual claimant-progress and residence-time checks.

### Compiler falsifier and native arbitration distinction

2026-09-08 12:18 +08:00. First integration build rejects the proposed
same-variable guard drop/rebind shortcut, including its small require-Send
control. Use lexical synchronous scopes and owned I/O inputs. The compiler
check is useful precisely because a written intention to drop a guard is not
proof of the future's stored state. This is an integration failure, not a new
deployed performance defect or a reason to weaken the non-Send guard.

The receiver now owns Send-only deferred waits. Async flushes exclusively
borrow that receiver with `&mut`; shared accounting reads remain synchronous.
Requiring deferred futures to be Sync or declaring the receiver Sync unsafely
would misrepresent its ownership. This correction changes no arbitration.

Prepared source is not a published Original command. The actor's repair-only
queue view therefore serves critical then ordinary repair without consuming U;
the physical writer retains its existing control/repair/Original lane order.
This is not an assertion that the old actor's Data-before-ordinary-repair
queue order is unchanged. Retained-copy K/J/D, optional-copy accounting and
ordinary source authority remain unchanged. Verify repair pressure and actual
writer feeding; a queue-unit GREEN alone cannot prove service non-regression.

## Executed claim-boundary falsifiers — 2026-09-08 13:01 +08:00

The first coherent build passes after lexical guard/I/O scoping. The initial
integration run passes521/529; failures on legitimate PathProofData, missing
actual bulk proof, FIN accounting and pending terminal metadata were fixture
errors before their intended assertion, not Product defects. Strict fixtures
now consume only their known metadata and use the real challenge/ACK proof
path without fabricated rates or changed admission.

The corrected two-writer fixture executes two real retry cycles. A captures
A-ready/B-ready, B selects A and withdraws, then A rejects the changed whole-
Ready view. Each withdrawal wakes the other claimant. All source/admission
premises remain true, yet C remains0 instead of65536; the ordinary control
claims65536. This is a demonstrated liveness defect of the unaccepted candidate,
not a newly attributed deployed stall. Resampling other writer opportunities
at both selection stages and consuming only the chosen exact epoch removes
the false veto. Source, full membership, proof/qualification, negative ACK,
load, Native fence and W/P/E admission remain mandatory. Selected-path draining
and newly ready Regular-versus-Backup controls guard the opposite errors.

A separate actual two-claim fixture demonstrates the observer-clock defect:
all source/flight/stamp controls pass, but observing C later moves the fallback
anchor27ms past the successful claim. The delay uses the existing fixture PTO;
it is not a new runtime threshold. Store the successful claim time atomically
with C and use max(existing actor progress, actual claim time) when observing
new C. Failed claims and unchanged C do not update it; a newer ACK/control
anchor survives. No observation timestamp remains in that production helper.

Independent reviews pass and the completed warning-free rebuild passes533
focused checks in1.28s, including both actual physical multi-quantum/EOF transfers
and existing small TCP/QUIC controls. TCP's initial PathOpenTimedOut was caused
by the fixture omitting the carrier reconciliation owner; the wait-only open
predates this migration. The existing helper now starts concurrently with
ingress, asserting no initial carrier and doing no prewarming. No Product
timeout or setup policy changed. The Regular-becomes-ready control injects
before advisory selection; final readiness refresh is source-reviewed rather
than separately interleaved. Neither correction changes an existing claimed
assignment/copy deadline, renews evidence, or proves ordinary performance.
Request-only intermediate checkpoint is eligible; optimized ordinary timing,
response parity and the direction-neutral RFC amendment remain pending.

## Adverse composition and refused-claim readiness lifetime

2026-09-08 13:32 +08:00. Ordinary9720e4b fails settlement despite better early
forward delivery. PREPARED_ORIGINAL_ORDINARY_20260908 records the75s return
hold,69s target plateau and barely consumed client TCP receive backlog. No
practical promotion; do not treat the earlier component GREEN as this gate.

A second, distinct recurrence survives the valid-winner correction above:
when all current writer attempts refuse admission, A's Ready publish/drop
wakes deferred B, whose attempt wakes A. Actual receiver-owned deferred work
requeues the same weak token. Coalescing bounds token ownership, not frequency;
cooperative per-turn budgets do not eliminate recurrence. The proposed legal
counterexample prepares source first, then withdraws actual endpoint admission
without draining the writer. It does not assume impossible U under zero source
credit. Real singleton-park/two-receiver RED is the next required discriminator;
this source argument alone does not attribute the75s hold.

Independent pre-code review conditionally approves a simpler ownership rule:
Ready names physical idleness, not one metadata attempt. For R=Idle(epoch),
a refused/Busy/Empty metadata claim leaves R unchanged. Actual admitted data
consumes the exact epoch; control/data writes, awaited input routing, heartbeat
and drain withdraw it before occupation. An idle physical owner retains the
SAME guard across deferred source waits, irrespective of a logical stream's
cancellation. The claim borrows that guard; no Product/source ownership or
payload reservation is retained with it. A consumed guard is not current
merely because an Option contains it.

After genuine state changes settle, all-refused claims then change neither
R, U, C nor admission, so sibling-boundary notifications cannot perpetuate
themselves. Genuine return-to-idle changes still notify. This proves removal
of that recurrence, not arbitrary starvation freedom. Multiple streams share
the existing physical arbiter, not separate readiness flags per subscription.
QUIC successor claims remain within its existing bounded no-await imminent
batch; any live refusal guard must withdraw before flushing already-claimed
frames. TCP still protects one transaction. No timer, queue limit, controller
or policy preference is part of this proposed correction. Real RED/control
and used native-loop integration must precede an implementation/acceptance claim.

Executed13:37: the real producer/Failed-policy/weak-receiver case preserves
U, C0, cache/flight0, exact live attachments, model generation and zero native
charges. Singleton parks; two writers regenerate[2,2] retries against[0,0] over
two finite rounds. Initial idle opportunities are all published BEFORE waits,
excluding legitimate sibling appearance as the asserted defect. This is
current native lifecycle composition, not a full socket-loop or75s-stall proof.
The initial test build missed a namespace qualification; its old-binary zero-
match invocation is not verification. The completed1m12s rebuild executes both
tests and reaches only the intended pair assertion. Process success and a
nonzero matching test count must precede interpreting future verification.

Receiver-owned representation is independently preferred: one existing
exclusive ReliablePathCommandReceivers owns Option<ReadyGuard>. Reuse the
same current same-instance capability, refresh consumed/stale epochs only at
an actual next idle/imminent action, and withdraw before occupied operations.
This avoids passing an Option through every native helper without creating
another admission flag. A synchronous claim borrows the guard; that borrow
ends before receiver mutation or await. Receiver destruction invalidates its
receipt; claiming after destroying the physical owner is no longer a reachable
borrow-safe operation, not a regression case to preserve artificially.

Executed13:51: the receiver-owned correction builds warning-free in1m12s;
535 focused checks pass1.27s. The actual pair now parks without regenerated
metadata attempts, while the singleton control still parks. Genuine control
occupation/selected withdrawal, stale incarnation, successful consumption,
drain/drop, independent-source cancellation, source/flight conservation,
claim-clock and actual TCP/QUIC multi-quantum/EOF cases remain GREEN.
Independent audit confirms all normal admission refusals precede Ready
consumption. Post-consume source errors disable claims; qualification rejection
requires violated serialized ownership/configuration invariants, not ordinary
unchanged capacity. Do not add an unproven late-refusal workaround.

Practical question before the next ordinary run: does eliminating this proved
zero-progress recurrence remove the candidate's persistent client receive hold
and settlement failure without losing its early delivery improvement? Use the
same mixed/asymmetric500Mbps profile and ordinary optimized binary, no observer,
no configuration changes and no build overlap. Compare with the already
preserved9720e4b failure; a single changed-mechanism realization can falsify
recovery but cannot attribute all timing differences or establish universal
acceptance. Stop promotion if it still holds or fails settlement, then locate
the exact receive/decode/handoff stage. Component GREEN is internal tracking,
not a user-facing performance milestone.

## Native callback service: advisory owner contention

2026-09-08 15:25 +08:00. Pre-change model for the bounded transaction in
CURRENT_CLOSURE_PLAN. The completed COPY_DEBT_SERVICE_20260908 trace proves
winning-reply delays before and after local decode, with different cost
composition across phases. It does not measure individual mutex waits.

Origin9720e4b deliberately kept two advisory Product acquisitions outside
Native and made only the final Native-fenced acquisition nonblocking. That
satisfies the earlier deadlock argument. It leaves a different reachable
service failure: an actual native writer synchronously executing either
advisory std-mutex acquisition parks its executor thread and cannot process
its other ready input until the Product holder releases ownership. An empty
source inspection can wait too. The correction is a stronger implementation
service contract, not a claim that the earlier RFC specified try-locks.

Every Product acquisition inside a native writer claim should use the existing
prearmed nonblocking protocol. Initial contention returns Busy without reading
or mutating Product. Second-cut contention drops the advisory frame/receipts;
the successor starts again from current state. Each cut freshly arms after the
preceding guard has been released. A notification armed before its own unlock
must not become a reusable release credit. Final Native fencing and all exact
registration, lane, source, Ready, qualification and authority checks remain.

Busy grants no payload, DSN, flight, admission failure or Backup preference.
The writer defers its weak work token on owner release/cancellation and keeps
the same physically idle Ready epoch. It does not await owner release inline
or change actor-side serialized ownership. Therefore contention contributes
no blocking Product-acquisition edge to the native callback, while successful
claims preserve U→Original conservation and exact final publication. This is
not a starvation bound or a bound on other synchronous callback work.

Tradeoffs must be measured: two extra prearmed OwnedNotified allocations on
an uncontended successful claim; deferred requeue latency for short contention;
discarded/repeated advisory work at the second cut; and more simultaneous
release wakeups. Existing Notify-all semantics are not an excuse to invent a
new wake policy here. Expensive Product handlers and other Native/context
locks remain unchanged. This alone cannot promise removal of multi-second gaps.

Required actual producer controls retain the real owner across each cut,
observe Busy before releasing it, preserve bytes/flight/charges/readiness,
then release before first wait poll and retry the same lowest source. The old
implementation must reach this intended blocking assertion after cleanup,
not fail a fixture premise. Uncontended and cancellation/current-registration
opposites remain. Only then implement; focused GREEN/audit precede ordinary
full timing/completion/cost comparison. RFC serialized ownership, zero-commit
wakes and final arbitration are unchanged; request/response parity is still
a separate retained obligation, not silently completed by this correction.

Executed15:32: warning-free RED build1m12s. The real uncontended producer
control passes; both advisory cuts and the cancellation case reach only the
intended returned-before-release assertion after unchanged held-state checks
and complete thread cleanup.1pass/3fail in1.00s; no fixture/admission failure.
Independent review confirms the current proof/source/Ready provenance and
that no extra validation unlock masks the tested notification. This establishes
the actual native claim's blocking mechanism, not its share of ordinary gaps.

Executed15:34: only the two advisory acquisitions now use freshly prearmed
try-lock/Busy; final Native fencing and scheduling are unchanged. Warning-free
GREEN rebuild1m12s;542 focused controls pass1.28s, including actual contention,
unlock-before-first-poll, same-source retry/cancellation, prior all-refused
idle recurrence, selected withdrawal, half-close and actual TCP/QUIC EOF.
Two independent source/fixture reviews pass. The ordinary optimized candidate
build and unchanged-profile pair follow; this checkpoint is not performance
acceptance and does not supersede the control's5.052667s maximum gap.

## One stale-preference owner — pre-change proof, 2026-09-08 17:07 +08:00

PREPARED_CLAIM_SERVICE_20260908 separates two observed boundaries. The longest
pending-source interval overlaps an occupied QUIC write and repeated TCP
claims; its server's lower blocking range is already assigned. A different
late sample has stale QUIC physically Ready but raw can_enqueue=false while
fresh TCP writers are occupied. Neither observation alone proves all target
resource authority is available. Do not equate a source-hold interval with a
same-duration user-visible hold or claim that fallback repairs native delay.

The source exposes a definite composition error to test. Let R_i mean current
exact resource eligibility and D_i actual imminent writer readiness. The
prepared chooser must attempt its ordered fresh/stale Regular/Backup classes
over R_i AND D_i, retaining full membership for exact debt/position authority.
Its reused legacy observation instead applies an earlier gate:

    G_i = NOT stale_i OR NOT(any active/scorable nonstale attachment)

so the chooser receives R_i AND G_i and cannot restore a candidate later.
With fresh A occupied (D_A=false), stale B Ready (D_B=true), and positive
current B authority, G_B=false even though A cannot claim. Every tier then
refuses B. Repeated source/ACK/native wakes cannot repair this unchanged false
predicate. This contradicts the earlier imminent-opportunity contract.

Origin5d660f3b's gate usefully enabled stale sole-survivor fallback instead of
an unconditional stale ban.9720e4b introduced a Ready-aware failed-pass model
but retained the earlier attached-set policy. The correction is separation,
not removal of stale preference: named legacy observation keeps its policy
and short-circuit probe order; prepared resource observation omits that policy,
and its existing finite tier pass owns the preference exactly once. Share the
existing captured-evidence projection, not another Native read or cached view.

The real-producer RED must first establish legal current source/proof/credit,
actual stale transition and actual Ready ownership, then expose the refusal.
Fresh Ready selection, exhausted stale P/E, selected withdrawal, byte/flight
conservation and no false requalification are opposite controls. Successful
stale service must keep its existing evidence-ineligible provenance. No lower
resource limit, increased E, implicit protocol preference or new timer follows.
Conditional benefit is earlier legitimate Original assignment when preferred
writers cannot act; possible cost is use of a worse path under unchanged
resource authority. The ordinary paired timing/completion/cost gate determines
practical disposition; the large already-native stalls remain distinct.

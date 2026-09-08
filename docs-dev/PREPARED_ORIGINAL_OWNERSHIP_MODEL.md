# Prepared Original ownership — pre-implementation model

2026-09-08 09:40 +08:00. Category: bounded architectural candidate for the
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
If several exact writers are at boundaries, their candidate ordering and
cooperative successor service must be specified before implementation.

This readiness representation is not a per-frame actor handshake. A singleton
writer must synchronously claim its next prepared action immediately after its
previous native transaction, with no actor roundtrip or Product ACK. A successful
claim may cover the existing imminent encoder transaction geometry, but not
prefetch another fixed-target private suffix waiting behind it. No guard may
span native I/O. The lock/fence order and cancellation of ready claims remain
explicit implementation-proof obligations; their absence would block code.

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
The first producer fixture must hold TCP before its next protected transaction,
prepare multiple legal quanta, then make QUIC genuinely able to claim an
Original under unchanged authority. The lower unclaimed prefix must enter
that writer with no old-writer submission and no duplicate bytes. A fixture
that simply lowers queue capacity or cannot satisfy P/E is not the intended RED.

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

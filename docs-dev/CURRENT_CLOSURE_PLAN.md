# Current deterministic closure plan

Updated:2026-09-08 18:15 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full previous ledger, including all earlier failed
approaches, remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`.
Detailed evidence is linked below; shortening obsolete next-action prose does
not discard findings, adverse results or acceptance gates.

## Active discriminator: already-read response service residence

**Observed failure / exact question:** terminal capture on unchanged b783cd6
completes424017920B/56.939769s but contains a7.996908s confirmation gap.
Client reply F999@Unix1788861542294→F1013@1550291; the winning14bytes were
already read by server at1543136, leaving7.155s after server read. A later
exact reply range[1041,1055) spans server read1544756→clientdelivery1560442,
15.686s. All1208reply bytes reconcile across both observers. Which stage
retains these bytes: response admission/publication, native write/receipt,
client input routing/Product mux, or local delivery? No native attribution
from these end-to-end timestamps, and no whole-interval blocked actor claim.

**Existing evidence / model / competing causes:** earlier reply-stage
captures already contain reusable exact frame/write/decode/mux observations;
their different-version intervals are not silently attributed to this run.
The current terminal-only capture lacks these intervening payload boundaries.
T391950079 separately remains flat for14.999s in sampled server observations;
source EOF has32MB unclaimed data. Product work/feedback service, Native
ordered delay and unavailable exact source admission remain distinct owners.
No invariant or threshold is changed to force a selected cause.

**Smallest next action / falsifier / stop:** read and reuse the smallest
existing reply-stage observer on current source before authoring more hooks.
Observe exact server publication/native-write/client-decode/mux boundaries
for small reverse replies only, retaining the existing source/read/delivery
timestamps. Prompt write/decode excludes those stages; delayed publication
selects its actual authority/service owner. Exact same-range joins precede
any fix or wider Native packet/timer capture. No large cost framework, new
harness, favorable repeat or performance promotion from diagnostic throughput.
**Preflight18:08:** reuse existing sparse reply-chain hooks only: server
enqueue/dispatch/copy acceptance; TCP/QUIC write and client authenticated
decode; shared input and mux application; terminal observer's reply read and
successful delivery endpoints. Retain existing QUIC preceding-nondata
mailbox-wait aggregate to distinguish local reader blocking from Native delay.
No new seam, cost scope, request/copy-debt/claim counter or packet trace is
needed. Selective temporary adaptation is authorized; two independent reviews,
archive/freeze/reverse before one unchanged capture. No runtime correction,
build or lab is running at this entry.

**Observer18:15:** root and independent whole18-file reviews pass. Existing
write/route results, admission, queue charges and awaits are unchanged. TCP
decode lacks physical identity and requires session/wire-path joins; QUIC H3
IDs are connection-local. Full interlocked mailboxes remain pending, and
preceding nondata send-await time is not packet delay or ACK-only CPU time.
Sparse exact range coverage, not record counts, will determine winning service.
REPLY_RESIDENCE_TRACE_20260908.patch archives the whole temporary overlay.
One diagnostic build is running with no lab; freeze/reverse before capture.

## Completed discriminator: upload terminal service after full target delivery

**Result17:59 — prior terminal geometry not reproduced:** warning-free
3m31s build,203 total diagnostic lines,57service samples, exact424017920B
in56.939769s. Maxconfirmation7.996908s/maxwrite4.374934s are not ordinary
performance. At source EOF1552966, C391950079+U32067841 equals final424017920;
all queued bytes are unclaimed Data. FIN cannot declare current C then without
excluding those bytes. This does not prove the entire8.909s prepublication
wait necessary: no intervening claim/refusal trace was enabled.

Two same-offset FIN publications/native writes at1561875 retain24.47MB
Product cache, excluding a cache-zero prerequisite. Server decodes3133,
handles3134 with F419210455 (not yet final), and final Data makes FIN ready
at3384. Target shutdown starts/completes that same millisecond; final13B reply
read3385, client27B delivery3421 and clean completion3422. Duplicate FINs are
normal replay, not conflicting offsets. The1.258s FINwrite→decode remains
composite predecode service, not measured packet delay. No terminal/RFC
correction is justified by this capture. Earlier postfull-target6s tail stays
unattributed; detailed raw/evidence in TERMINAL_SERVICE_20260908.

**Observed failure / current owner:** ordinary b783cd6 completes326041600B
in52.965807s, with6.082958s maximum confirmation gap. Candidate service rows
48–53 already have full target T326041600, while server-read/client-delivered
reply Rs=Rc1142 are unchanged. Only final row54 observes13new server-read
reply bytes. Local TCP receive queues are zero through this tail, but client
native TCP Send-Q drains8.70→2.69MB. Those queues are not exact Product debt.
This is the existing completion-service timing owner, not a new bug inventory.

**Competing causes / exact question:** determine where the request EOF/FIN
and terminal response spend that tail: source EOF recognition, pending FIN
publication/admission, selected native ordered service, server FIN/mux-ready
handling, target write-half-close, sink natural EOF/final reply production,
or response delivery. lab/tcp_sink.py checks its .2s ACK cadence only after
recv(data); it does not produce an idle periodic ACK. Final OK is generated
only after natural EOF. Thirteen bytes are consistent with the final OK line,
not a direct content observation. Full T and flat Rs do not establish a
6s already-produced reply stall, nor a6s missing forward payload.

**Existing evidence / smallest action:** PREPARED_STALE_SERVICE_20260908
retains the ordinary pair. Inspect current terminal origin/RFC and existing
diagnostic seams first, then add only missing EOF/FIN/half-close timestamps
to one temporary diagnostic. Preserve actual admission, ordering, wakes and
all policy. No native controller change or larger packet-tracing framework.
Archive/reverse hooks before one unchanged-profile capture; a different or
absent tail is non-reproduction, not a performance win.

**Falsifier / acceptance / stop:** prompt source FIN publication followed by
held decode excludes source publication; prompt server FIN-ready/shutdown
excludes that stage. Observe producer timestamp before attributing response
transport. Exact same-flow/stage joins precede a real-producer RED/control
and any coherent fix. The earlier already-written QUIC predecode payload
stalls remain distinct and unresolved. No policy tuning or favorable rerun.
Native receipt/hole/timer source preflight is retained only as an alternative
if later evidence selects it; no Native observer or correction implemented.

**Observer17:51:** root and independent whole-patch audits pass. Temporary
11-file terminal_service hooks cover both actual source EOF reads, first
already-owned pending-FIN state, successful selected publication/attach replay,
actual TCP/QUIC FIN write and authenticated decode, both server FIN-ready/
target-shutdown branches, server reply reads and successful client delivery.
No new await/lock/payload clone/authority read or policy change. Write success
is local acceptance, not peer delivery; reply read is not sink emission time.
QUIC H3 IDs are connection-local; correlate existing bindings and retain
ambiguity if reused. Attach replay publication lacks exact path identity.
Patch archived as TERMINAL_SERVICE_TRACE_20260908.patch. One diagnostic
build running, no lab; freeze/reverse all hooks before the declared capture.

**Capture17:58:** warning-free3m31s diagnostic build frozen separately as
terminal-service-20260908. All11runtime-file hooks reversed with source diff
verified empty before one unchanged-profile capture. Enabled only
terminal_service, terminal_fin_replay, client_stream_fin_received and
client_relay_result. No compilation now; target/release is the temporary
diagnostic, not the ordinary b783cd6 binary. Failure remains begin-only;
post-publication observations are not an earlier commit timestamp.

## Completed transaction: one owner for prepared stale-path preference

**Ordinary pair17:36 — no performance promotion:** control389218304B/47.562042s
versus candidate326041600B/52.965807s, exact complete accounting in both.
First confirmation.641281→.448263s; maximum confirmation4.845539→6.082958s.
First write.131315→.104305s; maximum write3.553143→2.223001s.65.467→49.246Mbps
does not pass the timing/throughput gate. Candidate early target delivery is
better at10s but worse at15s; peak client RSS775264→832336KiB with unequal
work. Independent full-phase/cost analysis distinguishes its terminal hold
from middle incomplete-target and early already-produced-reply holds. No
favorable third run or causal rate ratio from these random unequal workloads.
Runtime b783cd6 remains a mechanism-correct intermediate checkpoint, not
accepted performance. No rollback to the proven duplicate gate merely from
one average, and no assertion it fixed the largest captured stalls.

**Observed failure / exact cause boundary:** latest capture completes484507648B
in44.176499s but maxconfirmation4.478852s/maxwrite8.647851s remain. Longest
C329149108 hold4.987s has a9.273s already-selected QUIC write across it;
TCP writers attempt353+ claims inside the hold. No lost notice or critical
queue-front cause is supported for that interval. Separate lateC471356880
has Ready QUIC, positive unclaimed source and plan refusals atUnix1788857702445
and7703448; stale tier eligibility is true but its input can_enqueue is false,
while fresh TCP writers are not Ready. These sampled predicates select the
double policy filter, not a claim that every downstream resource is free.

**Origin / model / competing cause:**5d660f3b allowed stale fallback only when
no attached active/scorable nonstale output exists. Its useful intent was to
avoid feeding a stale output while permitting sole survivors.9720e4b later
introduced finite Ready+fresh/stale tiers but reused that prefiltered snapshot.
The first mask ignores writer Ready and downstream Product admission; the
second tier can only AND it and cannot recover an eligible stale survivor.
This is a false availability premise, not insufficient congestion aggression.
The capture's largest actual server stalls remain already-written/predecode;
they are not attributed to this narrower gate or waived by its correction.

**Predicted bounded correction / falsifier:** separate resource observation
from the legacy nonstale preference. Legacy callers retain their current
wrapper/policy; prepared claims consume unmasked resource observations and
apply their existing finite four-tier policy exactly once. Preserve full
membership/Original debt, W/P/E, qualification, actual Ready, source and Native
fences. No new rate, timer, hint or synthetic can_enqueue override. Test the
actual producer with fresh active/scorable but non-Ready output and stale
Ready output having positive exact authority. Require same lowest-source
claim, without requalification or minting credit. Opposites: fresh Ready wins,
selected withdrawal refuses, and exhausted stale P/E still refuses. No
runtime edit until semantic controls pass and intended assertion is RED.

**Acceptance / stop / order:** independent model/fixture audit, focused GREEN,
then fresh ordinary e476308 parent first and candidate second on unchanged
mixed upload. Parent may run during test-only preparation, never compilation.
Retain full completion/timing/cost; adverse or ambiguous pair stops promotion,
not a favorable retry. Expected benefit is removing avoidable fallback
starvation; cost risk is more service on a stale path, bounded by unchanged
authority. Critical queue priority and native recovery stay unchanged.

**RED preparation17:18:** minimal extension of the existing competing-writer
fixture uses actual source publication, current Native/member capture and
mark_request_path_stale. A pre-stale plan identifies A; current post-stale
authority is recomputed independently of selection, without changing observed
flags. This positive case is legitimately FirstPath, not an Additional/E
exhaustion proof. Fresh Ready opposite and stale-ACK no-requalification checks
are included; existing selected withdrawal stays intact. Independent fixture
audit passes; functional RED build now runs with no lab overlap. Fresh ordinary
parent already completed389218304B/47.562042s, maxconfirmation4.845539s and
maxwrite3.553143s. Runtime remains unchanged until the intended assertion fails.

**RED17:21:** warning-free1m12s build; actual two-test run finishes.01s.
Fresh-Ready opposite passes. Stale-Ready claim alone fails at the intended
0versus65536-byte assertion after current authority, source, membership and
Ready controls pass. No fabricated snapshot flags or setup failure. Implement
only the agreed observation/policy separation now; independent review and
focused GREEN precede the ordinary candidate. Existing W/P/E helper controls
are not misrepresented as an actual stale-Ready E-exhaustion claim test.

**Implementation17:29:** only shared resource capture/projection and named
legacy/prepared entrypoints changed, plus the existing batch-type internal
re-export. Root reviewed every advisory/final prepared caller: both consume
the same resource projection and retain the unchanged current four-tier,
full-membership W/P/E, selected proof/load/Ready/source/Native checks. Legacy
short-circuit/read order remains identical. One coherent GREEN rebuild is
running; independent consumer review pending, no ordinary build/lab overlap.

**GREEN17:32:** warning-free1m09s rebuild;544 focused checks pass1.27s.
Both new actual stale/fresh controls and unchanged W/P/E exhaustion, selected
withdrawal, Native refusal, idle/wake, half-close and real TCP/QUIC EOF controls
pass. Independent consumer audit passes. Coverage disposition: current stale
producer is FirstPath; E refusal is covered by unchanged exact-authority
component tests, not a falsely claimed facade exhaustion test. Implementing
that additional real-claim setup is unnecessary to change this policy mask.
Checkpoint the isolated correction, then one ordinary optimized candidate
build and the already-fixed paired comparison. No performance promotion yet.

**Ordinary17:35:** runtime checkpoint b783cd6; warning-free optimized build
3m29s, frozen as prepared-stale-20260908. Source is clean except user's
unrelated seven-line document edit. One unchanged-profile candidate running,
no compilation. Independent baseline review finds existing matched mirrored
raw/Xray/H2 controls in review-mirrored-0906: raw completes4.279Mbps with
1.610879s maximum confirmation gap; Xray/H2 are incomplete. Their router
epochs match, but they are historical unequal-work random realizations and
cannot attribute current gaps to Native or Product. Nonmirrored raw274.677Mbps
must not be substituted. No redundant baseline rerun merely to obtain wins.

## Completed discriminator: available request source is not claimed

**Observed failure / priority:** second capture's largest target hold is
F471135545 for6.295860s. Source S already485359616 atUnix1788855768480, but
Original C stays471135545 until1788855774162;14224071 consumed-source bytes
are unclaimed. Server reaches F at5770186, almost4s before that next claim.
Client ACKs keep advancing F423163193→435960841 during that interval. This
is not source starvation, already-assigned native delay, or a7s repair backlog.
QOS_FORWARD_PREFIX_20260908 continuation records exact proof and contrary case.

**Exact question / competing causes:** what prevents a physical Ready writer
from claiming the lowest prepared range while source exists? Separate owner
Busy at three cuts, critical queue-front precedence, mux window/cache refusal,
missing Original incarnation, no Ready/eligible lead or whole-frame Product
authority, and final source/Native/Ready/proof/load revalidation. Capture the
actual failing predicate; aggregate management/native flight cannot infer it.

**Origin/model:**9720e4b preserves byte conservation/late native assignment and
inherits the combined queue's critical-repair-before-Data front at both initial
read and RequestQueuedSourceCommit validation. A TCP-bound critical intent
could therefore veto independent QUIC source, but this is only a candidate
cause. RFC10.4's final-writer priority is not proof that this global barrier is
necessary. A missing retained Original incarnation is a separate pre-planning
gate; do not mislabel it as absence of every possible copy transmitter.

**Smallest discriminator / acceptance:** one diagnostic on unchanged e476308
and unchanged profile, using existing stage events plus bounded per-stage
claim counts, first/last occurrence timestamps and sampled actual state at
the existing1s diagnostic cadence. Counts are through each emitted sample,
not a promised final total; no intermediate state is reconstructed.
Observe queue front kind/cause/range/bound target, C/F/source bytes, Ready
identities and already-computed admission/refusal inputs. No per-attempt log
flood, extra Native sampling, queue scan for display, reservation or policy
change. Independently review, archive/reverse, then capture once. A long hold
must join actual refusals; absent holds are non-reproduction. Test a focused
real producer counterexample before any model correction. Do not jump from
empty native flight to removal of proven Product ownership.

**Observer preparation16:40:** count notice publication/coalescing, dequeue/
claim entry and deferred completion too: zero claims alone cannot distinguish
no notice, parked wait and a genuinely occupied writer. Do not add a select
or strong Product lifetime to instrument wake outcomes. A process-local
bounded-capture helper formats state only when emitting and releases its own
counter mutex before formatting/output; Busy has no new Product read. Reuse
the first exact forward-range overlay, not the now-answered repair-route trace.

**Retained alternative:** the same second capture has a different QoS5.138s
hold: repair local acceptance→decode4.899s, route27us. All1407 completed repair
routes total41.466ms/max3.494ms. That route alternative is closed for this
capture; native ordered delivery/task service remains unresolved. No threshold
change follows. Native reorder tolerance/priority source inspection is context,
not measured missing-packet attribution. No native trace/retuning now.

**Observer audit16:49:** actual claim-return stages and already-computed
 planner inputs are sampled separately from notice/wake outcomes. Planner
 counts are branch visits, not claim totals; shared candidate instrumentation
 excludes repair-mode calls. Queue acceptance is separate from notification
 activation. Biased wake labels identify selected branches, not exclusive
 causes; physical-key counts may merge attachment generations. Root and
 independent source reviews preserve all policy/Native reads and ownership.
 Exact16-file overlay retained in PREPARED_CLAIM_SERVICE_TRACE_20260908.patch;
 one diagnostic build next, no simultaneous lab. No runtime fix proposed.

**Capture16:54:** warning-free optimized diagnostic build3m33s. Frozen as
prepared-claim-service-20260908; all16 observer files reversed and src diff
verified empty before capture. One unchanged-profile mixed upload now runs,
with exact forward events plus request_prepared_claim/request_prepared_plan/
prepared_notice. No build/lab overlap; target/release is diagnostic-only.

**Result16:55:** runner0,45service samples,484507648 exactB/44.176499s.
10428Originals cover complete source;6367ACKs release exactly that total.
All Originals write/decode; four losing TCP copies lack positive completion.
Top actual server holds4.408347/3.514626s concern already-written QUIC Originals.
The prior long unclaimed-head geometry is not identically reproduced. Root
and independent joins exclude lost notice/global critical front as causes of
the largest pending-source interval. No runtime/performance acceptance.

## Completed discriminator: QoS forward-prefix stall

**Issue / evidence:** the completed e476308 ordinary run has a5.000s sampled
T173608233 hold during15–20s. By16s S−T=64MiB, Rs=Rc435 and all client TCP
Recv-Q values are zero. Native TCP ACK bytes and QUIC accounting still advance.
This is not the previous already-produced reply backlog. Separate Rc449 and
Rc841 holds with continued T/Rs progress remain recorded, not waived.
ADVISORY_OWNER_SERVICE_20260908 contains the full pair, phases and raw bins.

**Exact question / alternatives:** which original attachment owns the blocking
request prefix, when was it actually claimed/written, and did an admitted
recovery copy exist before server ordered delivery resumed? Alternatives are
no source assignment, native queued/lost service, unavailable exact recovery,
and local receive/ordering service. Aggregate S−T is neither claimed C nor one
64MiB native queue. Zero client Recv-Q does not prove zero server input delay.

**Smallest discriminator:** one diagnostic capture on unchanged e476308 and
the same profile, not another ordinary performance trial. Reuse server hole/
delivery-stall and client ACK/recovery events. Fill only absent exact range,
instance, claim/write and receiving-frontier boundaries needed to join the
blocking request; no per-attempt scheduler logging or aggregate cost overlay.
Archive/reverse every temporary hook before running its frozen binary. Review
its owner/lifetime and timestamp semantics independently before build.

**Falsifier / stop:** a different or absent long forward hold is non-reproduction,
not improvement. Exact source/range/instance joins precede blame; write completion
is local native acceptance, not remote arrival. Accepted ownership does not
prove protected write completion; duplicate arrival does not identify a winner.
Follow the earliest supported held boundary and retain unknown sub-stages.
No controller/resource/threshold correction without a real counterexample.

**Execution16:06:** source and independent whole-overlay audits pass; diagnostic
build3m34s, warning-free. Exact10-file temporary observer patch archived as
QOS_FORWARD_PREFIX_TRACE_20260908.patch and all source hooks reversed before
capture. Frozen qos-forward-prefix-20260908 binary now runs one unchanged
profile. Normal e476308 stays separate. Local success is not peer receipt;
QUIC split/coalesced byte coverage and connection-local H3 identities must be
joined honestly. Sparse mux events may leave duplicate winner ambiguous.

**Measured16:07:** complete374669312B/65.783216s, maxconfirmation5.592993s,
maxwrite3.933834s; diagnostic performance is not ordinary acceptance.
Independently checked18779 Original commits cover
exactly[0,374669312); all24009 Original/copy commits have successful local
writes, and applied ACK release matches total. Full raw capture retained in
QOS_FORWARD_PREFIX_20260908.raw.tar.gz. No build/lab running.

**Exact next boundary16:14:** QoS F185133452 andF187796428 holds3.432533/2.661961s
release within1ms of authenticated decode. Longest late F298838412 hold4.752859s
is won by a QUIC repair locally accepted9.751s before decode, then mux+5ms.
At admission5490376 preceding repair payload bytes were accepted but not
decoded; this is not yet native queue attribution. Between its predecessor
decode and winning decode4.750s later, ordinary QUIC decodes6970816B on the
same connection. This falsifies connection-wide decode absence but leaves
native ordered-stream delay versus per-repair routing/reader service.

**Bounded continuation / falsifier:** existing repair reader awaits routing
after each decoded frame and also admits requalification records invisible to
StreamData-only traces. Reuse the frozen-stage overlay and add only before-read,
read-complete and route-complete timing for that reader (including nondata
records), then one unchanged-profile capture. No controller/queue correction.
Long route await identifies local ownership; prompt route completion followed
by a held read excludes that await, but still does not measure wire arrival.
Counterfactual lack of the old hold remains non-reproduction. Independent
review, archive/reverse before capture, full completion/cost retained.

**Continuation build16:18:** repair-reader observation passes independent
source/semantics audit. Feature-gated local ordinal covers StreamData and
StreamRequalifyData, no new await or clone; EOF/error/cancel remains begin-only.
The complete reused overlay plus one reader observer is archived as
QOS_REPAIR_READ_TRACE_20260908.patch. One build underway, no lab overlap.
First-capture full evidence report and raw archive now retained; it disproves
neither all native delay nor all local delay, and no policy fix was made.

**Capture16:22:** diagnostic build3m32s, warning-free; frozen separately as
qos-repair-read-20260908. All11-file observer hooks reversed and src diff
verified empty before starting one unchanged-profile capture. No build now.

**Result16:23:** exact485359616B/52.139505s, maxconfirmation7.128953s. Full
raw53bins/accounting/read-ordinal joins retained. Four losing TCP copies lack
positive write completion; every Original completes and final ACK/cache
reconciles. Readoutcome without prior work availability would falsely label
the longest7.094s read a transport stall. Actual copy appears only252ms before
decode. This counterexample selects the earlier unclaimed-source boundary
above. No ordinary comparison, model fix or performance acceptance follows.

**Disposition / scope:** e476308 remains an isolated mechanism-correct
intermediate checkpoint, not a promoted performance fix. Its early receive
consumption and T improve in this realization; worse max gaps and later
settlement prohibit uniform benefit claims. No rollback of exact ownership
from a random average alone. Source audit of existing ACK-release cost found
an exact potential untouched-suffix work exclusion, but it is NOT the next
fix: no RED/implementation or new obligation follows while the observed worst
phase is forward starvation. Keep that contingent reasoning only for a later
cost-selected question. All global gates below are unchanged.

## Completed transaction: native claimant owner admission

**Ordinary pair complete15:39 — no promotion:** optimized e476308 build3m31s,
warning-free. Parent9ea25e2 completes355532800B/44.964483s; candidate completes
409796608B/49.163739s. Maximum confirmation5.052667→6.197568s and maximum
write1.356326→5.462440s worsen; first confirmation.848948→.713189s and
first write.580035→.122947s improve.63.256→66.683Mbps is not acceptance.
Full phase/socket/cost comparison is complete; all raw data and RED/GREEN/
build logs archived in ADVISORY_OWNER_SERVICE_20260908.raw.tar.gz. No build or
lab running, no further runtime edit. Different work and random realizations
prevent attributing all changes to contention. Preserve earlier adverse cases.

**Pre-change decision15:23:** the completed reply/cost joins below select a
reachable local service boundary, not a universal congestion explanation.
Two advisory `owner.lock()` acquisitions in `claim_prepared_request_data` can
park a native writer's executor thread while the Product actor holds its mutex
for ACK/recovery. The final Native-fenced acquisition already uses a prearmed
nonblocking try-lock; the two earlier acquisitions do not. Origin9720e4b
addressed the lock cycle, but absence of a lock cycle does not ensure responsive
native input service. An empty source check can also wait behind that owner.

**Question / competing causes:** does the actual prepared producer return
without blocking when either advisory acquisition meets retained Product
ownership? Actor work and FIFO input backpressure remain distinct causes of
the captured gaps. Claim elapsed includes contention and scheduling; it does
not measure mutex waiting separately or prove it caused every held reply.

**Model / predicted correction:** use the existing freshly prearmed try-lock
at each writer acquisition and return its existing Busy outcome on contention.
At the second cut, discard the advisory frame/receipt and retry current state
after unlock. Preserve exact registration/source/Ready/Native checks, final
fence, U→Original conservation, cancellation and all admission policy. Busy is
not evidence of a bad path or authority to select Backup. Actor-side ownership
remains serialized. No new timer, queue, coalescing, controller or threshold.

**Smallest action / falsifier:** test-only actual producer controls retain the
real Product mutex in another thread at each cut. Require Busy before release,
no committed byte/flight/charge change, unlock-before-first-poll wake and the
same lowest-source claim after release. Bound only test cleanup so the old
blocking implementation fails rather than hangs. Uncontended claims and stale
registration/terminal refusal remain opposite cases. Independently audit
fresh arming after the first unlock: reusing a pre-own-unlock wait could spin.
Only a real RED permits runtime editing. Then focused GREEN and one ordinary
candidate/parent pair on the unchanged profile, retaining full completion,
phase/gap and cost evidence. A worse/ambiguous result stops promotion; no
diagnostic-rate acceptance or unrelated model expansion.

**Pair order fixed before execution15:25:** fresh frozen ordinary9ea25e2 parent
first, then the candidate only after actual RED/GREEN/audit. Parent capture
can run during test-only fixture preparation, with no build running. This
uses host time without mixing a build into the lab; the older ordinary9ea
realization is context rather than a substituted control. No favorable retry.

**RED15:32:** actual uncontended control passes; both advisory acquisition
cases and cancellation reach the intended blocking assertion only after
semantic held-state controls and thread cleanup.1pass/3fail,1.00s;
warning-free build1m12s. Independent model/fixture review passes. Implement
only the two existing prearmed try-lock acquisitions now. Fresh ordinary
parent completes355532800B/44.964483s, maxconfirmation5.052667s; this variation
is retained alongside the earlier11s gap rather than called acceptance.

**GREEN15:34:** two advisory acquisitions changed, final fence unchanged;
542checks pass1.28s after warning-free1m12s rebuild. Independent model and
consumer/fixture reviews pass. Ordinary optimized candidate build next,
no lab overlap. No practical promotion from this checkpoint.

## Completed discriminator: remaining winning-reply service hold

**Executed15:00:** warning-free diagnostic build3m33s; all observer hooks
archived/reversed before capture. Runner0,366018560 exact bytes/43.726528s;
maxconfirmation4.375291s, maxwrite4.114246s. It does NOT reproduce the ordinary
11s gap and cannot establish better ordinary performance.4371total log lines,
44service samples; products/probes stopped. Full raw diagnostic preserved.
Exact winning-reply joins and aligned nested-cost analysis are complete in
COPY_DEBT_SERVICE_20260908. No additional runtime fix, build or lab is running.
The4.374s F639 hold is mostly before decode; F737 includes1.203s already-decoded
local residence. F821 has at least2.722s preceding reader-send-awaited overlap,
but different cost composition from F737/F68. Neither global ACK cost nor
recovery cost alone explains every hold.73 winning mux advances all reach the
local writer within4ms after mux. Native-writer owner contention is a source-
reachable mechanism to falsify next, not a conclusion from one aggregate.

**Issue / observed failure:** ordinary9ea25e2 completes254083072 exact bytes in
48.973579s, but maximum confirmation gap remains11.042148s. Candidate Rc433
holds10.001s (Unix1788849816832–1788849826833) while target/response production
continues. Separate target T230311809 holds9s at1788849821825–1788849830825.
These are real fluent-service failures, not solved by improved total Mbps.

**Competing causes / exact question:** is the winning missing reply still
before native decode, between decode and Product input/mux, or after mux at
local delivery? Native congestion/backpressure, local routing/executor service,
costly preparation/dispatch and local delivery remain alternatives. Ordinary
management/socket counters do not locate the exact frame. The older local
decode-to-mux2.226s attribution belongs to another version/realization.

**Existing evidence:** COPY_DEBT_SERVICE_20260908 retains the complete ordinary
candidate/parent pair and all raw bins/costs. PREPARED_REPLY_SERVICE_20260908
retains100 winning-frame joins and synchronous cost buckets from the prior
sparse diagnostic. Inside that prior held interval, Product debt changes
6068248B between snapshots: globally frozen recovery state is disproved.

**Smallest experiment:** reuse the archived sparse reply-stage and aggregate
observer on9ea25e2. Add nested synchronous scopes for recovery range preparation
(two phases), complete target selection, Native resolution, Product projection,
queued-copy debt, repair Apply and its fenced bookkeeping. No per-attempt logs,
new harness/profile, target-observation cache or runtime policy correction.
The reused hooks plus seven scopes passed independent review and were reversed
before running the frozen diagnostic binary. No ordinary run is active.

**Falsifier / stop / acceptance:** exact winning-frame joins precede attribution.
Prompt decode/mux rejects that local stage; prepublication/predecode delays
remain distinct. No new long gap means non-reproduction, not ordinary repair.
Nested elapsed totals are not independent CPU totals and cannot be added.
Diagnostic Mbps is never ordinary acceptance. No further code correction until
a reachable mechanism/control and a clean model justify it. If evidence selects
a different stage, follow it rather than force the recovery-cost hypothesis.

## Current source and latest practical disposition

Runtime checkpoint **b783cd6** separates prepared stale preference; actual RED
checkpoint f1b0900. Its ordinary pair above is mixed/adverse and not promoted.
Parent **e476308** adds the two nonblocking advisory acquisitions above; RED
checkpoint584b748. Its ordinary result also remains mixed/adverse in timing.
Earlier **9ea25e2** maintains exact additive accepted-copy debt
by attachment; RED checkpoint **3396087**, evidence checkpoint **ebad57f**.
No shelved sampler, new Native observation policy or congestion tuning is active.
The user's seven-line LIVE_OWNER_FRONTIER_WORK_BOUND.md edit is untouched and
must remain outside commits.

| Outcome | Parent b3dfef1 | Candidate9ea25e2 |
| --- | ---: | ---: |
| Exact completion |No,85s observation guard |Yes |
| Confirmed / accepted bytes |93570384 /180748288 |254083072 /254083072 |
| Elapsed seconds |85.948541 |48.973579 |
| First / max confirmation gap seconds |.467841 /66.779472 |.397566 /11.042148 |
| First / max write gap seconds |.154499 /15.796057 |.128872 /1.011139 |
| Longest sampled Rc / T hold seconds |66.001 /60.000 |10.001 /9.000 |
| Peak client / server RSS KiB |511988 /90968 |677496 /129508 |

Candidate first3s target delivery is worse, but10s delivery and settlement
improve. RSS is higher with more work. Random realizations, different duration
and unequal accepted/confirmed bytes prevent causal rate ratios or a uniform
non-downgrade claim. Parent reset follows guard teardown; its raw confirmation
bins are unavailable, not zero. Candidate49 raw bins include long zero spans.
No practical promotion or favorable third ordinary repeat.

### What the last correction actually proves

The real dispatcher emits the same4096B repair from69632 retained bytes and
performs two accepted-copy-debt queries. Fragmenting only an irrelevant Original
suffix raised visits4→130; the intended work assertion alone failed after
semantic controls passed.9ea25e2 maintains J_i=sum(retained copy bytes on exact i)
at append, ACK-fragment release and drain, replacing each full scan with one
lookup. Overlaps count separately; expiry/Native ACK do not erase debt; zero
keys retire and replacements remain distinct. Checked arithmetic is supported
by current final admission, not a new cap. No RFC quantity or policy changed.

Two independent reviews, a full-scan lifecycle oracle and538 focused checks
pass; RED/GREEN builds1m13s each, GREEN1.28s. Ordinary build3m29s. The practical
pair supports better settlement in this case, not attribution of every change
or acceptable remaining latency. See COPY_DEBT_SERVICE_20260908.md/raw.tar.gz.

A further Native observation-sharing shortcut is **not implemented**. It can
be a coherent Observe–Decide policy but is not equivalent when a path changes
mid-selection: current chosen Apply cannot recover a better omitted target.
Do not silently package it as source cleanup.

## Retained findings and boundaries

| Mechanism / checkpoint | Proven scope and remaining limitation |
| --- | --- |
| Native reordered-packet/history corrections |Real encrypted packet counterexamples; jitter-only.585→185.313Mbps. Not all network/recovery gates |
| Restart / late STARTUP / lifecycle ownership |Concrete scoped refusals, wake/reclamation checks;1941mixed+64single-mode churn reclaims owners. Deployed random RAM/CPU event not fully attributed |
| Request retained recovery011aee9 and unique ACK atoms765683b |Real ownership/attribution counterexamples. Their adverse/incomplete ordinary pairs remain; component GREEN never promoted them |
| Response retained recovery953a54f |Immutable assignment recovery, active/final admission preserved; better aggregate but worse6.080s gap in ordinary pair |
| Ranked-prefix query445011f |Exact output with fewer irrelevant visits; average improves but gaps worsen. No universal acceptance |
| Ordered direct structural recovery1436ff4 |Actual byte-order/overlap-work REDs; full allowance and independent-target service preserved. Ordinary early target service adverse |
| Prepared request ownership9720e4b |U→exact Original ownership at native claim; no premature TCP assignment.533checks/audit, then ordinary75s return hold/incomplete settlement |
| Persistent idle readinessb3dfef1 |Real all-refused two-writer retry recurrence removed;535checks/audit. Ordinary long return/target holds remain |
| Exact copy-debt index9ea25e2 |Work and conservation proved; ordinary settlement improves,11s gaps remain |
| Native advisory owner admissione476308 |Actual two-cut contention RED/GREEN; no blocking native Product acquisition. Ordinary completion with worse gaps; no practical promotion |
| Single prepared stale preferenceb783cd6 |Actual sole-Ready stale claim0→64KiB with fresh-Ready opposite and unchanged qualification;544 focused checks. Ordinary slower completion/larger confirmation gap; no promotion |
| Shelved paired-clock request sampler |Actual[2,2,2]→[2,3,4] sampling correction; ordinary incomplete/adverse, not stacked into runtime |

Preserve prepared-source conservation and exact chosen readiness epochs:
A=prepared source end, C=native claimed end, U=A−C, B=U+sum Original debt.
Claim U→O does not increase B; ACK cannot exceed C; FIN waits for U=0.
Failed protected writes retain exact ownership. Product→Native actor calls
coexist with Native→tryProduct writer calls; no blocking reverse edge or guard
across await. A deadlock-free graph alone is not a latency guarantee.

Structural recovery is globally byte-ordered but cannot let a blocked target
stop independently eligible work. Keep existing exact copy deadlines/J/Native
Apply. Do not resurrect old suffix queues, ACK-per-frame structural allowance,
renewable deadlines, scalar same-host protocol preferences or guessed capacity.
Direction-neutral response parity remains pending; do not silently treat the
request-only prepared migration as both-direction completion.

Rejected static ranking, relative ACK codec, ready-feedback batching,
wrapperless actor, absolute-delay reordering,3N1 and isolated raw-byte hysteresis
deletion remain rejected. No broader topology-inference framework, controller
retuning or new speculative inventory follows from these captures.
Original-QUIC-ignored claim was disproved by actual attachment timing.
No target/backpressure or losing-copy delay may be labelled the winning gap.

## Global gates — unchanged and not satisfied

| Order | Scope | Required evidence |
| --- | --- | --- |
|1 |Mixed allocation, upload sampling, cold/warm startup |Exact cause/model/real RED/control/audit/GREEN plus ordinary first-body/gaps/loaded latency/completion |
|2 |TCP/QUIC changing loss, jitter, QoS, blackhole/recovery |Both directions, same-request restart-free recovery; native receipt versus ordered user service |
|3 |Aggregation/shared contention |Single500Mbps, independent200Mbps each, shared cuts, asymmetric3–10%mean6 loss/jitter/QoS/outage combinations and ablations |
|4 |Experience/baselines |TCP,QUIC,default; cold/warm single/concurrent/real speed.cloudflare.com; raw TCP,Xray,Hysteria2 matched topology/configuration including failures |
|5 |Sustainability |Restart/churn, backpressure, ownership, CPU/RSS and post-load recovery; reopen only on contrary evidence |
|6 |Publication |Full timing/latency series and costs/completion with goodput; README/PERFORMANCE and release only after competitive gates |

Pinned current profile: routed/mirrored single500Mbps; upload70/20ms and
return30/5ms delay/jitter. Five-second upload loss[3,8,5,6,10,3,5,8]%mean6;
return[1,2,.5,3,2,.5,1,2]%. Upload10Mbps15–25s; UDP outage30–33s;40s load,
85s runner guard/90s probe boundary. Do not tune the profile to pass.
Random packet realizations are not identical controls; confirmation bins
above500Mbps can be buffered observation, not wire capacity.

## Execution, evidence and continuity

- Owned Docker only; no sudo, host shaping, outside-repo work or build/lab
  overlap. Products/probes stopped; no build/lab. Origins retained.
- Normal frozen candidate:`./.tmp/reflection/bin/prepared-stale-20260908/mptunnel`.
  Normal parent:`./.tmp/reflection/bin/advisory-owner-20260908/mptunnel`.
  Current diagnostic:`./.tmp/reflection/bin/terminal-service-20260908/mptunnel`.
  target/release is the temporary terminal diagnostic, not the ordinary binary.
- Exact intermediate commits only. Preserve raw evidence before scoped cleanup.
  No deletion this turn; ample root space. User7lines must stay unstaged.
- Telegram latest ordinary adverse-pair report sent during09:38UTC; next
  nonurgent not before10:42UTC (conservative hourly boundary). Respect the
  hourly minimum and soft-frequency advisory.
  No component-only success notification or unfinished completion claim.
- Reflection: exact symbolic conservation enabled one justified work deletion,
  but did not predict every timing phase. The ordinary pair supplies real
  settlement progress while exposing adverse early/RSS observations. Preserve
  those limits; trace the same winning-prefix stage instead of stacking a
  plausible policy shortcut or repeating until a favorable average appears.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  that does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.

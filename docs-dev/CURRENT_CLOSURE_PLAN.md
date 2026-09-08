# Current deterministic closure plan

Updated:2026-09-08 20:21 +08:00. Authoritative source is `./`.
**MPP is not performance-accepted. No release, push, or ideality claim.**

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before every
transaction and after compaction. This is the active scope/decision ledger,
not a new issue inventory. Full pre-condensation chronology is retained at
`git show 13876d6:docs-dev/CURRENT_CLOSURE_PLAN.md`; the subsequent observer18:56
entry is preserved below. Earlier history, including rejected approaches,
remains at `git show ebad57f:docs-dev/CURRENT_CLOSURE_PLAN.md`. Linked reports
retain exact ranges, raw timing bins, costs, RED/GREEN logs and observer patches.

## Active transaction: user-requested mixed-mode architectural redesign

**2026-09-08 19:52 +08:00 — scope:** the user explicitly asks why mixed-mode
still fails and requests a careful comprehensive redesign. This supersedes
another narrow native observer as the automatic next action. Runtime remains
d999fea; no new controller, threshold or deployment change is authorized by a
slow result alone. The completed native discriminator remains evidence below.

**Observed failure / exact question:** default mixed upload still has5.50s
confirmation gaps in its latest ordinary run; previous mixed download loses
interactive service despite substantial bulk throughput. Does the existing
allocation/ownership/recovery/feedback composition make avoidable cross-carrier
ordered-service dependencies, and which minimal replacement contract removes
them without disabling useful aggregation or restoring old hard admission?

**Alternatives / information forecast:** compare immediate independent-writer
claiming, a common ordered-service placement policy above unchanged resource
authority, and a change of native ordering scope only if evidence requires it.
Existing exact frontier and reader captures can establish or reject proposed
dependencies without another lab. The useful output is one coherent model,
explicit impossibility limits, retained/deleted responsibilities, a benefit
forecast and decisive old/new counterexamples. It is not an expected speed gain
from documentation. Do not promise to remove physical queue-drain or unobserved
packet-loss time; seconds-long policy-induced stalls are the material target.

**Smallest next action / stop:** root reviews RFC/history/code while independent
audits challenge authority, ordered service and causal evidence. Persist a
bounded redesign proposal and amend normative RFC text only for a demonstrated
wrong contract. No implementation before counterexamples, failure/recovery and
both-direction composition are covered; no new topology-estimator framework.
Then freeze the comparison basis before a candidate so a full product-level
baseline cannot again be displaced by a chain of local corrections.

**Redesign review20:04:** root inspected the65edae3 origin, current RFC10/15,
request prepared claims, response queue-time commitment and receive mailbox
boundaries. Three independent reviews separate placement, local receive service
and physical/native limits. [Proposed replacement](MIXED_SERVICE_REDESIGN_20260908.md)
retains exact ownership, configured pipeline and controllers; it explicitly
does not claim a specified allocator or authorize speculative performance-Defer.
Current ordinary comparison is now the first execution step, not another fix.

**Comparison forecast / execution:** frozen d999fea ordinary binary,
redesign-baseline-0908 tag. Existing runner/profile only, diagnostics disabled;
downstream phase runs TCP,QUIC,mixed,raw,VMess,H2, then upstream mirrors the
whole impairment. One initial cell per product/direction, no build overlap.
This separates current mixed-mode interaction from single-carrier limits and
provides an honest baseline snapshot; it is not a statistical universal ranking.
Retain all gaps, echo failures and censored outcomes. Stop only the failed cell,
verify its owned probe has exited before proceeding, and do not change profile
or protocol to obtain favorable results. No runtime candidate yet.

**Comparison complete20:20:** all twelve ordinary cells have outcomes;
TCP/H2 upload reached the existing settlement guard and their probes exited
before continuation. VMess upload ended before exact terminal confirmation.
No lab/build remains active. [Full current cohort](REDESIGN_BASELINE_20260908.md)
replaces historical controls as this transaction's comparison basis, not as
a statistical ranking or public release gate. QUIC/mixed downstream rates
69.511/37.212Mbps and gaps4.838/7.418s; upstream97.046/66.846Mbps, gaps5.294/
4.004s. All MPP downstream persistent echo probes time out; raw/VMess preserve
80/80 attempts at lower throughput. H2 also fails severely. Mixed is not
uniformly worse on every timing measure; single-mode stalls remain material.

**Architecture audit / next decision:** current response delivery takes the
request Product owner before reassembly and first local-write polling while
request claims plan under that same mutex. Independent review confirms the
dependency, not a present multi-second duration. Separate directional receive/
write ownership from expensive opposite planning; preserve small authoritative
attachment/terminal/ACK publication transitions. Before implementation, the
real claimant/receiver control must prove dependency and current useful-byte
residence must identify the material removable portion. No three try-lock
tweaks or guessed capacity. Response late placement is a separate boundary:
current small-reply captures bound publication-to-write to2–4ms, so it must
not be promoted as the upload-seconds fix. A bulk-response alternate-writer
counterexample remains needed. Both findings and exact preserved prerequisites
are in the redesign proposal. No further Native tuning or new audit inventory.

## Completed discriminator: native ordered-read service during recovery

**Issue / observed failure / exact question:** the completed d999fea forward
capture has a 6.095822s confirmation gap. In restored capacity, QUIC Original
[379841438,379853438) completes its local write at Unix1788865120575 but
decodes at5124171 (3.596s); mux follows4ms later. Its current-frontier hold is
1.270544s, not the whole residence. Was its native reader waiting for absent
ordered bytes, or were available bytes waiting for local polling/parsing?

**Existing evidence / competing causes:** exact forward claims, local writes,
decodes and ACKs reconcile. Repair route residence is tiny, but an ordinary
reader can wait on its input channel. QoS-period repair delays also coexist
with6.151MB queued on the shared10Mbps cut (4.921s aggregate drain), so do not
classify that physical effect as an MPP defect. During restored-capacity rows
41–43, the router backlog is only92–101KB at500Mbps and native QUIC ACKs barely
advance; Native queued bytes and missing offsets are not in those gauges.
A separate2.169s frontier gap has2.076s before Original claim and only93ms
write→decode/mux; preserve this assignment/feedback boundary, not all-Native blame.

**Smallest model / action / falsifier:** diagnostic-only ordered-read episodes
start at the wrapper's actual Poll::Pending, not a partial read that returns
bytes. Under the already-held native guard, retain exact connection/stream
identity and missing native offset r. First successful validated ingestion
covering r records availability; first nonempty ordered Chunk closes the
episode. Record producer times; repeated pending at the same r adds no state.
Cancellation/terminal episodes remain censored. Native offsets include H3
framing, not Product DSN; join connection/H3 identity and retain ambiguous
frame/chunk mappings. Early availability with late return selects local
service; late availability with prompt return selects pre-processed-receipt
delay, not automatically packet loss/PTO. No sender packet trace yet.

**Acceptance / stop:** reuse the exact Product forward/repair observer plus
only these sparse native transitions. Root and independent review before one
build; archive/freeze/reverse before one unchanged capture. No runtime policy,
queue, rate, timer, sampler or RFC correction is justified yet. No favorable
rerun or performance promotion from diagnostic Mbps/non-reproduction. Prove
an actual causal defect before implementing a correction.

**Execution19:18:** root and independent whole19-file reviews pass. Sparse
Native episodes preserve actual no-data Pending, validated ingestion and
nonempty Chunk boundaries; all bindings are lock-free existing identities.
New diagnostic fields are cfg-only, initialized/reset on every Recv lifetime.
Native clock/sequence are distinct from Product logs; canceled reads remain
broad local-service/censored cases. NATIVE_READ_SERVICE_TRACE_20260908.patch
archives the complete temporary overlay and validates as a reverse diff.
One diagnostic build is running, no lab. Freeze/reverse every source/Cargo
observer change before the unchanged capture; no runtime correction yet.

**Classification correction19:23:** the user's challenge is binding: an
unideal result can be physical loss/queuing, a throughput/latency tradeoff,
or a real defect. Q/C does not locate the blocking byte, and an already-created
queue does not prove earlier sending decisions optimal. Historical matched
raw completes4.279Mbps/maxgap1.611s; VMess and H2 are incomplete (H2 at the
observation boundary), with unequal work and no current exact MPTCP counterpart.
These do not establish an MPP-specific defect or universal superiority.
Compare matched controls before further model corrections; the current sparse
trace only separates Native processed-input availability from local service.
The mandatory method now explicitly records this classification rule.

**Capture19:23:** warning-free3m57s diagnostic build frozen separately as
native-read-service-20260908. All19source/Cargo changes reversed and their
diff verified empty before one unchanged-profile mixed upload; no build/lab
overlap. Exact runtime remains d999fea; target/release is diagnostic-only.

**Method requirement19:39:** before any next experiment or implementation,
record expected material user benefit, its removable critical portion,
plausible range/upper bound, assumptions, uncertainty, costs and falsifier;
compare the actual outcome against that forecast afterward. Diagnostics need
an explicit information/decision forecast instead of a promised speed gain.
Defer unsupported or negligible opportunities; correctness-only necessity
must not be presented as a performance improvement. This is mandatory in
PERFORMANCE_METHOD_AND_LESSONS, not a new numerical Product threshold.

**Completed native result:**391184384 exact bytes settle in49.043824s;
maximum confirmation/write gaps are3.594738/6.955278s. All553 completed
native Pending→availability→Chunk episodes reconcile. Availability→return
is at most5.457ms (p95 440us), excluding that local step as a multi-second
wait within these episodes. The winning repair for F165558377 has3.021829s
before processed head availability and72us afterward. The longest9.567s
native wait is not a useful winner: source work was initially absent and
TCP already advanced the frontier before that QUIC decode. Do not attribute
all native elapsed time to useful blocked work, transport defects or physical
loss. No runtime correction or ordinary performance promotion follows.
Root whole-report review and independent accounting/cost checks pass;
[exact joins and full timing history](NATIVE_READ_SERVICE_20260908.md) retain
the raw archive and observer patch. No next experiment is running. The next
causal decision must distinguish missing processed input from sender service,
physical loss/queuing and earlier placement; this observation alone does not
select a correction or justify another diagnostic without an information
forecast. Previously observed preclaim/return holds remain separate boundaries.

## Completed discriminator: forward prefix after ACK work reduction

**Observed failure / question:** ordinary d999fea still has5.497708s maximum
confirmation gap. QoS T234499808/Rs556 stay fixed for5.999s at server
Unix1788863964962→3970961; S298391872→301608672, Rc528→556. During separate
outage/recovery L32–36, S367394816/T300285952/Rs=Rc738 stay flat4.001s with
exact64MiB S−T. TCP Recv-Q is0, TCP native ACKs advance7.31MB, but exact
QUIC physical3 native ACK and62.97MB Product-debt counters remain flat.
These facts select forward-prefix service, not all-Native or local-reader
attribution. Which exact Original/copy owns the blocking byte; is it unclaimed,
not locally written, predecode, or decoded but not delivered to target?

**Model / existing evidence / alternatives:** preserve source A, claimed C,
retained Original/copy ownership, native acceptance and receiver ordered F as
distinct boundaries. Aggregate S−T and queue gauges cannot reconstruct C/F or
the winning carrier. Existing QOS_FORWARD_PREFIX and QOS_REPAIR_READ observers
already cover these boundaries, including repair-reader read/route residence.
No new cost/claim framework or Native-controller inference is necessary yet.

**Smallest next action / falsifier / stop:** reuse the archived11-file exact
forward+repair-service observer on unchanged d999fea, omitting noisy general
reinjection events. One unchanged diagnostic after independent review and
archive/freeze/reverse. Join all Original claims/write/decode and ACK totals,
then the actual winning-prefix interval. Prompt local delivery after decode
excludes that stage; source/commit absence selects assignment rather than
transport. Predecode alone still cannot assign Native loss/timer blame.
No new or resurrected correction before a reachable real counterexample;
diagnostic rate and non-reproduction are not ordinary improvement.

**Observer18:56:** root and independent whole11-file reviews pass. Exact
forward/repair hooks reused without policy, queue, lock or await changes;
fresh archive POST_ACK_FORWARD_TRACE_20260908.patch preserves valid diff
metadata. Repair read time includes scheduling/unavailable work; server F is
mux release, not target-write completion. Product ACK debt and Native pending
bytes have deliberately separate lifetimes; different counters are not a leak.
The diagnostic was frozen and its hooks reversed before the unchanged capture.

**Execution update:** warning-free3m32s diagnostic build completed and frozen
as post-ack-forward-20260908. All11observer files were reversed and the source
diff verified empty. The unchanged mixed-upload capture, post-ack-forward-0908,
completed:436797440 exact bytes/48.135091s, maximum confirmation/write gaps
6.095822/5.560319s;49 raw one-second bins and49 service rows. This is diagnostic
evidence, not ordinary acceptance. [Raw archive](POST_ACK_FORWARD_SERVICE_20260908.raw.tar.gz)
preserves exactly five results plus build/run logs; gzip integrity, exact member
list and byte-for-byte source comparison pass. No build or lab remains active;
target/release is diagnostic, not ordinary d999fea. Exact causal joins and the
next discriminator remain root-owned.

## Current source and ordinary disposition

Current runtime **d999fea** restricts request ACK-release work to exact support;
RED **0683286**,546focused checks. Parent **b783cd6** separates prepared stale
preference; RED **f1b0900**,544checks. Earlier **e476308** makes the two advisory
native-writer Product acquisitions nonblocking; RED **584b748**,542checks.
**9ea25e2** indexes exact additive copy debt; RED **3396087**,538checks and
evidence checkpoint **ebad57f**. All are mechanism checkpoints, not acceptance.
No shelved sampler, Native observation-sharing policy or congestion tuning is active.

| Ordinary pair | Parent → candidate exact bytes / seconds | Maximum confirmation / write gaps, seconds | Disposition / evidence |
| --- | --- | --- | --- |
| b3dfef1 → 9ea25e2 |93570384confirmed /180748288accepted /85.948541 →254083072 /48.973579 |66.779472 /15.796057 →11.042148 /1.011139 |Parent incomplete at guard; candidate settles, but11s gap remains. [Copy debt](COPY_DEBT_SERVICE_20260908.md) |
| 9ea25e2 → e476308 |355532800 /44.964483 →409796608 /49.163739 |5.052667 /1.356326 →6.197568 /5.462440 |Timing mixed/adverse; no promotion. [Advisory owner](ADVISORY_OWNER_SERVICE_20260908.md) |
| e476308 → b783cd6 |389218304 /47.562042 →326041600 /52.965807 |4.845539 /3.553143 →6.082958 /2.223001 |Less work, slower completion/larger confirmation gap; no promotion. [Stale preference](PREPARED_STALE_SERVICE_20260908.md) |
| b783cd6 → d999fea |473104384 /47.992259 →393150464 /46.768581 |3.060424 /4.396938 →5.497708 /5.665049 |Less work and worse gaps; no promotion. [ACK support](ACK_SUPPORT_SERVICE_20260908.md) |

Different work, duration and random packet realizations prevent causal rate
ratios or uniform non-regression claims. Preserve every adverse phase; no
favorable third run and no rollback to a proven defect from one average alone.
For9ea25e2, first3s target service worsened while10s service/settlement improved;
client/server peak RSS511988/90968→677496/129508KiB with more work. Parent reset
followed guard teardown; missing confirmation bins are unavailable, not zero.
Forb783cd6,10s target service improved but15s worsened; client peak RSS775264→
832336KiB. Detailed first-service, full-bin, socket and cost evidence remains
in each linked report and its raw archive.

### What the recent corrections prove

- **d999fea — ACK support:** fd32e60/f4206d0's global flight snapshot settled
  exact copies;765683b preserved byte-exact ambiguity. Only starts<H=max ACK
  end can contribute release/evidence. Extract that prefix, preserving crossing
  flights and their H-bucket precedence; leave unrelated keys untouched.
  When every key is eligible, keep the whole-map fast path. Real131072B Original
  plus14600B copy fixture failed only at66versus3record work; corrected cases
  each process2records with identical release/proof/receipt/debt/epoch/order.
  No ACK coalescing, policy, timer, cap or RFC change. Existing exact-event
  replay independently bounds4470499 needless suffix visits across6367ACKs;
  this is not measured CPU savings. [Evidence](ACK_SUPPORT_SERVICE_20260908.md).
- **b783cd6 — one stale preference:**5d660f3b's legacy fresh-output mask ignored
  Ready;9720e4b's finite claim tiers could not restore a masked stale survivor.
  Prepared claims now consume unmasked resource observations, then unchanged
  full-membership Ready/fresh/backup tiers and W/P/E/final fences. Legacy
  callers retain their policy. Actual stale-Ready claim RED0→65536B, fresh-Ready
  opposite and no-requalification checks pass. Positive fixture is FirstPath;
  E exhaustion is component coverage, not a falsely claimed facade test.
  [Evidence](PREPARED_STALE_SERVICE_20260908.md).
- **e476308 — advisory owner:** actual held-Product producer tests failed at
  both advisory acquisitions and cancellation, with cleanup guards rather
  than Product latency thresholds. Existing freshly prearmed try-lock/Busy
  now applies at both cuts; final Native fence, source, readiness and
  cancellation remain unchanged. No blocking reverse Product lock or new
  fallback permission. This does not attribute every local hold to contention.
  [Evidence](ADVISORY_OWNER_SERVICE_20260908.md).
- **9ea25e2 — additive copy debt:** actual dispatcher emits the same4096B
  repair from69632retained bytes with two queries; irrelevant fragmentation
  raised4→130record visits before RED. J_i=sum of retained copies on exact i
  is now maintained on append, ACK split and drain; overlap multiplicity,
  expiry/Native-ACK nonrelease, zero-key retirement and replacement identity
  remain exact. Full-scan lifecycle oracle and independent audits pass.
  [Model and ordinary comparison](COPY_DEBT_SERVICE_20260908.md).

## Completed discriminators — findings, not new fix queues

- **Reply residence on b783cd6:**387579904B/52.491225s diagnostic,
  maxconfirmation3.508291s. Winning[863,877) spent3.247s server-read→delivery,
  including599ms after authenticated decode; preceding nondata reader send
  awaited1.961027s within1.965s. Real local backpressure is established, not
  ACK-only CPU attribution or an all-Native explanation. This selected the
  existing ACK-work question; no new cost framework was added.
  [Reply residence](REPLY_RESIDENCE_20260908.md), evidence checkpoint **bd3b810**.
- **Terminal service:** ordinary b783cd6's6s postfull-target tail was not
  reproduced. Diagnostic424017920B/56.939769s,maxconfirmation7.996908s.
  EOF had C391950079+U32067841=final; FIN cannot omit U. Same-offset FINs
  published with24.47MB retained cache, disproving a cache-zero prerequisite.
  Server initially pended FIN, then final Data made it ready; target shutdown
  and final13B reply read were prompt. Duplicate FIN replay is normal.
  The8.909s source-EOF→publication and1.258s write→decode intervals remain
  composite. Nonterminal replies were already read7.155s/15.686s before
  delivery. No FIN/RFC fix justified; earlier6s tail remains unattributed.
  [Terminal evidence](TERMINAL_SERVICE_20260908.md), checkpoint **cd97c5d**.
- **Prepared claim service:**484507648B/44.176499s,maxconfirmation4.478852s/
  maxwrite8.647851s.10428Originals and6367ACKs reconcile exactly; all Originals
  write/decode, four losing TCP copies lack positive completion. Largest
  server holds4.408347/3.514626s concern already-written QUIC Originals.
  Pending-source C329149108 held4.987s inside a9.273s selected QUIC write;
 353+TCP claim attempts exclude lost notice/global critical-front explanations
  for that interval. Separate sampled stale-Ready veto selected b783cd6;
  it does not explain the largest holds or prove every resource was free.
  [Claim/notice evidence](PREPARED_CLAIM_SERVICE_20260908.md).
- **QoS forward/repair service:** first diagnostic374669312B/65.783216s,
  maxconfirmation5.592993s;18779Originals and24009total commits reconcile.
  QoS F185133452/F187796428 holds3.432533/2.661961s release within1ms of decode.
  Late F298838412 was repaired9.751s after local acceptance;4.750s between
  repair decodes coexisted with6970816B ordinary QUIC decode on that connection.
  This excludes connection-wide silence, not native stream delay versus routing.
  Follow-up485359616B/52.139505s,maxconfirmation7.128953s:1407completed repair
  routes total41.466ms/max3.494ms; QoS winning-copy route27us excludes that
  route as cause there. Long7.094s read included unavailable work: actual copy
  was published only252ms before decode. A different F471135545 hold had
 14224071consumed but unclaimed bytes. Four losing TCP copies lacked positive
  completion, not Original loss/corruption. No Native/queue/threshold fix.
  [Both captures and raw archives](QOS_FORWARD_PREFIX_20260908.md).
- **Earlier copy-debt service diagnostic:**366018560B/43.726528s,
  maxconfirmation4.375291s did not reproduce ordinary11s. F639 was mostly
  predecode; F737 had1.203s local residence; F821 had>=2.722s preceding reader
  send-await overlap with different cost composition.73winning mux advances
  reached local write within4ms.4786flight releases took7.808784s within
 8.160194s ACK-handler elapsed; contained windows include246899/285275us.
  Nested/concurrent elapsed is not additive CPU, nor proof of a sole cause.
  [Exact joins and cost limits](COPY_DEBT_SERVICE_20260908.md),
  [prior sparse evidence](PREPARED_REPLY_SERVICE_20260908.md).

Read/write/decode timestamps belong to their actual producer stages. Server F
means mux release, not completed target write; local native acceptance is not
transmission/receipt. Shared-send completion may follow concurrent dequeue.
QUIC IDs are connection-local; TCP wire IDs and runtime indices differ, and
per-side physical IDs are not interchangeable. Join exact byte coverage,
including split/coalesced records. Preceding nondata waits are not ACK-only,
and unlogged/left-censored intervals remain unknown. Losing copies, native
pending bytes and Product debt have different lifetimes—not automatic leaks.

## Retained checkpoints and unfinished boundaries

| Mechanism / checkpoint | Proven scope; remaining disposition |
| --- | --- |
| Native reordered-packet/history corrections |Real encrypted packet counterexamples; jitter-only.585→185.313Mbps. Other network/recovery gates remain open |
| Restart / late STARTUP / lifecycle ownership |Scoped refusals/wakes/reclamation;1941mixed+64single-mode churn reclaims owners. Deployed random RAM/CPU incident still not fully attributed |
| Request retained recovery **011aee9**, unique ACK atoms **765683b** |Real ownership/attribution counterexamples; adverse/incomplete ordinary pairs retained, not promoted |
| Response retained recovery **953a54f** |Immutable per-assignment recovery with active/final quantities preserved; ordinary aggregate improves but gap worsens to6.080s |
| Ranked-prefix query **445011f** |Exact output, fewer irrelevant visits; average improves but gaps worsen |
| Direct structural recovery **1436ff4** |Actual global-byte-order/overlap-work REDs; allowance and independent-target service preserved; ordinary early target service adverse |
| Prepared request ownership **9720e4b** |U→Original at physical claim, no premature TCP assignment;533checks/audit, then ordinary75s return hold/incomplete settlement |
| Persistent idle readiness **b3dfef1** |Real two-writer all-refused retry recurrence removed;535checks/audit; long ordinary return/target holds remain |
| **9ea25e2 / e476308 / b783cd6 / d999fea** |Exact work/admission mechanisms proved above; respective ordinary comparisons remain unaccepted |
| Shelved paired-clock request sampler |Actual[2,2,2]→[2,3,4] correction; ordinary incomplete/adverse, not stacked into runtime |

Earlier exact ownership and admission evidence remains in
[request cohort](REQUEST_COHORT_ORDINARY_20260907.md),
[request prefix service](REQUEST_PREFIX_SERVICE_20260908.md),
[response handoff](RESPONSE_HANDOFF_20260908.md) and
[prepared ownership model](PREPARED_ORIGINAL_OWNERSHIP_MODEL.md).
Historical SEEN/UNSEEN labels are dispositions, not automatic new obligations:
[change disposition](CHANGE_DISPOSITION_20260907.md),
[reflection](PERFORMANCE_REFLECTION_20260907.md),
[practical acceptance](REVIEW_AND_PRACTICAL_ACCEPTANCE.md).

Preserve A=prepared-source end, C=native-claimed end, U=A−C and
B=U+sum Original debt. Claim U→O does not increase B; ACK cannot exceed C;
FIN waits for U=0. Failed protected writes retain exact ownership. Preserve
chosen readiness epoch, current exact qualification and final Native fence.
Product→Native actor calls coexist with Native→tryProduct writer calls:
no blocking reverse edge or guard across await. Deadlock freedom is not latency.
Structural byte order must not block independently eligible targets; retain
exact copy deadlines/J/Native Apply. **Response direction-neutral prepared-source
parity remains pending**; request-only migration is not both-direction closure.

Rejected static ranking, relative ACK codec, ready-feedback batching,
wrapperless actor, absolute-delay reordering,3N1 and isolated raw-byte hysteresis
deletion stay rejected. Do not restore whole-ready-set invalidation or
metadata-refusal readiness churn; chosen epoch and physical idle lifetime
have real counterexamples. No suffix queues, ACK-per-frame structural allowance,
renewable deadlines, scalar same-host protocol preference or guessed capacity.
Original-QUIC-ignored claim was disproved by attachment timing.
Further Native observation-sharing is **not implemented**: an omitted better
target cannot be restored by chosen-target Apply. It is not equivalent cleanup.
No topology-inference framework, controller retuning or speculative inventory.

## Global gates — unchanged and not satisfied

| Order | Scope | Required evidence |
| --- | --- | --- |
|1 |Mixed allocation, upload sampling, cold/warm startup |Exact cause/model/real RED/control/audit/GREEN plus ordinary first-body/gaps/loaded latency/completion |
|2 |TCP/QUIC loss, jitter, QoS, blackhole/recovery |Both directions, same-request restart-free recovery; Native receipt versus ordered user service |
|3 |Aggregation/shared contention |Single500Mbps, independent200Mbps each, shared cuts, asymmetric3–10%mean6 loss/jitter/QoS/outage combinations and ablations |
|4 |Experience/baselines |TCP,QUIC,default; cold/warm single/concurrent/real speed.cloudflare.com; raw TCP,Xray,Hysteria2 with matched topology/configuration, including failures |
|5 |Sustainability |Restart/churn, backpressure, ownership, CPU/RSS and post-load recovery; reopen only on contrary evidence |
|6 |Publication |Full timing/latency series and costs/completion with goodput; README/PERFORMANCE and release only after competitive gates |

Pinned profile: routed/mirrored single500Mbps; upload70/20ms and return30/5ms
delay/jitter. Five-second upload loss[3,8,5,6,10,3,5,8]%mean6;
return[1,2,.5,3,2,.5,1,2]%. Upload10Mbps15–25s; UDP outage30–33s;40s load,
85s runner guard/90s probe boundary. Do not tune it to pass. Random realizations
are not packet-identical controls; bins above500Mbps can be buffered confirmation.
Matched mirrored review-mirrored-0906 controls remain historical context:
raw4.279Mbps/maxgap1.610879s completes; Xray/H2 incomplete. Nonmirrored
raw274.677Mbps is not a substitute; no redundant rerun just to obtain wins.

## Execution, evidence and continuity

- Root owns builds/labs. The frozen native-read-service-0908 capture is complete;
  no build/lab is active, and all observer source hooks are reversed.
  Owned Docker only: no sudo, host shaping, outside-repo work or build/lab overlap.
- Ordinary candidate:`./.tmp/reflection/bin/ack-support-20260908/mptunnel`.
  Ordinary parent:`./.tmp/reflection/bin/prepared-stale-20260908/mptunnel`.
  Current diagnostic:`./.tmp/reflection/bin/native-read-service-20260908/mptunnel`.
  Previous forward diagnostic:`./.tmp/reflection/bin/post-ack-forward-20260908/mptunnel`.
  Previous reply diagnostic:`./.tmp/reflection/bin/reply-residence-20260908/mptunnel`.
  Do not use diagnostic target/release as an ordinary comparator.
- Exact intermediate commits only; preserve raw evidence before scoped cleanup.
  No deletion in this condensation. User's seven-line
  LIVE_OWNER_FRONTIER_WORK_BOUND.md edit must remain untouched and unstaged.
- Telegram last full-comparison/redesign report:12:23UTC; next nonurgent not
  before13:24UTC. Respect hourly minimum/soft-frequency advice; no component-only
  success notification or unfinished completion claim.
- Method reflection: symbolic conservation justified exact work removal but
  did not predict every timing phase. Follow the same winning-prefix evidence,
  retaining adverse first-service, settlement and resource observations.
  No plausible shortcut stack, favorable-average rerun or silent coverage waiver.
- Universal clairvoyant optimum under arbitrary future outages is impossible;
  this does not waive avoidable delay or practical gates. Never call unfinished
  work ideal or promise cost-free capacity discovery.

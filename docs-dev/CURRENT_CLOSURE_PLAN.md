# Current deterministic closure plan

Updated: 2026-09-11 03:56 +08:00. Authoritative repository: ./.
**No performance/release acceptance, push or public README update.**
Continue the authorized closure loop; an intermediary commit is not completion.

Read [the mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md) before each new
transaction and after compaction. This is the active ledger, not a second RFC.
Complete earlier forecasts, failed experiments and attribution are preserved in
`git show 011b724:docs-dev/CURRENT_CLOSURE_PLAN.md` and
[the full evidence report](AUTHORITATIVE_GAP_VIEW_ORDINARY_20260910.md).
Condensation waives no failure and reactivates no rejected candidate.

## Current source and proven correction

Runtime checkpoint **011b724**, finite ordered ACK/MAX Input service, is retained.
Frozen ordinary binary: ./.tmp/reflection/bin/ordered-feedback-20260911/mptunnel.
target/release/mptunnel is the same ordinary candidate, not an observer.
Only the user's seven-line LIVE_OWNER_FRONTIER_WORK_BOUND.md remains unrelated
dirty source; never edit/stage it. No runtime changes are currently proposed.

Exact failure: ordinary a16 mixed UP had7.236s write/4.145s confirmation gaps.
Diagnostic61091 joins an ACK prefix already in client FIFO to actor processing
12.526s later; decoded-to-Apply16.659s. Server generation/admission/successful
native write are timely for the critical prefixes. Repeated client preselect
occupies11.277s of a12.121s full-flush window within the FIFO residence.
Reader/attachment backpressure can propagate upstream; this is not an
independent native/physical fault. Elapsed scopes overlap and are not CPU.

Origin/intention: ccfe817 centralized coherent ACK/state transitions;
faee89db retained inline recovery;79ddb41 expanded gap enumeration to cover real
omitted successors. Per-event discovery plus repeated preselect can postpone an
already-ready later positive ACK while doing work that it immediately invalidates.
The useful ownership guarantee is preserved at the finite Input quantum boundary:

- Keep every ACK's validation, positive release, negative scope, copy qualification,
  sampling, pruning, staleness and progress in the existing returned order.
- Apply actual coalesced MAX credit; snapshot one additional-attempt count.
  Do not wait, replenish the budget, merge ACKs or freeze credit contents.
- Retain other frames/errors/mismatched streams as exact barriers.
- Hold one Product guard; after novel ACK facts do fresh recovery before prepared
  publication, FIN decisions and unlock. Existing native notices can claim upon
  unlock, so postponing publication alone is not sufficient.
- Fatal feedback revokes existing prepared claim authority under that guard;
  terminating streams need no speculative recovery. Cleanup remains unchanged.
- Keep independent preselect/capacity/model/membership/deadline service.
  No new rate, congestion, queue, timer or preference parameter.
- Finite does not mean short: one cooperative turn now covers multiple full ACK
  transactions. Sampling/commit interleaving and initial timing observation can
  change. No wall-clock or timing-equivalence claim. Frozen assignment minima
  and exact native precommit checks remain.

Real-actor RED19106 uses actual native Original claims, receiver-generated ACK1
with a real gap and ready ACK2 filling it. Three intervening heavy discoveries
become zero. Exact readiness, both facts, intermediate gap and final release
are asserted first. Initial44817 was a fixture failure: cumulative full[0,A)
ACK2 correctly omits an empty negative-scope header; no runtime failure there.
GREEN27151 passes52distinct checks (33control,12client,5service,1splitACK,1copy).
Independent actual-source reviews pass, including fatal claim revocation.

## Ordinary evidence and current acceptance boundary

Ordinary build61255 closed0 in1m04s, pre-existing unused-wrapper warning only.
No diagnostics or compile/lab overlap. Exact six-file runtime/RFC/test patch:
./.tmp/reflection/ordered-feedback-candidate-0911.patch.

| Same200+200Mbps QoS UP cell | a16run23406 | 011b724run95903 |
|---|---:|---:|
| Exact confirmed bytes | 526385152 | 1091108864 |
| Elapsed,s / Mbps | 45.465808 /92.621 | 42.660189 /204.614 |
| Maximum write / confirmation gap,s | 7.235997 /4.145083 | 1.162112 /1.542599 |
| Raw bins / zeros | 46 /11 | 43 /1 |
| Raw pre-cut / strictcut / restored,Mbps | 96.384 /104.288 /83.343 | 225.303 /184.959 /205.211 |

All candidate adjacent target-write samples advance; earlier a16 had5s plateau.
First service essentially unchanged. Observed UP/DOWN wire per confirmed byte
falls29.88/21.27%; differing sampled windows are not exact lifetime amplification.
More useful work raises absolute lifetime CPU (clientfinal123→174% ofonecore,
server37.8→68.7%) and server peak RSS75776→126644KiB. No exact CPU-per-byte
claim from lifetimeps. Retained reverse native RTT is also higher; this UP probe
has no echo and cannot establish loaded latency. Preserve residual1.543s gap,
one zero bin, resource costs and native timing. This is material bounded support,
not optimal aggregation or release acceptance.

[Ordinary archive](ORDERED_FEEDBACK_ORDINARY_20260911.raw.tar.gz):
15regular files/249942B, every input byte verified by reader; root read179-line
appendix and verified integrity/manifest. Includes failed fixture, trueRED,
GREEN, exact patches/build/driver/results/runner/shape, no config or executable.

### Closed healthy independent-link pair

Predeclared a16control7360 then candidate91118, same cell but NO_QOS=1.
Tags ordered-feedback-healthy-{control,candidate}-0911.
Both1/1complete,0errors; every accepted byte confirmed.

| Healthy UP | a16 | 011b724 |
|---|---:|---:|
| Confirmed bytes | 526188544 | 1180565504 |
| Elapsed,s / Mbps | 41.740160 /100.850 | 42.766993 /220.837 |
| Worst write / confirmation gap,s | 2.002279 /4.587237 | .722869 /.532616 |
| First write / confirmation,s | .108411 /.411490 | .107294 /.410305 |
| Raw bins / zeros | 42 /8 | 43 /0 |

Independent preliminary review supports advancing: all raw phases improve;
candidate42/42 adjacent target samples advance, versus control5s and3s plateaus.
All rates200Mbps, UP70/DOWN30ms, no loss/jitter/QoS/outage or drops.
More work takes1.027s longer to settle; absolute CPU rises(clientfinal125→193%,
serverpeak50.6→80.9%). ServerRSSpeak87984→78580KiB; client346152→349380KiB.
Wire rises less than2.244xcompletedwork. Full paired native/cost/series appendix
and ORDERED_FEEDBACK_HEALTHY_20260911.raw.tar.gz are complete:148lines read by
root,14regular files/441963B, integrity/manifest and every input byte verified.
Mean220.8Mbps remains below nominal400; do not call this theoretical optimum.

## Next exact gate: shared500Mbps, both directions and loaded latency

Issue/question: does the finite Input quantum preserve ordinary single-cut
mixed service and competing short-request latency? Independent UP has improved,
but carries no echo and cannot validate the higher reverse native RTT observation.
No new runtime change is authorized from that scalar alone.

Forecast: preserve healthy shared-cut service while reducing avoidable queued
feedback work where present. No gain is promised when backlog is absent.
Larger synchronous quanta may harm tail latency or constrained-host headroom.
These are practical acceptance checks, not a forecast based only on call counts.

Smallest next action: four ordinary cells, sequential and predeclared:
a16UP,011b724UP,a16DOWN,011b724DOWN, system mixed/scenario combined.
Use single500Mbps, mirror=1 (UP70/DOWN30ms), NO_QOS/NO_LOSS/NO_JITTER/
NO_BLACKHOLE=1, management=1; no router/diagnostics or per-role override.
Tags ordered-feedback-shared-{control,candidate}-{up,down}-0911.
DOWN reuses the existing bulk+64B echo probe every500ms with3s timeout;
UP reuses the exact-confirmed single-upload probe,40s offered, same guards.
No new harness, queue/profile/controller change or build; both binaries exist.

Compare complete series, startup, all gaps/failures, settlement, native/service,
loaded DOWN echo distribution and wire/CPU/RSS. Preserve failures/censoring.
New material adverse/ambiguous timing or resource cost stops promotion and
selects one causal discriminator, not a favourable rerun or quota adjustment.
Only after this bounded gate proceed to the existing changing-impairment/baseline
matrix. A checkpoint is not authorization to skip any global gate.

Execution03:53+08: shared500 UP pair CLOSED0: control39206 exact428146688B/
45.523054s=75.240Mbps, candidate28579 exact1896284160B/41.853765s=362.459Mbps.
Worst write/confirmation1.489/8.879s becomes .404/.708s; raw zeros24/46→0/42.
All accepted bytes confirmed, no upload errors. This is bounded practical gain,
not final acceptance or a claim that short confirmation bins exceed link capacity.
Control-DOWN75855 and candidate-DOWN2857 also CLOSED0. Bulk366.522→397.802Mbps,
but echo median225.620→268.514ms,p95424.413→530.808ms,max535.156→581.266ms;
body maximum read gap .207720→.372282s. Both80/80successful echoes, no failures.
This adverse/ambiguous timing STOPS promotion; do not advance the matrix or
compensate with a queue/controller/quantum parameter. SharedUP sampled wire per
confirmedbyte also rises4.06%UP/18.18%DOWN; absolute CPU rises. More useful work
does not waive either cost. All four closed files are with the independent reader.

Next bounded attribution transaction: separate larger synchronous client Input
service from shared native/allocation variation and higher offered bulk load.
The correction directly handles client request ACK/MAX; DOWN bulk response ACK
processing is a different unchanged owner. Existing short requests may still
exercise the quantum, so do not assume it irrelevant. Root/independent reviewer
first trace actual request/response callers and exact adverse intervals using
the existing four captures. Information forecast: identify whether a current
owner observation is sufficient, or select ONE causal discriminator. No new
runtime proposal, quota change or diagnostic build.

04:01+08 decision: ONE order-reversed ordinary DOWN pair, candidate then control,
same500Mbps/70+30ms/zeroimpairment/40s bulk+echo and existing binaries. Tags
ordered-feedback-shared-reverse-{candidate,control}-down-0911. Independent source
review confirms high-volume response ACKs use unchanged server ServerFeedbackBatch;
the new client handler receives the one HTTP request's and80x64B echo requests'
feedback. A following echo DATA barrier can still wait, so direct effect is not
ruled out. Information forecast: distinguish repeatable adverse ordering from
run-order/native-allocation variation. This pair cannot separate a causal higher
offeredload effect from direct quantum cost. Preserve BOTH orders; no repeated
sampling until favourable. Repeated adverse timing selects an actual echo-stage/
shared-native discriminator; disappearance holds the regression attribution,
not proof of exact latency equivalence. No runtime change or build.

Reverse pair5956/7714 CLOSED0. Candidate404.090Mbps versus control377.547;
echo median348.040/240.086ms, p95526.798/342.481ms, max788.622/402.024ms.
Candidate79successful/0failed, control80/0; no censored failed attempts.
Body gap .274600/.346357s reverses ordering, so body-gap regression is not
repeatable here. Echo harm DOES repeat; no more ordinary repeats are selected.
Initial pair's larger native RTT and shared HTB backlog coexist with the echo
harm, but neither is an exact winning-byte timing join. Next source reviewer
is selecting the smallest actual echo/client-Input residence discriminator;
do not claim either local quantum or the network caused it from these scalars.

Selected next observation,04:07+08: a cfg-only, one-file client ACK/MAX quantum
observer on current011b724. Verify the actual target port10022 echo and8080 bulk
request streams; record start/guard-acquired/post-unlock end, entry ready count,
actual ACK/MAX and novel ACK counts, and existing deferred barrier metadata.
One log after unlock per quantum; no waits, policy, new queue or per-frame logs.
Include fatal/early-exit coverage or explicitly retain its censoring. The actual
clock is elapsed ownership/residence, NOT CPU attribution. Compare conservative
SUM/UNION across each adverse echo interval, not only the maximum single call.
Information forecast: if even all potentially blocking quantum occupancy is
far below the added100ms-class delay, direct synchronous batching is not its
dominant explanation. Large occupancy selects that exact owner instead. Neither
outcome resolves decoded/FIFO waiting or indirect allocation by itself. Reuse
one unchanged shared500 healthy DOWN cell with the observer, freeze exact patch/
binary and reverse source before running. Ordinary pairs remain the performance
evidence. No old membership injection or12-file observer transplanted.

## Separate open issue: one-core burst near20%QUIC loss

[Four ordinary500Mbps DOWN controls](QUIC_LOSS_CPU_20260910.md) on d44:
QUIC0/20loss and mixed0/20loss,100msRTT,no jitter/QoS/outage.
WholeMbps430.558/48.261/385.581/61.069; late30–40s441.397/1.906/371.853/3.021.
Q20echo has one actual timeout plus33later unavailable records, not34timeouts.

Startup process peaks101.2/138.1%ofonecore occur with substantial transferredwork.
Late server3.87/7.11% rules out sustained MPP CPU saturation as this late collapse's
cause, not external scheduling delay or the deployed random burst. Role/version/
platform remain unconfirmed; do not block the main closure waiting for them.
Startup native-byte-normalized CPU controls do not establish loss-specific
amplification; ratios are not intrinsic costs or an attribution of all work.
20%exceeds the default10% allowance +2%residual response boundary11.8%, but
authorized backoff does not prove near-zero service unavoidable/correctly calibrated.
No Rust/BBR/leak resolution or threshold fix is justified. Native contraction and
prior bounded journal fixes do not identify this incident. Preserve27-file evidence.
If a CPU-focused intervention becomes next, capture actual on-CPU ownership at
the burst, not elapsed diagnostics; no sudo or permission bypass.

## Preserved dispositions: do not revive or erase

- a16b404 exact-subsumption invalidation is a real44-check mechanism checkpoint,
  with mixed ordinary gain and adverse write gap; now underneath011b724.
- Pending-gap service trial3923:82.855Mbps, worse confirmation/precut/cut despite
  better write/restored phases. FULLY REMOVED8668cd8;47focused checks did not
  establish useful composition. Do not restore its pending owner/wait state.
- Recovery-attached global-projection removal44791:32.008Mbps,26.571s write/
  22.437s confirmation gaps. FULLY REMOVED8fd4800 despite28checks.
- Full observation cache rejected before runtime: independent Regular/Backup
  publication can invalidate unbound ranking despite immutable Product state.
- Queue-readiness filter not selected:93.2%ofrefusals early, ZERO in exactlate
  plateau. Stable-absent query pruning too small. Cyclic direct cursor can skip
  new higher-priority service; resetting on every publication can starve later work.
- ACK publication/batching/cadence trials remain rejected for ordinary timing
  costs. 011b724 does not change generated ACK cadence or native packaging.
- b0baca2 native TCP refill: actual1.353s unsent wait removed, TCP417→440DOWN/
  422→450UP and p951269→323ms. Both mixed orders lose~5%late with more copies;
  no reserve/copy-ban tune. Earlier independent aggregation D148→302/U169→316.
- ba56290 pre-target receipt liveness and364d417 impossible proof-round sequencing
  have real tests, not full stall closure; latter's outage tails worsen.
- 79ddb41 omitted successor service is real, but ordinary85s/26.666sgap; a747/d44
  reduce work without closing stalls. Their costs motivated current feedback fix.
- Request paired-clock sampler remains shelved: actual counterexample, but
  incomplete ordinary pair with adverse confirmation timing. Not in current source.
- Restart/churn1941mixed+64single reclaims owners; deployed random RAM/CPU remains
  unattributed. [Full dispositions](CHANGE_DISPOSITION_20260907.md).
- [12-cell harsh matrix](NATIVE_REFILL_COMBINED_20260910.md) retains failures/
  censoring and all baselines. No incomplete winning rank; all-baselines-poor
  sub100Mbps stress is diagnosis, not public performance acceptance.

## Unchanged global acceptance order

1. Close material mixed allocation/startup/stalls with exact causes and ordinary
   completion, first service, full time series/gaps and loaded latency.
2. Both directions/all3MPP modes: changing loss/jitter, suddenQoS, blackhole and
   restart-free recovery, with their ablations; no TCP/QUIC static preference.
3. Single500Mbps/independent200Mbps links, shared/asymmetric cuts and aggregation,
   including simultaneous changing impairments. No fixed bottleneck partitions.
4. Cold/warm short objects, single/concurrent work and actual speed.cloudflare.com;
   matched rawTCP,Xray,H2 and MPTCP where available.
5. Restart/churn, post-load reclamation, CPU/RSS and platform checks.
6. Only then public README/PERFORMANCE timing/latency curves, costs/completion and
   release if competitive. No universal instantaneous-optimum promise or avoidable
   stall waiver; preserve working declared functionality.

Pinned harsh profile: routed500Mbps,UP70±20/DOWN30±5ms;5s UP loss
[3,8,5,6,10,3,5,8]%mean6,DOWN[1,2,.5,3,2,.5,1,2]%; UP10Mbps15–25s,
wholeUDP30–33s;40soffered,85srunner/90sprobe guards. Do not change it to pass.
Healthy/heterogeneous service, not crossing100Mbps, determines usability.

## Execution and continuity safeguards

Root alone builds/runs, no compilation/lab overlap. Existing owned Docker only;
no sudo, host shaping, outside-root work, /mnt/storage use or deletion now.
Direct topology clienteth0=46/eth1=47;servereth1=46/eth0=47. Two independent
200Mbps cuts in aggregate, each shared by its TCP+QUIC—not eight physical links.
Single configured mixed endpoints use their one actual cut; verify class traffic.
Preserve exact source/copy/incarnation ownership, eligibility, ranked Apply,
nonrenewing clocks, half-close/cancel and retained capacity wakes.

Build/artifact identities and failed candidates remain explicit; never run an
old target/release by assumption. Exact commits only; docs-dev requires force-add.
PROGRESS is ignored continuity. AGENTS.md immutable; userdoc+7lines untouched.
Telegram last19:21UTC, next nonurgent>=20:21UTC. Commentary within60s; verification
polls by minutes. Before compaction record current sessions, next decision,
source/binary identities and open/adverse outcomes. Do not stop at a checkpoint.

# Recent SEEN change reflection

Status: internal design and acceptance record. This is not a release claim.

## Why earlier milestones were premature

The recent corrections were not all wrong, but their acceptance labels were.
Several component-level RED/GREEN proofs were reported as if they closed the
cross-layer performance symptom. They did not exercise the complete timing
surface: cold and warm start, short objects, sustained single-stream work,
concurrent download/upload, impairment recovery, and application-visible
gaps in all three carrier variants. Random aggregate goodput also hid long
ordered-delivery stalls.

The corrected rule is deterministic:

1. Freeze the complete promoted SEEN scope and its acceptance cells.
2. Capture an exact causal trace.
3. State the invariant and symbolic counterexample before production code.
4. Demonstrate the exact pre-change RED without sleeps or random timing.
5. Apply one causal correction and obtain focused GREEN.
6. Obtain independent bounded review.
7. Run the affected ordinary-build runtime cell. Diagnostic builds establish
   causality only; they do not establish performance.

A component commit is a milestone, not acceptance. A randomized runtime cell
needs repeated independent realizations and application-visible timing, but
that statistical gate must not be used to invent or tune the model.

## Commit verdicts

| Commit | Intended correction | Evidence boundary | Verdict |
| --- | --- | --- | --- |
| `38286aa` | Stop response acquisition state from overriding ordinary ECF placement. | Exact service RED/GREEN; full runtime matrix remains open. | Retain; matrix-pending. |
| `a4679b5` | Keep a configured TCP startup-rate hint bound to the exact response output rather than replacing it with untyped telemetry. | The explicit-hint plumbing has a strong RED/GREEN. The same commit also demoted fresh kernel/peer response evidence and uses `max(C0, qualified Product receipt)`; with no hint the 350.75-Kbit/s prior can persist, while a high hint can resist a QoS downshift. | Split verdict: retain the provenance correction; do not accept the bundled response-rate authority policy until the startup/recovery transaction proves or replaces it. |
| `842a0cc` | Apply the same ordinary ECF rule to request OriginalData and remove acquisition-order arbitration. | Direction-symmetric RED/GREEN; runtime composition remains open. | Retain; matrix-pending. |
| `444fb38` | Publish MPP ACK/startup control after Product acceptance without waiting for a blocked local application write. | Exact blocked-sink RED/GREEN; it changes no scheduler or congestion control. | Retain; matrix-pending. |
| `0b50e9a` | Bound renewable live-owner repair by cumulative optional authority and stop a proven repair flood. | The pre-change flood is exact evidence for non-renewal. The cumulative percentage is nevertheless a hard recovery-admission gate, which violates the rule that extra-traffic percentages are hints. | Retain as an attribution checkpoint, not a final authority model. Preserve non-renewal while replacing percentage-gated liveness with structural copy identity. |
| `a9450d8` | Apply the selected latency priority to the actual Quinn stream. | Exact native-priority RED/GREEN and two ordinary non-downgrade runs; priority still cannot preempt bytes already accepted into one native ordered stream. | Retain as an isolated focused-GREEN checkpoint; wider native-HOL and release-matrix acceptance remain open. |
| `53d9ab5` | Restore one bounded live-owner frontier opportunity and preserve its exact owner, target, epoch, and wake lifecycle. | Exact target/epoch/wake ownership is useful, but the hard over-credit token and sequential percentage-gated service are not final under hint semantics. The 28-file change also weakens attribution. | Retain as an intermediary attribution checkpoint; redesign the authority model before release. Runtime acceptance is RED. |
| `dc4853d` | Compare an alternate with projected owner delivery rather than with an unrelated authority timer. | The timer says when fallback authority exists, not when the accepted bytes will arrive. The comparator is advisory and aggregate; it is not an exact byte-position oracle. | Retain the local timing correction inside the authority redesign; runtime acceptance remains RED. |
| `93e6284` | Do not charge the already accepted owner frontier as new payload a second time. | Both request and response exact tests proved duplicate Product/native debt. The diagnostic replay did not show this comparator winning, so no broad runtime gain is attributed to it. | Retain the accounting correction; affected runtime evidence remains pending. |
| `ee237c5`, `655cb95` | Freeze exclusive directional service authority and keep portable TCP telemetry diagnostic. | Documentation/model decision only. The independent TCP adapter audit disproves `cwnd/SRTT` and current portable fields as gain-free service. | Retain the authority boundary; no performance claim. |
| `1679968` | Represent startup/finite/Unlimited directional service without `f64` loss or a numeric Unlimited sentinel. | Pure typed component with arithmetic/scope tests; no owner consumes it by itself. | Retain as component groundwork. |
| `b5b4b5a` | Publish the typed rate sidecar and fence QUIC Apply against the current native activation/shape. | Exact provenance/fence REDs are GREEN. The same checkpoint also projected typed startup into `PathSnapshot.delivery_rate_bps`, which the still-live legacy scorer consumes; because T03 runtime migration is rejected, that sequencing can replace dynamic legacy TCP evidence with 351,472 bit/s before its successor exists. | Split verdict: retain typed sidecars, direction/incarnation fences, and diagnostic separation; restore the complete pre-checkpoint scalar source, precedence, scope, and Unlimited behavior until a complete allocator consumes the sidecar. Selectively omitting generic, Product, peer, or carrier branches would be another unproved production change. This is transaction isolation, not endorsement of the legacy heuristic. |
| `42bc328` | Freeze the exact one-action formula in RFC and internal design. | Checked component model was sound, but its first wording treated uniform `A=0` plus a static key as sufficient runtime placement. | Retain component definitions; correct RFC/runtime-completion claims with the sustained static-winner proof. |
| `898c66e` | Implement checked normalized action work, score arithmetic, rankability, identity, and incumbent uncertainty. |  Exact component tests only; the module is not a sustained allocator. | Retain isolated; do not wire runtime owners. |
| `d4b94e5` | Publish one coherent exact-direction timing tuple for a future score consumer. | Producer REDs caught and fixed an idle-fanout defect: an unchanged tuple now retains its epoch. Existing runtime rank remains unchanged. | Retain isolated timing provenance; no scheduling/performance claim. |

There is no evidence for a blanket revert: that would restore exact placement,
control-progress, priority, repair-renewal, and debt-accounting defects. There
is also no basis for accepting every commit wholesale. `a4679b5` combines one
valid provenance correction with an unresolved rate-authority policy;
`0b50e9a` and `53d9ab5` combine valid anti-renewal/identity mechanisms with a
hard percentage authority model that must be replaced. None of these local
proofs, by itself, establishes sustained or startup performance.

## Why the conflicting policies existed

The generic jitter, loss, confidence, `Suspect`, and active-flow score terms
predate the typed `C/U` model and the later proposed exact carrier-work `D`.
They were reasonable heuristic proxies for contention and uncertainty when
the inputs had no explicit roles. They nevertheless double-represent evidence
and can reverse even a simple service-time order: 100 idle browser flows can
turn a 500-Mbit/s physical-capacity observation into a 5-Mbit/s ranking value.
Their historical intent belongs in typed rate/timing evidence and incumbent
uncertainty, not in independent penalties.

## Post-checkpoint correction: exact carrier ledger rejected for v0.4.7

Three independent owner audits found that the all-stage carrier-work model
added to RFC Section 15.1 in non-release checkpoint `3a6d0ea` is not present in
the runtime. Current queue counters have the wrong scope, unit, and lifetime:
response scoring can count its dequeued subset twice, QUIC sibling writers do
not share queue ownership, payload-like charges are not encoded work, and a
local flush removes the charge before peer processing. The v10 `STREAM_ACK`
wire frame also has no service vector.

That does **not** justify implementing the proposed ledger. A separate model
audit disproved it as a Core prerequisite for this release:

- exact token stages would not make remaining native service exact because
  the proposed `Z` still drains using predicted `C`;
- retaining a finite `N^B` until a peer receipt imposes the operational bound
  `rate <= 8*N^B/receipt_delay`, regardless of calling it a resource limit;
- conservative cross-writer predecessor accounting can suppress independently
  ordered QUIC work; and
- the change would couple reservation, every writer, cancellation, recovery,
  and a new wire receipt before addressing the observed stale/underestimated
  rate authority.

For example, retaining 64 MiB for a five-second service-receipt delay caps
publication near 107 Mbit/s. That is the same family of feedback-clocked
underfill already rejected elsewhere. A partial ledger would be worse: it
would add the cap while still lacking the receipt that releases it.

The v0.4.7 model therefore keeps exact Product ownership, configured resource
limits, bounded stage-local queues, lifecycle checks, and native transport
backpressure as admission authorities. Path ranking is strictly advisory and
is not a receiver- or application-completion ETA. For one exact, positive
scheduling action it may use only a coherent local pre-native predecessor term
`A`, the action work `M` in the same declared normalized-MPP unit, typed
positive carrier-direction service `C`, propagation proxy `T`, and incumbent
timing variation `U`:

```text
S = T + ceil(8 * (A + M) / C)
U = max(J, 1 ms)
```

`M` is the complete encoded MPP action at the MPP boundary; native transport
framing, retransmission, and headers are excluded. `A` ends at native handoff.
It excludes Product/Data-ACK flight, native flight, loss, confidence,
active-flow count, and the `Suspect` label. If the owner cannot prove a
coherent `A` for every candidate in one comparison domain, it omits `A`
uniformly rather than treating missing evidence as zero. Actual writer
reservation and Product/resource revalidation remain the commit authority. A
finite frozen order tries every structurally eligible candidate; an infinite
advisory rank sorts last but cannot deny the only successful commit. Equal
ranks use the exact action/output/carrier/incarnation identity supplied by the
caller, not a bare path number or input order. An incumbent changes only
across `U_old + U_best`.

This smaller model proves only the checked ordering of **one already chosen
action**: no second congestion controller, deterministic base order,
monotonic response to lower `T/A` or higher `C`, no intentional double
counting inside that component, and a timing-variation deadband. It does not
prove sustained work conservation. With `A=0`, fixed observations make the
same path win every repeated action, while writer slots reopen at native
write/flush rather than network delivery. A bounded 64-MiB Product envelope
therefore bounds instantaneous ownership but does not give another ready path
a finite service opportunity; each Data-ACK-released quantum can return to
the same winner.

The post-checkpoint audit consequently rejects runtime-owner migration to the
component score. It also rejects shrinking the high-BDP writer queue or adding
a one-action pull lease: those changes would undo intentional pipeline and
shared-writer invariants without making native-buffer acceptance a network
service observation. A future sustained allocator needs one atomic
physical-carrier/direction owner, a remaining-work lifetime aligned with its
rate, dynamic TCP service or explicit unknown-rate exploration, and exact
replacement/refund semantics. Until those prerequisites exist, the current
runtime scheduler remains unchanged.

The component also does not claim a statistical rate-confidence bound,
receiver-completion prediction, independent bottlenecks, restart-free rate
recovery, or superiority to a baseline. In particular,
`U = max(J, 1 ms)` alone cannot prove that an approximately ten-percent
estimated-rate change is significant; T09 must either derive a typed
duration-valued uncertainty or explicitly leave that stronger no-flap claim
unmade. Those questions remain frozen gates. The unsupported all-stage/
service-receipt text must be removed from the authoritative RFC as a rejected
draft model, not implemented by stealth or left as a known code/RFC mismatch.

The hard ECF/completion-horizon admission branches were introduced to protect
receive-hole and reorder exposure. The current model now has explicit Product
window/headroom and configured queue/session resource contracts for that
safety. A numeric ETA comparison is ranking evidence, not resource ownership;
using it to reject otherwise lifecycle-valid Product work crosses that layer
boundary. Each reachable branch still needs an exact RED before production is
changed.

The optional percentage gate was introduced to stop duplicate wire
amplification, and the pre-change flood proves that goal necessary. Its defect
is not conservatism but authority: a cumulative percentage makes a performance
hint decide whether recovery is permitted at all. The replacement must retain
explicit copy identity, bounded simultaneous work, and wire accounting; simply
deleting the guard would reintroduce the original flood.

## Current SEEN-6E proof boundary

At clean commit `93e6284`, two valid ordinary repetitions disagreed: one had a
0.812670-second maximum read gap and the other a 3.580808-second gap. That is
enough to reject both single-run acceptance and any claim that the owner-debt
correction solved the end-to-end symptom.

The focused diagnostic replay then isolated the exact missing range
`[634464056, 634478656)`, 14,600 bytes. QUIC won original placement by only
7.535 ms and entered its writer in 2 ms. The client exposed the hole 571 ms
later; evidence reached the server actor after another 184 ms; existing owner
fallback added 99 ms. Three distinct TCP domains then accepted exact copies
sequentially, with immutable retry intervals of 273.364, 252.699, and 256.246
ms. Product-command wait was zero, native writer handoff was 1--2 ms, and the
third domain delivered the frontier 251 ms after handoff. The resulting
application gap was 1.064233 seconds.

This trace proves that the implementation followed the current RFC. It also
proves that another timer tweak, queue-wakeup tweak, same-domain retry, or
owner reset cannot be justified from this failure. The remaining question is
the authority model: whether a structurally finite frontier epoch may expose
one exact copy to each distinct native ordering domain concurrently. That is
a proposal, not an accepted fix. It first requires a symbolic safety proof for
identity, non-renewal, native admission, direction symmetry, and bounded copy
cardinality, followed by the exact pre/post counterexample.

The extra-traffic percentage may rank the cost of that operation but cannot
deny it as a hard admission budget. Conversely, the proposal cannot claim a
finite delivery bound when every native domain supplies zero service. Nor may
it assume that concurrent copies leave native service times unchanged: shared
bottlenecks can make the copies slow one another. Its conditional service
claim must therefore be expressed against the induced joint load and proved
under both independent and shared-resource cases; physical non-service remains
an honest theoretical constraint.

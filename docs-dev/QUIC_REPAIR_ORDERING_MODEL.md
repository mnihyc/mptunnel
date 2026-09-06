# Independent repair ordering within a QUIC attachment

2026-09-06 13:56 UTC. Design candidate, not an accepted runtime change.
Continues ORDERED_REPAIR_SERVICE_BOUNDARY and CURRENT_CLOSURE_PLAN.

## Exact causal evidence

The complete native-position trace keeps the shared500-Mbps directional cuts
and asymmetric variable loss/jitter, without the deliberate QoS step or outage.
At Product frontier297867012, the original65,536 bytes were assigned to TCP0
at Unixms1788701584634. A14,600-byte QUIC repair was dispatched at1788701588342
with zero Product queue delay. H3 accepted it in12us on native stream4.

Its first native byte was358082676; Quinn's first-unsent offset was308756942.
Thus49,325,734 native bytes still had to be serialized before that repair.
Full coalesced-write mapping attributes49,259,458 bytes to new OriginalData,
65,740 to an unmatched record, and536 to intervening framing. No earlier
repair record overlaps that span. This is bulk FIFO obstruction, not a repair
flood. All12,155 handoff mappings satisfy the native offset bounds;18 have no
matching Product dispatch event.

The first repair record crossed the observed first-unsent cursor between
2.553 and2.742s after handoff. Its contiguous native ACK prefix crossed at
3.292s. A TCP copy released the client frontier2.961s after that QUIC handoff;
the application gap was3.280s, with65,300,800 reordered Product bytes held at
release. Native ACK is not Product ACK and the crossing interval is sampled,
not an exact packet-send timestamp. The diagnostic mean133.719Mbps is not an
ordinary acceptance result. Complete40-bin throughput, interactive attempts,
process series and exact events are in NATIVE_REPAIR_POSITION_EVIDENCE_20260906.

An earlier singleton-write trace independently finds11,116,600 unsent bytes
ahead of a frontier repair. Its incomplete batch attribution must not be used
to classify that backlog. The complete batch trace above supersedes it for
that question. Both raw runs remain under .tmp/reflection/results/.

## Why the existing model failed

Native TCP/QUIC independence does not imply independent ordered Product
progress. An original on TCP can hold the Product frontier while QUIC carries
its suffix. Appending repair to that same QUIC stream appends behind the
suffix. The pre-native recovery queue introduced sensible priority and
bounded accounting, but that priority disappears at native stream handoff.
The earlier native-priority fix correctly programs Quinn; it cannot reorder
offsets within one stream. Neither fact is an upstream QUIC protocol defect.

For repair r, write B for unsent native predecessors and x(t) for available
native service. Same-stream serialization requires integral(x(t)dt)>=B before
r is selected. Changing r's pre-native priority cannot remove B. Separate
native stream offsets remove this specific prerequisite; shared native
flow control, congestion/pacing and actual packet service remain prerequisites.
This does not prove an unconditional finite delay under zero service or full
connection credit. It also does not recover already committed lower-stream
bandwidth or solve unknown-path discovery and TCP-only ordering.

The actual pinned Quinn engine passes three focused capability tests:
same-stream priority cannot overtake bulk; independent priority can serve the
repair first; a second stream does not mint connection send credit. Test
geometry is512KiB bulk/1MiB native send window, not a new runtime parameter.
RFC9000 Sections2.2--2.3 and4 distinguish stream ordering, application-directed
priority and shared flow control. RFC9114 Section6.1 requires client-initiated
bidirectional request streams; server-initiated request streams are not an
alternative. See [QUIC](https://datatracker.ietf.org/doc/html/rfc9000) and
[HTTP/3](https://datatracker.ietf.org/doc/html/rfc9114).

## Bounded candidate and owners

One reliable QUIC Product attachment may own a pair of H3 request streams:
ordinary/control and repair. Both use the same authenticated QUIC connection,
configured slot, physical instance, Product stream and attachment lifetime.
This is not a new carrier, return-plan enrollment, target, native controller,
capacity estimate, Product qualification or copy slot. TCP/L3/datagram mappings
are not changed by this transaction.

The client allocates the two native request streams before publishing the
Product OPEN_STREAM. A connection-local pair-allocation mutex prevents many
concurrent opens from each retaining half a pair and exhausting native stream
credit. It does not protect native I/O or wait for peer acceptance. The
companion's MPP opening frame is sent after ordinary attachment acceptance,
without waiting for companion acceptance on the ordinary-open completion path. Its opening
frame identifies the existing Product stream and the exact parent native
request-stream ID. The server's connection-local registry contains only live
accepted parents and transfers a companion to its parent at most once.
Publication precedes parent acceptance. No pending child can create a parent;
unknown, retired, mismatched and duplicate bindings are refused locally.
Native IDs are not reused within the connection. Dropping the parent removes
its registry entry, including cancellation before child arrival. No global
nonce table, speculative rendezvous or arbitrary expiry timer is needed.

The existing bounded reinjection receiver moves to the companion writer; it
does not gain queue slots or byte budget. The command purpose survives this
split. OriginalData never enters that stream; requalification, already sharing
this queue, retains its existing non-delivering proof semantics. Receive frames
go through the same exact attachment/Product authority, not directly to a
target socket. Data ACK, FIN, RESET, return-plan and credit ownership stay with
the original Product machinery. The repair stream gets native priority above
ordinary bulk but never additional native credit or an independent pacer.

The parent task owns both stream futures; it polls repair work independently
of a pending ordinary write. Its retirement cancels both and reconciles each
queue charge once. A premature companion error is an attachment-local failure,
not physical-carrier or session retirement. A normal half-close of the Product
or ordinary native receive side must not retire a still-needed send half.
No child task may outlive its parent. Operation stream-count geometry must
account for two native requests per reliable attachment plus carrier control;
this is mapping arithmetic, not a higher Product admission limit. An explicit
max_quic_concurrent_bidi_streams remains an actual native ceiling. Defaults
and the cap derived from max_streams must account for this mapping to retain
the former logical concurrency. Native credit shortage remains backpressure,
not permission to exceed the configured ceiling. Pair allocation remains
under existing Product-open cancellation/deadline ownership; cancellation must
dispose of both native halves even when only the first was allocated.

## Foreseen counterexamples / required proofs

1. A repair arrives while the ordinary native write is Pending: the companion
   must still be polled. Native credit exhaustion remains honest backpressure;
   priority cannot be claimed to reserve or create native credit.
2. Child before parent, duplicate child, retired/replaced parent or a child
   from another connection: no target creation or attachment replacement.
3. Cancellation while child open/claim/write is pending: no detached task,
   leaked registry entry, forgotten byte charge or reusable copy authority.
4. A child EOF/reset must not terminate sibling Product flows or the connection.
   Parent FIN/half-close and terminal drain must retain their existing meanings.
5. Both directions and requalification use exact existing Product validation;
   neither companion acceptance nor native receipt becomes Product progress.
6. One or many logical flows, all existing repair slots occupied, a reversed
   TCP/QUIC quality ordering, full native credit and real outage: no protocol
   preference or newly manufactured recovery capacity. Persistent repair
   overload cannot be advertised as starvation-free ordinary bulk service.

## Acceptance / next action

2026-09-06 14:38 UTC: integration is an UNACCEPTED candidate. Queue transfer,
connection-local binding, codec mapping and actual QUIC half-close/sibling
isolation tests pass. A native ceiling of2 (control+one incomplete pair half)
also verifies cancellation returns the half's native credit without closing
the carrier. Ordinary download first comparison reduces maximum gap2.689s to
.384s, but upload has a32.816s confirmation gap and26.641Mbps overall despite
eventual exact completion. The control also fails to drain before observation
ends. This is not a usable or non-regressing composition. Next trace the exact
upload prefix and original/repair assignment; no tuning or acceptance from
the better download number. Full comparison JSONs retain every outcome.

The precondition for historical P2/T07 investigation is now observed, so it is
in scope. Do not implement the broader speculative multi-domain allocator.
First prove paired ownership/queue transfer against real runtime primitives,
then integrate and update affected RFC6.2/10.4/15.2 and wire mapping explicitly.
The current RFC does not silently promise this capability. Compare an ordinary
candidate with its exact ordinary parent in the affected loss/jitter and QoS
cases, both directions, including read gaps, latency and RSS. An apparent high
mean cannot accept this candidate. Shared/independent aggregation, browser,
startup, sustainability and final baseline gates remain open.

Reproduction: build the archived native diagnostic patches with lab-diagnostics
and forward MPTUNNEL_NATIVE_PROGRESS_TRACE=1 alongside the runner's existing
MPTUNNEL_NATIVE_RECOVERY_TRACE. Run mixed/combined/down with ROUTED, MANAGEMENT,
DIAG and NATIVE_TRACE enabled, NO_QOS=1 and NO_BLACKHOLE=1. The saved diagnostic
binary is .tmp/reflection/bin/native-repair-batch-position-diag/mptunnel.
Both diagnostic source patches are removed from the active runtime tree.
Focused native proof: cargo test --release --manifest-path
crates/quinn-proto/Cargo.toml --target-dir target --lib repair_ordering.
All3 passed. The workspace -p invocation lacks this dependency's dev-dependencies;
use the manifest command above. This was not a model or platform failure.

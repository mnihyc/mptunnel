# Existing allocation/discovery owners

2026-09-07. Read-only source audit of the already-seen busy-fast/free-slow
allocation issue. No allocator, numeric budget, timer or RFC change approved.
Read alongside `BOUNDED_PLACEMENT_DEFERRAL_PROPOSAL.md` and
`T03_ADVISORY_SCORE.md`; this note inventories existing authority, not a new
algorithm.

## Concrete producers and boundaries

| Existing producer | Usable fact | What it does not own |
| --- | --- | --- |
| `src/runtime/path/authority.rs`: `NativeCarrierRateAuthorityHandle`, physical-connection `NativeCarrierRateAuthorityBinding`, `accepted_change_cursor` | Exact QUIC carrier/direction, activation-bound native rate/shape and retained publication wake | Allocation opportunity, independent marginal capacity, or Product qualification |
| `src/runtime/path/state.rs`: `record_relay_path_send`, `release_relay_path_inflight`; `health.rs`: matching instance-fenced counters | Shared client C→S outstanding uniquely enqueued Product work | A cumulative bypass count, an owed trial, or atomic selection ownership across streams |
| `src/runtime/path/model.rs`: `ClientPathObservation` Product sample count/bytes, last delivery and expiry | Recent achieved Product delivery with its recorded scope | NativeOperational authority or permission to declare all logical attachments qualified |
| `src/runtime/stream/request/state.rs`: `RequestPathStates`; `response/attachment.rs`: `ResponseStreamOutputEntry` | Exact per-stream attachment qualification, unique flight, ACK rate epoch and incarnation | Shared physical-carrier allocation history |
| `src/model/product_qualification.rs`: `deficit_bytes`, `qualified`; `capacity.rs`: F/E geometry | Remaining evidence volume and existing bounded admission before qualification | A recurring discovery cadence or guarantee that eligible work is ever selected |
| `src/model/request_evidence.rs`: `RequestProductRateEpoch`; `response/ack_clock.rs`: epoch installation | Fixed evidence expiry after an exact ACK observation | A deadline for a never-tried path, or a mandatory opportunity when evidence expires |
| `src/runtime/path/queue.rs`: capacity notification and exact queue reservations; `stream/handle.rs`: membership updates | Actual readiness, membership and cancellation/commit fences | Timer-like progress for a free unused queue; queue state is not common physical QUIC allocation state |
| `src/model/requalification.rs`; `response/requalification.rs` | Per-stream stale-candidate cursor, immutable probe receipt deadline, subsequent unique-OriginalData qualification | Initial healthy-unknown discovery or a substitute for uniquely attributed Product evidence |

TCP multiplexed streams share their ordered writer queue, whereas QUIC native
streams have separate writer queues (`queue.rs` load-registration contract).
Those queues must not be relabelled as one transport-neutral physical-carrier
allocation owner. The old TCP capacity-probe controller and enqueue producer
are `#[cfg(test)]` (`sender/request/multipath.rs`, `path/queue.rs`): they are not
live discovery machinery available for reuse.

## Counterexamples that existing fields do not resolve

1. An actual 500-Mbit/s path with a portable approximately 351-Kbit/s prior can
   stay unused while another path repeatedly reopens. A finite wait per source
   head does not force the unknown path to receive a head.
2. Qualification is durable; a previously qualified slow path does not become
   unqualified merely because its service later improves. Conversely, a
   never-tried path has no ACK-derived epoch whose expiry could drive discovery.
3. Per-stream cursors and qualification can let concurrent streams make the
   same independent choice. There is no shared claim/refund or outstanding
   discovery obligation in those records.
4. Open Product-flow lifetime is not a busy-demand epoch. Browser keepalives
   can remain open through sparse traffic; using their count to retain/reset
   allocation history would attach unrelated old work to later sparse work.
5. Duplicate retained payload can test requalification reachability, but it
   cannot establish which physical copy delivered a Product range. It cannot
   replace the unique-copy qualification receipt.
6. A failed/unavailable alternative must not hold surviving traffic. Current
   exact lifecycle and writer wakes can end a wait, but do not justify waiting
   before an eligible alternative has an actual owned obligation.

## Bounded conclusion

Existing identity, native evidence, exact Product receipts and wake owners are
sufficient building blocks for validation and one explicitly selected trial.
They do not prove finite recurring discovery across physical carriers and
concurrent streams. Neither F/E nor a resource window can silently become a
discovery cadence; reducing them to force spillover would restore earlier
high-BDP/admission defects.

Fresh, scope-comparable-evidence-only deferral while retaining immediate
eligibility for unknown alternatives is the smallest boundary supportable
without adding discovery ownership. It does **not** solve the demonstrated
portable-rate branch. A stronger finite-discovery claim needs an explicitly
chosen allocation policy and owner; no numeric B or hard window was selected
in this audit.

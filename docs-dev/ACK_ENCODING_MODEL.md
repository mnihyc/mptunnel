# Bounded ACK wire-cost correction candidate

Date: 2026-09-06 22:29 UTC. Status: model candidate, not accepted performance.

## Evidence and provenance

The clean-link mixed download publishes69,170 complete ACK snapshots containing
4,815,231 range entries; reverse-cut traffic is348MB versus about24MB for
single-carrier controls. Full snapshots intentionally preserve negative-gap
authority and independent attachment recovery. Their fixed-width wire encoding
spends16bytes for every range, even when gaps and lengths are small. This is
real representational overhead, not proof that it causes all observed stalls.
See CLEAN_SERVICE_ACK_EVIDENCE_20260906.json. Publication count/cadence and
full-snapshot semantics are not changed by this candidate.

## Minimal model and symbolic bounds

Keep the logical Frame and every complete/partial bit and range exactly. Add
an independent per-frame packed representation to STREAM_ACK: canonical
unsigned base128 integers encode (gap from previous end, range length), with
initial previous end0. The flags byte uses bit0=complete and bit1=packed;
all other bits are invalid. Fixed start/end pairs remain a representation
for unordered/overlapping inputs or when packing is not smaller, not legacy
wire compatibility. The clean-break version becomes13.

For each ordered range, s_i=e_(i-1)+gap_i and e_i=s_i+length_i. Checked u64
addition reconstructs exactly. No inter-frame dictionary, session history,
timer, cadence change or actor cache exists. A new/retired attachment or a
cancelled write therefore cannot desynchronize future decoding. Every decoded
ACK (not merely its positive union) is identical to the original Frame, so
negative horizon, recovery, retirement and publication semantics are unchanged.

For n ranges, old frame size is21+16n bytes. Choose packed only if
sum(vlen(gap_i)+vlen(length_i))<16n; otherwise fixed. Therefore each frame is
no larger than before, including arbitrarily large offsets and pathological
nonmonotone inputs. Empty ACKs retain their existing representation. Each
varint takes at most10bytes; reject overflow, non-minimal representation,
truncation and zero-length/overflowing extents. Existing range-count/frame
limits remain enforced before allocation. Extra live memory is O(1) outside
the existing decoded vector; encode/decode work remains O(n).

## Tradeoff and explicit non-claims

This removes redundant wire bytes, not repeated logical snapshot construction:
it does NOT solve O(number_of_updates * retained_ranges) CPU work. A stateful
cross-frame dictionary could remove more repetition but adds cancellation and
per-stream ownership complexity without current justification. A positive-only
ACK policy could remove information needed by the already-fixed gap model.
Neither follows from this candidate. No congestion, queue, exploration,
initial-rate, loss-compensation or ACK-publication parameter changes.

## Acceptance sequence

1. Existing encoder fails compact-size regression; unchanged semantic roundtrip
   remains the invariant. Cover maximum offsets, nonmonotone inputs, fragmented
   snapshots, partial ACKs, limits and malformed packed records.
2. Prove exact roundtrip and never-larger size over deterministic wide fixtures;
   run existing protocol/transport/feedback controls, then strict compilation.
3. Repeat unchanged ordinary clean/mixed and affected asymmetric loss/QoS cases.
   Examine reverse bytes, CPU, startup, gaps and loaded latency, not mean alone.
4. Retain only justified component changes; no release or global improvement
   claim from codec tests or theoretical byte savings. Allocation/discovery,
   browser, aggregation and sustainability gates remain unchanged.

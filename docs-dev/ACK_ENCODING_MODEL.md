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

History: fixed start/end pairs date to transport foundations46ac66b; the
complete flag dates to282b8e1. They offer a simple bounded stateless format.
They are not newly introduced corruption or evidence that prior correctness
fixes were wrong. The shortcoming is their now-measured repeated bandwidth
cost under fragmented multipath delivery, not their logical ACK semantics.

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

Exact decoded equality proves semantic preservation, not identical timing in
a closed-loop native transport. Packet sizes/feedback service change, so a
byte-efficiency improvement can still alter allocations and loaded latency.
The ordinary timing gate cannot be waived by the codec proof.

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

## Component result — 22:35 UTC

The original encoder fails the focused representational-cost assertion:
234 fragmented ranges require3,765 bytes instead of956. Decoded Frame equality
already holds; this is unnecessary encoding overhead, not corrupt old ACKs.
The candidate retains exact equality at956bytes. All five new codec tests pass,
including8,192 deterministic vectors in both complete/partial forms, full-u64
boundaries, arbitrary ordering/overlap, malformed integers, count/frame limits.
Two existing golden byte arrays initially retained version12; their only
difference was the explicitly changed version byte. Expectations now use13.
No controller, timer, actor or source-debt state was changed. Caller/ordinary
tests are still pending; this does not establish a runtime performance gain.

## Ordinary clean controls — 22:43 UTC

All58protocol,123transport,29feedback controls and strict Clippy pass. The
optimized build takes2m05s, then runs without diagnostic logging or concurrent
compilation. Two mixed controls/candidates respectively:

| Pair | Control Mbps / p95ms / reverseMB | Candidate Mbps / p95ms / reverseMB |
| --- | --- | --- |
| 1 | 400.977 / 548.209 / 348.160 | 398.134 / 910.115 / 164.949 |
| 2 | 395.746 / 838.263 / 421.419 | 401.108 / 651.034 / 161.804 |

Mixed reverse byte/body-byte ratios improve0.278 to0.133 in the first pair;
the second reduction is also real. Latency ordering reverses across pairs:
neither consistent regression nor a stable timing gain is established. QUIC
controls428.049/428.886Mbps versus429.695/430.813; echo-p95166/229ms versus
273/214ms, all50/50echoes. TCP353.297/953ms versus398.375/1103ms;42/42versus
35/35echoes (fewer attempts are serial slowdown, not failures). Full untrimmed
probes/series are in ACK_ENCODING_COMPARISON_20260906.json. No release claim.

Four existing-profile adverse mixed checks now compare both binaries under
forward-changing loss/QoS/blackhole for download and mirrored upload. After
that bounded decision, resume allocation/recovery; no compression parameter
sweep or new speculative compression state is authorized by this evidence.

## Adverse observations — 22:49 UTC

Mixed download candidate79.683Mbps/maxgap3.843s versus control56.653/4.883s;
both lose echo at QoS and only30 requests succeed. Successful echo-p95 is
904ms versus470ms; censoring makes that statistic insufficient. Mirrored
upload candidate confirms all523,239,424bytes at94.451Mbps/gap3.884s;
control confirms all431,357,952bytes at77.747Mbps/gap7.949s. No spontaneous
upload reset. Full records are ACK_ENCODING_ADVERSE_20260906.json. These are
observations, not stable overall improvement or an accepted release.

One direct feedback-bottleneck discrimination is justified before closing this
branch: unchanged clean500Mbps forward/10Mbps return,100ms RTT, no deliberate
loss/jitter, comparing raw/QUIC/mixed control and mixed compact. Existing runner
adds only optional REFLECTION_RETURN_RATE substitution for the client's500mbit
plateau, before existing mirror logic. Omission is unchanged; other adverse
observations above used omission. No production config/rate/threshold changes.
This tests the observed reverse traffic against a finite return service instead
of assuming reverse500Mbps means feedback cost is harmless.

## Decisive return-cut result — 22:52 UTC

Verified router rate/ceil:62,500,000B/s forward,1,250,000B/s return. Clean
100ms RTT, no deliberate loss/jitter. Raw443.814Mbps/echo-p95128.553ms;
QUIC431.679/239.752ms; mixed control46.249/2468.458ms with an echo failure.
Its last return queue holds3,094,592B, representing2.476s of10Mbps service;
forward queue is only425,928B. Compact mixed82.010/2032.016ms still fails
interactive service and has a2.411s read gap. Full untrimmed records are
ACK_RETURN_BOTTLENECK_20260906.json. This is direct evidence of a harmful
mixed feedback bottleneck, not an inference from retained bandwidth estimates.
It does not prove that feedback is the only cause of prior500/500 stalls.

Decision: stateless integer packing alone is NOT accepted as the root fix.
It materially lowers wire cost but retains repeated snapshot transmission.
Keep the candidate uncommitted while examining lossless cross-snapshot encoding
on the existing reliable ordered transport. No cadence, receive window, native
congestion knob or selective-negative-information policy change is justified.

## Next proof gate: one bounded transport dictionary (not implemented)

The native authenticated ordered stream can encode a complete canonical ACK
as additions to its previous complete canonical ACK of the same logical stream.
The receiver reconstructs the identical full Frame BEFORE Product publication,
so complete/partial semantics, all negative horizons and ingress coalescing
remain unchanged. This is encoding, not a partial ACK pretending to be complete.

Retain at most ONE prior ACK vector per native ordered direction, bounded by
max_ack_ranges, never a map of logical streams or lifetime history. A changed
logical stream, noncanonical/nonmonotone vector, absent basis or non-saving
difference uses an independent full encoding. Partial frames do not mutate the
complete basis. Fresh/reconnected/companion streams begin without a basis.
Merged extents and limits are checked before updating decoder state. Full-u64
arithmetic, expanded count and actual frame bounds still apply. Full encoding
always resets the basis, so it also provides an unconditional resynchronization.

Important cancellation proof: form a prospective encoder state locally, then
clear the live encoder basis BEFORE awaiting native write. Publish the
prospective basis only after success. If the write is cancelled, failed or only
partly accepted, the next ACK must be independently full. Whether the receiver
consumed none, some or all of the old batch, the next full ACK resets its basis.
Existing native framing/failure handling remains responsible for partial byte
records; no new path-close, retry delay or lifecycle state is added. Codec
failure before any write leaves the old basis valid. Decode state advances only
on a consumed complete record, never a speculative/coalescing lookahead.

Before implementation, verify these native write and read ownership premises in
both TCP Noise/TLS and H3 ordinary/companion paths, and define exact roundtrip,
cancellation/late-join/interleaving/count-bound tests. Do not create a new
logical ACK history, alter negative-gap policy, assume callbacks are atomic,
or hide a cancelled-write desynchronization with path restart. The criterion
is exact Frame-sequence preservation plus removal of repeated wire ranges;
ordinary500/10 and prior controls still decide practical usefulness.

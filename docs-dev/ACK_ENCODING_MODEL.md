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

Native premise check: H3SendOperation already retires a cancelled request stream
(not its QUIC connection), with an actual constrained-write/replacement test.
Split TLS writers already poison uncertain writes; Noise retains nonce/partial
record poison state through splitting. Preserve all these existing semantics.
The codec basis reset is not permission to reuse an invalid native byte stream.
TLS/Noise helpers encode before their awaited write; H3 encodes a batch before
send_data. These are the correct prospective-state/clear/commit boundaries.
Both H3 buffered receive paths own complete records before decoding, while its
StreamData coalescing lookahead must remain non-mutating for dictionary state.

One complete basis, not a per-logical-stream map, bounds lifetime retention.
Interleaved TCP logical streams can miss/evict the basis and use full frames;
this is an explicit compression-efficiency limitation, not missing ACK state
or an excuse to add an unbounded cache. H3 already has per-request stream
ownership, including separate ordinary/repair requests. Full/nonmonotone/
noncanonical frames preserve exact input order via independent representation;
only canonical complete snapshots with monotone positive coverage are eligible
for relative encoding. Delta reconstruction must validate positive-set union,
expanded range count and all arithmetic before committing its one basis.

Actual transport RED23:02UTC: repeated_complete_ack_wire_cost_remains_delta_sized
sends200 monotonically growing complete snapshots while checking every received
Frame for exact equality. Current packed-only TLS spends84,724bytes and Noise
84,628bytes. Both deliver correct Frames but exceed the16KiB regression bound;
relative one-range updates plus the initial full basis fit below that bound.
This is production encrypted framing over an in-memory byte stream, not a
standalone ideal codec simulation. No runtime dictionary is implemented yet.

## Relative candidate integration — 23:17 UTC

The codec now has one bounded encoder/decoder basis. Relative flags are accepted
only by ordered contextual decoders, never stateless single/batch decoding.
Both decode paths share the structural ACK parser; native datagrams remain
stateless. Encrypted TCP (including admission writes and split transfer) and
both H3 consumed-record paths are integrated; peek/coalescing stays stateless.
Codec failure leaves live encoder/decoder basis untouched. Prospective changes
commit after successful writes; uncertain writes clear the encoder basis without
changing existing TLS/Noise poison or H3 request-retirement semantics.

Actual protected sequence GREEN: TLS6,007bytes and Noise6,003bytes versus
84,724/84,628 packed-only bytes, while every decoded complete Frame is identical.
126transport tests pass, including split-basis transfer, actual H3 multi-frame/
multi-write roundtrip and existing cancelled-request/healthy-connection test.
287ACK-filtered controls and strict all-target/all-feature Clippy pass; earlier
43codec controls include context/missing-basis, expanded limits, failed decode
nonmutation, interleaving/nonmonotone full frames and uncertain-write reset.
This is component proof, not a network result. Optimized relative-ack build
is next; no public performance claim, source commit or release is accepted.

## Network verdict and bounded next discriminator — 23:37 UTC

The first ordinary relative candidate on the unchanged 500/10 Mbps return-cut
case gives 174.755 Mbps, a 1.666 s read gap, and 1,335 ms echo p95. It is an
improvement over 46.249 Mbps but NOT an accepted fix. Its 39 echo attempts all
succeed; serial slowdown prevents the intended 50 attempts. The separate
ACK-origin diagnostic remains complete (maximum 245 ranges), so incomplete
chunking is not the reason compression is insufficient in that capture.

An opt-in aggregate encoding trace verifies that the dictionary works in the
actual runtime: about 175 thousand relative frames against only 144 full ones.
About 100 thousand small credit updates accompany them. There are no partial
ACKs in that capture. Feedback packetization is the next bounded discriminator;
see FEEDBACK_PACKETIZATION_MODEL. The temporary aggregate trace hook is archived
under .tmp/reflection and removed from active source. No logical publication,
fanout, negative authority or controller policy has changed. Source remains
uncommitted, with all ordinary and diagnostic records preserved separately.

## Affected ordinary controls — 23:59 UTC

The ready-feedback batching trial is removed after its failed timing repeat;
see FEEDBACK_PACKETIZATION_MODEL. Only the frozen relative encoding candidate
is used below, without logging or concurrent compilation.

Clean 500/500 Mbps, 100 ms: mixed 399.386 Mbps / echo p95 534 ms / gap 0.303 s;
QUIC 431.324 / 154 ms / 0.101 s; TCP 392.586 / 1,151 ms / 0.305 s. Mixed and
QUIC have all 50 successful echoes. TCP has 37 successful attempts because
serial latency consumes the test time. These do not establish a TCP latency
improvement; clean controls previously varied substantially.

Combined asymmetric loss/QoS/blackhole mixed download: 71.342 Mbps, 6.673 s
read gap, 56 successful echo attempts, no failed echo. The gap is at
18.294--24.967 s, during the 15--25 s QoS, BEFORE the 30--33 s UDP outage.
At 19--24 s the return queue is only hundreds of bytes, while QUIC's native
ACKed bytes advance from 218.65 MB to 224.09 MB and Product application bytes
stay near 217.95 MB. The forward queue holds roughly 2.5--2.9 MB. Thus return
feedback saturation is not the explanation for this particular residual
stall. Exact current missing-range carrier attribution is not contained in
these aggregate counters; prior direct ordered-prefix traces remain the
relevant independent proof. Do not claim every gap is a TCP hole.

Mirrored mixed upload confirms all 460,849,152 bytes without a reset, at
65.176 Mbps and 2.487 s worst confirmation gap. It takes 56.567 s including
16.567 s after the 40 s load. Mean is below the prior 77.747 Mbps control,
but the maximum confirmation gap is shorter than its 7.949 s. A matched
control/candidate upload repeat is required before the component decision;
do not cherry-pick either metric or accept a new regression. Full records are
ACK_RELATIVE_CONTROLS_20260906.json. Global acceptance remains red.

The dictionary bounds one vector per native ordered direction, not aggregate
memory for all configured concurrency. It also does not compress the queued
logical Frame vectors before native encoding. No claim that it fixes the
uncaptured RAM incident, control-queue retention or whole-process RSS follows.

## Upload check and component disposition — 2026-09-07 00:07 UTC

The ordinary mirrored control/candidate repeat confirms every byte in both
cases, but timing does not pass. Control: 475,004,928 bytes, 81.518 Mbps,
5.629 s worst confirmation gap, 46.616 s total. Relative candidate:
374,341,632 bytes, 44.909 Mbps, 13.646 s gap, 66.684 s total. The load lasts
40 s, so candidate drain is 26.684 s. Complete records, including all one-second
observations, are ACK_RELATIVE_UPLOAD_CHECK_20260907.json.

This is not deterministic attribution to the codec: random loss/reordering and
attachment completion order differ, while decoding preserves every Frame.
It IS decisive against accepting the composition as timing-nonregressing.
The codec remains held, uncommitted and excluded from release acceptance.
Its clean asymmetric-link efficiency is a genuine but insufficient benefit.
No further encoding or feedback-policy experiment follows before ordered
allocation/recovery attribution. The rejected ready-feedback batching remains
removed. An existing-event-only upload trace is the next bounded discriminator;
its timing numbers will not replace these ordinary observations.

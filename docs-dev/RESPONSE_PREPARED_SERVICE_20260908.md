# Response prepared Original service

2026-09-08. Verified candidate implementation, six healthy and four harsh ordinary cells.
**No performance acceptance. High-BDP bulk service remains, but startup,
loaded-tail latency and sampled costs have adverse results. Final focused
verification is 692/692 GREEN.** This report follows the
[active closure transaction](CURRENT_CLOSURE_PLAN.md) and
[mandatory method](PERFORMANCE_METHOD_AND_LESSONS.md).

## Mechanism, origin and prospective benefit

The candidate moves response Original assignment from the actor's private
carrier command publication to an imminent native writer claim. Until that
claim, source bytes remain shared unassigned U. The actual claim commits the
exact source prefix, cumulative sent offset C, retained send cache and selected
Original owner together. It consumes the source prefix once; it does not
copy or cancel an already-started native write. Actors continue dispatching
repairs/controls, and their request receive/local-I/O work is outside the
response Product guard.

This preserves the structural W/P/E admission established by `65edae3`;
see [T04b's qualified disposition](T04B_STRUCTURAL_PRODUCT_ADMISSION.md).
Resource permission is not a requirement to bind an entire future queue
immediately. There is no smaller source window, guessed capacity, protocol
preference, changed quantum, new congestion controller or timer. C+U and
retained unique debt+U are invariant across a claim. Session reservations
remain charged until actual Product ACK release. EOF keeps claims active;
FIN requires U=0 and uses exact C, while retained ownership remains recoverable
after FIN. Successful claims carry first/last producer timestamps rather than
later actor observation time.

Each weak registration is tied to the exact output incarnation and lane.
Actual writers use prearmed nonblocking Product acquisition, re-rank current
Ready candidates from full membership, and fence the chosen Native/identity/
Ready authority. A busy owner parks with a release wake; cancellation revokes
registrations. The existing actor Product→Native repair path is retained:
writers never block on the reverse edge. This is not a receive-service
isolation fix, and deadlock freedom alone is not a throughput guarantee.

The motivating [DOWN capture and raw archive](MIXED_RESPONSE_PLACEMENT_SERVICE_20260908.md)
prove 128 earlier TCP Originals spanning [283493924,291208666),
7,714,742B, already published but unstarted at Unix1788871829040. QUIC
subsequently positively writes 57,120,620B of later Originals. Finishing that
lower interval takes a 9.486s advancing sequence and releases a large suffix.
The forecast is fewer multi-second ordering sequences and less useless
reassembly by assigning earlier shared bytes at those actual service
opportunities—not a promised 9.486s saving or extra QUIC capacity. The largest
individual 3.589s hold was already native-accepted. Physical queues, loss,
feedback, selection cost and actor service can leave the gain zero or adverse.

The same UP capture's 1.112s already-decoded useful-reply residence and
6.403s frontier hold remain unresolved. Its three measured owner sections
peak at 13.405ms. Response placement does not establish a fix for that
separate receive boundary.

## Reachability and verification status

The [archived structural RED](MIXED_SERVICE_STRUCTURAL_RED_20260908.patch),
preserved with its logs in the
[placement raw archive](MIXED_RESPONSE_PLACEMENT_SERVICE_20260908.raw.tar.gz),
first proves actual B admission, protected write and peer receipt. The intended
assertion then finds 131,072B prematurely owned by never-consumed A, expected 0;
the opposite started-A ownership control passes. This is a prospective
ownership-contract RED, not a claim that the old RFC already required late
claiming. The separate paused-receive-owner RED is retained as structural
evidence, not promoted to the measured seconds cause.

Current completed checks, without conflating fixture failures with model RED:

| Check | Observed result / disposition |
| --- | --- |
| Protected response writers plus Native/source controls |7 pass in 0.06s: earliest prepared prefix, started owner, queued notice after ACK, losing Ready/control service, weak cancellation, source conversion and Native-fenced Busy. |
| Sender/response/TCP-server/QUIC/control regressions |601 pass in 3.57s. |
| First actor regression pass |87 pass/1 failure: old requalification fixture expected a prebound SendFrame, not a real prepared claim. |
| Initial EOF-with-U control |Failed because it watched only output A for FIN; unchanged control selection may choose empty B. Fixture now checks both outputs before/after claim, without changing the deadline. |
| Corrected lifecycle checks |Final lifecycle4: all4 pass in0.28s, including requalification, real EOF→claim→FIN→post-FIN repair, deferred-probe and idle-Ready/Busy service. |
| Intermediate real-lifecycle cohort |691 pass/1 failure in7.21s. The deferred-probe case passed after restoring its actual Native metrics cadence; Busy's later single claim still omitted that fixture cadence and timed out. This failed result remains retained. |
| Final verification |Warning-free build6 in39.06s; regressions-verified gives692 pass/0 failure in3.71s. Only fixture changes followed the frozen ordinary release2 binary; no runtime substitution between cells. |
| Payload-stat correction |Actual EOF test exposed 130 versus 128. Two FIN/replay queue-control units were counted as payload by the legacy non-reinjection branch present in `4f584213`. Removed only that statistic mutation/plumbing; control resource/budget charges remain. Corrected EOF test passes with 128; no duplicate source and no speed claim. |
| Ordinary candidate pilot |Three healthy cells complete using the frozen default-feature response-claim-20260908 binary. The predeclared21:55 execution deviation allowed the ordinary pilot while the last test-only cadence repair awaited rebuild; this does not waive final verification. No compiler/runtime changes overlapped these cells. |

The first compile-only import/unused-binding failures were integration errors,
not intended REDs. Real QUIC composition review also found legal latency notices
in the priority lane and beside deferred probe work; no protocol rejection or
idle-Ready withdrawal should be manufactured by a metadata refusal. Those
paths are covered by the final lifecycle checks above, not just their earlier
helper assertions.

Logs remain under `./.tmp/reflection/`:
`response_pre_native-0908.log`,
`response-claim-0908-{protected,regressions,regressions-final,regressions-verified,actor-controls,eof,lifecycle,lifecycle2,lifecycle3,lifecycle4}.log`
and the corresponding build/release logs. Failed intermediate outcomes are
retained, not rewritten. The source checkpoint remains to be recorded by root.

## Healthy ordinary comparison

All three controls use ordinary `d999fea`,
`./.tmp/reflection/bin/ack-support-20260908/mptunnel`, at both endpoints;
diagnostics/native tracing are off. Existing routed runner, sequential
TCP→QUIC→mixed, tag `response-claim-healthy-control-0908`. This is the declared
500Mbps/100ms healthy high-BDP ablation, not the harsh impairment case or an
Internet competitiveness claim. The later candidate cohort uses
`./.tmp/reflection/bin/response-claim-20260908/mptunnel` at both endpoints,
also TCP→QUIC→mixed, tag `response-claim-healthy-candidate-0908`; diagnostics
are off. These are six sequential cells, not an interleaved or repeated pair.

The existing `REFLECTION_NO_LOSS=1`, `REFLECTION_NO_JITTER=1`,
`REFLECTION_NO_QOS=1`, `REFLECTION_NO_BLACKHOLE=1` settings retain 70ms
downstream plus 30ms upstream delay and the configured queues. All 41 service
rows in each of the six cells confirm both class rates 62500000 bytes/s (500Mbps), netem
jitter 0/no loss configuration, no UDP blackhole, no management errors and
one unchanged product PID per endpoint. Epoch labels 0–7 still advance every 5s;
they do **not** represent QoS, changing loss or outage in this ablation.
The `jitter_removed=false` field denotes the separate recovery scenario flag,
not nonzero jitter here.

Each bulk probe is HTTP 200, status/bulk_status=ok, exactly one deliberately
duration-limited partial request from an 8GiB body, **zero complete full-body
requests**. The observation ends normally at 40s; this is not an all-source
EOF/ACK settlement test.

| Mode/build | Received body bytes | Bulk time (s) | Goodput (Mbps) | First body (s) | Maximum read gap (s), interval |
| --- | ---: | ---: | ---: | ---: | --- |
|TCP control|2,058,940,632|40.000339|411.785|0.447975|0.402187 (32.702125–33.104312)|
|TCP candidate|2,104,175,379|40.001478|420.820|0.579549|0.400775 (32.390548–32.791323)|
|QUIC control|2,169,825,012|40.000429|433.960|0.414485|0.100711 (0.515059–0.615770)|
|QUIC candidate|2,135,520,340|40.002035|427.082|0.413889|0.101400 (0.413889–0.515289)|
|MIXED control|2,050,152,648|40.000516|410.025|0.441418|0.550554 (23.026331–23.576885)|
|MIXED candidate|2,022,143,600|40.023555|404.191|0.613272|0.399058 (33.489799–33.888856)|

Persistent 64B echo uses a 0.500s interval and 3.000s timeout. All actual attempts
succeed; there is no disconnect or censored failure in all six cells.
Counts differ because this probe's serial loaded requests can exceed the
nominal interval. Successful latency is not interpreted as 80 scheduled
successes when fewer actual attempts occurred.

| Mode/build | Echo success/attempts | Failures | p50 (s) | p95 (s) | Max (s) | Max success gap (s) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
|TCP control|48/48|0|0.860333|1.177138|1.311345|1.311367|
|TCP candidate|46/46|0|0.847044|1.211578|1.589731|1.589752|
|QUIC control|80/80|0|0.105185|0.164760|0.317630|0.678603|
|QUIC candidate|80/80|0|0.104852|0.214734|0.309604|0.673112|
|MIXED control|74/74|0|0.383713|0.706973|1.109810|1.109832|
|MIXED candidate|78/78|0|0.369798|0.652406|0.912637|1.050346|

The full cohort preserves a roughly404–427Mbps candidate pipeline, not a
one-frame stop/wait collapse. The result is nevertheless mixed/adverse:

- TCP: bulk rises411.785→420.820Mbps and max read gap is nearly unchanged.
  First body worsens0.447975→0.579549s; echo p95/max worsen
  1.177138/1.311345→1.211578/1.589731s.
- QUIC: bulk falls433.960→427.082Mbps; first body/max read gap are nearly
  unchanged. All80 echoes succeed, but p95 worsens0.164760→0.214734s.
- Mixed: max read gap improves0.550554→0.399058s and echo p95/max improve
  0.706973/1.109810→0.652406/0.912637s. First body worsens
  0.441418→0.613272s and bulk falls410.025→404.191Mbps. The final35–40s
  five-bin mean is412.397→353.247Mbps; retain this adverse late phase.
  Sampled server RSS/CPU also rise, as reported below.

These are single sequential realizations, not a causal attribution to a
particular lock or selection pass. Faster bulk cannot erase worse first/tail
service; lower echo latency alongside slightly less bulk is not free gain.

## Full raw bulk timing

Forty untrimmed one-second body-delivery bins per cell, Mbps. Index labels
are the starting bin index. All values, including early startup and bursts,
are preserved; values above 500 reflect buffered application delivery over a
bin, not a physical link capacity measurement.

```text
TCP control
0: 2.621,142.135,450.363,292.553,681.050,300.941,642.777,433.586,386.925,484.966
10: 490.733,276.825,430.441,438.303,412.619,474.539,423.983,454.121,414.099,457.972
20: 420.762,432.347,285.377,447.312,407.202,450.603,441.698,401.854,476.387,433.361
30: 422.384,456.933,275.812,449.839,442.499,419.164,428.978,427.069,458.265,401.601

QUIC control
0: 9.553,380.729,460.672,450.976,451.889,470.540,457.637,466.144,399.813,455.500
10: 451.949,465.034,457.704,460.763,392.273,450.699,459.085,468.551,451.406,451.266
20: 394.230,450.330,452.786,457.096,456.886,425.404,449.854,465.949,447.797,392.302
30: 447.429,461.294,455.158,466.919,442.916,455.958,429.138,445.900,462.994,385.551

MIXED control
0: 2.621,164.363,181.596,677.218,253.733,649.741,292.545,366.130,667.765,379.238
10: 496.377,408.403,386.095,497.444,444.445,441.413,466.290,421.450,196.516,302.836
20: 683.532,364.626,500.503,204.060,433.198,433.586,620.978,460.561,356.294,485.202
30: 353.108,509.372,415.303,395.423,427.170,403.029,331.401,418.121,481.074,428.359

TCP candidate
0: 2.621,253.755,348.652,338.690,641.244,309.814,426.400,617.929,415.443,353.330
10: 378.155,494.013,395.599,422.466,644.232,272.273,401.961,613.312,481.296,421.266
20: 464.747,339.110,336.469,454.881,479.104,382.563,525.861,442.984,450.908,399.373
30: 454.893,454.558,192.204,440.460,520.093,422.644,447.785,517.995,401.593,472.203

QUIC candidate
0: 9.554,370.339,447.602,442.174,433.911,444.994,452.920,421.151,402.365,434.317
10: 437.125,461.310,444.962,441.221,458.413,453.904,462.107,454.242,432.069,447.542
20: 450.596,442.075,447.394,375.672,441.268,440.035,466.359,444.125,442.806,382.949
30: 437.033,452.053,445.663,460.724,445.668,439.753,393.552,441.924,436.064,445.857

MIXED candidate
0: 1.573,90.729,256.281,737.630,396.029,480.557,284.546,628.226,412.947,431.676
10: 237.350,487.403,515.308,484.165,415.842,398.683,439.722,345.662,544.927,415.862
20: 431.824,421.481,400.751,521.596,392.328,413.867,372.718,522.953,357.958,459.391
30: 417.218,400.640,407.600,230.464,654.908,428.343,413.711,359.769,338.018,226.396
```

Full echo attempt start/end/latency/outcome series, probe fields, logs, native
socket observations and management snapshots remain in the five files of each
`./.tmp/reflection/results/{tcp,quic,mixed}-combined-down-response-claim-healthy-{control,candidate}-0908/`
directory. No replacement trial or selected-bin trimming is authorized.

## Sampled resource and wire context

RSS is KiB. CPU is the sampled `ps %CPU` **process-lifetime average**, not
instantaneous CPU, exclusive handler cost or cumulative CPU seconds. Above 100%
means more than one core's process-average usage. Peak and last are sampled
values; the final sample near 40s is not a post-load resource-reclamation proof.

| Mode/build | Client RSS peak/last (KiB) | Server RSS peak/last (KiB) | Client CPU max/last (%) | Server CPU max/last (%) |
| --- | ---: | ---: | ---: | ---: |
|TCP control|75660/52940|181512/174172|61.9/61.8|73.5/73.5|
|TCP candidate|64800/44364|185160/174612|53.8/53.8|118/118|
|QUIC control|36084/36084|367924/367924|99.5/99.1|154/154|
|QUIC candidate|36384/36384|313572/312388|96.2/95.7|154/154|
|MIXED control|107984/101144|284368/277652|110/110|158/158|
|MIXED candidate|97112/93392|331864/331864|114/113|196/195|

Router **HTB class first→last deltas**, first array downstream and second
upstream; do not also sum parent/child qdisc counters. Bytes include protocol,
feedback, repair and possible unmatched buffered work; they are not repair-only
overhead or exact useful-body bytes. Different completed work and finite sample
coverage prevent a normalized candidate cost claim.

| Mode/build | Down bytes/packets | Up bytes/packets | Max class backlog down/up (B) | Drops down/up |
| --- | ---: | ---: | ---: | ---: |
|TCP control|2,372,836,254/1,598,520|43,152,178/235,619|20,326,276/92,347|0/0|
|TCP candidate|2,389,773,096/1,609,394|46,055,301/237,075|16,698,114/52,416|0/0|
|QUIC control|2,300,902,736/1,540,106|36,157,626/335,836|10,785,186/39,486|0/0|
|QUIC candidate|2,267,267,991/1,517,589|35,476,018/329,082|10,121,850/37,838|0/0|
|MIXED control|2,431,904,839/1,743,238|266,206,629/629,922|48,489,044/514,818|0/0|
|MIXED candidate|2,402,540,186/1,754,963|133,503,852/776,953|25,882,920/183,298|0/0|

TCP's sampled server CPU maximum rises73.5→118%; mixed rises158→196%,
with mixed server RSS peak284368→331864KiB. These costs matter even though
bulk remains high. They do not identify exclusive prepared-claim CPU time.
Mixed upstream class bytes decrease, but packet count rises; neither is a
repair-only measure. The upstream traffic and downstream backlog are observable
costs, not proof of a specific ACK/repair/native defect. Native acceptance,
Product receipt/ordered delivery and probe body consumption remain separate
domains. This observer-free control has no exact per-range attribution.

| Mode/build | Client/server PID | Client sample Unix-ms range | Server sample Unix-ms range | Last runner sample (s) |
| --- | --- | --- | --- | ---: |
|TCP control|297843/303708|1788873606695→1788873646695|1788873606697→1788873646697|40.004756|
|TCP candidate|300684/306528|1788875759358→1788875799357|1788875759359→1788875799358|40.004456|
|QUIC control|298789/304647|1788873686037→1788873726037|1788873686040→1788873726039|40.008972|
|QUIC candidate|301632/307473|1788875820447→1788875860447|1788875820448→1788875860448|40.004459|
|MIXED control|299735/305585|1788873802567→1788873842567|1788873802568→1788873842568|40.007116|
|MIXED candidate|302581/308412|1788875907390→1788875947390|1788875907392→1788875947392|40.010490|

## Harsh ordinary mixed comparison

Both candidate cells are ordinary, diagnostics off, using the same frozen
response-claim-20260908 binary. The existing controls are the mixed cells from
the earlier [ordinary redesign cohort](REDESIGN_BASELINE_20260908.md), frozen
d999fea; they were not rerun alongside this candidate. Preserve that temporal/
single-realization limitation. All ten result directories and the20 build/
test/run logs are linked in the [raw archive](RESPONSE_PREPARED_SERVICE_20260908.raw.tar.gz).
All70 members pass gzip integrity, exact member count and byte-for-byte
comparison with their retained originals; no source tree or credentials are
included.

The existing combined profile retains500Mbps, forward70ms/20ms and return
30ms/5ms delay/jitter, five-second losses3/8/5/6/10/3/5/8% forward and
1/2/0.5/3/2/0.5/1/2% return, the10Mbps QoS cut at15–25s and UDP
blackhole at30–33s. Upload mirrors impairment direction. There is no100Mbps
target, altered queue, favorable repeat or diagnostic-binary speed comparison.

### Download: smaller bulk gap, worse startup and echo failure

| Build | Body bytes | Time (s) | Mbps | First body (s) | Max read gap (s), interval |
| --- | ---: | ---: | ---: | ---: | --- |
|control|187,819,168|40.377782|37.212|0.545778|7.418422 (29.956625–37.375048)|
|candidate|350,875,360|40.000068|70.175|1.054610|3.719639 (15.645592–19.365230)|

Both HTTP200 bulk observations are deliberately duration-limited partial8GiB
requests (0 complete/1 partial), bulk_status=ok but overall status=loss.
Candidate's higher bulk rate does not erase these adverse interactive outcomes:

- Control:42/75 echo successes, then one timeout at21.359509–24.362199s
  and32 unavailable-after-disconnect slots. Successful p50/p95/max:
  0.118890/0.290358/0.712929s.
- Candidate:30/75 successes, then one timeout at15.474884–18.476885s
  and44 unavailable-after-disconnect slots. Successful p50/p95/max:
  0.140097/0.592507/0.861896s. The failure happens earlier, during QoS.
- First body worsens by0.508832s. The7.418422→3.719639s maximum-gap
  improvement is not a full experience or completion win.

For DOWN, S is server locally read response bytes; T is client locally written
response bytes, both node-wide reliable I/O including HTTP framing and echo,
not exact C/F or probe body bytes. Candidate S312073776/T244964848 stays
flat over sampled16–19s: server Unix1788876392890→1788876395890 and
client1788876392888→1788876395888. This overlaps the actual probe's
15.645592–19.365230s maximum read gap. Later S389037040 stays flat
32–38s while T continues advancing; source flat alone is not a delivery stall.
Without exact range joins these observations do not identify a remaining
preclaim, Native, receiver-ordering or shared-cut cause.

### Upload: exact completion, but worse confirmation gap and return residence

| Build | Confirmed = locally accepted bytes | Total time (s) | Mbps | First confirmation/write (s) | Max confirmation/write gap (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
|control|432,078,848 = 432,078,848|51.710359|66.846|0.460686/0.133158|4.004233/3.730152|
|candidate|419,823,616 = 419,823,616|49.378940|68.017|0.419667/0.119936|5.164575/3.303759|

Both have complete=true, exact valid ACK accounting and no probe errors.
Candidate's shorter total also processes fewer bytes; it is not fixed-work
acceleration. Time beyond the nominal40s load is11.710359→9.378940s,
not an exact EOF-to-final-confirmation drain measurement. Candidate source
consumption first reaches its final total in the45.189s sample, not at an
observed exact claim/EOF event. This workload has no separate echo probe.

For UP, S=client local source consumed, T=server ordered target socket writes,
Rs=server reply bytes read, Rc=client reply bytes locally written. S is not C,
and Rc is not mux F. Two distinct candidate phases remain:

- QoS: T212625711 and Rs=Rc532 remain flat18–22s (server
  Unix1788876477282→1788876481282). This is a real sampled forward-service
  hold; at20–22s S279734575 is exactly64MiB ahead of T.
- Tail: Rc remains966 at41.189–45.189s (client
  Unix1788876500280→1788876504280), while T346028343→411260663
  increases65,232,320B and Rs966→1190. Useful reply work has already
  entered the server relay but has not reached the client's local write.
  This locates a return-stage boundary, not its actor/carrier cause.
- T reaches all419823616B by the47.189s sample; Rs1287 stays constant
  through49.189s, while Rc moves1050→1120 and is still behind at that
  last sample. The probe subsequently completes at49.378940s.

The scalar5.164575s maximum confirmation gap is not an exact interval join:
do not assign all of it to either of these sampled holds or call every zero
confirmation bin a forward outage. The already-observed UP receive boundary
is not shown fixed.

### Full untrimmed harsh timing and sampled costs

Raw one-second Mbps bins: DOWN40/40, UP52/50. These are ordered body bytes
or target-confirmation batches as appropriate, not physical wire rate.
Full echo attempts, including every failure, remain in probe.json.

```text
DOWN control
0: 0.990,23.211,26.214,17.826,8.389,6.816,38.390,1.980,2.097,1.690
10: 1.980,2.097,2.621,1.573,2.214,3.029,3.146,2.738,4.078,3.263
20: 2.331,536.850,0.021,0.117,0.000,103.439,263.193,55.959,197.849,187.931
30: 0.000,0.000,0.000,0.000,0.000,0.000,0.000,0.524,0.000,0.000

DOWN candidate
0: 0.000,10.553,8.913,4.835,540.014,126.992,167.152,139.032,110.100,6.099
10: 6.076,268.864,14.746,15.900,3.029,537.395,0.000,0.000,0.000,1.818
20: 0.000,0.000,4.578,0.000,7.864,50.900,0.000,354.292,119.994,0.117
30: 2.605,73.537,0.000,0.524,1.241,1.477,52.953,44.993,12.871,117.441

UP control
0: 4.676,19.998,314.049,70.543,112.294,101.328,11.534,3.670,373.153,58.336
10: 23.261,58.004,106.643,186.143,157.578,16.991,0.000,0.524,0.000,0.000
20: 0.000,8.389,0.000,0.000,10.367,463.447,19.614,0.000,16.157,0.000
30: 0.000,0.000,66.489,281.190,51.863,85.817,4.191,1.669,77.325,3.434
40: 9.956,102.151,24.021,47.375,10.461,96.681,0.000,11.106,15.869,118.931
50: 176.871,134.535

UP candidate
0: 3.144,6.000,296.513,70.018,40.038,51.425,101.623,0.000,49.327,76.310
10: 109.812,0.000,241.409,138.464,460.690,0.000,0.000,1.477,0.000,0.000
20: 0.000,0.000,54.851,0.000,0.000,27.598,31.694,77.065,0.000,0.000
30: 238.587,0.000,158.589,81.888,130.195,11.202,57.576,34.079,16.585,35.511
40: 70.779,0.000,0.000,0.000,0.000,103.539,123.678,117.765,84.410,256.748
```

Same RSS/ps-CPU definitions and non-additivity limitations as the healthy
section. Rows/last sample(s) make unequal observation coverage explicit.

| Direction/build | Client RSS peak/last (KiB) | Server RSS peak/last (KiB) | Client CPU max/last (%) | Server CPU max/last (%) | Rows/last sample (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
|down control|130708/119292|284156/284156|29.1/20.6|36.3/18.8|41/40.180551|
|down candidate|156016/145772|334744/334744|41.9/22.6|61.3/34.4|40/39.114656|
|up control|370020/369040|156288/143544|115/61.4|46.3/17.2|52/51.171009|
|up candidate|781556/763860|127688/124248|121/98.7|44.8/20.6|50/49.189856|

UP candidate client RSS peaks at781556KiB versus370020KiB control, with
less completed work; this is materially adverse sampled resource evidence,
not proof of a leak or one handler's cost. DOWN candidate also increases both
endpoint RSS peaks. No post-load reclamation claim follows from the last row.

Physical router class deltas are down/up regardless of workload direction;
the mirrored upload's load is in the second column. Socket Recv-Q maxima
aggregate carrier TCP sockets only, not exact Product ranges or useful replies.

| Direction/build | Down class B/packets/drops | Up class B/packets/drops | TCP Recv-Q peak client/server (B) |
| --- | ---: | ---: | ---: |
|down control|321,259,082/263,890/3,535|16,746,580/80,425/704|0/66,662|
|down candidate|507,612,867/428,050/7,985|33,451,043/143,302/1,605|0/0|
|up control|23,278,513/117,197/1,574|569,193,602/461,808/7,552|188,083/0|
|up candidate|28,699,235/153,551/1,912|549,867,678/466,343/8,492|855,871/105,190|

These wire and memory totals cover different work/sample lengths and do not
justify normalized efficiency or repair-only amplification claims. The larger
UP client Recv-Q is context consistent with local consumption pressure, not
proof that its contents are the particular held useful reply.

Raw candidate directories are
`./.tmp/reflection/results/mixed-combined-{down,up}-response-claim-combined-candidate-0908/`;
controls end `redesign-baseline-0908`. All cells ended before this report was
finalized; root owns runs and archive verification. No further trial is part
of this evidence section.

## Practical disposition

The healthy cohort preserves high-BDP bulk service but is not a clean
non-regression: startup, some loaded latency, late mixed service and sampled
server costs worsen. Harsh DOWN improves bulk/max-gap while starting later
and losing more echo slots; harsh UP completes less work somewhat earlier
but worsens maximum confirmation gap, retains already-read return residence
and more than doubles sampled peak client RSS. These are mixed/adverse
practical outcomes, not performance acceptance.

The 692 focused controls establish the implemented ownership/lifecycle contract,
not the forecasted sustained gain. The ordinary snapshots cannot assign the
remaining holds to exact ranges or one local mechanism. Do not waive healthy
adverse costs because one harsh bulk average rises, normalize unequal work
into a fixed-work speedup, or compensate with gain/window/queue changes.
No release, push or claim that the UP receive/already-native boundaries are
fixed follows from this report.

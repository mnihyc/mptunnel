# Initial owner: bounded causal ablation

2026-09-08 04:11 +08:00. Diagnosis only, not a candidate product policy or
performance acceptance. Source checkpoint765683b, following evidence93370dc.

## Question and intervention

The preceding exact membership capture placed12,189,643B on the only initial
TCP attachment in25ms. QUIC was already opening, attached91ms and used93ms.
That refutes an ignored ready QUIC path; it does not show how much of the
whole-profile delay would disappear with another initial owner.

For an ordered stream, fast suffix delivery cannot bypass an unreceived
prefix. Under a conditional constant4Mbps service,12.19MB costs about24.4s;
under400Mbps it costs about0.244s, before propagation/recovery. These are
illustrative service assumptions, not rates inferred from Product debt or
claims about what a counterfactual native path would deliver.

The intervention moves the first existing UDP candidate to the front of the
local opening-attempt vector, after its ordinary rank. It does not alter the
frozen plan, candidate ordinals, remaining candidate order, resource windows,
native controllers, qualification or subsequent scheduler. Both cells retain
three TCP carriers plus QUIC. The server executable stays unchanged. The
ten-line overlay is [archived](INITIAL_OWNER_ABLATION_20260908.patch), guarded
by the lab-diagnostics compile feature and fully reversed after binary freeze.
Experimental binary: `./.tmp/reflection/bin/initial-quic-owner-ablation-20260908/mptunnel`;
control/server: `./.tmp/reflection/bin/ack-atoms-20260908/mptunnel`.
Build2m03s. Independent diff review passes; this is not production acceptance.

Config path order would affect later ties; rate hints and backup policies
would also persist. None isolates this question. Existing retry handling stays,
including last-alternative timeout semantics. A failed QUIC attempt followed
by TCP success would invalidate the intended contrast.

## Frozen comparison and limitations

Use the unchanged mixed combined upload probe/profile in CURRENT_CLOSURE_PLAN:
500Mbps, asymmetric70/20ms and30/5ms delay/jitter, changing loss with upload
mean6%,10Mbps upload QoS15--25s and UDP outage30--33s. One40s source workload;
retain85s runner censoring and full confirmation/drain results. These are
independent unseeded loss realizations, not packet-identical replay.

Only existing setup/open/additional-attachment events are enabled in both
cells. No bulk or repair trace. Keep initial handshake and prewarming evidence:
the one-second runner startup pause is not a native-readiness barrier. Native
connection age/history, attachment order, qualification evolution and subsequent
traffic are not separable constants when the first owner changes.

Compare first/max gaps, all raw confirmation bins, complete byte equality,
early source/target/instance debt, post-load drain and directional wire/RSS/CPU.
Success only in the early interval supports that mechanism, not a permanent
QUIC-first preference. Persistent later stalls remain failures. No third valid
trial seeking a better mean and no automatic policy/threshold change.

## Excluded environment attempt

`mixed-combined-up-initial-owner-control-0908` is not a mixed performance
observation. Client log13 records QUIC certificate rejection: the owned pinned
test certificate expired2026-09-07 19:49:30UTC, before this attempt. The runner
reaches its existing guard and products/probe terminate. Retain its raw output
as an invalid environment cell, not as an MPP regression or control result.

The certificate alone was renewed with the same key, CN/SAN localhost and
critical CA:FALSE. New validity2026-09-07 20:09:45UTC through2026-10-07
20:09:45UTC covers this continuing lab task. Both valid cells use it; TLS
verification remains enabled. The old public certificate is retained locally.
Prior19:30UTC membership evidence preceded expiry and is unaffected.

Valid pair labels are `mixed-combined-up-initial-owner-valid-{control,quic}-0908`.
Validity and actual QUIC establishment are checked, not assumed from the file.
Results follow after both cells finish; no run-in-progress numbers are acceptance.

## Completed valid pair — 2026-09-08

Raw evidence: [INITIAL_OWNER_ABLATION_20260908.raw.tar.gz](INITIAL_OWNER_ABLATION_20260908.raw.tar.gz),
containing the excluded certificate-expired attempt and both valid result
directories. Only `initial-owner-valid-{control,quic}-0908` contributes to the
comparison below. All probes and products have finished; no third valid run.

The QUIC-first intervention completes more work in less time and removes the
control's prolonged initial forward-target deficit. It does not eliminate
multi-second confirmation gaps, and first confirmation is worse. This supports
initial-owner importance within this observed execution, not a permanent
QUIC-first policy, a quantified causal speedup, or performance acceptance.

### Intervention and physical-readiness verification

Both cells already have three TCP carriers and one QUIC carrier active in
their first cached management snapshot, with zero active flows and zero
OriginalData debt. These snapshots precede the first logical open attempt:

| Setup observation | Control | QUIC-first intervention |
| --- | ---: | ---: |
| First client/server snapshot Unix ms | 1788811787340 / 1788811787342 | 1788811914304 / 1788811914306 |
| Client snapshot precedes first logical open | 51 ms | 42 ms |
| First logical attempt, Unix ms | 1788811787391, TCP index 0 | 1788811914346, UDP index 0 |
| First logical success, Unix ms; measured open elapsed | 1788811787464; 73.181 ms | 1788811914436; 90.466 ms |
| Additional QUIC attachment, Unix ms | 1788811787569 | Already initial owner |
| Additional TCP attachments, Unix ms | 1788811787597 / 1788811787604 | 1788811914550 / 1788811914558 / 1788811914558 |
| Initial client QUIC physical instance; native ACKed bytes | 1; 7,507 B | 2; 7,480 B |
| Initial client QUIC sampled RTT | 88.739 ms | 125.687 ms |

Client log lines 1--14 in each valid directory establish successful first
selection and all additional attachments; neither first open falls back or
times out. Client and server first snapshots each report four active physical
paths. There is therefore no missing-QUIC/prewarm failure analogous to the
excluded certificate cell. Physical readiness is not identical native service:
RTT, loss history and native connection state differ, and logical opening costs
remain included. A cached snapshot's generation time, collector `elapsed`,
sequential socket/process collection and probe clocks are not interchangeable.

Client management identities remain stable across all samples. Control has
TCP wire path/physical instance pairs 0/2, 1/4, 2/3 and QUIC 0/1; intervention
has TCP 0/3, 1/1, 2/4 and QUIC 0/2. Wire path IDs are not the setup log's
configured path indices. Setup-only logging does not reproduce exact bulk
offset assignments or every request-local qualification transition.

### Completion and all confirmation bins

| Probe observation | Control | QUIC-first intervention |
| --- | ---: | ---: |
| Complete / exact accounting / ACK accounting valid | true / true / true | true / true / true |
| Complete / failed streams; probe errors | 1 / 0; none | 1 / 0; none |
| Confirmed = locally accepted bytes | 235,470,848 B | 374,734,848 B |
| Probe elapsed | 53.153263 s | 49.660557 s |
| Whole-transfer confirmed goodput | 35.440 Mbps | 60.367 Mbps |
| First sink confirmation | 0.417902 s | 1.551972 s |
| Maximum confirmation-progress gap | 5.697254 s | 5.256939 s |
| First local write | 0.099557 s | 0.105823 s |
| Maximum local-write gap | 9.670569 s | 4.932933 s |
| Completion beyond planned 40-second load | 13.153263 s | 9.660557 s |

Both report `status=ok` and `upload_accounting_source=target_sink_ack`.
Intervention completes 139,264,000 more bytes in 3.492706 fewer probe seconds.
Runner elapsed is separately 54.083038/50.210326 s; it is not the denominator
of the probe goodput. Elapsed minus planned load is not exact last-write-to-
confirmation drain: the probe does not emit its final local-write timestamp.

All `interval_goodput_raw_mbps` entries follow, with no trimming. These are
rounded confirmation-progress bins, not native wire rate or exact delivery
events. Control's 537.276-Mbps bin is a buffered confirmation release.

```text
Control
0–9:   6.29, 23.069, 13.748, 17.185, 6.291, 3.766, 3.146, 3.574, 2.621, 2.621
10–19: 3.146, 2.834, 1.98, 3.67, 3.263, 537.276, 0.0, 0.0, 0.0, 0.0
20–29: 3.439, 0.0, 0.0, 0.0, 0.0, 8.236, 39.394, 87.055, 46.137, 97.015
30–39: 0.0, 0.0, 0.0, 0.0, 160.795, 0.0, 0.0, 0.0, 0.0, 0.0
40–49: 129.307, 0.0, 35.319, 0.0, 47.282, 28.12, 0.0, 94.564, 0.0, 122.299
50–53: 0.0, 0.0, 203.712, 146.613

QUIC-first intervention
0–9:   0.0, 2.289, 34.603, 11.534, 0.0, 0.0, 130.836, 0.0, 0.0, 129.639
10–19: 0.0, 35.511, 136.027, 311.951, 5.864, 54.29, 1.285, 0.0, 0.192, 0.0
20–29: 0.0, 0.0, 0.0, 20.019, 0.0, 236.262, 165.247, 156.718, 76.995, 0.0
30–39: 208.838, 0.0, 0.0, 0.0, 244.414, 83.598, 0.117, 0.0, 205.109, 4.386
40–49: 0.0, 33.887, 0.0, 0.0, 0.0, 0.0, 0.0, 76.974, 172.151, 459.144
```

Approximate confirmed MB from sums of those rounded bins:

| Probe-bin interval | Control | Intervention |
| --- | ---: | ---: |
| [0,15), before nominal QoS | 12.150500 | 99.781750 |
| [15,25), nominal QoS | 67.589375 | 9.473250 |
| [25,30) | 34.729625 | 79.402750 |
| [30,33), nominal UDP blackout | 0 | 26.104750 |
| [33,40) | 20.099375 | 67.203000 |
| [40,end), unequal completion windows | 100.902000 | 92.769500 |

The control's larger QoS-bin total is chiefly the old buffered prefix/suffix
release, not greater service on the 10-Mbps class. Actual collector transitions
are QoS start/end 15.001616/25.002824 s control and 15.001682/25.002675 s
intervention; UDP blackout start/end 30.003286/33.052816 and
30.003212/33.194087 s respectively. Collector operations take time; these are
not claims that every probe bin boundary exactly matches a shaping operation.

### Early ordered service and remaining feedback stalls

All 54 control and 50 intervention service rows were inspected. Here S is
client local-source bytes consumed and T is server ordered target-socket bytes
written. S is not local probe acceptance; T is not the sink-confirmation arrival
at the client. Debt is un-DataACKed OriginalData, not necessarily undelivered
payload. Sample times are collector `elapsed`, rounded to six decimals.

| Cell / sample s | S B | T B | Leading TCP wire 1 debt B | QUIC original debt B |
| --- | ---: | ---: | ---: | ---: |
| Control 4.000474 | 74,710,881 | 7,667,553 | 4,587,520 | 214,144 |
| Control 10.001110 | 77,361,521 | 10,288,993 | 1,951,480 | 14,600 |
| Control 14.001500 | 78,854,249 | 11,796,321 | 458,752 | 65,536 |
| Control 15.001616 | 79,262,065 | 79,247,465 | 50,936 | 14,600 |
| Control 16.001739 | 146,418,929 | 79,310,065 | 0 | 67,108,864 |
| Intervention 1.000171 | 68,413,995 | 1,763,883 | 369,375 | 65,834,197 |
| Intervention 2.000283 | 70,278,539 | 31,796,907 | 840,180 | 61,038,069 |
| Intervention 4.000496 | 82,914,987 | 81,312,587 | 13,083,412 | 51,195,669 |
| Intervention 6.000715 | 88,366,475 | 88,354,475 | 18,433,364 | 42,455,381 |
| Intervention 9.001040 | 88,366,475 | 88,354,475 | 18,367,828 | 29,217,109 |
| Intervention 14.001576 | 166,026,635 | 99,179,915 | 11,218,656 | 45,958,272 |
| Intervention 16.001780 | 173,825,419 | 106,716,555 | 3,646,016 | 31,999,104 |

The leading TCP column is physical instance 4 in control and instance 1 in
intervention; these are different owners, not one persistent identity across
runs. Control's other TCP original debts are zero throughout 4--16 s. From
4.000474 to 14.001500 s, its target advances exactly 4,128,768 B as that leading
TCP debt falls by 4,128,768 B. Source-target separation remains close to 64 MiB
until the large ordered release around 15 s. QUIC native ACKs meanwhile advance
61,788,434 -> 67,446,348 B, so this is not total alternative native silence.

Intervention starts with almost 64 MiB of original QUIC debt and reaches
81.313 MB at the target by 4 s, versus control's 7.668 MB. QUIC native ACKs
advance 3,233,049 -> 70,693,605 B from the 1 to 4 s samples. Its large remaining
QUIC Product debt at 4--9 s cannot all be counted as undelivered upload: S and T
are only 12,000 B apart throughout the 6--9 s plateau. Subsequent TCP placement,
feedback debt and renewed 64-MiB separation still occur; changing the first
owner does not freeze the rest of the execution into an equivalent control.

Return-stage evidence prevents misclassifying every zero confirmation bin as
a forward stop. Intervention service rows 5--7, server Unix
1788811918305--1788811920305, show T advancing 81,312,587 -> 88,354,475 B and
sink-reply bytes read by the server advancing 171 -> 262 B, while client
returned bytes stay 67 B (client Unix 1788811918303--1788811920303). Similarly,
rows 44--48 at 43.209530--47.209955 s show T advancing
365,261,611 -> 374,734,848 B and server reply reads 1,166 -> 1,347 B while
client returned bytes remain 942 B. This localizes held feedback between
server reply read and client local write, not to an exact carrier or actor.

Real forward plateaus also remain: intervention T is 109,242,923 B and S is
176,351,787 B at 22.002373--23.002475 s, exactly 64 MiB apart. QUIC native ACKs
advance 139,988,474 -> 141,644,474 B while TCP debt remains 1,197,184 B.
During/after UDP blackout, intervention T is flat at 244,068,939 B over
31.193868--34.208522 s; control T is flat at 155,135,217 B over
31.052592--37.081065 s. No exact missing-range/repair-receipt trace was collected
in this setup-only pair, so these are stage bounds, not winning-copy claims.

Final source consumption is first observed at 47.082192 s control and
46.209849 s intervention; target writes first equal the complete probe total
at 51.082653/47.209955 s. Last sampled original ACK debt is still
10,765,935/47,268,117 B respectively. Exact later probe completion does not
turn those last snapshots into zero-debt or post-quiet reclamation evidence.

### Directional wire and sampled process cost

Control samples span 0.000056811--53.082881882 s; intervention spans
0.000060410--49.210174781 s. Neither has management collection errors. One
continuous process identity is observed per endpoint: client/server PIDs
243393/249570 control and 244653/250819 intervention. Router class counters
are monotone. Count eth0 return and eth1 upload class `1:10` once, not nested
qdisc counters again.

| Sampled cost | Control | Intervention |
| --- | ---: | ---: |
| Upload class bytes first -> last | 770,153 -> 324,319,643 B | 658,975 -> 562,686,081 B |
| Upload class-byte delta | 323,549,490 B | 562,027,106 B |
| Return class bytes first -> last | 38,612 -> 11,630,785 B | 34,697 -> 20,712,632 B |
| Return class-byte delta | 11,592,173 B | 20,677,935 B |
| Upload / return class-drop delta | 3,536 / 734 | 6,786 / 1,177 |
| Client RSS first / peak / final | 57,788 / 869,088 / 704,876 KiB | 168,276 / 796,116 / 779,156 KiB |
| Server RSS first / peak / final | 30,616 / 142,132 / 136,764 KiB | 30,564 / 106,728 / 103,140 KiB |
| Final lifetime-average CPU, client / server | 61.2% / 8.8% | 83.1% / 16.4% |

More intervention bytes and unequal observation windows prevent interpreting
absolute cost as equal-work efficiency. Class bytes include framing, control,
native retransmission and Product copies, not repair-only amplification. CPU
is `ps` lifetime average, not interval utilization. RSS is sampled after the
probe has launched, including the first row; that row is not a pre-load RSS
baseline. No post-quiet reclamation or concurrent loaded-latency task exists.

Intervention client log 15 records `tcp.session_command_failed` at
2026-09-07 20:12:44.515 UTC, about 30.169 s after its first open attempt:
`path_index=1 path_instance_id=4`, reliable path session closed. Retain this
execution difference. All later management rows still report four active
physical paths with the same identities; that does not prove every logical
attachment remained usable. Setup logs contain no subsequent attachment event.
Control's two server `H3_NO_ERROR` warnings occur during final teardown after
the last management sample. Neither warning is silently treated as equivalent
to a healthy, identical return path across the pair.

**Disposition:** the planned falsifier of a similarly prolonged initial
forward-target deficit after successful QUIC first-owner selection was not
observed. The large early target-service contrast supports initial placement
as an important contributor to that deficit. The one unseeded ablation does
not isolate an exact saved duration, equal native history, or a universal
transport preference. Later native/ordered and return-feedback stalls persist,
including a worse first confirmation and a 5.257-second maximum confirmation
gap. Initial-owner change is not sufficient for the full practical objective.
Keep all ordinary failures and the rejected copy-ahead decision intact; no
production policy, new fix, third valid run or release promotion follows.

# Response retained-frontier: ordinary mixed upload

2026-09-08 +08:00. One completed, unseeded control/candidate pair. Component
correction remains proven; practical promotion is stopped because maximum
confirmation silence worsens. No third ordinary run or full-matrix acceptance.

Raw evidence:
[RESPONSE_RETAINED_ORDINARY_20260908.raw.tar.gz](RESPONSE_RETAINED_ORDINARY_20260908.raw.tar.gz).
It contains both `mixed-combined-up-response-retained-{control,candidate}-0908`
directories, each with `probe.json`, `service.jsonl`, endpoint logs and probe
stderr. Live copies are under `./.tmp/reflection/results/`. Line numbers below
refer to that cell's original `service.jsonl`, not a newly sampled series.

## Frozen comparison and disposition

Control is checkpoint 765683b, `./.tmp/reflection/bin/ack-atoms-20260908/mptunnel`;
candidate 953a54f is `./.tmp/reflection/bin/response-retained-20260908/mptunnel`.
Both endpoints use their cell's frozen binary. The candidate contains only
the declared response-local retained-ownership recovery correction, including
per-assignment deadlines and preserved Active/FinalDrain publication forms.
The 333 focused checks (88 server, 155 response-stream, 90 response-sender) are
component GREEN, not ordinary acceptance. No sampler, initial-owner intervention,
native-controller setting, runtime tracing or concurrent build enters this pair.

The existing `run.py mixed combined up` path is routed, mirrored and management
sampled: three TCP carriers plus QUIC, 500 Mbps configured link rate, upload
70/20 ms delay/jitter and return 30/5 ms. Five-second upload loss epochs are
[3,8,5,6,10,3,5,8]%; return [1,2,.5,3,2,.5,1,2]%. Upload QoS is 10 Mbps during
15–25 s, with UDP blackhole 30–33 s. One 40 s source workload, 85 s runner guard and
90 s probe completion boundary remain unchanged. The probe's 50 s completion
timeout is additional to load duration, not a 50 s whole-run censoring boundary.

| Completion / timing | Control | Candidate |
| --- | ---: | ---: |
| Complete; exact ACK accounting | true; true | true; true |
| Confirmed = locally accepted = final bytes | 234,815,488 | 259,457,024 |
| Complete / failed streams | 1 / 0 | 1 / 0 |
| Whole probe time (s) | 52.755080 | 47.594305 |
| Whole confirmed goodput (Mbps) | 35.608 | 43.611 |
| First positive confirmation (s) | 0.463283 | 0.406435 |
| Maximum positive-confirmation gap (s) | 4.799931 | 6.080193 |
| First local write (s) | 0.127674 | 0.108190 |
| Maximum local-write gap (s) | 4.655458 | 3.440146 |
| Elapsed beyond planned 40 s load boundary (s) | 12.755080 | 7.594305 |

Both probes report valid exact `target_sink_ack` accounting, exit 0 and no probe
errors. The candidate completes 24,641,536 B more work in 5.160775 s less time, but
its maximum confirmation gap worsens by 1.280262 s. Higher average, earlier
first confirmation and smaller local-write gap do not waive that timing result.
Independent unseeded loss realizations and unequal work prevent assigning
these differences entirely to the response change. The practical finding is
mixed/adverse timing, not a causal regression estimate or a blanket benefit.

The final row is time beyond the planned load boundary, **not exact drain from
the last successful source write**: that timestamp is not retained in the
probe summary. Whole time includes setup and final confirmation. Recovery-gap
fields are 0 because failover is disabled; they are not proof of stall-free
recovery. This upload probe has no concurrent echo/interactive-latency series.

## Full observed confirmation series

These are every `interval_goodput_raw_mbps` entry, without discarding the
first/last three bins. Index i is [i,i+1)s on the probe's own monotonic clock.
The probe assigns positive cumulative sink-ACK deltas at **client observation**,
not at server target write or physical wire transmission. A delayed ACK flush
can therefore produce a bin above the 500 Mbps configured link rate. Values
are rounded to three decimals by the producer. A dash is absent data after
that cell's series, not a reconstructed zero.

| One-second bin | Control Mbps | Candidate Mbps |
| --- | ---: | ---: |
| 0 | 1.049 | 2.621 |
| 1 | 13.106 | 7.338 |
| 2 | 9.437 | 7.864 |
| 3 | 8.913 | 7.340 |
| 4 | 7.436 | 9.961 |
| 5 | 7.768 | 5.767 |
| 6 | 1.573 | 5.767 |
| 7 | 5.339 | 3.146 |
| 8 | 2.642 | 4.194 |
| 9 | 1.980 | 4.194 |
| 10 | 2.310 | 3.670 |
| 11 | 3.982 | 4.719 |
| 12 | 2.834 | 4.290 |
| 13 | 2.525 | 566.423 |
| 14 | 612.443 | 143.035 |
| 15 | 64.392 | 24.117 |
| 16 | 0.000 | 0.000 |
| 17 | 0.000 | 76.214 |
| 18 | 3.766 | 0.000 |
| 19 | 11.438 | 0.000 |
| 20 | 0.000 | 0.000 |
| 21 | 0.000 | 0.000 |
| 22 | 0.000 | 0.000 |
| 23 | 1.477 | 16.393 |
| 24 | 0.000 | 0.000 |
| 25 | 28.932 | 17.398 |
| 26 | 56.099 | 0.000 |
| 27 | 66.585 | 0.000 |
| 28 | 4.386 | 0.000 |
| 29 | 0.000 | 132.217 |
| 30 | 0.000 | 63.535 |
| 31 | 0.000 | 46.557 |
| 32 | 96.897 | 0.000 |
| 33 | 0.000 | 0.000 |
| 34 | 0.000 | 96.241 |
| 35 | 0.000 | 0.000 |
| 36 | 86.936 | 0.000 |
| 37 | 30.076 | 64.628 |
| 38 | 0.000 | 0.000 |
| 39 | 35.607 | 66.304 |
| 40 | 0.000 | 0.000 |
| 41 | 27.979 | 0.000 |
| 42 | 0.000 | 0.000 |
| 43 | 0.000 | 0.000 |
| 44 | 0.000 | 0.000 |
| 45 | 99.563 | 0.000 |
| 46 | 1.090 | 235.162 |
| 47 | 1.864 | 456.561 |
| 48 | 1.731 | — |
| 49 | 0.234 | — |
| 50 | 1.726 | — |
| 51 | 2.697 | — |
| 52 | 571.711 | — |

## Stage accounting and actual phase times

The independent counters are:

- S: client `traffic.reliable.io.to_peer_bytes`, successful local-source reads.
- T: server `traffic.reliable.io.from_peer_bytes`, successful ordered writes
  to the target socket, not raw Product receipt/reassembly or sink confirmation.
- Rs: server `traffic.reliable.io.to_peer_bytes`, successful reads of the
  target's reply stream into the relay.
- Rc: client `traffic.reliable.io.from_peer_bytes`, successful writes of
  reconstructed replies to the local probe socket, not proof of application parsing.

These meanings follow `ObservedProductIo::poll_read/poll_write` in
`src/runtime/telemetry.rs` and the client-local/server-target wrappers in
`src/runtime/relay/{control,server}.rs`. S is not the probe's local-acceptance
counter; S−T spans several buffers and sender/receiver stages. The source and
reply directions must not be collapsed into one throughput series.

Rows carry runner elapsed time, measured before sequential collection.
Client/server management `generated_unix_ms` are separate cached snapshot
times. Thus the phase labels and probe bins are not a common timing barrier.
Actual first sampled phase transitions are:

| Phase | Control elapsed (s) | Candidate elapsed (s) |
| --- | ---: | ---: |
| epoch 0 | 0.000055 | 0.000141 |
| epoch 1 | 5.000640 | 5.000629 |
| epoch 2 | 10.001200 | 10.001398 |
| epoch 3; 10 Mbps | 15.001735 | 15.001877 |
| epoch 4; 10 Mbps | 20.002285 | 20.002395 |
| epoch 5; 500 Mbps restored | 25.002790 | 25.003012 |
| epoch 6; UDP blocked | 30.003305 | 30.003562 |
| UDP unblocked | 33.086465 | 33.079305 |
| epoch 7 | 35.124590 | 35.101018 |

Selected exact byte snapshots retain both forward and return stages:

| Cell | Line | Elapsed (s) | S bytes | T bytes | Rs bytes | Rc bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| control | 2 | 1.000167 | 67,436,491 | 327,627 | 21 | 21 |
| control | 14 | 13.001529 | 75,657,585 | 8,548,721 | 571 | 571 |
| control | 15 | 14.001631 | 76,036,201 | 8,978,273 | 607 | 607 |
| control | 16 | 15.001735 | 155,868,449 | 89,516,481 | 659 | 659 |
| control | 21 | 20.002285 | 162,475,585 | 95,366,721 | 711 | 711 |
| control | 30 | 29.003197 | 186,330,689 | 162,278,977 | 877 | 779 |
| control | 45 | 44.125723 | 229,161,697 | 162,278,977 | 877 | 849 |
| control | 46 | 45.125823 | 229,387,841 | 162,293,577 | 891 | 849 |
| control | 53 | 52.126550 | 230,463,017 | 163,354,153 | 1171 | 1171 |
| candidate | 2 | 1.000240 | 57,147,127 | 393,057 | 32 | 32 |
| candidate | 13 | 12.001595 | 75,446,271 | 8,337,407 | 534 | 534 |
| candidate | 14 | 13.001696 | 76,021,495 | 8,912,631 | 582 | 582 |
| candidate | 15 | 14.001794 | 153,485,047 | 86,376,183 | 633 | 633 |
| candidate | 20 | 19.002287 | 179,235,159 | 112,126,295 | 728 | 714 |
| candidate | 23 | 22.002700 | 179,235,159 | 114,294,519 | 742 | 714 |
| candidate | 31 | 30.003562 | 204,233,911 | 201,874,615 | 910 | 770 |
| candidate | 34 | 33.079305 | 217,365,111 | 202,386,903 | 924 | 798 |
| candidate | 41 | 40.101511 | 241,387,287 | 240,683,927 | 1218 | 840 |
| candidate | 47 | 46.102124 | 259,296,151 | 254,488,023 | 1526 | 840 |
| candidate | 48 | 47.102227 | 259,457,024 | 259,457,024 | 1553 | 924 |

### Early forward limitation, not only confirmation delay

Both cells initially consume about 64 MiB ahead of ordered target writes, while
Rs and Rc mostly track each other. At control 13.001529 s and
candidate 13.001696 s, each S−T is exactly 67,108,864 B. The corresponding large
target burst is sampled one second earlier in the candidate (13–14 s versus
14–15 s), near the end of the initial 500 Mbps phase. This is a timing difference, not
evidence that response repair caused the earlier forward release.

The client native/OriginalData views support a surviving early TCP obligation
beside fast QUIC suffix progress, but do not locate the blocking byte.
At control 13.001529 s TCP path 1/instance 1 has 509,688 B Product debt,
370,730 B reported native queue and 99,912 B native flight, while QUIC
path 0/instance 3 has 68,037,081 B native ACK progress and 29,200 B Product debt.
At candidate 13.001696 s TCP path 1/instance 3 has 131,072 B Product debt,
zero reported queue and 86,880 B native flight; QUIC path 0/instance 1 has
68,682,972 B native ACK progress and 65,536 B Product debt. These are distinct
carrier-native and Product byte domains, not a per-range ownership trace.
There are no setup events proving the exact first logical owner in this
ordinary pair; four paths listed active in the first snapshot do not
establish their readiness at a preceding assignment.

### Later forward and return stalls are different observations

Control T remains 162,278,977 B from line 30 to 45 (29.003197–44.125723 s),
while S increases 42,831,008 B. Its exact server snapshot times are
1788816567812–1788816582812 ms. Rs remains 877 B; Rc rises 779→849 B, so
some confirmations during this true forward plateau report earlier target work.
T then advances only 14,600 B at 45.125823 s. At the last sampled row, S−T is
again 64 MiB; the large final settlement occurs after that snapshot and is
preserved by raw bin 52 and exact final probe equality.

Across those same control lines, client TCP path 2/instance 4 native ACKed
bytes stay 878,845 while Product debt stays 429,216 B; native flight is
421,368→425,712 B. QUIC path 0/instance 3 native ACKed bytes advance
177,831,417→222,860,724 B. Thus this is not a whole-carrier-set native freeze.
It does not prove that the frozen TCP debt owns the exact missing frontier.

Candidate T has an outage-associated plateau at 202,386,903 B from
31.079102–34.100908 s (lines 32–35), before resuming. Its later longest
confirmation silence is not the same forward-only geometry:

- Lines 41–47, 40.101511–46.102124 s: Rc is 840 B throughout, while
  Rs grows 1218→1526 B and T grows 240,683,927→254,488,023 B.
- Exact client snapshot times are 1788816667220–1788816673220 ms;
  server times 1788816667222–1788816673221 ms. Thus 6 s of held return
  work coexists with 13,804,096 B additional ordered target writes and 308 B
  newly read reply input. Raw confirmation bins 40–45 are all zero.
- At line 48, 47.102227 s, T and S already equal all 259,457,024 payload
  bytes, but Rc is 924 B versus Rs 1553 B. Completion follows at 47.594305 s.
  Final payload settlement therefore precedes complete return/application
  confirmation in the sampled record.

This establishes a boundary between server reply read and client local reply
write, not a particular response frame, admission gate or native loss.
Native return carriers are not entirely silent during lines 41–47:

| Server return path / instance | Native epoch | Native ACKed bytes, start→end | Reported queue, start→end | Native flight, start→end |
| --- | --- | ---: | ---: | ---: |
| TCP0 / 4 | 11172496486600301153 | 558,898→567,844 | 0→56 | 11,584→20,272 |
| TCP1 / 3 | 5730130308172196430 | 558,926→568,224 | 0→0 | 8,688→7,240 |
| TCP2 / 2 | 9835779173501536355 | 556,851→566,087 | 0→0 | 8,688→7,240 |
| QUIC0 / 1 | 6984567122264236780 | 502,953→515,294 | 0→0 | 25,328→25,328 |

These native counters include protocol/control traffic and cannot identify
which response byte was served. Small/zero reported queues do not establish
that the missing byte was admitted or received. Server management exposes
no per-path Product flight value here (`data_level_bytes_in_flight=null`);
no exact response owner, repair count or stale predicate is inferable.
At candidate line 48, client QUIC still reports 43,861,385 B Product debt even
though every payload byte is target-written. Such debt must not be renamed
undelivered target payload.

Within each cell, path/instance identities remain stable through all sampled
rows. TCP native epochs are absent in the first snapshot, become populated
in line 2, and remain unchanged thereafter; QUIC epochs remain unchanged.
Control client TCP identities are 1/1, 2/4, 0/2 and
QUIC 0/3; server TCP 0/3, 1/1, 2/4 and QUIC 0/2. Candidate client TCP 1/3, 2/2, 0/4
and QUIC 0/1; server TCP 0/4, 1/3, 2/2 and QUIC 0/1. Local index/instance labels
are not automatically the same cross-endpoint physical mapping. There are
no management collection errors. Both client logs are empty; each server's
single H3_NO_ERROR connection-close warning follows its last snapshot during
teardown, not the early stall.

## Directional wire and process cost

Only the router's class 1:10 counters are differenced: eth1 is upload and
eth0 return in this mirrored routed profile. Do not sum the same traffic
again across nested qdiscs or endpoint classes. All class byte counters are
monotone. Each interval is first-to-last service sample, not a full physical
capture: control 0.000055–52.126550 s; candidate 0.000141–47.102227 s.

| Router direction / counter | Control | Candidate |
| --- | ---: | ---: |
| Upload bytes, first→last | 562,396→310,384,811 | 527,114→325,193,221 |
| Upload byte delta | 309,822,415 | 324,666,107 |
| Upload packet / drop delta | 255,423 / 3,641 | 268,474 / 3,624 |
| Return bytes, first→last | 29,841→12,609,301 | 28,299→12,177,363 |
| Return byte delta | 12,579,460 | 12,149,064 |
| Return packet / drop delta | 70,899 / 708 | 67,159 / 709 |

These are whole-class wire-work proxies: framing, control, native
retransmissions and Product copies are mixed. They are not repair-only bytes
or exact amplification factors. Unequal final payloads and unequal sample
endpoints prevent a matched-work efficiency claim; notably the control's final
large target settlement is outside its final service snapshot.

| Process / sampled metric | Control | Candidate |
| --- | ---: | ---: |
| Client PID | 245829 | 247063 |
| Client RSS first / peak / last (KiB) | 52,904 / 1,044,792 / 370,232 | 52,224 / 1,290,688 / 1,231,240 |
| Client peak-RSS elapsed (s) | 45.125823 | 46.102124 |
| Client maximum observed / last ps %CPU | 62.7 / 56.0 | 72.6 / 72.6 |
| Server PID | 251990 | 253223 |
| Server RSS first / peak / last (KiB) | 30,256 / 140,644 / 140,644 | 30,880 / 126,932 / 109,284 |
| Server peak-RSS elapsed (s) | 52.126550 | 17.002083 |
| Server maximum observed / last ps %CPU | 27.5 / 9.8 | 30.3 / 10.9 |

PIDs are stable within each cell. RSS is sampled resident KiB, not a continuous
peak or idle-baseline delta. `ps pcpu` is a process-lifetime average;
the maximum of those observations is not peak interval CPU or CPU seconds.
The candidate's higher client RSS/CPU observations deserve retention, not
normalization into a causal overhead estimate from this unequal-work pair.
There is no quiet post-load reclamation series, so neither a leak nor
equivalent retained ownership is established.

## One next causal discrimination

Which exact lowest response range is held during a recurrence of the
server-Rs/client-Rc divergence: an unadmitted retained original/copy, a copy
accepted but awaiting native service, or already received bytes blocked by a
different prefix/local delivery? The ordinary snapshots locate the stage but
cannot answer that question.

The decisive join is original/copy range and exact owner, admission timestamp,
client receive frontier and local reply write, with current native epochs.
A timely admitted copy for the held range falsifies the old eligibility gate
as that episode's explanation; a receive frontier already beyond the held
local-write frontier moves the cause downstream. Missing events cannot prove
native loss. If the geometry does not recur, record that limit rather than
rerunning ordinary cells until the mean improves. No interpretation here
authorizes a new policy, tuning change or practical promotion.

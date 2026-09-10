# Return-round outage observer

Updated: 2026-09-10 12:14 +08. Category: bounded causal observation, not ordinary
performance acceptance. Candidate `364d417`, existing diagnostic events only.
Raw capture: `./.tmp/reflection/results/mixed-combined-down-return-round-outage-observer-0910/`
(`probe.json`, `client.log`, `server.log`, `service.jsonl`). No runtime edits or
additional experiment were made for this analysis.
The root-created, tar-listed [raw archive](RETURN_ROUND_OUTAGE_OBSERVER_20260910.raw.tar.gz)
retains all five capture files (including `probe.err`) and the 64-second feature
build log. No configuration or secret files are included.

## Bulk stream 1: exact observed gap

The ordinary candidate's 1.097872-second gap near 37 seconds did **not** recur.
This diagnostic's maximum body-read gap is 0.475828 seconds, from probe time
33.122808 to 33.598636, with body counters 1,602,827,436 to 1,602,842,036.
The following account explains this smaller observed gap only. Diagnostic
throughput is not compared with ordinary throughput.

The matching mux frontier transitions are 1,602,827,644 to 1,602,842,244:
both counters differ from the body counters by exactly 208 bytes. The existing
probe strips the HTTP response header before counting body bytes; MPP carries
the complete response. This is the observed DSN/body alignment, consistent
with that excluded header, not 208 bytes of MPP framing. Raw header bytes were
not retained, so do not present their contents or an independent header parse.

All logged session identities are `4762801455009708722`; the 40 management
snapshots retain that session and the same four physical carriers. Exact
client probe-token to server reply-binding joins establish these mappings:

| Carrier | Client index / physical / attachment | Wire path | Server physical / stream incarnation | Joined tokens |
| --- | --- | --- | --- | ---: |
| TCP | 0 / 2 / 0 | 1 | 2 / 1 | 46 |
| TCP | 2 / 3 / 1 | 0 | 4 / 2 | 44 |
| TCP | 1 / 4 / 2 | 2 | 3 / 3 | 82 |
| QUIC | 0 / 1 / 3 | 0 | 1 / 4 | 35 |

Physical IDs are endpoint-local; the two TCP IDs 3/4 must not be equated across
endpoints. Receiver release events lack session/incarnation themselves; their
join is justified here by this verified single-session, stable mapping.

For compact absolute chronology, let `T = 1789013280000` Unix milliseconds.
These are diagnostic log timestamps, not a manufactured common probe-start
epoch. The two matching frontier timestamps are 476 ms apart, consistent with
the independent 475.828 ms body-read gap and millisecond log resolution.

| Time | Actual event |
| --- | --- |
| T−991 ms | Selected client feedback token 158 expires; publication returns to full fanout. |
| T+114 ms | TCP wire 1 advances mux frontier to 1,602,827,644. Reorder storage remains 17,644,640 B. |
| T+149–152 ms | The server applies discovery probes 167–169 on the three TCP return outputs and admits their replies. |
| T+184 ms | Server ACK frontier is 1,602,827,644. It queues and actually admits a 14,600 B persistent-gap copy `[1602827644,1602842244)` on TCP wire 2, incarnation 3. |
| T+587 ms | QUIC probe 170 expires; TCP wire 2 receipt 169 selects that return output. |
| T+590 ms | The exact 14,600 B extent arrives on client index 1 / TCP wire 2 and advances the frontier. Reorder storage is 40,133,280 B before and after this small release. |
| T+647 ms | A 65,536 B frame `[1602827644,1602893180)` arrives on client index 2 / TCP wire 0 and advances frontier to 1,606,425,660, releasing 3,583,416 B. |
| T+660 ms | Server learns frontier 1,602,842,244 and admits the next 14,600 B persistent-gap copy; the larger T+647 ms receiver advance has not yet reached this sender observation. |

The T+184 ms recovery event identifies the head's Original owner as TCP wire 0,
incarnation 2, with observed age 1.063319 seconds. Its candidate deadline is
already 554.435 ms past, target ETA is 1.551333 seconds, and both base/service
limits are 14,600 B. These are model observations, not measured packet delays.
The complete accepted-repair log contains exactly one copy covering the first
blocked DSN: the T+184 ms extent above. No stale-output recovery record covers
that DSN. Repair admission precedes its matching-carrier release by 406 ms;
the original-owner carrier's larger frame follows 57 ms later. This tightly
matches the accepted copy to the release without inventing a native packet or
copy identifier absent from these events.

Full fanout was already active 1.105 seconds before the body gap began and
remained active until three milliseconds before its matching release. The
sender already knew the exact missing prefix and admitted its repair 70 ms
after the last frontier advance. Consequently this gap was not waiting for
feedback-policy fallback to become eligible. It exposes ordered TCP-prefix
service with growing buffered suffix; it does not establish why the original
or admitted copy took that long, nor attribute the ordinary 1.098-second gap.

## Physical QUIC progress around restoration

The runner records UDP blackhole true at elapsed 30.003632, 31.095719 and
32.095943 seconds, then false at 33.096042 seconds. That elapsed value is read
before the firewall command sequence; it is not the exact packet-level
restoration instant. Management snapshots are cached and do not share the
probe's start clock.

The server's exact QUIC physical instance 1 retains native-delivery epoch
`7498134200128432564`. Its acknowledged-byte counter is 786,245,562 at samples
31 through 35, despite the recorded restoration. Sample 36 increases it by
19,824 B to 786,265,386; sample 37 increases it by another 18,963,120 B to
805,228,506. QUIC probe 170, created at T+78 ms and expired at T+587 ms, reaches
the actual server owner only at T+2806 ms, where its reply is admitted. Thus
late QUIC service persists independently of the TCP prefix release; an Active
label is not evidence of prompt native ACK progress.

These samples locate a lack of native acknowledgment progress, not its cause.
They cannot separate native retransmission/packetization, network queues, or
receiver readiness. The release boundary is before application socket write;
its near-exact match to this body gap does not turn it into a general write-
latency measurement.

## Observation cost and next boundary

The client emits 61,720 release events, 1,506 feedback events and one hole
timer signal; the server emits 13,647 accepted-repair, 10,267 ACK-gap recovery,
1,862 stale-output, 1,502 feedback and 341 recovery-wake events. Logs total
32,854,576 B. Even this filtered observer is nontrivial; its event timing is
causal evidence, not an ordinary performance control. The lone hole-timer
signal is at startup, not this gap, so its absence cannot waive the directly
observed later hole.

The bounded discriminator rules out delayed full-fanout activation for the
observed maximum diagnostic gap and identifies an actual TCP-owned missing
prefix plus admitted repair. It leaves post-admission service and the separate
ordinary adverse interval unattributed. No timer, repair-quantum, controller or
new performance policy follows from these observations alone.

## Echo: TCP-only selection and a later loaded return stage

Independent echo audit: stream 0 has 73 successes, 0 failures, 4,672 exact
request/response bytes, median 369.429 ms, p95 719.225 ms and maximum
1,686.272 ms. Its two feedback outputs are TCP only in both roles: client
index 0 / physical 2 / attachment 0 corresponds to server wire path 1 /
incarnation 1; client index 1 / physical 4 / attachment 1 corresponds to
server wire path 2 / incarnation 2. All 120 client and 153 server probe-created
events, and all 92/72 selection events respectively, use those outputs. No
QUIC probe or selection occurs. The QUIC outage therefore does not exercise
loss of echo's selected feedback output. Do not reuse bulk's stream-scoped
attachment/incarnation mapping.

Relevant attempts, in the probe's own clock, all succeed:

| Attempt | Start–end (seconds) | Duration (ms) |
| ---: | --- | ---: |
| 62 | 31.965444–32.943447 | 978.003 |
| 63 | 32.943472–33.662697 | 719.225 |
| 64–67 | Four intervening exchanges | 101–146 each |
| 68 | 35.664296–36.246469 | 582.173 |
| 69 | 36.246495–37.932767 | 1,686.272 |
| 70 | 37.932785–39.094565 | 1,161.780 |

Long echo service also occurs well after restoration, with four short
intervening attempts. Worst request 69 maps to response bytes `[4416,4480)`:
the probe performs sequential exact 64 B exchanges, and feedback generation
reaches 70 for this response. The following joins use the same absolute
`T = 1789013280000` Unix-millisecond base as above, not subtraction of
independent diagnostic monotonic origins.

| Time | Actual event |
| --- | --- |
| T+2887 ms | Server TCP proof 138 expires; route returns to full fanout and stays unselected until T+6474 ms. |
| T+3238 ms | Client admits proof 110 for response ACK generation 69 on TCP index 1. |
| T+3308 ms | Server logical owner receives proof 110 with required=applied MAX 67,113,216 and admits its reply on TCP path 2 / incarnation 2. ACK-before-Probe FIFO has therefore already carried the preceding positive receipt. |
| T+3308–3309 ms | Server creates and admits generation-70 probes 141/142 on both TCP outputs, selected=None. |
| T+3637 ms | Client proof 110 expires; client restores full fanout. Its timer is 4,068 microseconds late, not a multi-second late wake. |
| T+3682 ms | Server actually admits response tail repair `[4416,4480)` on TCP path 1 / incarnation 1, with 574,620 microseconds of accepted-copy deadline remaining. |
| T+4257 ms | Accepted-copy wake runs 347 microseconds late; server ACK frontier remains 4416, sent offset 4480. |
| T+4924 ms | Client receives now-obsolete proof 110 receipt and creates/admit response generation-70 probes. The proof reply spent 1,616 ms after server admission before client logical receipt. |

Proof 110 separates a prompt 70 ms client-admission-to-server-owner leg from
the long 1,616 ms reverse admitted-to-owner leg. This is not waiting for the
preceding ACK to reach the server or for missing stream credit: actual applied
MAX exceeds the entire 4,672 B workload. Response repair is accepted 1,242 ms
before generation 70 becomes visible; accepted repair does not establish
native handoff or which copy wins. Generation visibility is not an exported
target/read timestamp, and the probe lacks a shared wall-clock anchor. Keep
its timing separate rather than treating this join as an exact decomposition
of the 1,686 ms request.

Across the entire echo trace, the client has no receive-hole release or timer
signal; the server has 17 accepted 64 B tail repairs and six accepted-copy
wakes, but no stream-0 scoped-negative/stale-output recovery event. Releases
only log advancing frontier with preexisting reorder bytes, so their absence
is not an independent per-DATA arrival log. Every probe is admitted within
1 ms on the client / 2 ms on the server; all recorded expiry lateness is at
most 4,068 / 2,992 microseconds respectively. These facts do not show an
avoidable Product timer/admission hold. Loaded service after ordinary queue
admission remains material, without a split between native writer, transport
and client-owner stages, a copy winner, or a proved physical/native defect.
They do not attribute the absent ordinary bulk 1.098-second gap.

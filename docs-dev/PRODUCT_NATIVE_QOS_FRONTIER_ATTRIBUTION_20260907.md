# Product/native QoS frontier attribution — 2026-09-07

## Scope and evidence

Read-only attribution of `mixed-combined-down-product-native-qos-diag-0907`:
the unchanged combined mixed-download profile, frozen pruned model plus
temporary observers. This is not an ordinary performance acceptance or a
reproduction of every earlier stall. The previous 9.859789s gap did not recur.

[Capture summary](PRODUCT_NATIVE_QOS_DIAGNOSTIC_20260907.json) contains the
complete probe result, invocation, ownership, clocks and 41 compact samples.
[Compressed originals](PRODUCT_NATIVE_QOS_DIAGNOSTIC_20260907.raw.tar.gz)
contain `client.log`, `server.log`, `service.jsonl`, `probe.json` and `probe.err`.
All log line references below refer to those unchanged archive members.
Client PID215909 and server PID222231 are separate process/sequence spaces;
Unix timestamps align them, whereas native and probe elapsed clocks differ.

## Exact bulk prefix, admission and release

The probe's longest closed read gap is 2.771242s, at
14.828760–17.600003s, body bytes218414688→218426688. Product stream1 includes
the HTTP header prefix; the probe counts only body bytes
([parser](../lab/mixed_workload_probe.py#L232),
[body accounting](../lab/mixed_workload_probe.py#L388)). At both boundaries,
Product offset minus body count is208B: the missing Product prefix is
`g=218414896`, and its first release ends at218426896.

| Observation | Unix ms | Exact archive reference |
|---|---:|---|
| Original `[218407432,218472968)` admitted to TCP path0 |1788761705376|`server.log:5840` |
| Client advances to `g` and then retains that frontier |1788761708661|`client.log:11821`, `:11822` |
| Recovery confirms TCP0/incarnation4 ownership; queues `[g,218429496)` to QUIC0/incarnation3 |1788761708696|`server.log:7529` |
| That same14,600B repair is dispatched to the carrier |1788761708696|`server.log:7530` |
| Last later-data arrival while the frontier remains `g` |1788761708796|`client.log:12220` |
| QUIC0 frame `[g,218426896)` advances the frontier |1788761711433|`client.log:12235` |

There is one original and one dispatched repair covering `g` in the complete
server dispatch log. The lower original is TCP-owned: the UDP path on a
`receive_hole` row describes the later-arriving frame, not the missing owner.

The distinction is **35ms to repair admission, then 2.737s from admission to
client reassembly**. Dispatch means successful carrier-command admission and
range ownership, not completed native write or physical transmission.
`receive_hole_release` precedes the client socket write; here its first12,000B
release also matches the probe's first resumed body read.

The398 later-data observations at `g` all occur in the first135ms, growing
reordered bytes18,548,544→22,891,456. There is no demonstrated continued
decoded Product arrival throughout the remaining2.637s. Recovery is not asleep:
accepted-copy wakes are1,058µs and818µs late (`server.log:7583`, `:7607`);
subsequent stale-owner evaluations explicitly report `no_target`, including
`server.log:7729`. That disposition does not identify which target filter failed.

## Native progress and shared physical work

Server native snapshots `server.log:7590` and `:7731`, at Unix1708826ms and
1710835ms relative to the common1788760000000ms prefix, show live-controller
ACK bytes244306963→244716163, ACK frames31959→32371 and continuing loss-time
processing. Native flight3,376,666B exceeds the newly reduced422,830B window
at the latter observation. By `server.log:7757`, flight drains to2,343,527B
and ACK bytes increase to245749302. This is not the separate no-ACK plateau
around the later deliberate UDP blackhole.

The router's forward10Mbps class has4,394,196B queued at elapsed15.002s,
then3,972,504 /2,809,782 /1,878,822 /653,938B at16/17/18/19s
(`service.jsonl:16` through`:20`). The first value is3.515s of
**service-equivalent work**, not measured residence of the specific repair.
The class includes shared traffic and netem timed staging; packet position,
native stream offset and exact native write completion were not traced.

Consequently, the capture establishes a TCP-owned Product gap with timely
QUIC repair admission, followed by delay inside the admitted-carrier/native/
wire/receive pipeline during substantial shared queued service. It does not
prove that the remaining delay is an MPP timer bug, native HOL, or entirely FIFO
residence. Native ACK bytes are not acknowledgements of this specific Product range.

## Echo cross-check

MPP logical stream0 response `[1856,1920)` is admitted to QUIC0 at1788761709070ms
(`server.log:7655`). Copies dispatch to TCP2 at1709293ms (+223ms;
`:7674`) and TCP1 at1709495ms (+425ms; `:7691`), using the same Unix prefix.
The first output update showing Product ACK frontier1920 is at1712867ms
(`:7777`), 3.797s after original admission. This is confirmation by the server,
not an exact client delivery timestamp or proof of which copy won.

Probe attempt29 actually times out at15.210584–18.214280s; subsequent44 slots
are unavailable after disconnect, not44 additional network timeouts. An
in-order64B response need not emit any receive-hole/release event, so absence
of such events is not evidence that nothing decoded. Both original and repair
dispatch are observed; shared queued service remains a relevant constraint.

## Conditional limit, not a waiver

If `Q` bytes must still be served ahead of a packet on the same nonpreemptive
service capped at `C` bytes/s, without discarding or bypassing that work, the
packet needs at least `Q/C` seconds there. An application scheduler cannot
withdraw packets already queued remotely. Sustaining500Mbps over100ms RTT
requires roughly6.25MB in the delivery pipeline; a sudden500→10Mbps cut can
turn previously useful flight into seconds of queued work. This does **not**
locate all6.25MB at one queue or assert that the observed repair had that work ahead.

Full utilization and uniformly zero added delay across arbitrary sudden
capacity cuts cannot both be guaranteed. Practical excessive queues, extra
Product blocking and persistent post-recovery plateaus still require diagnosis;
this limit is not permission to waive them.

## Decision

Close the unsupported **forgotten repair wake / missing repair enqueue**
hypothesis for this exact gap. No timer, congestion gain, or repair-quantum
change is justified by this capture. Keep the existing mixed-placement and
wire-pipeline gates open: the original TCP ownership is proved, but the earlier
placement decision and precise residence of the accepted repair are not resolved.
No additional run, hook, source change or release acceptance follows from this note.

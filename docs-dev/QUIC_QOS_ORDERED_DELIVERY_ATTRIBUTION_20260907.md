# QUIC QoS ordered-delivery attribution

Date: 2026-09-07 UTC. Category: bounded ordinary-run timing attribution.
No implementation, new instrumentation, compilation or additional run.

Evidence: [three-cell timing controls](TERMINAL_RETIREMENT_TIMING_CONTROLS_20260907.json),
cell `quic-combined-down-terminal-retirement-controls-0907`; full management,
router and process observations in
`./.tmp/reflection/results/quic-combined-down-terminal-retirement-controls-0907/service.jsonl`,
and its `probe.json`. Exact numerical claims below were extracted from parsed
JSON, not from a truncated whole-file display.

## Measured result and limits

The ordinary candidate delivered 108.314 Mbps over the finite workload, but
had a 5.683657-second bulk read gap, from probe time 18.709254 to 24.392911.
This ends with a new 64-KiB read, so it is not right-censored. The test requests
a partial 8-GiB response over a 40-second workload; this is not a full-transfer
completion result. This timing cell is unsatisfactory despite its average.

The router changes the impaired direction from 500 to 10 Mbps at the recorded
15.001697-second sampling iteration and restores it at 25.003051. The later
30–33-second UDP outage cannot cause this earlier gap. Runner elapsed and
probe elapsed have separately established origins; sequential collection and
one-second sampling prevent exact packet-level alignment between them.

## Echo timeout is on return delivery

The probe's first failed echo starts at 15.177471 and times out at 18.178991.
There are 30 successes, one actual three-second timeout, then 44 unavailable
schedule slots after disconnection—not 45 independently observed failures.

At management elapsed 15, reliable flow 1 has 1,920 bytes in both directions
at both endpoints. At elapsed 16–18:

- Client: 1,984 bytes read from the application, 1,920 written back.
- Server: 1,984 bytes written to the echo target and 1,984 read back.

Thus the next 64-byte request reached the server and its target generated a
reply by the elapsed-16 observation. The missing client reply is downstream
of that server read. The snapshots do not identify when the reply entered its
MPP command queue, native stream, or router queue.

## Bulk ordered delivery stops while native service continues

Between management elapsed 19.002112 and 24.002942:

| Observation | Start | End |
| --- | ---: | ---: |
| Client bulk-flow bytes written to local socket | 296,045,452 | 296,045,452 |
| Server bulk-flow bytes read from origin | 363,154,316 | 363,154,316 |
| Server native live-packet ACK bytes | 307,569,034 | 312,845,434 |
| Server native sample clock, microseconds | 19,442,457 | 23,973,236 |
| Forward router dequeued bytes | 380,342,826 | 386,612,442 |

Native counted delivery increases by 5,276,400 bytes while contiguous local
delivery stays flat. The forward router serves 6,269,616 bytes, approximately
10.03 Mbps over this sampled interval. RTT and native flight also change.
This is neither a frozen observer nor absence of all native ACKs. More
generally, even a flat live-packet counter would not exclude retained-only
or duplicate ACKs, as documented in
[the prior ACK attribution correction](UPLOAD_RECOVERY_GATE_ATTRIBUTION.md).

The server-read/client-write separation is exactly 64 MiB throughout these
samples. `ObservedProductIo` counts successful endpoint reads/writes
(`src/runtime/telemetry.rs:853`), not native wire transmissions. This proves
buffered logical exposure, not an independently measured 64-MiB native queue
or exact Product-ACK flight. It is consistent with bounded source backpressure
after ordered receive progress stops, not origin starvation alone.

## Physical queue service and unresolved owner

The forward backlog is 3,715,968 bytes at the rate cut, and 5,073,570–5,977,746
bytes during the stalled interval. At 10 Mbps these correspond to about 2.97
and 4.06–4.78 seconds of serialization, before loss recovery. The 64-KiB HTB
burst represents approximately 52 ms at that rate. These are service-work
equivalents, not measured residence of the missing response or bulk prefix.
`REFLECTION_FIFO=0` installs netem delay/jitter without an explicit pfifo;
packet ordering, drops, arrivals and individual missing offsets are not traced.

Ordinary management contains no exact QUIC receive-stream offset, native loss
timer, MPP receive frontier or repair-copy arrival trace. Consequently the
remaining causal split is native reliable-stream ordering versus MPP
reassembly/dispatch/feedback, with multi-second physical queue work already
present. Cross-TCP/QUIC allocation is not required for this QUIC-only symptom.
The evidence does not establish a particular controller gain as its cause.

RSS is approximately stable during the pause; sampled process-lifetime CPU
averages decline. Those averages cannot exclude short scheduling stalls or
prove host-wide absence of pressure. Both management snapshots and native
service continue, so a full-process freeze is not the observed behavior.

## Reflection boundary

This result is not a release-versus-candidate causal A/B and cannot establish
that lifecycle corrections improved or worsened throughput. It does establish
that the current timing experience is not acceptable. An allocator or rate
rewrite is not yet justified for this gap: first identify the exact ordered
owner and recovery event. The separate
[rate-scope audit](RESPONSE_PLACEMENT_RATE_SCOPE_AUDIT_20260907.md) explains why
existing numeric fields cannot simply be substituted across carrier families.

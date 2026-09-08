# Prepared stale-path service: completed ordinary pair

Recorded:2026-09-08. Category: bounded model RED/GREEN and ordinary comparison.
**Mixed/adverse practical result; no promotion or release acceptance.**
The candidate completes fewer useful bytes later, with a larger confirmation
gap, despite better first service and maximum local-write gap.

## Origin, correction and executed controls

The preceding [prepared-claim diagnostic](PREPARED_CLAIM_SERVICE_20260908.md)
separates already-written/native-waiting heads from a narrower late Ready/stale
refusal. Legacy5d660f3b preferred an attached active/scorable nonstale output;
9720e4b later added finite Ready and fresh/stale tiers while consuming that
already-masked observation. A non-Ready fresh output could therefore suppress
a Ready stale survivor before the existing prepared arbitration considered it.
The original intention was to avoid feeding stale outputs while permitting
sole survivors, not to invent downstream bandwidth or ignore Product limits.

Actual-producer RED checkpoint f1b0900 reaches the intended0-versus65536-byte
claim assertion after current source, membership, Ready and exact authority
checks. The fresh-Ready opposite passes:1pass/1fail in.01s after a warning-free
1m12s build. Runtime b783cd6 separates resource observation from legacy stale
preference; prepared claims apply their existing four-tier policy once. Legacy
callers retain their wrapper/read order. Full-membership ownership, W/P/E,
qualification, source/Ready/Native fences, critical repair priority and all
timers remain unchanged. Independent consumer audit passes.

Warning-free GREEN build1m09s;544 focused checks pass1.27s. The actual stale
producer case is FirstPath. Existing exact-authority tests cover E refusal;
they are not misrepresented as an additional stale-Ready facade exhaustion
case. Withdrawal, Native refusal, idle/wake, half-close and physical TCP/QUIC
EOF controls remain green. Component evidence does not accept the ordinary
composition below.

## Fixed order, profile and artifacts

Fresh ordinary e476308 parent first, then b783cd6 after GREEN and the
warning-free3m29s optimized build. Both endpoints in each cell use the same
selected ordinary binary; diagnostics and native tracing are off. Parent:
`./.tmp/reflection/bin/advisory-owner-20260908/mptunnel`; candidate:
`./.tmp/reflection/bin/prepared-stale-20260908/mptunnel`.
There was no build/lab overlap or favourable third trial.

Existing runner `./.tmp/reflection/run.py mixed combined up`, unchanged
[profile](ADVISORY_OWNER_SERVICE_20260908.md#fixed-ordinary-profile-and-order):
routed/mirrored500Mbps, upload70/20ms and return30/5ms delay/jitter;
five-second loss epochs upload[3,8,5,6,10,3,5,8]% (mean6%) and
return[1,2,.5,3,2,.5,1,2]%; upload10Mbps at15–25s, UDP outage30–33s;
40s offered load,85s runner guard and50s post-load probe timeout/90s total.
Management sampling remains enabled, FIFO override and impairment ablations
disabled. Identical configured epochs are not identical packet realizations.

Raw directories are
`./.tmp/reflection/results/mixed-combined-up-prepared-stale-control-0908/`
and `./.tmp/reflection/results/mixed-combined-up-prepared-stale-candidate-0908/`.
Both complete result directories, RED/GREEN build/test logs, optimized build
log and both runner logs are preserved in
[PREPARED_STALE_SERVICE_20260908.raw.tar.gz](PREPARED_STALE_SERVICE_20260908.raw.tar.gz).
The archive contains17 files; compressed integrity was checked.

## Complete timing, including opposite outcomes

| Metric | Parent e476308 | Candidate b783cd6 |
| --- | ---: | ---: |
| Accepted = target-confirmed bytes |389218304 |326041600 |
| Probe elapsed seconds |47.562042 |52.965807 |
| Exact goodput Mbps |65.467 |49.246 |
| First confirmation seconds |.641281 |.448263 |
| Maximum closed confirmation gap seconds |4.845539 |6.082958 |
| First local write seconds |.131315 |.104305 |
| Maximum local-write gap seconds |3.553143 |2.223001 |
| Elapsed beyond40s offered-load boundary |7.562042 |12.965807 |
| Runner elapsed seconds |48.152258 |54.096009 |
| Complete / failed streams; probe and runner exits |1 /0;0 /0 |1 /0;0 /0 |

Both are exact, valid target-sink-ACK accounting, not lower bounds, with no
probe errors. The candidate completes63176704 fewer bytes5.403765s later.
First service and local-write gaps improve, but neither improvement waives
the longer useful-completion tail and confirmation gaps. Offered duration is
not an equal-byte experiment; buffered source can still be consumed after40s.

All raw one-second confirmation bins follow in Mbps. Labels are zero-based
starting indices; each final bin is partial. These are confirmation-arrival
bins, not wire rates. Buffered release can exceed500Mbps. No zero span is
removed in favour of the probe's trimmed interval average.

Parent,48 bins:

```text
 0: 2.097, 3.263, 3.556, 3.146, 3.649, 3.574, 1.669, 2.621, 2.621, 2.001
10: 560.878, 81.361, 257.265, 84.438, 37.194, 58.884, 272.726, 78.818, 37.098, 0
20: 8.81, 0.716, 0, 0, 17.811, 0.716, 115.447, 56.685, 0, 0
30: 0, 100.375, 0, 0, 90.318, 56.291, 72.607, 61.816, 194.209, 98.758
40: 11.63, 0, 0, 0, 145.408, 270.117, 264.659, 50.514
```

Candidate,53 bins:

```text
 0: 3.145, 14.68, 8.389, 381.445, 193.462, 0, 0, 67.301, 47.614, 41.419
10: 44.04, 58.861, 127.786, 125.017, 113.342, 0, 25.166, 0, 0, 0
20: 82.313, 13.631, 14.303, 0, 0, 37.077, 0, 59.533, 100.996, 123.208
30: 0, 0, 3.242, 0, 12.722, 19.4, 10.109, 0, 0, 141.27
40: 135.362, 120.919, 121.295, 0.384, 0, 2.481, 55.05, 0, 0, 0
50: 0, 0, 303.372
```

## Forward, return and terminal stages

L denotes a one-based `service.jsonl` row. S is client-consumed local source;
T is server ordered target-socket acceptance; Rs is reply bytes read by the
server relay from the target; Rc is reply bytes written to the client's local
socket. S is not claimed C, and Rc is not mux F. No exact C/F/range-owner trace
exists in this ordinary pair. Collector elapsed time, endpoint generated Unix
times and probe-relative bins have different anchors. Constant-span lengths
below use the relevant endpoint's Unix clock, not collector drift; these are
sampled lower bounds, not exact per-byte stalls.

| Approximate collector phase | Parent ordered T bytes | Candidate ordered T bytes |
| --- | ---: | ---: |
|10s |70648126 |136839062 |
|15s |182458104 |165937046 |
|20s |188203072 |170227350 |
|25s |190524566 |174861942 |
|30s |273571582 |210471990 |
|35s |277552964 |213004822 |
|40s |336753711 |280529974 |

Early parent L2–L10, approximately1–9s, has S−T exactly64MiB in each paired
sample: T grows262091→3277118B while replies keep moving. L11@10s has
T=S70648126. Candidate forward service is much earlier, but this ordering
reverses by15s; it cannot be summarized as uniform improvement.

Distinct held phases:

- Parent forward T188203072 is constant L18–L21, server Unix1788858190698
  →1788858193699,3.001s. T273571582 is constant L30–L33,1788858202698
  →1788858205699, another3.001s. The first is within QoS; the second crosses
  the UDP outage. Neither identifies an exact missing range or its owner.
- Candidate early return hold L6–L8, client Unix1788860104040→1788860106040:
  Rc179 stays flat2.000s while T100174902→136183702 and Rs218→316. Zero
  confirmation bins5–6 therefore are not zero forward target service. Summed
  client TCP Recv-Q grows541951→757545B while bytes_received grows297878B:
  the socket-consumption difference is82284B, not complete input silence.
- Candidate forward T170227350 is constant L19–L22, server Unix1788860117049
  →1788860120049,3.000s. S227338742→235470742; Rs582 stays flat while
  Rc540→568 catches up. This is a real target-service plateau, not solely
  reply delivery. Later T288096150 is flat L45–L47 for2.001s before the
  remaining payload reaches the target socket.
- Parent late return hold L42–L45, client Unix1788858214696→1788858217697:
  Rc1020 is flat3.001s despite T347295322→363303600 and Rs1090→1160.
  TCP Recv-Q444356→435108B with received+174332B implies183580B socket
  consumption. This contrary return-stage limitation is retained, not waived.

TCP socket differences cover all incoming protocol bytes, not just the sink's
application replies. They do not identify a particular response range, carrier
winner or Product actor's service cost.

### Candidate's longest confirmation tail is not continuing payload starvation

Final zero bins47–51 and the303.372Mbps buffered confirmation in bin52 locate
the longest6.082958s closed confirmation gap at the terminal tail. Candidate
S reaches326041600 at L43, approximately42.095s. T reaches that exact full
total at L48, approximately47.095s, server Unix1788860146049. At L48,
Rs=Rc1142. Through L53, approximately52.096s, T remains complete and Rs=Rc1142
(server1788860151048/client1788860151040). That is4.999s of sampled flat
server reply input after complete target-socket acceptance, not proof of
already-produced reply bytes trapped in the return path throughout the gap.

L54, approximately53.096s, records Rs1155/Rc1142. The13-byte addition is
consistent with `OK 326041600\n`, but byte count alone is not captured content.
The exact probe completes normally in52.965807s on its own start clock;
do not derive teardown or fine ordering from subtracting those unlike anchors.

The sink's ACK cadence is checked only after `recv(data)`; there is no periodic
ACK while idle. Its final `OK` is emitted only after natural EOF
([tcp_sink.py](../lab/tcp_sink.py)). T is target-socket acceptance, not exact
sink consumption or EOF receipt. Thus the next bounded question is:
**after the final target write, where does the terminal chain wait—source
EOF/request FIN, target write-half-close/natural EOF, or terminal OK publication
and delivery?** Exact winning terminal handoffs, not a new repair policy, must
distinguish these alternatives.

Context is consistent with remaining native work but not its causal ownership:
at L48–L53 every client TCP Recv-Q is zero and each bytes_received counter is
unchanged. Summed TCP Send-Q falls8702774→2694158B; by L54 it is1874582B.
Every sampled path's data-level debt is zero from L49, while TCP native debt
persists. These are different byte domains; they do not prove which queue owns
FIN, that C=F is observable here, or that native debt blocks terminal service.

## Sampled cost and path context

All four paths remain represented as active through the final snapshots.
Management uses exact per-session instances, not cross-run ordinal identity:
parent TCP wire2/1/0 is physical3/2/1, QUIC wire0 physical4; candidate TCP
wire1/2/0 is physical4/1/3, QUIC wire0 physical2. No session-spanning equality
is inferred from those integers. Candidate QUIC sampled native flight can be
zero while Product debt remains: L11@10s shows0 versus34406400B. That does
not establish a Ready writer, exact target admission or physical spare service.

| Sampled process observation | Parent | Candidate |
| --- | ---: | ---: |
| Client peak / last RSS KiB |775264 /614444 |832336 /764608 |
| Server peak / last RSS KiB |127920 /113544 |130972 /116712 |
| Client peak / last `ps %CPU` |88.3 /87.3 |108.0 /87.2 |
| Server peak / last `ps %CPU` |31.2 /17.3 |41.1 /13.4 |
| Final collector elapsed seconds |47.152115 |53.095856 |
| Router upload class byte delta |509382592 |434944828 |
| Router return class byte delta |21930425 |17795637 |

RSS is sampled resident memory, not an exact allocation peak. `ps %CPU` is a
process-lifetime average observed at each sample, not interval utilization or
claim cost. Router deltas use one directional HTB class each, never parent+
child double counting: parent upload774032→510156624 and return35062→21965487;
candidate upload841584→435786412 and return35745→17831382. The unequal
observation windows need not include every final packet. Counts include framing,
control, copies and retransmission, not repair-only cost. Fewer bytes on wire
alongside fewer useful bytes is not demonstrated efficiency improvement.

## Retained matched baseline context

Existing `./.tmp/reflection/results/{raw,xray,h2}-combined-up-review-mirrored-0906/`
router snapshots match the current directional rates, delay/jitter, loss epochs,
netem limit8192 and outage schedule. They are actual retained controls, not a
reason to substitute the unmirrored274.677Mbps raw result.

| Historical matched upload | Exact disposition |
| --- | --- |
| Raw TCP |24903680B exact in46.561749s;4.279Mbps; maximum confirmation/write gaps1.610879/1.846012s;47 raw bins |
| Xray |31641915/31653888B confirmed/accepted in73.15772s; terminal ACK missing; no accepted rate or retained confirmation bins |
| Hysteria2 |46729668/56623104B in85.371064s; guard-censored, then teardown reset; no accepted rate or retained confirmation bins |

The [historical cohort](REVIEW_COMBINED_COHORT_20260906.json) records H2's
explicit500Mbps prior, unlike MPP. Historical independent random realizations,
unequal work, missing native/endpoint phase observations and unverified offload
equivalence prevent causal or release rankings. Raw shows that the profile
permitted completed lower-rate service without a4s closed confirmation gap in
that realization. It does not explain the current exact MPP terminal or
already-written/predecode holds. Xray/H2 do not supply completed comparators.

## Disposition

The duplicate stale preference has a real producer RED/GREEN and a narrowly
reviewed correction. Its ordinary pair is mixed/adverse: stop promotion, retain
both phase histories and costs, and answer the terminal-handoff question before
another proposed correction. The prior already-written/predecode alternative
remains unresolved, not silently assigned to this policy change or to QoS.
No additional run, harness change or runtime proposal is made by this report.

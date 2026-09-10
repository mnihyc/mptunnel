# Confirmed return service: mirrored upload comparison

Recorded:2026-09-10. Category: existing mixed return-feedback service owner.
Both uploads settle with exact target confirmation; the candidate shortens
worst confirmation/write gaps but loses3.07% whole goodput. Return bytes rise,
not fall. This supports bounded completion, not an upload speed/cost improvement
or release acceptance. CURRENT_CLOSURE_PLAN retains the overall disposition.

## Contract, forecast and effective case

Root ran unchanged ordinary optimized control `b2aa215` then candidate
`0cab2b5`, without observers or suppression. The
[ordinary download report](CONFIRMED_RETURN_ORDINARY_20260910.md) and
[scoped model](SCOPED_ACK_SERVICE_MODEL.md) retain source/mechanism provenance.
The question was whether server-owned feedback routing preserves UP service
and settlement through a RETURN restriction. Forecast: possible return-work
reduction, no quantified upload gain; stalled settlement or adverse critical
service stops promotion, without a sampler/native/timing rescue.

Inputs are the five files per completed cell under
`./.tmp/reflection/results/mixed-combined-up-confirmed-return-{control,candidate}-0910/`.
Root archived and listed these ten raw files in
[CONFIRMED_RETURN_UPLOAD_20260910.raw.tar.gz](CONFIRMED_RETURN_UPLOAD_20260910.raw.tar.gz).
No build, lab rerun or source change was made for this analysis.

The existing runner uses one40s SOCKS5 TCP upload, not a concurrent echo load.
The probe's `protocol=tcp-upload` names its application socket; MPP remains
mixed, with three TCP carriers plus QUIC sharing one cut. All84service rows
show four active paths. `REFLECTION_MIRROR_IMPAIRMENT=0` means **UP data stays
500Mbps; DOWN return is500→10→500Mbps at nominal15–25s**. It is not a10Mbps
upload-data test. Actual router samples verify UP30ms/DOWN70ms delay,
zero configured loss/jitter/blackhole, HTB rate=ceil,65,536B burst/cburst and
netem limit8,192packets. Epochs still reapply the matching shaping settings.

| Cell | First DOWN10 sample, s | First DOWN500 restored sample, s | Sampled interval, s |
|---|---:|---:|---|
| Control | 15.001790 | 25.005313 | 0.000045–41.007138 |
| Candidate | 15.002209 | 25.003315 | 0.000049–41.005101 |

The runner starts its clock after launching the probe; no shared wall anchor
is recorded. Router/native/management reads are serial and management snapshots
are cached. Probe bins and service rows provide phase context, not an exact
packet/queue join. Matching settings are not identical native histories.

## Receiver-confirmed service and settlement

Both records are metricv2 `target_sink_ack`, exact and ACK-accounting-valid,
with `complete=true`, status`ok`, exit0, one completed stream, zero failed
streams and no probe errors. Local acceptance equals final target confirmation
only because the terminal sink acknowledgement validates all accepted bytes.
Neither local acceptance nor management I/O counters substitute for this proof.

| Metric | Control | Candidate |
|---|---:|---:|
| Target-confirmed / local-accepted bytes | 2,071,920,640 /2,071,920,640 | 2,011,234,304 /2,011,234,304 |
| Whole elapsed, s | 41.627353 | 41.687789 |
| Confirmed whole goodput, Mbps | 398.184 | 385.961 |
| First local write, s | 0.105246 | 0.106091 |
| First positive target confirmation, s | 0.412953 | 0.409693 |
| Maximum confirmation gap, s | 0.664826 | 0.496251 |
| Maximum local write gap, s | 0.484657 | 0.348209 |
| Elapsed after nominal40s load cutoff, s | 1.627353 | 1.687789 |
| Confirmed after40s, approximate MB | 95.764 | 92.102 |

Whole goodput includes settlement. This is time-window offered work, not a
fixed-byte race: candidate confirms2.93% fewer bytes. Time after40s includes
the final loop/half-close, sink acknowledgement and worker join; the exact last
write timestamp is not exported, so it is not an exact last-write-to-ACK drain
measurement. Both complete well before the50s completion timeout. The final
two bins retain post-load confirmation, including a partially occupied41–42s
bin; no settlement bytes are discarded. Approximate MB derives from rounded
bin rates, not an independent exact byte counter.

The probe records positive sink-ACK arrival at the client, not target receive
timestamps. Confirmation gaps therefore include return-path and local probe
service. Maxima exclude startup, which is separately retained; exact gap
intervals and local-write bins are not exported. Both recovery-gap fields are0
with `failover_after_s=-1`: they do not prove zero restriction recovery delay.
No short-object or loaded echo result is available in this upload workload.

| Nominal probe phase | Control mean confirmed Mbps | Candidate mean confirmed Mbps | Change |
|---|---:|---:|---:|
| Startup0–5s | 357.260 | 328.737 | −7.98% |
| Healthy5–15s | 433.934 | 415.659 | −4.21% |
| Restricted return15–25s | 344.481 | 329.009 | −4.49% |
| Restored25–40s | 415.920 | 417.513 | +0.38% |

All raw one-second confirmation bins follow, without the probe's optional
three-bin trim at either end. Values are confirmed bytes arriving in the bin,
normalized by one second; buffering can produce values above500Mbps. The
last bin is partial and must not be interpreted as a full-second sustained
rate. No bins are missing or reconstructed from unrelated counters.

```text
second control candidate (Mbps)
 0      12.582    11.305
 1     318.243   209.907
 2     456.130   624.944
 3     335.981   215.777
 4     663.365   581.751
 5     454.270   418.162
 6     157.390   187.939
 7     725.138   546.692
 8     232.784   654.762
 9     564.754   273.553
10     420.546   611.744
11     370.941   378.367
12     399.555   513.751
13     482.677   452.464
14     531.285   119.158
15     275.166   594.344
16     261.203   338.603
17     387.892   274.070
18     421.575   359.366
19     349.047   285.742
20     349.781   307.516
21     339.144   237.683
22     224.141   245.245
23     626.119   392.773
24     210.739   254.744
25     674.094   386.039
26     426.866   402.793
27     291.504   250.766
28     508.087   755.384
29     380.153   398.075
30     223.066   334.259
31     721.856   151.091
32     382.871   121.679
33      83.591   800.691
34     746.793   437.972
35     391.023   188.744
36     379.917   766.457
37      43.280   385.928
38     211.332   268.819
39     774.373   613.993
40     307.329   171.442
41     458.784   565.377
```

## Return traffic, queues and resources

Cost uses one HTB child per direction, last-minus-first cumulative counters,
not parent plus child/netem. These sampled windows end before probe completion;
they are not exact whole-transfer wire amplification. Packet units are
offload-sensitive kernel accounting, not exact physical packets/codec frames.

| Cell | UP data bytes / packet units | DOWN return bytes / packet units | UP / DOWN peak backlog, B | UP / DOWN drops |
|---|---|---|---|---|
| Control | 2,375,709,923 /1,647,260 | 47,837,873 /473,180 | 17,584,024 /430,338 | 0 /0 |
| Candidate | 2,330,908,397 /1,612,962 | 48,432,584 /467,660 | 19,645,930 /697,094 | 0 /0 |

Return bytes rise1.24% while packet units fall1.17%; UP bytes fall1.89% beside
less useful work. The forecast's potential return-cost saving does not appear
in this upload pair. No per-kind/selected-proof observer exists here: these
totals cannot identify ACK/MAX/Probe overhead or prove actual selected-output
participation. Do not import the download pair's16% saving into this direction.

Strict-interior sampled windows use rows2–14,16–24 and26–39, independently
of the probe bins above:

| Cell / phase | Exact sampled interval, s | UP bytes / Mbps | DOWN bytes / Mbps | DOWN backlog min / median / max, B |
|---|---|---|---|---|
| Control healthy | 2.000312–14.001687 | 741,348,122 /494.175 | 12,684,261 /8.455 | 54,732 /71,395 /99,871 |
| Candidate healthy | 2.000289–14.002092 | 749,134,334 /499.348 | 12,548,932 /8.365 | 38,813 /60,203 /142,436 |
| Control restricted | 16.001901–24.005216 | 450,635,866 /450.449 | 9,982,127 /9.978 | 65,077 /279,059 /404,685 |
| Candidate restricted | 16.002317–24.003206 | 360,382,592 /360.343 | 9,840,653 /9.840 | 255,058 /631,202 /697,094 |
| Control restored | 26.005435–39.006950 | 739,360,044 /454.938 | 14,992,347 /9.225 | 21,684 /99,246 /136,351 |
| Candidate restored | 26.003436–39.004884 | 765,048,924 /470.747 | 16,344,279 /10.057 | 26,577 /66,335 /144,837 |

The restricted return remains near10Mbps, with candidate median/peak backlog
higher despite its shorter whole maximum confirmation gap. These serial queue
samples do not locate the blocking byte or attribute that gap to proof expiry,
native service, a selected carrier or ACK processing.

| Cell | Client RSS peak / final, KiB | Server RSS peak / final, KiB | Client / server peak reported %CPU |
|---|---|---|---|
| Control | 306,312 /299,076 | 115,608 /115,608 | 184 /100 |
| Candidate | 358,888 /328,092 | 121,704 /106,492 | 184 /100 |

Client peak RSS rises17.16%. `%CPU` is `ps` lifetime-average multicore
utilization, not instantaneous CPU saturation. Each role has one stable MPP
PID/start identity across42samples; no management retrieval error or admission
rejection occurs. Last cached snapshots still show one active logical flow
and four active carriers: these are load/settlement samples, not a post-teardown
resource reclamation check. Both probe stderr and client logs are empty;
candidate server logs one remote QUIC `H3_NO_ERROR` closure warning, retained
beside successful terminal confirmation rather than labelled a failed upload.

## Disposition and reproducibility

Observed versus forecast: exact completion and subsecond maximum gaps survive
the mirrored return restriction, but there is no demonstrated upload throughput
or total return-cost gain. Startup/healthy/restricted confirmation means are
lower; restored mean is approximately unchanged. Retain the reduced gaps,
slightly longer nominal settlement and higher client peak RSS together. This
single sequential pair does not establish causality, universal non-regression,
selected-output blackhole coverage or performance acceptance; no favorable
rerun, tuning change or new observer is authorized by this report.

Read each completed `probe.json` with `jq .`; use
`interval_goodput_raw_mbps[a:b] | add / (b-a)` for the stated complete-bin phase
means. Existing `lab/bulk_upload_probe.py` defines sink-ACK timestamping,
terminal exactness and one-second normalization. In each `service.jsonl` row,
split `router` on newlines and parse the first two JSON arrays: index0 is DOWN
and index1 UP in this routed case. Use each sole class's `stats` once, with
the explicit endpoints above. Existing `./.tmp/reflection/run.py` and
`shape.sh` define phase routing and serial sampling. Raw cells preserve all
42service rows and the five-file logs; no new collection script is needed.

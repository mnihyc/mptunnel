# Upload recovery: exact missing prefix and idle QUIC

2026-09-07 00:20 UTC. Continuation of the existing startup/QoS/recovery owner,
not a new issue batch or an accepted production change.

## Observations

Ordinary relative-ACK upload controls fail timing; see ACK_RELATIVE_UPLOAD_CHECK.
A first diagnostic enabled per-frame repair attempts and generated 2,594,859
`stale_path_reinjection queued=false` entries. These count candidate Frames,
NOT actor turns. Only 1,635 groups are separated by another event; the largest
group contains8,556 entries. The run reaches its existing85s observation guard
and is then torn down. Its reset is censored, not an independent native failure.
Do not claim a busy loop or use logging-perturbed throughput as acceptance.

The quieter existing-event trace disables those attempt logs. A40s source
confirms all422,969,344bytes in78.973s, max confirmation gap5.200s. Its server
records an actual4.847s application-delivery stall at wall epoch1788739901629.
The first released range starts338,590,103 and has65,536bytes. Its original
was committed to TCP1 at1788739891793; a recovery copy was committed to TCP2
at1788739895691. The receive event alone cannot distinguish which copy won.
Another prefix335,968,663 starts on TCP0 at1788739877904; later TCP copies
exist, but it is not copied to QUIC in the captured event sequence.

The same run's QUIC carrier remains Active throughout. Native ACKed bytes
advance from325.67MB at30s to332.82MB at50s, then only tiny control increments
through70s. Native flight is zero in most40--70s snapshots, despite outstanding
Product work. TCP queues retain multiple megabytes, and the target's delivered
prefix advances slowly. QUIC data emission resumes near74s. This locates a
missing Product service/discovery opportunity, not proof that BBR's retained
rate is actual current application service.

A separate selection trace completes faster (44.108s, all502,464,512bytes),
but observes QUIC's logical request attachment entering Stale at35.645s,
after the30--33s UDP outage. TCP siblings subsequently become stale; QUIC
repair and originals resume near41--42s. Carrier Active and logical stale
attachment are different scopes. Do not erase that distinction to make the
table or selector look healthy. The large realization-to-realization variation
is itself why neither one mean nor one favorable recovery qualifies the build.

## Next bounded discriminator

Trace publication and exact receipt of the EXISTING data-bearing
requalification transaction, including frozen validity and accepted/unmatched
classification. This is an observation-only overlay in request/multipath.rs;
freeze the diagnostic executable and restore the source before running it.
Then distinguish absent probe opportunity, native queue residence, expired
receipt, and admitted-but-unscheduled acquisition. No new probe, timeout,
traffic hint, native gain or stale-state bypass follows from the current data.

Preflight rejects two premature interpretations. The critical enqueue uses
FIFO push_back, not reversed range order. The probe has an existing critical
minimum even when optional traffic credit is exhausted, so optional-credit
debt alone cannot prohibit recovery. The snapshot cannot identify a native
carrier failure or attribute the earlier deployed RAM incident. Independent
audit remains unavailable under the current usage limit.

Full quiet probe and one-second observations are in
UPLOAD_RECOVERY_OBSERVATION_20260907.json. Exact event logs remain under
.tmp/reflection/results/mixed-combined-up-relative-upload-exact-0907/.
The broad attempt log is diagnostic overhead evidence, not a result to ship.
The separate codec remains held; rejected packet batching remains removed.

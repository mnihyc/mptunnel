# Confirmed return observer: upload participation

Recorded:2026-09-10. Category: bounded causal review of the unaccepted
`0cab2b5` confirmed-return trial with root's feature-only transition observer.
Ordinary binaries remain unchanged. **Server bulk return selection occupies
only about6.4% of the observed route lifetime.** Serialized successor deadlines
cause every observed loss of selection, but most individual expired attempts
are fresh discovery rounds with genuinely longer logical proof paths. These
are separate findings, not a justification for increasing the interval.

Raw input is the completed five-file cell
`./.tmp/reflection/results/mixed-combined-up-confirmed-return-observer-0910/`.
The capture answers the participation question in CURRENT_CLOSURE_PLAN;
it is not an ordinary speed comparison. No runtime edit, build or lab rerun
was made for this analysis. The existing
[ordinary upload report](CONFIRMED_RETURN_UPLOAD_20260910.md) retains the
unfavorable whole/phase throughput and return-cost comparison.

## Identity, clocks and scope

Join server-created directional tokens with client `owner_received`,
`reply_bound`, `reply_admitted`, then server `selected`/`ignored_receipt`.
The exact owner is server/session`2736436903623281802`/stream`0`. Token numbers
also exist in the opposite direction; token alone is not a valid join key.
Preserve each server exact output and captured client reply tuple, rather
than equating their locally numbered path identities.

All645server-created tokens have exactly one admission, opposite logical-owner
receipt, reply binding/admission and returning logical receipt. Every recorded
reply has actual applied peer MAX at least its required offset. No missing
owner/reply or credit-deficit event explains these expiries.

`t_mono_ms` starts at each process's first diagnostic stamp, not a shared
epoch. Server's origin is approximately unix1789010920042ms; client's is
1789010920113ms,71ms later. Within-role durations use that role's monotonic
clock; cross-role stage estimates use shared-host `ts_unix_ms`. Both have
millisecond quantization. Frozen remaining deadlines are logged in microseconds
relative to their observed Instant, with `observation_age_us`; they are not
absolute unix microseconds. The two observed unix-minus-monotonic offsets each
vary by only1ms, with no visible clock jump.

The43service rows confirm the declared UP500Mbps / DOWN500→10→500Mbps return
restriction, UP30ms/DOWN70ms, no configured random loss/jitter/blackhole and
`mirrored_impairment=false`. First10/restored samples are at runner elapsed
15.001783/25.002957s. Runner/probe/diagnostic origins are different; cached
management timestamps are not command retrieval times. Phase summaries below
are coarse interior context, not exact workload-phase exposure or queue joins.

The observer upload itself completes exactly:2,108,030,976target-confirmed and
locally accepted bytes in42.045088s, status`ok`, no probe errors. Its401.099Mbps
and confirmation/write maxima0.621515/0.520007s are diagnostic context only,
not an improvement over the frozen ordinary pair.

## Participation and complete token accounting

| Attempt kind | Created/admitted | Selected receipts | Expired then ignored | Other ignored |
|---|---:|---:|---:|---:|
| Fresh discovery | 596 | 44 | 427 | 125 |
| Inherited successor | 49 | 5 | 44 | 0 |
| Total | 645 | 49 | 471 | 125 |

The125other ignored tokens comprise121invalidated by another discovery winner
and4invalidated at terminal. Thus596ignored receipts do **not** mean596expiry
failures. Replay of creation/expiry/selection/terminal events leaves no pending
token unexplained. Every inherited attempt is the immediate same-output child
of a selected receipt, retaining that predecessor's successor deadline.

There are44entries into selected state and5successful renewals, followed by
44expiry exits. Summing complete selected episodes, without resetting residence
on renewal, gives2.613s out of40.869s from first server probe through terminal:
6.39%selected,93.61%baseline policy. Wall-clock subtraction gives2.614s, the
expected quantization difference. Episode median/p95/max is36/182/254ms.
This is route-policy residence, not a measured percentage of ACK bytes saved.

| Approximate context; server observer-clock interval | Selected residence | Attempts created | Fresh / inherited expiries among those attempts |
|---|---:|---:|---:|
| Healthy interior5–14s | 394ms /4.38% | 139 | 104 /7 |
| Restricted interior16–24s | 517ms /6.46% | 92 | 62 /6 |
| Restored interior26–39s | 1,109ms /8.53% | 231 | 130 /20 |

Outcomes are grouped by attempt creation; the returning receipt may occur
later. Residence clips complete selected episodes to each interval. Poor
participation and both expiry mechanisms occur beyond the restriction alone.

## Measured proof stages

Entries are median / p95 / maximum milliseconds, sorted-index quantiles using
`round((n−1)*rank)`. Rows contain different cohorts, not independent trials.

| Stage | All645attempts | Fresh expired427 | Inherited49 |
|---|---|---|---|
| Create→local probe admission | 0 /1 /2 | 0 /1 /2 | 0 /0 /1 |
| Probe admitted→opposite owner | 99 /370 /661 | 138 /436 /661 | 72 /171 /218 |
| Opposite owner→reply admitted | 0 /1 /2 | 0 /1 /2 | 0 /1 /1 |
| Reply admitted→server logical receipt | 439 /1,580 /2,563 | 618 /1,699 /2,563 | 65 /335 /833 |
| Create→server logical receipt | 566 /1,840 /2,705 | 811 /1,919 /2,705 | 136 /406 /1,050 |
| Remaining deadline at creation | 255.818 /404.237 /842.861 | 295.091 /464.873 /842.861 | 36.128 /144.080 /201.609 |

Local probe admission and owner-to-reply admission are prompt in this capture.
The long measured stage is primarily after reply queue admission, which still
includes native writer/transport, UP data-direction queueing, server dispatch
and logical-actor service. It does not identify which of those owns the wait.
An ordinary FIFO admission is not a native write or peer receipt timestamp.
Do not turn these stage totals into a per-packet physical-queue attribution.

All427fresh-expired receipts arrive after their original full creation
deadline, not merely an earlier inherited deadline. Some newer native intervals
shorten their active successor bound, but none would have met the original
creation bound either. This independently limits an explanation based solely
on serialization.

Example: fresh QUIC token84, server output`Udp/PathId(0)/incarnation4`, is
created/admitted at unix1789010925079ms with319.573msremaining. The client owner
receives and admits its reply at1789010925257ms. It expires at1789010925399ms;
server ignores the returning receipt at1789010927199ms. The exchange takes
2,120ms, including1,942msafter reply admission. Source lines: server560/564/575/785,
client507/509. This healthy-interior example requires no inherited deadline.

## Decisive serialized-successor counterexample

Same server output`Tcp/PathId(1)/incarnation1`; captured client reply tuple is
`Tcp/index0/path_instance4/attachment0`. All rows have the same session/stream
identity above. Unix timestamps join roles; only server rows use its monotonic
origin. This lies safely inside the sampled restricted-return interval.

| Event | Unix ms | Server monotonic ms | Evidence |
|---|---:|---:|---|
| Token281created/admitted | 1789010939891 | 19849 | ACK generation14081, MAX1,042,841,255;131.517msremaining |
| Newer feedback anchors successor | 1789010939896 | 19853 | Generation14082; frozen interval131.626ms |
| Client owns281and admits reply | 1789010939962 | — | Required/applied MAX both1,042,841,255 |
| Receipt281selects; creates/admit285 | 1789010939993 | 19951 | Child generation14093, MAX1,053,547,623; only34.396msremaining |
| Token285expires; full fanout resumes | 1789010940027 | 19985 | Admitted token, selected output;132µslate to its effective deadline |
| Client owns285and admits reply | 1789010940069 | — | Required/applied MAX both1,053,547,623 |
| Server ignores receipt285 | 1789010940105 | 20063 | Old expired token cannot restore selection |

Server source lines1901/1905/1909/1917–1919/1923/1939; client1689–1691/1704–1706.
The exchanges individually take102ms and112ms, each less than the131.626ms
frozen successor interval. But the child is created only after the parent
receipt, about98msafter its own debt anchor. The remaining34.396ms cannot
cover its112mslogical round trip. No admission blockage, absent reply or
unapplied MAX is needed. This is the previously predicted recurrence:
`child_receipt = parent_receipt + child_exchange`, while
`child_deadline = first_newer_feedback + frozen_interval`.

Across49inherited children,44expire and5renew selection. Of those44expired
children,27complete their own exchange within the original frozen successor
interval;17would still be late on that arithmetic comparison. Forty start with
under100msremaining. The44inherited expiries are9.34%of all471expiry events,
but100%of selected-state exits. These denominators matter: serialization
explains observed selection losses, not every failed discovery or all low
participation. The27comparison is a timing discriminator, not an implemented
fresh-deadline policy or a predicted throughput gain.

## Disposition and reproducibility

The capture confirms that baseline fanout dominates this UP route and that
the serialized-successor condition occurs in real producer/queue/owner service.
It also rules out that condition as the sole explanation: fresh logical proof
paths frequently exceed their full frozen intervals. Current RFC §8.4.1
explicitly requires nonrenewing successor debt, so this is a measured model
consequence, not a new receipt-truth or credit-authority violation. No timer
increase, native/controller change or assumed bandwidth improvement follows.

Reproduce from `event=feedback_return` records, preserving role, session,
stream, token, sequence and exact output. Join only the opposite role's
owner/reply events for each creating direction. Classify inherited children
by the immediate same-output `selected`→`probe_created` transition; replay
active tokens to distinguish expiry, competing winner and terminal removal.
For residence, start on `selected previous=None`, preserve that start through
renewals, and end on selected expiry/terminal/detach. Retain integer timestamp
resolution and the separate clock origins; no new collection script is needed.

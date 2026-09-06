# Bounded receive-history candidate

2026-09-06 UTC. Tested candidate, not accepted for production or release.
Exact patches: `QUIC_RECEIVE_HISTORY_CANDIDATE.patch` (receive-only) and
`QUIC_REORDERING_EXCESS_CANDIDATE.patch` (later composition). See the
[34-run evidence](receive-history-evidence-20260906.json).

## Scope and derivation before implementation

The confirmed counterexample is an authenticated original overtaken by 130
packets in about 13 ms. A 129-packet duplicate bitmap discards it. Separately,
an already emitted one-byte packet number can reconstruct incorrectly after
128 overtakes. Both limits predate the recent MPP fixes.

Candidate: choose a minimum two-byte packet number and a matching exact circular
bitmap. Let `b=16`, `H=2^(b-1)` and `next=highest_received+1`. A received original
is unambiguously reconstructible when `next-packet < H`. Retain exactly that
interval, `[max(0,next-(H-1)),next)`. It covers 32,767 packet numbers, represented
in 32,768 bits (4 KiB). The extra bit is storage alignment, not permission to
accept an ambiguous boundary. Wider native encodings remain available when the
unacknowledged forward span requires them.

This is an explicit resource/wire tradeoff, not a rate-independent timing
guarantee. One byte cannot represent the reproduced displacement. Two bytes is
the next available QUIC width; using its whole backward decoding interval costs
4 KiB per packet-number space (12 KiB across a native QUIC connection's three
spaces), versus about 1 MiB per space for the three-byte interval.
At 500 Mbps with 1,200-byte packets it covers roughly 629 ms of overtaking;
at 10 Gbps about 31 ms. These are dimensional examples, not chosen test limits
or a reason to conceal failure beyond that horizon. No loss compensation,
congestion gain, application admission, recovery timer or displayed rate changes.

## Safety and complexity

For each packet-number space independently:

1. Authentication still precedes duplicate admission.
2. A packet below the monotone retirement floor is always rejected.
3. A packet at/above the floor is accepted exactly once, recorded by its bit.
4. Advancing `next` clears only newly entering ring positions; a jump of at
   least the storage capacity clears the whole bitmap. Retired bits can never
   be interpreted as evidence that a forgotten packet was unseen.
5. Storage is fixed 4 KiB plus owner metadata; advancing contiguous traffic
   costs constant work, with at most 512 words cleared on a large jump. Missing
   interval queries use word masks, bounded by those same 512 words.
6. Received ACK ranges and their existing wire-size bound retain their own
   purpose. Exact duplicate history continues to reject previously accepted
   packets even after their pending ACK ranges have been omitted or confirmed.

Both directions use the same model independently. Receiving old-peer one-byte
packets remains supported, but cannot recover information absent from their
encoding. An old receiver can decode a new two-byte packet yet still apply its
old 129-packet retention limit. Full benefit requires corrected endpoints; this
is ordinary QUIC header encoding, not a new negotiation or MPP wire version.

This follows RFC 9000 sections
[13.2.3](https://www.rfc-editor.org/rfc/rfc9000.html#section-13.2.3) and
[17.1](https://www.rfc-editor.org/rfc/rfc9000.html#section-17.1): preserve duplicate
exclusion when forgetting history and choose enough encoding for reordering.
It is not a claim that the RFC mandates this particular resource tradeoff.

## Acceptance boundaries

First RED/GREEN: real authenticated reordered packets with short and already-wide
encoding; duplicate copies; ring wrap, large jumps and exact retirement bounds;
the existing ACK-frequency and packet codec cases. Then ordinary-build no-loss
jitter, loss-only and combined QoS/recovery/blackhole timing series, plus reverse
traffic. Keep the unchanged sender as the ablation; do not silently bring back
the rejected sender-only candidates.

A higher mean does not accept the candidate if read gaps, loaded latency,
startup, genuine-loss recovery or resource cost regresses materially. Passing
the exact receive invariant does not declare the global QUIC/mixed/browser/
sustainability matrix complete. Any remaining sender issue keeps its separate
causal evidence and decision.

## First result and bounded composition experiment

The receive-only ordinary build does not solve the collapse: jitter-only QUIC
averaged 0.682 Mbps versus 0.585 Mbps control, with a 3.370 s versus 1.573 s
maximum read gap. Mixed averaged 81.691 versus 72.442 Mbps, but its gap grew
from 0.545 to 2.198 s. One sample cannot attribute every tail difference to this
change, but it certainly cannot establish non-downgrade. The full native suite
passed 454 tests; its sole failure was an explicit GSO byte total still assuming
a one-byte number. Correcting 29 to 30 bytes of overhead preserves the one-batch
assertion and passes the targeted test. The two real-packet RED/GREEN controls
and exact duplicate/retirement invariants are established, not global service.

Next experiment composes this receiver/codec with the already archived
observed-deadline sender candidate. This is justified by the separately proven
premature sender loss and receiver erasure owners; neither standalone variant
is being accepted. The unchanged receiver-only binary and exact patch remain
available as ablations. No compensation/rate/queue threshold changes are added.
The sender's history-induced genuine-loss delay remains a decisive countercase,
alongside the complete interval series and interactive outcomes.

That countercase subsequently reproduced and rejected the absolute-delay model.
The [excess-delay replacement](QUIC_REORDERING_EXCESS_DELAY_MODEL.md) improves
recovery but does not close mixed timing acceptance. The production tree is
restored; no candidate is silently left enabled.

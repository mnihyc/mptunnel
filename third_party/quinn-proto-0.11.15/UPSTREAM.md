# quinn-proto 0.11.15

This directory contains the crates.io source for `quinn-proto` 0.11.15,
checksum `4fcb935c5bec503c2f0e306bdd3e58bb9029dcb14fa8d9ac76e3a5256ac0763e`.
The upstream MIT and Apache-2.0 licenses are included unchanged.

The local patch keeps Quinn's public congestion-controller boundary while
aligning its experimental BBR implementation with the delivery-rate and pacing
model used by the reference algorithm. The semantic deviations from upstream
0.11.15 are:

- each ack-eliciting packet records the controller's delivery snapshot,
  application-limited state, packet number, and transmit flight;
- BBR derives delivery rate from the maximum of send and acknowledgement
  intervals, filters application-limited samples, and updates once per ACK
  batch;
- BBR publishes its gain-adjusted pacing rate to Quinn's token-bucket pacer,
  which preserves fractional refill time and bounds burst capacity;
- startup, round, recovery, ACK-aggregation, and congestion-window updates use
  the corrected delivery samples and packet identities; and
- an absent minimum-RTT timestamp is not expired, and the timestamp starts with
  the first RTT sample instead of entering ProbeRTT on the first useful ACK.

The added BBR, bandwidth-estimator, and pacer tests cover these changes. Other
differences in the vendored source are formatting-only.

Remove the path override only when an upstream `quinn-proto` release includes
the same delivery-sampling, pacing, and minimum-RTT behavior. References:

- <https://datatracker.ietf.org/doc/draft-ietf-ccwg-bbr/>
- <https://quiche.googlesource.com/quiche/+/refs/heads/main/quiche/quic/core/congestion_control/bbr_sender.cc>

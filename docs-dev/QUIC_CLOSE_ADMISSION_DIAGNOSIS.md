# Congested native close: bounded upstream correction

Date:2026-09-05T19:34Z. Reproduced in the MPTUNNEL Quinn fork with both
CUBIC and BBR3; this is not proof that every reported slow retirement had
this cause.

The native send gate classified work using ordinary pending STREAM frames,
but a closing connection emits only ACK and CONNECTION_CLOSE. Closing also
stops ordinary ACK processing and loss timers, so a full-window gate cannot
reopen. This defect predates the MPP controller model: upstream reports the
same mechanism in [issue2785](https://github.com/quinn-rs/quinn/issues/2785).

The applied correction is the close-only exemption in upstream
[PR2787,e916f3e](https://github.com/quinn-rs/quinn/pull/2787). It changes no
ordinary pacing, congestion window, loss algorithm or timer. The preceding
anti-amplification check still applies. The packet being sent determines
admission; unsent ordinary work cannot classify it.

The two local tests queue normal payload without returning ACKs, close, and
require the peer to observe the exact application close code/reason without
advancing simulated time. Both fail before the correction and pass afterward.
Existing close-with-ACK, loss-probe and ordinary transport tests remain GREEN;
the complete native suite passes450 tests plus3 doctests.

The first assertion used frame_tx.connection_close, which this encoder does
not populate. That counter still read zero even when the trace showed a close
packet being sent. The final RED/GREEN proof uses the peer's decoded terminal
event instead, not that unpopulated statistic. No unrelated statistics patch
was bundled into this correction. Logs: `.tmp/quic-close-peer-red.log` and
`.tmp/quic-close-peer-green.log`.

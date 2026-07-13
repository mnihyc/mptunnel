use super::*;
use tokio::net::lookup_host;
use tokio::sync::Mutex as AsyncMutex;

// QUIC-backed carrier runtime for paths whose underlay is UDP. It owns
// connection reuse, retry, carrier frame I/O, and native QUIC evidence; product
// offsets, response ownership, and cross-carrier ranking stay in reliable_path
// and sender_service. Application UDP target flows are handled separately.

#[derive(Clone)]
pub(super) struct ClientUdpPathSessionHandle {
    runtime: ClientUdpPathSessionRuntime,
    connection: Arc<AsyncMutex<Option<ClientUdpPathConnection>>>,
}

impl std::fmt::Debug for ClientUdpPathSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientUdpPathSessionHandle")
            .finish_non_exhaustive()
    }
}

impl ClientUdpPathSessionHandle {
    pub(super) fn new(runtime: ClientUdpPathSessionRuntime) -> Self {
        Self {
            runtime,
            connection: Arc::new(AsyncMutex::new(None)),
        }
    }

    #[cfg(test)]
    pub(super) fn session_id(&self) -> SessionId {
        self.runtime.session_id
    }

    pub(super) async fn open_stream(
        &self,
        stream_id: StreamId,
        target: TargetAddr,
        ingress: IngressKind,
        lane: FlowLane,
        options: UdpStreamOpenOptions,
    ) -> Result<ReliablePathStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_stream_on_connection(
            connection,
            stream_id,
            target.clone(),
            ingress,
            lane,
            options,
            self.runtime.clone(),
        )
        .await
        {
            Ok(stream) => Ok(stream),
            Err(err) if quic_path_open_error_is_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_stream_on_connection(
                    connection,
                    stream_id,
                    target,
                    ingress,
                    lane,
                    options,
                    self.runtime.clone(),
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    pub(super) async fn open_datagram_stream(
        &self,
    ) -> Result<ClientUdpDatagramStream, RuntimeError> {
        let connection = self.ensure_connection().await?;
        match open_client_udp_datagram_stream(connection, self.runtime.clone()).await {
            Ok(stream) => Ok(stream),
            Err(err) if quic_path_open_error_is_retryable(&err) => {
                self.drop_connection().await;
                let connection = self.ensure_connection().await?;
                open_client_udp_datagram_stream(connection, self.runtime.clone()).await
            }
            Err(err) => Err(err),
        }
    }

    async fn ensure_connection(&self) -> Result<UdpPathConnection, RuntimeError> {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.as_ref() {
            return Ok(connection.connection.clone());
        }
        let connection = connect_client_udp_path(&self.runtime).await?;
        let carrier_connection = connection.connection.clone();
        spawn_client_udp_path_metrics(self.runtime.clone(), carrier_connection.clone());
        *current = Some(connection);
        Ok(carrier_connection)
    }

    async fn drop_connection(&self) {
        let mut current = self.connection.lock().await;
        if let Some(connection) = current.take() {
            connection.connection.close();
        }
    }
}

#[derive(Clone)]
pub(super) struct ClientUdpPathSessionRuntime {
    pub(super) path: PathSpec,
    pub(super) path_index: usize,
    pub(super) session_id: SessionId,
    pub(super) security: SecurityConfig,
    pub(super) codec_limits: CodecLimits,
    pub(super) mux_limits: MuxLimits,
    pub(super) stream_frame_queue: usize,
    pub(super) health: Arc<Mutex<ClientPathHealth>>,
}

struct ClientUdpPathConnection {
    _endpoint: UdpPathEndpoint,
    connection: UdpPathConnection,
}

#[derive(Debug)]
pub(super) struct UdpPathEndpoint {
    endpoint: quic_carrier::Endpoint,
}

#[derive(Debug, Clone)]
pub(super) struct UdpPathConnection {
    connection: quic_carrier::Connection,
}

#[derive(Debug, Default)]
struct UdpPathMetricTracker {
    quic: QuicPathMetricTracker,
}

#[derive(Debug, Default)]
struct QuicPathMetricTracker {
    last_delivery_evidence_written_bytes: u64,
    delivery_evidence_pending_ack_bytes: u64,
    delivery_rate_bps: Option<f64>,
    ack_derived_data_seen: bool,
    pending_non_app_limited_sample_bytes: u64,
    pending_non_app_limited_sample_count: u64,
    pending_non_app_limited_sample_elapsed: Duration,
    delivery_sample_count: u64,
    delivery_sample_bytes: u64,
    last_delivery_sample_at: Option<Instant>,
    bulk_proof_expires_at: Option<Instant>,
    // Carrier snapshots are cumulative and sticky. Remember registry acceptance,
    // not observation, so a transient publication race may retry the same token.
    last_accepted_capacity_probe_token: Option<u64>,
    pending_capacity_proof_candidate: Option<QuicCapacityProofCandidate>,
    min_rtt: Option<Duration>,
}

#[derive(Debug)]
pub(super) struct UdpPathSendStream {
    stream: quic_carrier::SendStream,
    connection: UdpPathConnection,
}

#[derive(Debug)]
pub(super) struct UdpPathRecvStream {
    stream: quic_carrier::RecvStream,
}

impl UdpPathEndpoint {
    async fn bind_server(
        path: &PathSpec,
        context: &ServerPathContext,
    ) -> Result<Self, RuntimeError> {
        let addr = resolve_first_socket_addr(path).await?;
        Ok(Self {
            endpoint: quic_carrier::Endpoint::bind_server(
                addr,
                context.security.secret.as_bytes(),
                context.mux_limits,
            )
            .await?,
        })
    }

    async fn bind_client(
        _path: &PathSpec,
        local_addr: SocketAddr,
        runtime: &ClientUdpPathSessionRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            endpoint: quic_carrier::Endpoint::bind_client(
                local_addr,
                runtime.security.secret.as_bytes(),
                runtime.mux_limits,
            )
            .await?,
        })
    }

    async fn connect(&self, remote_addr: SocketAddr) -> Result<UdpPathConnection, RuntimeError> {
        Ok(UdpPathConnection {
            connection: self.endpoint.connect(remote_addr).await?,
        })
    }

    async fn accept(&self) -> Option<UdpPathConnection> {
        self.endpoint
            .accept()
            .await
            .map(|connection| UdpPathConnection { connection })
    }

    #[cfg(test)]
    pub(super) fn local_addr(&self) -> Result<SocketAddr, std::io::Error> {
        self.endpoint.local_addr()
    }
}

impl UdpPathConnection {
    async fn open_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.open_bi().await?;
        Ok((
            UdpPathSendStream {
                stream: send,
                connection: self.clone(),
            },
            UdpPathRecvStream { stream: recv },
        ))
    }

    async fn accept_bi(&self) -> Result<(UdpPathSendStream, UdpPathRecvStream), RuntimeError> {
        let (send, recv) = self.connection.accept_bi().await?;
        Ok((
            UdpPathSendStream {
                stream: send,
                connection: self.clone(),
            },
            UdpPathRecvStream { stream: recv },
        ))
    }

    fn close(&self) {
        self.connection.close();
    }

    fn capacity_probe_active(&self) -> bool {
        self.connection.capacity_probe_active()
    }

    async fn wait_for_capacity_probe_release(&self) {
        self.connection.wait_for_capacity_probe_release().await;
    }

    fn is_closed(&self) -> bool {
        self.connection.is_closed()
    }

    fn retire_capacity_probe(&self, token: u64) -> bool {
        self.connection.retire_capacity_probe(token)
    }

    fn cancel_capacity_probe(&self, token: u64) -> bool {
        self.connection.cancel_capacity_probe(token)
    }

    fn confirm_capacity_probe_receipt(
        &self,
        token: u64,
        received_payload_bytes: u64,
        received_at: Instant,
    ) -> bool {
        self.connection
            .confirm_capacity_probe_receipt(token, received_payload_bytes, received_at)
    }

    async fn tx_metrics(
        &self,
        tracker: &mut UdpPathMetricTracker,
        direction: u8,
    ) -> Option<UdpPathMetrics> {
        let stats = self.connection.stats();
        let congestion = self.connection.congestion_metrics();
        Some(tracker.quic.observe(stats, congestion, direction))
    }
}

impl QuicPathMetricTracker {
    fn expire_stale_bulk_proof(&mut self, now: Instant) {
        let proof_is_stale = self
            .bulk_proof_expires_at
            .is_some_and(|expires_at| now >= expires_at);
        if !proof_is_stale {
            return;
        }

        // The deadline owns placement authority, not estimator history. Keep
        // the measured rate/sample state for scheduling; `app_limited` and age
        // prevent it from silently renewing the expired right.
        self.bulk_proof_expires_at = None;
    }

    fn capacity_proof_candidate(
        &mut self,
        probe: Option<quic_carrier::CapacityProbeMetrics>,
        now: Instant,
    ) -> Option<QuicCapacityProofCandidate> {
        let probe = probe?;
        if self.last_accepted_capacity_probe_token == Some(probe.token) {
            return None;
        }
        // Receipt and a committed write atomically terminalize the carrier
        // epoch. Accepting the same fields in a nonterminal phase hides a broken
        // carrier transition rather than preserving useful proof.
        if probe.phase != quic_carrier::CapacityProbePhase::Proven {
            return None;
        }
        if let Some(candidate) = self
            .pending_capacity_proof_candidate
            .filter(|candidate| candidate.token == probe.token)
        {
            return (now < candidate.expires_at).then_some(candidate);
        }
        if !probe.write_committed
            || probe.train_payload_bytes == 0
            || probe.written_payload_bytes != probe.train_payload_bytes
            || probe.written_data_frame_count == 0
            || probe.required_timed_carrier_bytes == 0
            || probe.required_timed_carrier_bytes
                != probe.sample_floor_bytes.saturating_sub(
                    (PATH_OPEN_SCORE_BYTES as u64).min(probe.sample_floor_bytes / 8),
                )
            || probe.sample_floor_bytes > probe.train_payload_bytes
            || probe
                .warmup_carrier_bytes
                .saturating_add(probe.required_timed_carrier_bytes)
                > probe.train_payload_bytes
            || probe.proof_validity.is_zero()
            || probe.receipt_received_payload_bytes != probe.train_payload_bytes
        {
            return None;
        }
        let receipt_at = probe.receipt_at?;
        if probe.proved_at != Some(receipt_at) || receipt_at >= probe.expires_at {
            return None;
        }
        let receipt_elapsed = probe.receipt_elapsed.filter(|elapsed| !elapsed.is_zero())?;
        // Receipt time owns both the service interval and proof lifetime.
        // Use its full cold-start interval: subtracting an RTT can create an
        // unstable near-zero denominator, while native timing keeps changing.
        let proof_elapsed = receipt_elapsed.max(QUIC_TIMER_GRANULARITY);
        let expires_at = receipt_at.checked_add(probe.proof_validity)?;
        if now >= expires_at {
            return None;
        }
        let rate_bps = quic_capacity_receipt_rate_bps(probe.train_payload_bytes, proof_elapsed)?;
        let candidate = QuicCapacityProofCandidate {
            token: probe.token,
            train_bytes: probe.train_payload_bytes,
            sample_floor_bytes: probe.sample_floor_bytes,
            accounting_slack_bytes: probe
                .sample_floor_bytes
                .saturating_sub(probe.required_timed_carrier_bytes),
            warmup_bytes: probe.warmup_carrier_bytes,
            required_proof_bytes: probe.required_timed_carrier_bytes,
            written_bytes: probe.written_payload_bytes,
            written_data_frame_count: probe.written_data_frame_count,
            receipt_confirmed: true,
            received_bytes: probe.receipt_received_payload_bytes,
            proof_elapsed,
            rate_bps,
            accepted_at: receipt_at,
            expires_at,
            proof_validity: probe.proof_validity,
        };
        // Freeze rate and freshness on first sight. Repeated registry attempts
        // reuse this exact candidate instead of extending a delayed proof.
        self.pending_capacity_proof_candidate = Some(candidate);
        (now < expires_at).then_some(candidate)
    }

    fn accept_capacity_proof(
        &mut self,
        _metrics: &mut UdpPathMetrics,
        candidate: QuicCapacityProofCandidate,
    ) {
        debug_assert_ne!(
            self.last_accepted_capacity_probe_token,
            Some(candidate.token)
        );
        self.last_accepted_capacity_probe_token = Some(candidate.token);
        self.pending_capacity_proof_candidate = None;
    }

    fn terminal_capacity_probe_to_retire(
        &self,
        probe: Option<quic_carrier::CapacityProbeMetrics>,
        now: Instant,
    ) -> Option<u64> {
        let probe = probe?;
        match probe.phase {
            quic_carrier::CapacityProbePhase::Expired
            | quic_carrier::CapacityProbePhase::Aborted => Some(probe.token),
            quic_carrier::CapacityProbePhase::Proven => {
                if self.last_accepted_capacity_probe_token == Some(probe.token) {
                    return Some(probe.token);
                }
                match self.pending_capacity_proof_candidate {
                    Some(candidate) if candidate.token == probe.token => {
                        (now >= candidate.expires_at).then_some(probe.token)
                    }
                    // A terminal snapshot that cannot form a proof must not
                    // retain the exclusive carrier epoch indefinitely.
                    _ => Some(probe.token),
                }
            }
            quic_carrier::CapacityProbePhase::Writing
            | quic_carrier::CapacityProbePhase::Measuring
            | quic_carrier::CapacityProbePhase::ProvenDraining => None,
        }
    }

    fn retire_capacity_candidate(&mut self, token: u64) {
        if self
            .pending_capacity_proof_candidate
            .is_some_and(|candidate| candidate.token == token)
        {
            self.pending_capacity_proof_candidate = None;
        }
    }

    fn observe(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_carrier::CongestionMetrics,
        direction: u8,
    ) -> UdpPathMetrics {
        self.observe_at(stats, congestion, direction, Instant::now())
    }

    fn observe_at(
        &mut self,
        stats: quinn::ConnectionStats,
        congestion: quic_carrier::CongestionMetrics,
        direction: u8,
        now: Instant,
    ) -> UdpPathMetrics {
        let delivery_evidence_delta = congestion
            .delivery_evidence_written_bytes
            .saturating_sub(self.last_delivery_evidence_written_bytes);
        self.last_delivery_evidence_written_bytes = congestion.delivery_evidence_written_bytes;
        self.delivery_evidence_pending_ack_bytes = self
            .delivery_evidence_pending_ack_bytes
            .saturating_add(delivery_evidence_delta);

        if stats.path.rtt > Duration::ZERO {
            self.min_rtt = Some(
                self.min_rtt
                    .map_or(stats.path.rtt, |previous| previous.min(stats.path.rtt)),
            );
        }
        let rtt = stats.path.rtt.max(QUIC_TIMER_GRANULARITY);
        let rttvar = rtt / 4;
        let min_rtt = self.min_rtt.unwrap_or(rtt);
        let bulk_proof_freshness_horizon = quic_bulk_proof_freshness_horizon(rtt, rttvar);
        self.expire_stale_bulk_proof(now);
        let congestion_window = congestion.congestion_window.max(stats.path.cwnd);
        let carrier_capacity_known = congestion.pacing_rate_bps.is_some() || congestion_window > 0;
        let bytes_in_flight = congestion.bytes_in_flight.unwrap_or(0);
        let inflight_hi = if carrier_capacity_known {
            congestion_window.max(stats.path.current_mtu as u64) as usize
        } else {
            0
        };
        let startup_rate = default_path_rate_bps(UnderlayProtocol::Udp);
        let raw_pacing_rate = congestion.pacing_rate_bps.map(|rate| rate.max(1) as f64);
        let usable_pacing_rate = raw_pacing_rate.map(|rate| {
            if self.delivery_sample_count == 0 {
                rate.max(startup_rate)
            } else {
                rate
            }
        });
        let fallback_rate = usable_pacing_rate.unwrap_or_else(|| {
            if carrier_capacity_known {
                let cwnd_rate = inflight_hi as f64 * 8.0
                    / rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64());
                if self.delivery_sample_count == 0 {
                    cwnd_rate.max(startup_rate)
                } else {
                    cwnd_rate
                }
            } else {
                startup_rate
            }
        });
        let evidence_inflight_hi = if inflight_hi > 0 {
            inflight_hi as u64
        } else {
            (fallback_rate / 8.0 * rtt.as_secs_f64().max(QUIC_TIMER_GRANULARITY.as_secs_f64()))
                .ceil()
                .max(1.0) as u64
        };

        let newly_acked_bytes = congestion.newly_acked_bytes.unwrap_or(0);
        let non_app_limited_acked_bytes = congestion
            .non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(newly_acked_bytes);
        let timed_non_app_limited_acked_bytes = congestion
            .timed_non_app_limited_acked_bytes
            .unwrap_or(0)
            .min(non_app_limited_acked_bytes);
        let delivery_evidence_pending_before_ack = self.delivery_evidence_pending_ack_bytes;
        let delivery_evidence_newly_acked_bytes =
            newly_acked_bytes.min(delivery_evidence_pending_before_ack);
        let timed_non_app_limited_delivery_evidence_bytes =
            timed_non_app_limited_acked_bytes.min(delivery_evidence_newly_acked_bytes);
        let carrier_ack_elapsed = congestion
            .non_app_limited_ack_elapsed
            .filter(|elapsed| !elapsed.is_zero());
        let timed_non_app_limited_evidence =
            carrier_ack_elapsed.is_some() && timed_non_app_limited_delivery_evidence_bytes > 0;
        // A first/compressed zero-span ACK batch proves reachability but has no
        // carrier-clock denominator and therefore cannot enter the rate model.
        if delivery_evidence_newly_acked_bytes > 0 {
            self.ack_derived_data_seen = true;
            self.delivery_evidence_pending_ack_bytes = self
                .delivery_evidence_pending_ack_bytes
                .saturating_sub(delivery_evidence_newly_acked_bytes);
        }
        // Generic evidence counts product payload only. The connection-wide
        // pending/flight counters still include an exclusive capacity train so
        // scheduling cannot treat carrier debt as an empty path.
        let carrier_committed_bytes = self
            .delivery_evidence_pending_ack_bytes
            .max(congestion.pending_bytes)
            .max(bytes_in_flight);

        let confidence_sample_floor = QUIC_INITIAL_WINDOW_PACKETS as u64;
        let capacity_sample_cap = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES.saturating_div(2);
        let preconfidence_publish_floor = evidence_inflight_hi
            .max(PATH_OPEN_SCORE_BYTES as u64)
            .min(capacity_sample_cap);
        let durable_bulk_sample_floor = evidence_inflight_hi
            .max((PATH_OPEN_SCORE_BYTES as u64).saturating_mul(4))
            .min(capacity_sample_cap);
        let mut publishable_sample_bytes = if timed_non_app_limited_evidence {
            timed_non_app_limited_delivery_evidence_bytes
        } else {
            0
        };
        let mut publishable_sample_count = timed_non_app_limited_evidence
            .then_some(congestion.timed_non_app_limited_delivery_sample_count)
            .unwrap_or(0)
            .max(u64::from(publishable_sample_bytes > 0));
        let mut publishable_sample_elapsed = carrier_ack_elapsed.unwrap_or_default();
        if publishable_sample_bytes > 0 {
            self.pending_non_app_limited_sample_bytes = self
                .pending_non_app_limited_sample_bytes
                .saturating_add(publishable_sample_bytes);
            self.pending_non_app_limited_sample_count = self
                .pending_non_app_limited_sample_count
                .saturating_add(publishable_sample_count);
            self.pending_non_app_limited_sample_elapsed = self
                .pending_non_app_limited_sample_elapsed
                .saturating_add(publishable_sample_elapsed);
            let publish_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                durable_bulk_sample_floor
            };
            if self.pending_non_app_limited_sample_bytes < publish_floor {
                publishable_sample_bytes = 0;
                if self.delivery_evidence_pending_ack_bytes == 0 {
                    if self.delivery_sample_count > 0 {
                        let next_sample_bytes = self
                            .delivery_sample_bytes
                            .saturating_add(self.pending_non_app_limited_sample_bytes);
                        let candidate_sample_count = self
                            .delivery_sample_count
                            .saturating_add(self.pending_non_app_limited_sample_count);
                        let confidence_has_byte_volume = next_sample_bytes
                            >= evidence_inflight_hi.max(PATH_OPEN_SCORE_BYTES as u64);
                        self.delivery_sample_count = if self.delivery_sample_count
                            < confidence_sample_floor
                            && candidate_sample_count >= confidence_sample_floor
                            && !confidence_has_byte_volume
                        {
                            confidence_sample_floor.saturating_sub(1)
                        } else {
                            candidate_sample_count
                        };
                        self.delivery_sample_bytes = next_sample_bytes;
                    }
                    self.pending_non_app_limited_sample_bytes = 0;
                    self.pending_non_app_limited_sample_count = 0;
                    self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
                }
            } else {
                publishable_sample_bytes = self.pending_non_app_limited_sample_bytes;
                publishable_sample_count = self.pending_non_app_limited_sample_count;
                publishable_sample_elapsed = self.pending_non_app_limited_sample_elapsed;
                self.pending_non_app_limited_sample_bytes = 0;
                self.pending_non_app_limited_sample_count = 0;
                self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
            }
        } else if publishable_sample_bytes == 0 && self.delivery_evidence_pending_ack_bytes == 0 {
            self.pending_non_app_limited_sample_bytes = 0;
            self.pending_non_app_limited_sample_count = 0;
            self.pending_non_app_limited_sample_elapsed = Duration::ZERO;
        }

        let mut latest_delivery_sample_bytes = 0;
        let mut latest_delivery_sample_count = 0;
        let mut latest_carrier_ack_elapsed = None;
        let mut latest_rate_sample_elapsed = None;
        if publishable_sample_bytes > 0 {
            latest_delivery_sample_bytes = publishable_sample_bytes;
            latest_delivery_sample_count = publishable_sample_count;
            latest_carrier_ack_elapsed = Some(publishable_sample_elapsed);
            publishable_sample_elapsed = publishable_sample_elapsed.max(QUIC_TIMER_GRANULARITY);
            latest_rate_sample_elapsed = Some(publishable_sample_elapsed);
            let sample_rate = (publishable_sample_bytes as f64 * 8.0
                / publishable_sample_elapsed.as_secs_f64())
            .max(1.0);
            let delivery_evidence_floor = if self.delivery_sample_count == 0 {
                preconfidence_publish_floor
            } else {
                evidence_inflight_hi
            };
            let previous_sample_count = self.delivery_sample_count;
            let next_sample_bytes = self
                .delivery_sample_bytes
                .saturating_add(publishable_sample_bytes);
            // Carrier-timed fragments are aggregated into full transport
            // windows, so poll boundaries cannot create or refresh a proof.
            let refreshes_bulk_proof = publishable_sample_bytes >= durable_bulk_sample_floor;
            let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
            let current_rate = if self.delivery_sample_count < confidence_sample_floor {
                estimated_rate.max(fallback_rate)
            } else {
                estimated_rate
            };
            let bounded_sample = sample_rate.min(current_rate * BBR_DEFAULT_CWND_GAIN);
            let candidate_sample_count = self
                .delivery_sample_count
                .saturating_add(publishable_sample_count);
            let confidence_has_byte_volume =
                next_sample_bytes >= delivery_evidence_floor.max(PATH_OPEN_SCORE_BYTES as u64);
            let next_sample_count = if previous_sample_count < confidence_sample_floor
                && candidate_sample_count >= confidence_sample_floor
                && !confidence_has_byte_volume
            {
                confidence_sample_floor.saturating_sub(1)
            } else {
                candidate_sample_count
            };
            let establishes_measured_rate = previous_sample_count < confidence_sample_floor
                && next_sample_count >= confidence_sample_floor;
            self.delivery_sample_count = next_sample_count;
            self.delivery_sample_bytes = next_sample_bytes;
            if refreshes_bulk_proof {
                self.last_delivery_sample_at = Some(now);
                self.bulk_proof_expires_at = now.checked_add(bulk_proof_freshness_horizon);
                self.delivery_rate_bps = Some(match self.delivery_rate_bps {
                    Some(_) | None if establishes_measured_rate => bounded_sample,
                    Some(previous) if bounded_sample > previous => {
                        previous.mul_add(0.25, bounded_sample * 0.75)
                    }
                    // A stale overestimate can misplace a whole response flow,
                    // so full lower windows get the same 75% new-sample weight.
                    Some(previous) => previous.mul_add(0.25, bounded_sample * 0.75),
                    None => bounded_sample,
                });
            }
        }

        let bulk_proof_is_fresh = self
            .bulk_proof_expires_at
            .is_some_and(|expires_at| now < expires_at);
        // Bulk eligibility follows a recent full transport window; an idle
        // connection retains ACK reachability without retaining placement.
        let app_limited = !bulk_proof_is_fresh;

        let estimated_rate = self.delivery_rate_bps.unwrap_or(fallback_rate).max(1.0);
        let delivery_rate_bps = if self.delivery_sample_count < confidence_sample_floor {
            estimated_rate.max(fallback_rate)
        } else {
            estimated_rate
        };
        let pacing_rate_bps = usable_pacing_rate
            .unwrap_or(delivery_rate_bps)
            .max(delivery_rate_bps);
        let capacity_proof_candidate =
            self.capacity_proof_candidate(congestion.capacity_probe, now);
        UdpPathMetrics {
            direction,
            srtt: rtt,
            rttvar,
            min_rtt,
            min_rtt_observed: stats.path.rtt > Duration::ZERO,
            delivery_rate_bps,
            pacing_rate_bps,
            inflight_hi,
            bytes_in_flight: usize::try_from(bytes_in_flight).unwrap_or(usize::MAX),
            pending_bytes: usize::try_from(carrier_committed_bytes).unwrap_or(usize::MAX),
            loss_ppm: congestion.loss_ppm,
            ecn_ppm: congestion.ecn_ppm,
            app_limited,
            ack_derived_data_seen: self.ack_derived_data_seen,
            delivery_sample_count: self.delivery_sample_count,
            delivery_sample_bytes: self.delivery_sample_bytes,
            last_delivery_sample_at: self.last_delivery_sample_at,
            bulk_proof_expires_at: self.bulk_proof_expires_at,
            latest_delivery_sample_bytes,
            latest_delivery_sample_count,
            latest_carrier_ack_elapsed,
            latest_rate_sample_elapsed,
            capacity_proof_candidate,
            capacity_probe: congestion.capacity_probe,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics {
                newly_acked_bytes,
                non_app_limited_acked_bytes,
                timed_non_app_limited_acked_bytes,
                ack_elapsed: carrier_ack_elapsed.unwrap_or_default(),
                delivery_sample_count: congestion.delivery_sample_count,
                non_app_limited_sample_count: congestion.non_app_limited_delivery_sample_count,
                timed_non_app_limited_sample_count: congestion
                    .timed_non_app_limited_delivery_sample_count,
                carrier_app_limited: congestion.app_limited,
                delivery_evidence_written_delta: delivery_evidence_delta,
                delivery_evidence_newly_acked_bytes,
                delivery_evidence_pending_ack_bytes: self.delivery_evidence_pending_ack_bytes,
                pending_sample_bytes: self.pending_non_app_limited_sample_bytes,
                pending_sample_count: self.pending_non_app_limited_sample_count,
                pending_sample_elapsed: self.pending_non_app_limited_sample_elapsed,
            },
        }
    }
}

pub(super) async fn udp_path_read_frame(
    recv: &mut UdpPathRecvStream,
    codec_limits: CodecLimits,
) -> Result<Frame, RuntimeError> {
    Ok(quic_carrier::read_frame(&mut recv.stream, codec_limits).await?)
}

pub(super) async fn udp_path_write_frame(
    send: &mut UdpPathSendStream,
    frame: &Frame,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_carrier::write_frame(&mut send.stream, frame, codec_limits).await?;
    Ok(())
}

async fn udp_path_write_frames(
    send: &mut UdpPathSendStream,
    frames: &[Frame],
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_carrier::write_frames(&mut send.stream, frames, codec_limits).await?;
    Ok(())
}

async fn udp_path_write_capacity_probe(
    send: &mut UdpPathSendStream,
    probe: &QuicCapacityProbeCommand,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    // Only this entry point can turn a carrier-neutral command into a QUIC ACK
    // epoch; ordinary frame batching must never absorb capacity payloads.
    quic_carrier::write_capacity_probe(
        &mut send.stream,
        probe.path_id,
        quic_carrier::CapacityProbeSpec {
            token: probe.calibration_id,
            train_payload_bytes: probe.train_payload_bytes,
            sample_floor_bytes: probe.sample_floor_bytes,
            warmup_carrier_bytes: probe.warmup_carrier_bytes,
            required_timed_carrier_bytes: probe.required_timed_carrier_bytes,
            proof_validity: probe.proof_validity,
            expires_at: probe.expires_at,
        },
        mux_limits.max_payload_bytes,
        codec_limits,
    )
    .await?;
    Ok(())
}

async fn udp_path_write_capacity_receipt(
    send: &mut UdpPathSendStream,
    path_id: PathId,
    calibration_id: u64,
    received_payload_bytes: u64,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    quic_carrier::write_capacity_receipt(
        &mut send.stream,
        path_id,
        calibration_id,
        received_payload_bytes,
        codec_limits,
    )
    .await?;
    Ok(())
}

fn quic_capacity_command_drop_reason(
    probe: &QuicCapacityProbeCommand,
    now: Instant,
) -> Option<&'static str> {
    if !probe.ticket.is_current() {
        Some("ownership_invalidated")
    } else if now >= probe.expires_at {
        Some("deadline_elapsed_before_start")
    } else {
        None
    }
}

fn quic_capacity_start_rejection_reason(err: &RuntimeError) -> Option<&'static str> {
    match err {
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::InvalidCapacityProbe) => {
            Some("invalid_specification")
        }
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::CapacityProbeBusy) => {
            Some("carrier_epoch_busy")
        }
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::CapacityProbeNotIdle) => {
            Some("carrier_not_idle")
        }
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::CapacityProbeExpired) => {
            Some("carrier_deadline_elapsed")
        }
        _ => None,
    }
}

async fn flush_udp_frame_batch(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    frames.clear();
    Ok(())
}

async fn flush_udp_frame_batch_with_path_proofs(
    send: &mut UdpPathSendStream,
    frames: &mut Vec<Frame>,
    codec_limits: CodecLimits,
    path_proofs: &mut PathProofTracker,
) -> Result<(), RuntimeError> {
    if frames.is_empty() {
        return Ok(());
    }
    udp_path_write_frames(send, frames, codec_limits).await?;
    for frame in frames.iter() {
        path_proofs.record_sent_frame(frame);
    }
    frames.clear();
    Ok(())
}

pub(super) fn udp_path_finish_stream(send: &mut UdpPathSendStream) -> Result<(), RuntimeError> {
    Ok(quic_carrier::finish_stream(&mut send.stream)?)
}

// Product-level UDP reliable frame size. This is intentionally the same kind of
// BDP/service quantum used by TCP. Do not cap this to a QUIC packet train: doing
// so turns the carrier record size into the application pacing unit and
// underfeeds QUIC. QUIC-specific recordization is performed inside
// transport::quic_carrier while preserving this product quantum.
fn udp_path_max_stream_payload_bytes(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    quic_carrier::max_stream_payload_bytes(codec_limits)
        .min(mux_limits.max_reliable_relay_chunk_bytes)
        .max(1)
}

fn udp_reliable_stream_frame_queue(codec_limits: CodecLimits, mux_limits: MuxLimits) -> usize {
    reliable_stream_frame_queue_for_payload(
        mux_limits,
        udp_path_max_stream_payload_bytes(codec_limits, mux_limits),
    )
}

fn spawn_client_udp_path_metrics(
    runtime: ClientUdpPathSessionRuntime,
    connection: UdpPathConnection,
) {
    tokio::spawn(async move {
        let mut tracker = UdpPathMetricTracker::default();
        loop {
            if connection.is_closed() {
                return;
            }
            let Some(metrics) = connection.tx_metrics(&mut tracker, 1).await else {
                tokio::time::sleep(default_transport_pto()).await;
                continue;
            };
            if let Some(record) = runtime
                .health
                .lock()
                .expect("client QUIC UDP path health lock")
                .udp
                .get_mut(runtime.path_index)
            {
                record.mark_quic_path_metrics(metrics);
            }
            tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
        }
    });
}

fn quic_path_metrics_poll_interval(metrics: UdpPathMetrics) -> Duration {
    if metrics.capacity_probe.is_some_and(|probe| {
        matches!(
            probe.phase,
            quic_carrier::CapacityProbePhase::Writing
                | quic_carrier::CapacityProbePhase::Measuring
                | quic_carrier::CapacityProbePhase::ProvenDraining
                | quic_carrier::CapacityProbePhase::Proven
        )
    }) {
        // Receipt and retirement are short-lived control transitions. Poll at
        // quarter RTT, bounded by timer precision and QUIC's max ACK delay.
        return (metrics.srtt / 4).clamp(QUIC_TIMER_GRANULARITY, QUIC_MAX_ACK_DELAY);
    }
    if metrics.app_limited {
        transport_pto_from_ms(
            metrics.srtt.as_secs_f64() * 1000.0,
            metrics.rttvar.as_secs_f64() * 1000.0,
        )
    } else {
        (metrics.srtt / 2).max(QUIC_TIMER_GRANULARITY)
    }
}

pub(super) struct ClientUdpDatagramStream {
    pub(super) send: UdpPathSendStream,
    pub(super) frames: mpsc::Receiver<Result<Frame, RuntimeError>>,
    pub(super) runtime: ClientUdpPathSessionRuntime,
    pub(super) path_id: PathId,
}

pub(super) async fn bind_server_udp_endpoint(
    path: &PathSpec,
    context: &ServerPathContext,
) -> Result<UdpPathEndpoint, RuntimeError> {
    UdpPathEndpoint::bind_server(path, context).await
}

pub(super) async fn run_server_udp_listener(
    endpoint: UdpPathEndpoint,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    loop {
        let Some(connection) = endpoint.accept().await else {
            return Err(RuntimeError::Protocol("QUIC UDP path endpoint closed"));
        };
        let context = context.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_connection(connection, context).await {
                warn_unexpected_udp_runtime_error("server QUIC UDP path connection failed", &err);
            }
        });
    }
}

async fn connect_client_udp_path(
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<ClientUdpPathConnection, RuntimeError> {
    let remote_addr = resolve_first_socket_addr(&runtime.path).await?;
    let local_addr = if remote_addr.ip().is_loopback() {
        SocketAddr::new(remote_addr.ip(), 0)
    } else if remote_addr.is_ipv4() {
        SocketAddr::from(([0, 0, 0, 0], 0))
    } else {
        "[::]:0"
            .parse()
            .expect("static IPv6 unspecified socket addr")
    };
    let endpoint = UdpPathEndpoint::bind_client(&runtime.path, local_addr, runtime).await?;
    let connection = endpoint.connect(remote_addr).await?;
    perform_client_udp_path_handshake(&connection, runtime).await?;
    Ok(ClientUdpPathConnection {
        _endpoint: endpoint,
        connection,
    })
}

async fn perform_client_udp_path_handshake(
    connection: &UdpPathConnection,
    runtime: &ClientUdpPathSessionRuntime,
) -> Result<(), RuntimeError> {
    let (mut send, mut recv) = connection.open_bi().await?;
    let path_id = PathId(runtime.path_index as u16);
    let (session_hello, session_auth, path_join) = authenticated_path_join_frames_for_session(
        &runtime.security,
        &runtime.path,
        path_id,
        UnderlayProtocol::Udp,
        runtime.session_id,
    )?;
    udp_path_write_frame(&mut send, &session_hello, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &session_auth, runtime.codec_limits).await?;
    udp_path_write_frame(&mut send, &path_join, runtime.codec_limits).await?;
    udp_path_finish_stream(&mut send)?;

    let mut session_ready = false;
    let mut path_active = false;
    while !session_ready || !path_active {
        match udp_path_read_frame(&mut recv, runtime.codec_limits).await? {
            Frame::SessionReady => session_ready = true,
            Frame::PathStatus {
                status: crate::protocol::PathStatus::Active,
                ..
            } => path_active = true,
            Frame::PathStatus { .. } => {
                return Err(RuntimeError::Protocol(
                    "UDP path session did not become active",
                ));
            }
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected UDP path handshake frame",
                ));
            }
        }
    }
    Ok(())
}

async fn open_client_udp_stream_on_connection(
    connection: UdpPathConnection,
    stream_id: StreamId,
    target: TargetAddr,
    ingress: IngressKind,
    lane: FlowLane,
    options: UdpStreamOpenOptions,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ReliablePathStream, RuntimeError> {
    let UdpStreamOpenOptions {
        wait_for_accept,
        role,
    } = options;
    let (mut send, mut recv) = connection.open_bi().await?;
    let open = Frame::OpenStream {
        stream_id,
        target,
        ingress,
        outbound: OutboundPolicy::Direct,
        demand: stream_demand_hint_for_lane(lane),
        role,
    };
    udp_path_write_frame(&mut send, &open, runtime.codec_limits).await?;
    let accepted_max_offset = if wait_for_accept {
        Some(read_client_udp_stream_open_accept(&mut recv, stream_id, runtime.codec_limits).await?)
    } else {
        None
    };
    let max_offset = udp_stream_open_initial_max_offset(options, accepted_max_offset);
    let (commands, receivers) = reliable_path_command_channels(udp_path_command_queue(
        runtime.mux_limits,
        runtime.codec_limits,
    ));
    let stream_frame_queue =
        udp_reliable_stream_frame_queue(runtime.codec_limits, runtime.mux_limits);
    let (frames_tx, frames_rx) = mpsc::channel(stream_frame_queue);
    tokio::spawn(run_client_udp_stream(
        send,
        recv,
        stream_id,
        runtime.path_index,
        runtime.codec_limits,
        runtime.mux_limits,
        stream_frame_queue,
        runtime.health.clone(),
        receivers,
        frames_tx,
    ));
    Ok(ReliablePathStream {
        stream_id,
        max_offset,
        lane,
        underlay: UnderlayProtocol::Udp,
        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
            runtime.codec_limits,
            runtime.mux_limits,
        ),
        output: ReliablePathStreamOutput::fixed_with_snapshot(
            path_startup_snapshot(&runtime.path, runtime.path_index),
            commands,
            runtime.mux_limits,
        ),
        frames: frames_rx,
    })
}

async fn read_client_udp_stream_open_accept(
    recv: &mut UdpPathRecvStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
) -> Result<u64, RuntimeError> {
    loop {
        match udp_path_read_frame(recv, codec_limits).await? {
            Frame::StreamMaxData {
                stream_id: max_stream_id,
                max_offset,
            } if max_stream_id == stream_id => return Ok(max_offset),
            Frame::StreamReset {
                stream_id: reset_stream_id,
                reason,
            } if reset_stream_id == stream_id => return Err(RuntimeError::RemoteReset(reason)),
            Frame::PathStatus { .. } | Frame::SessionReady => {}
            Frame::SessionClose { reason } => return Err(RuntimeError::RemoteClosed(reason)),
            _ => {
                return Err(RuntimeError::Protocol(
                    "unexpected QUIC UDP path stream open frame",
                ));
            }
        }
    }
}

fn udp_stream_open_initial_max_offset(
    options: UdpStreamOpenOptions,
    accepted_max_offset: Option<u64>,
) -> u64 {
    if options.wait_for_accept {
        accepted_max_offset.unwrap_or(0)
    } else {
        0
    }
}

async fn open_client_udp_datagram_stream(
    connection: UdpPathConnection,
    runtime: ClientUdpPathSessionRuntime,
) -> Result<ClientUdpDatagramStream, RuntimeError> {
    let (send, recv) = connection.open_bi().await?;
    let frames = spawn_quic_path_reader(recv, runtime.codec_limits, runtime.stream_frame_queue);
    Ok(ClientUdpDatagramStream {
        send,
        frames,
        path_id: PathId(runtime.path_index as u16),
        runtime,
    })
}

fn spawn_quic_path_reader(
    mut recv: UdpPathRecvStream,
    codec_limits: CodecLimits,
    queue_size: usize,
) -> mpsc::Receiver<Result<Frame, RuntimeError>> {
    let (frames_tx, frames_rx) = mpsc::channel(queue_size);
    tokio::spawn(async move {
        loop {
            let frame = match udp_path_read_frame(&mut recv, codec_limits).await {
                Ok(frame) => Ok(frame),
                Err(err) if udp_path_frame_finished(&err) => {
                    Err(RuntimeError::ReliablePathSessionClosed)
                }
                Err(err) => Err(err),
            };
            let done = frame.is_err();
            if frames_tx.send(frame).await.is_err() || done {
                return;
            }
        }
    });
    frames_rx
}

async fn run_client_udp_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_id: StreamId,
    path_index: usize,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    reader_queue_size: usize,
    health: Arc<Mutex<ClientPathHealth>>,
    mut commands: ReliablePathCommandReceivers,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
) {
    let mut carrier_frames = spawn_quic_path_reader(recv, codec_limits, reader_queue_size);
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();
    let mut capacity_receive = CapacityReceiveTracker::new(
        reliable_quic_capacity_calibration_session_limit_bytes(mux_limits),
    );
    let path_id = PathId(path_index as u16);
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands);
        if !command_may_recv {
            let _ = udp_path_finish_stream(&mut send);
            return;
        }
        if let Some(command) = try_recv_reliable_path_priority_command(&mut commands) {
            let result = drain_client_udp_stream_commands(
                command,
                &mut commands,
                &mut send,
                stream_id,
                codec_limits,
                mux_limits,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            match result {
                Ok(false) => {}
                Ok(true) => return,
                Err(err) => {
                    let _ = frames.send(Err(err)).await;
                    return;
                }
            }
            continue;
        }
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::Ping { nonce })) => {
                        if let Err(err) = udp_path_write_frame(&mut send, &Frame::Pong { nonce }, codec_limits).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        if let Err(err) = udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            codec_limits,
                        ).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(record) = health
                                .lock()
                                .expect("client path health lock")
                                .udp
                                .get_mut(path_index)
                        {
                            record.mark_path_proof_success(observation);
                        }
                    }
                    Some(Ok(Frame::PathCapacityData {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload,
                    })) => {
                        if capacity_path_id != path_id
                            || capacity_receive
                                .record_data(calibration_id, payload.len())
                                .is_err()
                        {
                            let _ = frames.send(Err(RuntimeError::Protocol(
                                "invalid QUIC capacity data epoch",
                            ))).await;
                            return;
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "quic_capacity_data_received",
                            format_args!(
                                "role=client path_id={} stream_id={} calibration_id={} payload_bytes={}",
                                path_id.0,
                                stream_id.0,
                                calibration_id,
                                payload.len(),
                            ),
                        );
                    }
                    Some(Ok(Frame::PathCapacityFinish {
                        path_id: capacity_path_id,
                        calibration_id,
                        payload_bytes,
                    })) => {
                        if capacity_path_id != path_id {
                            let _ = frames.send(Err(RuntimeError::Protocol(
                                "QUIC capacity finish path mismatch",
                            ))).await;
                            return;
                        }
                        let received_payload_bytes = match capacity_receive
                            .finish(calibration_id, payload_bytes)
                        {
                            Ok(bytes) => bytes,
                            Err(err) => {
                                let _ = frames.send(Err(err)).await;
                                return;
                            }
                        };
                        if let Err(err) = udp_path_write_capacity_receipt(
                            &mut send,
                            path_id,
                            calibration_id,
                            received_payload_bytes,
                            codec_limits,
                        ).await {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "quic_capacity_receipt",
                            format_args!(
                                "role=client phase=sent path_id={} stream_id={} calibration_id={} received_payload_bytes={}",
                                path_id.0,
                                stream_id.0,
                                calibration_id,
                                received_payload_bytes,
                            ),
                        );
                    }
                    Some(Ok(Frame::PathCapacityReceipt { .. })) => {
                        let _ = frames.send(Err(RuntimeError::Protocol(
                            "client QUIC path received server capacity receipt",
                        ))).await;
                        return;
                    }
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(frame @ Frame::PathStatus { .. })) => {
                        if frames.send(Ok(frame)).await.is_err() {
                            return;
                        }
                    }
                    Some(Ok(Frame::SessionClose { reason })) => {
                        let _ = frames.send(Err(RuntimeError::RemoteClosed(reason))).await;
                        return;
                    }
                    Some(Ok(_)) => {
                        let _ = frames
                            .send(Err(RuntimeError::Protocol("unexpected QUIC UDP path reliable stream frame")))
                            .await;
                        return;
                    }
                    Some(Err(err)) => {
                        let _ = frames.send(Err(err)).await;
                        return;
                    }
                    None => {
                        let _ = frames.send(Err(RuntimeError::ReliablePathSessionClosed)).await;
                        return;
                    }
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands) {
                    let result = drain_client_udp_stream_commands(
                        command,
                        &mut commands,
                        &mut send,
                        stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        )
                    .await;
                    match result {
                        Ok(false) => {}
                        Ok(true) => return,
                        Err(err) => {
                            let _ = frames.send(Err(err)).await;
                            return;
                        }
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_client_udp_stream_commands(
                            command,
                            &mut commands,
                            &mut send,
                            stream_id,
                            codec_limits,
                            mux_limits,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
                        match result {
                            Ok(false) => {}
                            Ok(true) => return,
                            Err(err) => {
                                let _ = frames.send(Err(err)).await;
                                return;
                            }
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

async fn drain_client_udp_stream_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    stream_id: StreamId,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client QUIC path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch_with_path_proofs(
                        send,
                        pending_frames,
                        codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            stream_id.0,
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client QUIC path received server response capacity command",
                ));
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "client QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "client QUIC UDP path stream received open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch_with_path_proofs(send, pending_frames, codec_limits, path_proofs)
                .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=client underlay=Udp stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}

async fn handle_server_udp_connection(
    connection: UdpPathConnection,
    context: ServerPathContext,
) -> Result<(), RuntimeError> {
    let (session_id, path_id, capabilities) =
        accept_server_udp_path_handshake(&connection, &context).await?;
    let path_registration =
        context
            .reliable_streams
            .register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
    spawn_server_quic_path_metrics(
        context.clone(),
        path_registration.clone(),
        connection.clone(),
    );
    loop {
        let (send, recv) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(err) => return Err(err),
        };
        let context = context.clone();
        let path_registration = path_registration.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_server_udp_bidi_stream(
                send,
                recv,
                context,
                session_id,
                path_id,
                path_registration,
                capabilities,
            )
            .await
            {
                warn_unexpected_udp_runtime_error("server QUIC UDP path stream failed", &err);
            }
        });
    }
}

fn spawn_server_quic_path_metrics(
    context: ServerPathContext,
    path_registration: ServerCarrierPathRegistration,
    connection: UdpPathConnection,
) {
    tokio::spawn(async move {
        let path_id = path_registration.path_id();
        #[cfg(feature = "lab-diagnostics")]
        let path_instance_id = path_registration.path_instance_id();
        #[cfg(feature = "lab-diagnostics")]
        let session_id = path_registration.session_id();
        let mut tracker = UdpPathMetricTracker::default();
        #[cfg(feature = "lab-diagnostics")]
        let mut last_metrics_poll_at = None;
        loop {
            if connection.is_closed() {
                return;
            }
            let Some(mut metrics) = connection.tx_metrics(&mut tracker, 2).await else {
                tokio::time::sleep(default_transport_pto()).await;
                continue;
            };
            #[cfg(feature = "lab-diagnostics")]
            let metrics_poll_at = Instant::now();
            #[cfg(feature = "lab-diagnostics")]
            let poll_elapsed = last_metrics_poll_at
                .replace(metrics_poll_at)
                .map(|previous| metrics_poll_at.saturating_duration_since(previous))
                .unwrap_or_default();
            #[cfg(feature = "lab-diagnostics")]
            log_quic_ack_poll_diagnostics(
                session_id,
                path_id,
                path_instance_id,
                metrics,
                poll_elapsed,
            );

            let capacity_proof_accepted = metrics.capacity_proof_candidate.is_some_and(|candidate| {
                let proof_metrics = path_metrics_from_quic_capacity_proof(
                    path_id,
                    metrics,
                    candidate,
                );
                if context.reliable_streams.record_local_quic_capacity_proof(
                    &path_registration,
                    proof_metrics,
                    candidate,
                ) {
                    tracker.quic.accept_capacity_proof(&mut metrics, candidate);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "quic_capacity_proof",
                        format_args!(
                            "phase=accepted session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} written_data_frame_count={} received_bytes={} proof_elapsed_us={} rate_bps={} proof_validity_ms={}",
                            session_id.0,
                            path_id.0,
                            path_instance_id.as_u64(),
                            candidate.token,
                            candidate.train_bytes,
                            candidate.sample_floor_bytes,
                            candidate.warmup_bytes,
                            candidate.required_proof_bytes,
                            candidate.written_data_frame_count,
                            candidate.received_bytes,
                            candidate.proof_elapsed.as_micros(),
                            candidate.rate_bps,
                            candidate.proof_validity.as_millis(),
                        ),
                    );
                    true
                } else {
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "quic_capacity_proof",
                        format_args!(
                            "phase=rejected session_id={} path_id={} path_instance_id={} calibration_id={} train_bytes={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} written_data_frame_count={} received_bytes={} proof_elapsed_us={} rate_bps={}",
                            session_id.0,
                            path_id.0,
                            path_instance_id.as_u64(),
                            candidate.token,
                            candidate.train_bytes,
                            candidate.sample_floor_bytes,
                            candidate.warmup_bytes,
                            candidate.required_proof_bytes,
                            candidate.written_data_frame_count,
                            candidate.received_bytes,
                            candidate.proof_elapsed.as_micros(),
                            candidate.rate_bps,
                        ),
                    );
                    false
                }
            });
            if let Some(token) = tracker
                .quic
                .terminal_capacity_probe_to_retire(metrics.capacity_probe, Instant::now())
            {
                let _retired = connection.retire_capacity_probe(token);
                tracker.quic.retire_capacity_candidate(token);
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "quic_capacity_probe_retired",
                    format_args!(
                        "session_id={} path_id={} path_instance_id={} calibration_id={} proof_accepted={} carrier_retired={}",
                        session_id.0,
                        path_id.0,
                        path_instance_id.as_u64(),
                        token,
                        capacity_proof_accepted,
                        _retired,
                    ),
                );
            }
            if quic_path_metrics_should_publish_local_sender(metrics) {
                #[cfg(feature = "lab-diagnostics")]
                if let (Some(carrier_elapsed), Some(rate_elapsed)) = (
                    metrics.latest_carrier_ack_elapsed,
                    metrics.latest_rate_sample_elapsed,
                ) {
                    let raw_rate_bps = (metrics.latest_delivery_sample_bytes as f64 * 8.0
                        / rate_elapsed.as_secs_f64())
                    .round() as u64;
                    lab_diagnostic(
                        "quic_carrier_rate_sample",
                        format_args!(
                            "session_id={} path_id={} path_instance_id={} direction={} rate_source=quic_send_ack_max sample_bytes={} sample_count={} carrier_elapsed_us={} sample_elapsed_us={} raw_rate_bps={} published_rate_bps={} poll_elapsed_us={} total_sample_count={} total_sample_bytes={} app_limited={}",
                            session_id.0,
                            path_id.0,
                            path_instance_id.as_u64(),
                            metrics.direction,
                            metrics.latest_delivery_sample_bytes,
                            metrics.latest_delivery_sample_count,
                            carrier_elapsed.as_micros(),
                            rate_elapsed.as_micros(),
                            raw_rate_bps,
                            metrics.delivery_rate_bps.round() as u64,
                            poll_elapsed.as_micros(),
                            metrics.delivery_sample_count,
                            metrics.delivery_sample_bytes,
                            metrics.app_limited,
                        ),
                    );
                }
                if !capacity_proof_accepted {
                    context.reliable_streams.record_local_path_metrics(
                        &path_registration,
                        path_metrics_from_quic_path(path_id, metrics),
                    );
                }
            }
            tokio::time::sleep(quic_path_metrics_poll_interval(metrics)).await;
        }
    });
}

fn quic_path_metrics_should_publish_local_sender(metrics: UdpPathMetrics) -> bool {
    metrics.delivery_sample_count > 0 || metrics.ack_derived_data_seen
}

#[cfg(feature = "lab-diagnostics")]
fn log_quic_ack_poll_diagnostics(
    session_id: SessionId,
    path_id: PathId,
    path_instance_id: ServerCarrierPathInstanceId,
    metrics: UdpPathMetrics,
    poll_elapsed: Duration,
) {
    let ack = metrics.ack_poll;
    if ack.newly_acked_bytes > 0
        || ack.delivery_evidence_written_delta > 0
        || ack.pending_sample_bytes > 0
        || metrics.capacity_probe.is_some()
    {
        lab_diagnostic(
            "quic_carrier_ack_poll",
            format_args!(
                "session_id={} path_id={} path_instance_id={} direction={} poll_elapsed_us={} newly_acked_bytes={} non_app_limited_acked_bytes={} timed_non_app_limited_acked_bytes={} ack_elapsed_us={} sample_count={} non_app_limited_sample_count={} timed_non_app_limited_sample_count={} carrier_app_limited={} evidence_written_delta={} evidence_newly_acked_bytes={} evidence_pending_ack_bytes={} pending_sample_bytes={} pending_sample_count={} pending_sample_elapsed_us={} proof_expires_in_us={}",
                session_id.0,
                path_id.0,
                path_instance_id.as_u64(),
                metrics.direction,
                poll_elapsed.as_micros(),
                ack.newly_acked_bytes,
                ack.non_app_limited_acked_bytes,
                ack.timed_non_app_limited_acked_bytes,
                ack.ack_elapsed.as_micros(),
                ack.delivery_sample_count,
                ack.non_app_limited_sample_count,
                ack.timed_non_app_limited_sample_count,
                ack.carrier_app_limited,
                ack.delivery_evidence_written_delta,
                ack.delivery_evidence_newly_acked_bytes,
                ack.delivery_evidence_pending_ack_bytes,
                ack.pending_sample_bytes,
                ack.pending_sample_count,
                ack.pending_sample_elapsed.as_micros(),
                metrics
                    .bulk_proof_expires_at
                    .map(|expires_at| expires_at
                        .saturating_duration_since(Instant::now())
                        .as_micros())
                    .unwrap_or(0),
            ),
        );
    }
    if let Some(probe) = metrics.capacity_probe {
        let now = Instant::now();
        lab_diagnostic(
            "quic_capacity_ack_poll",
            format_args!(
                "session_id={} path_id={} path_instance_id={} direction={} calibration_id={} phase={:?} write_committed={} train_bytes={} written_bytes={} written_data_frame_count={} sample_floor_bytes={} warmup_bytes={} required_proof_bytes={} native_started_clean={} native_total_acked_bytes={} native_total_ack_count={} native_warmup_acked_bytes={} native_warmup_ack_count={} native_measurement_acked_bytes={} native_measurement_ack_count={} native_timed_measurement_acked_bytes={} native_timed_measurement_ack_count={} native_app_limited_acked_bytes={} native_app_limited_ack_count={} native_timed_elapsed_us={} native_proved_age_us={} receipt_received_bytes={} receipt_elapsed_us={} receipt_rtt_us={} receipt_age_us={} last_authoritative_bif_bytes={} last_authoritative_bif_age_us={} last_authoritative_sent_watermark={} receipt_frozen_sent_watermark={} current_sent_watermark={} proof_validity_ms={} proved_age_us={} attempt_remaining_us={} candidate_emitted={}",
                session_id.0,
                path_id.0,
                path_instance_id.as_u64(),
                metrics.direction,
                probe.token,
                probe.phase,
                probe.write_committed,
                probe.train_payload_bytes,
                probe.written_payload_bytes,
                probe.written_data_frame_count,
                probe.sample_floor_bytes,
                probe.warmup_carrier_bytes,
                probe.required_timed_carrier_bytes,
                probe.started_clean,
                probe.total_acked_carrier_bytes,
                probe.total_ack_sample_count,
                probe.warmup_acked_carrier_bytes,
                probe.warmup_ack_sample_count,
                probe.measurement_acked_carrier_bytes,
                probe.measurement_ack_sample_count,
                probe.timed_measurement_acked_carrier_bytes,
                probe.timed_measurement_ack_sample_count,
                probe.app_limited_acked_carrier_bytes,
                probe.app_limited_ack_sample_count,
                probe
                    .timed_measurement_ack_elapsed
                    .unwrap_or_default()
                    .as_micros(),
                probe
                    .native_proved_at
                    .map(|proved_at| now.saturating_duration_since(proved_at).as_micros())
                    .unwrap_or(0),
                probe.receipt_received_payload_bytes,
                probe.receipt_elapsed.unwrap_or_default().as_micros(),
                probe.receipt_rtt.unwrap_or_default().as_micros(),
                probe
                    .receipt_at
                    .map(|receipt_at| now.saturating_duration_since(receipt_at).as_micros())
                    .unwrap_or(0),
                probe.last_authoritative_in_flight.unwrap_or(0),
                probe
                    .last_authoritative_in_flight_at
                    .map(|observed_at| now.saturating_duration_since(observed_at).as_micros())
                    .unwrap_or(0),
                probe.last_authoritative_sent_watermark.unwrap_or(0),
                probe.receipt_frozen_sent_watermark.unwrap_or(0),
                probe.current_sent_watermark,
                probe.proof_validity.as_millis(),
                probe
                    .proved_at
                    .map(|proved_at| now.saturating_duration_since(proved_at).as_micros())
                    .unwrap_or(0),
                probe.expires_at.saturating_duration_since(now).as_micros(),
                metrics.capacity_proof_candidate.is_some(),
            ),
        );
    }
}

fn path_metrics_from_quic_path(path_id: PathId, metrics: UdpPathMetrics) -> PathMetrics {
    PathMetrics {
        path_id,
        underlay: UnderlayProtocol::Udp,
        direction: match metrics.direction {
            1 => PathMetricDirection::ClientToServer,
            2 => PathMetricDirection::ServerToClient,
            _ => PathMetricDirection::ServerToClient,
        },
        metric_epoch: metric_epoch_now(),
        metric_age_us: metrics
            .last_delivery_sample_at
            .map(|seen| {
                let micros = Instant::now().saturating_duration_since(seen).as_micros();
                u32::try_from(micros).unwrap_or(u32::MAX)
            })
            .unwrap_or(0),
        min_rtt_us: duration_to_micros_u32(metrics.min_rtt),
        srtt_us: duration_to_micros_u32(metrics.srtt),
        rttvar_us: duration_to_micros_u32(metrics.rttvar),
        jitter_us: duration_to_micros_u32(metrics.rttvar),
        delivery_rate_bps: metrics.delivery_rate_bps.max(1.0).round() as u64,
        pacing_rate_bps: metrics.pacing_rate_bps.max(1.0).round() as u64,
        loss_ppm: metrics.loss_ppm.unwrap_or(0),
        ecn_ppm: metrics.ecn_ppm.unwrap_or(0),
        loss_observed: metrics.loss_ppm.is_some(),
        ecn_observed: metrics.ecn_ppm.is_some(),
        bytes_in_flight: metrics.bytes_in_flight as u64,
        queue_bytes: metrics
            .pending_bytes
            .saturating_sub(metrics.bytes_in_flight) as u64,
        inflight_limit_bytes: metrics.inflight_hi as u64,
        inflight_hi_bytes: metrics.inflight_hi as u64,
        confidence_ppm: ratio_to_ppm(
            (metrics.delivery_sample_count as f64 / QUIC_INITIAL_WINDOW_PACKETS as f64)
                .clamp(0.0, 1.0),
        ),
        app_limited: metrics.app_limited,
        has_ack_derived_data_sample: metrics.ack_derived_data_seen,
        data_sample_count: u32::try_from(metrics.delivery_sample_count).unwrap_or(u32::MAX),
        data_sample_bytes: metrics.delivery_sample_bytes,
    }
}

fn path_metrics_from_quic_capacity_proof(
    path_id: PathId,
    metrics: UdpPathMetrics,
    _candidate: QuicCapacityProofCandidate,
) -> PathMetrics {
    path_metrics_from_quic_path(path_id, metrics)
}

fn duration_to_micros_u32(duration: Duration) -> u32 {
    u32::try_from(duration.as_micros()).unwrap_or(u32::MAX)
}

async fn accept_server_udp_path_handshake(
    connection: &UdpPathConnection,
    context: &ServerPathContext,
) -> Result<(SessionId, PathId, PathCapabilities), RuntimeError> {
    let (mut send, mut recv) = connection.accept_bi().await?;
    let session_id = match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionHello { session_id } => session_id,
        _ => {
            return Err(RuntimeError::Protocol(
                "expected QUIC UDP path SESSION_HELLO",
            ));
        }
    };
    let authenticator = SessionAuthenticator::new(context.security.secret.as_bytes())?;
    let now_unix_secs = current_unix_secs()?;
    let auth_freshness_window_secs = context.security.auth_freshness_window.as_secs();
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::SessionAuth {
            session_id: auth_session_id,
            nonce,
            issued_at_unix_secs,
            auth_tag,
        } if auth_session_id == session_id
            && authenticator.verify_session_auth(SessionAuthCheck {
                session_id,
                nonce,
                issued_at_unix_secs,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            }) => {}
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path SESSION_AUTH")),
    }
    let (path_id, capabilities) = match udp_path_read_frame(&mut recv, context.codec_limits).await?
    {
        Frame::PathJoin {
            session_id: join_session_id,
            path_id,
            underlay,
            nonce,
            issued_at_unix_secs,
            capabilities,
            auth_tag,
        } if join_session_id == session_id
            && underlay == UnderlayProtocol::Udp
            && authenticator.verify_path_join(PathJoinAuthCheck {
                session_id,
                path_id,
                underlay,
                nonce,
                issued_at_unix_secs,
                capabilities,
                tag: auth_tag,
                now_unix_secs,
                freshness_window_secs: auth_freshness_window_secs,
            })
            && context.accept_path_join_nonce(session_id, path_id, underlay, nonce) =>
        {
            (path_id, capabilities)
        }
        _ => return Err(RuntimeError::Protocol("invalid QUIC UDP path PATH_JOIN")),
    };

    udp_path_write_frame(&mut send, &Frame::SessionReady, context.codec_limits).await?;
    udp_path_write_frame(
        &mut send,
        &Frame::PathStatus {
            path_id,
            status: crate::protocol::PathStatus::Active,
            capabilities,
        },
        context.codec_limits,
    )
    .await?;
    udp_path_finish_stream(&mut send)?;
    Ok((session_id, path_id, capabilities))
}

async fn handle_server_udp_bidi_stream(
    mut send: UdpPathSendStream,
    mut recv: UdpPathRecvStream,
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    capabilities: PathCapabilities,
) -> Result<(), RuntimeError> {
    match udp_path_read_frame(&mut recv, context.codec_limits).await? {
        Frame::OpenStream {
            stream_id,
            target,
            demand,
            role,
            ..
        } => {
            let lane = flow_lane_from_stream_demand_hint(demand);
            handle_server_udp_reliable_stream(
                send,
                recv,
                context,
                ServerUdpReliableStreamContext {
                    session_id,
                    path_id,
                    path_registration,
                    capabilities,
                    stream_id,
                    target,
                    lane,
                    role,
                },
            )
            .await
        }
        Frame::OpenDatagramFlow {
            flow_id, target, ..
        } => {
            handle_server_udp_datagram_stream(
                send,
                recv,
                context,
                ServerUdpDatagramStreamContext {
                    session_id,
                    flow_id,
                    target,
                    lane: FlowLane::RealtimeDatagram,
                },
            )
            .await
        }
        Frame::Ping { nonce } => {
            udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
            udp_path_finish_stream(&mut send)?;
            Ok(())
        }
        _ => Err(RuntimeError::Protocol(
            "unexpected first QUIC UDP path stream frame",
        )),
    }
}

struct ServerUdpReliableStreamContext {
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    capabilities: PathCapabilities,
    stream_id: StreamId,
    target: TargetAddr,
    lane: FlowLane,
    role: StreamOpenRole,
}

struct ServerUdpReliableOutputDetachGuard {
    registry: Arc<ServerReliableStreamRegistry>,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
    commands: ReliablePathCommandSender,
}

impl Drop for ServerUdpReliableOutputDetachGuard {
    fn drop(&mut self) {
        self.registry.detach_path(
            self.session_id,
            self.stream_id,
            UnderlayProtocol::Udp,
            self.path_id,
            &self.commands,
        );
    }
}

async fn handle_server_udp_reliable_stream(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpReliableStreamContext,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamContext {
        session_id,
        path_id,
        path_registration,
        capabilities,
        stream_id,
        target,
        lane,
        role,
    } = stream_context;
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Tcp)?;
    let duplicate_open_target = target.clone();
    let (commands_tx, commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let _output_detach_guard = ServerUdpReliableOutputDetachGuard {
        registry: context.reliable_streams.clone(),
        session_id,
        stream_id,
        path_id,
        commands: commands_tx.clone(),
    };
    match context.reliable_streams.open_or_attach(
        ServerReliableStreamOpenRequest {
            session_id,
            stream_id,
            target: &target,
            lane,
            attachment: ServerReliablePathAttachment {
                path_registration: path_registration.clone(),
                commands: commands_tx.clone(),
                max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                    context.codec_limits,
                    context.mux_limits,
                ),
                role,
                initial_metrics: context.local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
            },
        },
        context.mux_limits,
        context.max_reliable_streams,
    )? {
        ServerReliableStreamOpen::New(stream) => {
            let stream_context = context.clone();
            let target = target.clone();
            tokio::spawn(async move {
                if let Err(err) =
                    run_server_tcp_stream(stream_context, session_id, stream, target).await
                {
                    eprintln!("warning: server reliable stream failed: {err}");
                }
            });
        }
        ServerReliableStreamOpen::Existing => {
            context
                .reliable_streams
                .route_frame(
                    session_id,
                    stream_id,
                    Frame::PathStatus {
                        path_id,
                        status: crate::protocol::PathStatus::Active,
                        capabilities,
                    },
                )
                .await?;
            udp_path_write_frame(
                &mut send,
                &Frame::StreamMaxData {
                    stream_id,
                    max_offset: reliable_stream_initial_advertised_window_bytes(
                        UnderlayProtocol::Udp,
                        lane,
                        context.mux_limits,
                    ),
                },
                context.codec_limits,
            )
            .await?;
        }
        ServerReliableStreamOpen::DuplicateLiveIgnored => {
            let _ = udp_path_finish_stream(&mut send);
            return Ok(());
        }
        ServerReliableStreamOpen::Rejected => {
            udp_path_write_frame(
                &mut send,
                &Frame::StreamReset {
                    stream_id,
                    reason: ResetReason::Refused,
                },
                context.codec_limits,
            )
            .await?;
            return Ok(());
        }
    }
    run_server_udp_reliable_stream_loop(
        send,
        recv,
        ServerUdpReliableStreamLoop {
            context,
            session_id,
            path_id,
            path_registration,
            capabilities,
            stream_id,
            target: duplicate_open_target,
            lane,
            role,
            commands_tx,
            commands_rx,
        },
    )
    .await
}

struct ServerUdpReliableStreamLoop {
    context: ServerPathContext,
    session_id: SessionId,
    path_id: PathId,
    path_registration: ServerCarrierPathRegistration,
    capabilities: PathCapabilities,
    stream_id: StreamId,
    target: TargetAddr,
    lane: FlowLane,
    role: StreamOpenRole,
    commands_tx: ReliablePathCommandSender,
    commands_rx: ReliablePathCommandReceivers,
}

#[allow(clippy::too_many_arguments)]
fn confirm_server_quic_capacity_receipt(
    send: &UdpPathSendStream,
    _session_id: SessionId,
    path_id: PathId,
    _path_instance_id: ServerCarrierPathInstanceId,
    _stream_id: StreamId,
    receipt_path_id: PathId,
    calibration_id: u64,
    received_payload_bytes: u64,
) -> Result<(), RuntimeError> {
    if receipt_path_id != path_id
        || calibration_id == 0
        || received_payload_bytes == 0
        || !send.connection.confirm_capacity_probe_receipt(
            calibration_id,
            received_payload_bytes,
            Instant::now(),
        )
    {
        return Err(RuntimeError::Protocol("invalid QUIC capacity receipt"));
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "quic_capacity_receipt",
        format_args!(
            "role=server phase=confirmed session_id={} path_id={} path_instance_id={} stream_id={} calibration_id={} received_payload_bytes={}",
            _session_id.0,
            path_id.0,
            _path_instance_id.as_u64(),
            _stream_id.0,
            calibration_id,
            received_payload_bytes,
        ),
    );
    Ok(())
}

async fn run_server_udp_reliable_stream_loop(
    mut send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    stream_context: ServerUdpReliableStreamLoop,
) -> Result<(), RuntimeError> {
    let ServerUdpReliableStreamLoop {
        context,
        session_id,
        path_id,
        path_registration,
        capabilities,
        stream_id,
        target,
        lane,
        role: _role,
        commands_tx,
        mut commands_rx,
    } = stream_context;
    let carrier_frame_queue =
        udp_reliable_stream_frame_queue(context.codec_limits, context.mux_limits);
    let mut carrier_frames =
        spawn_quic_path_reader(recv, context.codec_limits, carrier_frame_queue);
    let mut deferred_capacity_frames = std::collections::VecDeque::<Frame>::new();
    let mut pending_frames = Vec::<Frame>::new();
    let mut path_proofs = PathProofTracker::default();

    loop {
        // Receipt confirmation releases the connection-wide writer gate. This
        // task owns that receipt, so awaiting any ordinary write here would
        // self-deadlock until the probe fail-closed the whole QUIC connection.
        if send.connection.capacity_probe_active() {
            let release_connection = send.connection.clone();
            tokio::select! {
                biased;
                frame = carrier_frames.recv() => {
                    match frame {
                        Some(Ok(Frame::PathCapacityReceipt {
                            path_id: receipt_path_id,
                            calibration_id,
                            received_payload_bytes,
                        })) => {
                            confirm_server_quic_capacity_receipt(
                                &send,
                                session_id,
                                path_id,
                                path_registration.path_instance_id(),
                                stream_id,
                                receipt_path_id,
                                calibration_id,
                                received_payload_bytes,
                            )?;
                        }
                        Some(Ok(frame)) => {
                            if deferred_capacity_frames.len() >= carrier_frame_queue {
                                return Err(RuntimeError::Protocol(
                                    "QUIC capacity receipt defer queue exceeded",
                                ));
                            }
                            deferred_capacity_frames.push_back(frame);
                        }
                        Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                            context.reliable_streams.detach_path(
                                session_id,
                                stream_id,
                                UnderlayProtocol::Udp,
                                path_id,
                                &commands_tx,
                            );
                            return Ok(());
                        }
                        Some(Err(err)) => return Err(err),
                    }
                }
                _ = release_connection.wait_for_capacity_probe_release() => {}
            }
            continue;
        }

        let replaying_capacity_frame = !deferred_capacity_frames.is_empty();
        let command_may_recv =
            !replaying_capacity_frame && !reliable_path_receivers_closed(&commands_rx);
        if !replaying_capacity_frame
            && let Some(command) = try_recv_reliable_path_priority_command(&mut commands_rx)
        {
            let result = drain_server_udp_reliable_commands(
                command,
                &mut commands_rx,
                &mut send,
                &context,
                session_id,
                stream_id,
                path_id,
                path_registration.path_instance_id(),
                &commands_tx,
                &mut pending_frames,
                &mut path_proofs,
            )
            .await;
            if result? {
                return Ok(());
            }
            continue;
        }
        let replay_frame = deferred_capacity_frames.pop_front();
        tokio::select! {
            biased;
            frame = async {
                match replay_frame {
                    Some(frame) => Some(Ok::<Frame, RuntimeError>(frame)),
                    None => carrier_frames.recv().await,
                }
            } => {
                match frame {
                    Some(Ok(frame @ (Frame::StreamData { stream_id: received_stream_id, .. }
                        | Frame::StreamAck { stream_id: received_stream_id, .. }
                        | Frame::StreamMaxData { stream_id: received_stream_id, .. }
                        | Frame::StreamFin { stream_id: received_stream_id, .. }
                        | Frame::StreamReset { stream_id: received_stream_id, .. })))
                        if received_stream_id == stream_id =>
                    {
                        context.reliable_streams.route_frame(session_id, stream_id, frame).await?;
                    }
                    Some(Ok(Frame::StreamDetach { stream_id: detach_stream_id }))
                        if detach_stream_id == stream_id =>
                    {
                        context.reliable_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        let _ = udp_path_finish_stream(&mut send);
                        return Ok(());
                    }
                    Some(Ok(Frame::PathMetrics { metrics })) if metrics.path_id == path_id => {
                        context.reliable_streams.record_path_metrics(
                            &path_registration,
                            metrics,
                        );
                    }
                    Some(Ok(Frame::OpenStream {
                        stream_id: open_stream_id,
                        target: open_target,
                        demand: open_demand,
                        role: open_role,
                        ..
                    })) if open_stream_id == stream_id && open_target == target =>
                    {
                        let updated_lane = flow_lane_from_stream_demand_hint(open_demand);
                        match context.reliable_streams.open_or_attach(
                            ServerReliableStreamOpenRequest {
                                session_id,
                                stream_id,
                                target: &target,
                                lane: updated_lane,
                                attachment: ServerReliablePathAttachment {
                                    path_registration: path_registration.clone(),
                                    commands: commands_tx.clone(),
                                    max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                                        context.codec_limits,
                                        context.mux_limits,
                                    ),
                                    role: open_role,
                                    initial_metrics: context
                                        .local_path_startup_metrics(UnderlayProtocol::Udp, path_id),
                                },
                            },
                            context.mux_limits,
                            context.max_reliable_streams,
                        )? {
                            ServerReliableStreamOpen::Existing => {
                                context
                                    .reliable_streams
                                    .route_frame(
                                        session_id,
                                        stream_id,
                                        Frame::PathStatus {
                                            path_id,
                                            status: crate::protocol::PathStatus::Active,
                                            capabilities,
                                        },
                                    )
                                    .await?;
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamMaxData {
                                        stream_id,
                                        max_offset: reliable_stream_initial_advertised_window_bytes(
                                            UnderlayProtocol::Udp,
                                            lane,
                                            context.mux_limits,
                                        ),
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                            ServerReliableStreamOpen::New(_) => {
                                return Err(RuntimeError::Protocol(
                                    "QUIC UDP path reannouncement opened duplicate stream",
                                ));
                            }
                            ServerReliableStreamOpen::DuplicateLiveIgnored => {
                                let _ = udp_path_finish_stream(&mut send);
                                return Ok(());
                            }
                            ServerReliableStreamOpen::Rejected => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::StreamReset {
                                        stream_id,
                                        reason: ResetReason::Refused,
                                    },
                                    context.codec_limits,
                                )
                                .await?;
                            }
                        }
                        continue;
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::PathProofData {
                        path_id: proof_path_id,
                        proof_id,
                        payload,
                    })) if proof_path_id == path_id => {
                        udp_path_write_frame(
                            &mut send,
                            &path_proof_ack_frame(path_id, proof_id, payload.len()),
                            context.codec_limits,
                        )
                        .await?;
                    }
                    Some(Ok(Frame::PathProofAck {
                        path_id: proof_path_id,
                        proof_id,
                        payload_bytes,
                    })) if proof_path_id == path_id => {
                        if let Some(observation) =
                            path_proofs.acknowledge(path_id, proof_id, payload_bytes)
                            && let Some(metrics) = path_proof_metrics(
                                path_id,
                                UnderlayProtocol::Udp,
                                PathMetricDirection::ServerToClient,
                                observation,
                            )
                        {
                            context.reliable_streams.record_local_path_metrics(
                                &path_registration,
                                metrics,
                            );
                        }
                    }
                    Some(Ok(Frame::PathCapacityReceipt {
                        path_id: receipt_path_id,
                        calibration_id,
                        received_payload_bytes,
                    })) => {
                        confirm_server_quic_capacity_receipt(
                            &send,
                            session_id,
                            path_id,
                            path_registration.path_instance_id(),
                            stream_id,
                            receipt_path_id,
                            calibration_id,
                            received_payload_bytes,
                        )?;
                    }
                    Some(Ok(Frame::PathCapacityData { .. } | Frame::PathCapacityFinish { .. })) => {
                        return Err(RuntimeError::Protocol(
                            "server QUIC path received client response-capacity output",
                        ));
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(frame)) => {
                        log_unexpected_stream_relay_frame(
                            "server QUIC UDP path reliable",
                            stream_id,
                            &frame,
                        );
                        return Err(RuntimeError::Protocol("unexpected server QUIC UDP path reliable stream frame"));
                    }
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => {
                        context.reliable_streams.detach_path(
                            session_id,
                            stream_id,
                            UnderlayProtocol::Udp,
                            path_id,
                            &commands_tx,
                        );
                        return Ok(());
                    }
                    Some(Err(err)) => return Err(err),
                }
                if !send.connection.capacity_probe_active()
                    && let Some(command) = try_recv_reliable_path_command(&mut commands_rx)
                {
                    let result = drain_server_udp_reliable_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        session_id,
                        stream_id,
                        path_id,
                        path_registration.path_instance_id(),
                        &commands_tx,
                        &mut pending_frames,
                        &mut path_proofs,
                    )
                    .await?;
                    if result {
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_reliable_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            session_id,
                            stream_id,
                            path_id,
                            path_registration.path_instance_id(),
                            &commands_tx,
                            &mut pending_frames,
                            &mut path_proofs,
                        ).await;
                        if result? {
                            return Ok(());
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

async fn drain_server_udp_reliable_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    session_id: SessionId,
    stream_id: StreamId,
    path_id: PathId,
    path_instance_id: ServerCarrierPathInstanceId,
    commands_tx: &ReliablePathCommandSender,
    pending_frames: &mut Vec<Frame>,
    path_proofs: &mut PathProofTracker,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received an untyped capacity frame",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch_with_path_proofs(
                        send,
                        pending_frames,
                        context.codec_limits,
                        path_proofs,
                    )
                    .await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            path_id.0,
                            stream_id.0,
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(mut probe) => {
                if probe.path_id != path_id || probe.path_instance_id != path_instance_id {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    return Err(RuntimeError::Protocol(
                        "server QUIC capacity command path does not match writer",
                    ));
                }
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                if let Some(_reason) = quic_capacity_command_drop_reason(&probe, Instant::now()) {
                    probe.ticket.cancel();
                    commands.release_pending_command_bytes(pending_bytes);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_quic_capacity_calibration",
                        format_args!(
                            "phase=command_dropped reason={} session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                            _reason,
                            session_id.0,
                            probe.binding_instance_id,
                            path_id.0,
                            probe.path_instance_id.as_u64(),
                            probe.calibration_id,
                            probe.train_payload_bytes,
                        ),
                    );
                    return Ok(false);
                }
                let result = {
                    let write = udp_path_write_capacity_probe(
                        send,
                        &probe,
                        context.codec_limits,
                        context.mux_limits,
                    );
                    tokio::pin!(write);
                    tokio::select! {
                        biased;
                        _ = probe.ticket.cancelled() => None,
                        result = &mut write => Some(result),
                    }
                };
                commands.release_pending_command_bytes(pending_bytes);
                let Some(result) = result else {
                    let _ = send.connection.cancel_capacity_probe(probe.calibration_id);
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "response_quic_capacity_calibration",
                        format_args!(
                            "phase=command_cancelled reason=ownership_invalidated_during_write session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                            session_id.0,
                            probe.binding_instance_id,
                            path_id.0,
                            probe.path_instance_id.as_u64(),
                            probe.calibration_id,
                            probe.train_payload_bytes,
                        ),
                    );
                    return Ok(false);
                };
                if let Err(err) = result {
                    if let Some(_reason) = quic_capacity_start_rejection_reason(&err) {
                        probe.ticket.cancel();
                        #[cfg(feature = "lab-diagnostics")]
                        lab_diagnostic(
                            "response_quic_capacity_calibration",
                            format_args!(
                                "phase=command_rejected reason={} session_id={} binding_instance_id={} underlay=Udp path_id={} path_instance_id={} calibration_id={} train_bytes={}",
                                _reason,
                                session_id.0,
                                probe.binding_instance_id,
                                path_id.0,
                                probe.path_instance_id.as_u64(),
                                probe.calibration_id,
                                probe.train_payload_bytes,
                            ),
                        );
                        return Ok(false);
                    }
                    return Err(err);
                }
                // The carrier epoch now owns cancellation. Before this point,
                // dropping a dequeued command must invalidate its session lease.
                probe.disarm_drop_cancellation();
                let cancellation_connection = send.connection.clone();
                let cancellation_ticket = probe.ticket.clone();
                let cancellation_token = probe.calibration_id;
                tokio::spawn(async move {
                    if cancellation_ticket.resolved().await
                        == QuicCapacityProbeCommandResolution::Cancelled
                    {
                        let _ = cancellation_connection.cancel_capacity_probe(cancellation_token);
                    }
                });
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "path_writer_drain",
                    format_args!(
                        "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={} capacity_probe=true calibration_id={}",
                        path_id.0,
                        stream_id.0,
                        sent_items.saturating_add(1),
                        sent_bytes.saturating_add(writer_run_bytes),
                        byte_budget,
                        item_budget,
                        commands.pending_bytes(),
                        drain_started.elapsed().as_micros(),
                        true,
                        false,
                        probe.calibration_id,
                    ),
                );
                // End the run at the epoch boundary. A later dequeue may block
                // on the carrier gate, but cannot enter this write transaction.
                return Ok(false);
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC path received TCP capacity command",
                ));
            }
            ReliablePathCommand::CloseStream(close_stream_id) => {
                flush_udp_frame_batch_with_path_proofs(
                    send,
                    pending_frames,
                    context.codec_limits,
                    path_proofs,
                )
                .await?;
                if close_stream_id == stream_id {
                    context.reliable_streams.detach_path(
                        session_id,
                        stream_id,
                        UnderlayProtocol::Udp,
                        path_id,
                        commands_tx,
                    );
                    let _ = udp_path_finish_stream(send);
                    true
                } else {
                    false
                }
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path stream received client open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch_with_path_proofs(
                send,
                pending_frames,
                context.codec_limits,
                path_proofs,
            )
            .await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp path_id={} stream_id={} sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    path_id.0,
                    stream_id.0,
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}

struct ServerUdpDatagramStreamContext {
    session_id: SessionId,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    lane: FlowLane,
}

async fn handle_server_udp_datagram_stream(
    send: UdpPathSendStream,
    recv: UdpPathRecvStream,
    context: ServerPathContext,
    stream_context: ServerUdpDatagramStreamContext,
) -> Result<(), RuntimeError> {
    let (commands_tx, mut commands_rx) = reliable_path_command_channels(udp_path_command_queue(
        context.mux_limits,
        context.codec_limits,
    ));
    let mut send = send;
    let mut carrier_frames = spawn_quic_path_reader(
        recv,
        context.codec_limits,
        udp_path_command_queue(context.mux_limits, context.codec_limits),
    );
    let mut flows = Vec::<ServerUdpDatagramFlow>::new();
    let mut pending_frames = Vec::<Frame>::new();
    open_server_udp_datagram_flow(
        &context,
        &commands_tx,
        &mut send,
        &mut flows,
        stream_context.session_id,
        stream_context.flow_id,
        stream_context.target,
        stream_context.lane,
    )
    .await?;
    loop {
        let command_may_recv = !reliable_path_receivers_closed(&commands_rx);
        tokio::select! {
            biased;
            frame = carrier_frames.recv() => {
                match frame {
                    Some(Ok(Frame::OpenDatagramFlow { flow_id, target, .. })) => {
                        open_server_udp_datagram_flow(
                            &context,
                            &commands_tx,
                            &mut send,
                            &mut flows,
                            stream_context.session_id,
                            flow_id,
                            target,
                            FlowLane::RealtimeDatagram,
                        ).await?;
                    }
                    Some(Ok(Frame::DatagramData { flow_id, datagram_id, ttl_ms, payload })) => {
                        if ttl_ms == 0 {
                            return Err(RuntimeError::Protocol("expired QUIC UDP path datagram received"));
                        }
                        let flow_index = flows
                            .iter()
                            .position(|flow| flow.flow_id == flow_id)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?;
                        let requests = flows
                            .get(flow_index)
                            .ok_or(RuntimeError::Protocol("unknown QUIC UDP path datagram flow"))?
                            .requests
                            .clone();
                        match requests.try_send(ServerUdpDatagramRequest { datagram_id, ttl_ms, payload }) {
                            Ok(()) => {
                                udp_path_write_frame(
                                    &mut send,
                                    &Frame::DatagramFeedback {
                                        flow_id,
                                        received: vec![datagram_ack_range(datagram_id)?],
                                    },
                                    context.codec_limits,
                                ).await?;
                            }
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                eprintln!("warning: QUIC UDP path datagram worker queue full; dropping request");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                flows.retain(|flow| flow.flow_id != flow_id);
                                udp_path_write_frame(&mut send, &Frame::DatagramClose { flow_id }, context.codec_limits).await?;
                            }
                        }
                    }
                    Some(Ok(Frame::DatagramFeedback { .. })) => {}
                    Some(Ok(Frame::DatagramClose { flow_id })) => {
                        flows.retain(|flow| flow.flow_id != flow_id);
                        if flows.is_empty() {
                            let _ = udp_path_finish_stream(&mut send);
                            return Ok(());
                        }
                    }
                    Some(Ok(Frame::Ping { nonce })) => {
                        udp_path_write_frame(&mut send, &Frame::Pong { nonce }, context.codec_limits).await?;
                    }
                    Some(Ok(Frame::SessionClose { reason })) => return Err(RuntimeError::RemoteClosed(reason)),
                    Some(Ok(_)) => return Err(RuntimeError::Protocol("unexpected server QUIC UDP path datagram stream frame")),
                    Some(Err(RuntimeError::ReliablePathSessionClosed)) | None => return Ok(()),
                    Some(Err(err)) => return Err(err),
                }
                if let Some(command) = try_recv_reliable_path_command(&mut commands_rx) {
                    let result = drain_server_udp_datagram_commands(
                        command,
                        &mut commands_rx,
                        &mut send,
                        &context,
                        &mut flows,
                        &mut pending_frames,
                    )
                    .await?;
                    if result {
                        return Ok(());
                    }
                }
            }
            command = recv_reliable_path_command(&mut commands_rx), if command_may_recv => {
                match command {
                    Some(command) => {
                        let result = drain_server_udp_datagram_commands(
                            command,
                            &mut commands_rx,
                            &mut send,
                            &context,
                            &mut flows,
                            &mut pending_frames,
                        ).await;
                        if result? {
                            return Ok(());
                        }
                    }
                    None => {}
                }
            }
        }
    }
}

async fn drain_server_udp_datagram_commands(
    first_command: ReliablePathCommand,
    commands: &mut ReliablePathCommandReceivers,
    send: &mut UdpPathSendStream,
    context: &ServerPathContext,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    pending_frames: &mut Vec<Frame>,
) -> Result<bool, RuntimeError> {
    #[cfg(feature = "lab-diagnostics")]
    let drain_started = Instant::now();
    let byte_budget = reliable_path_command_writer_run_budget_bytes(context.mux_limits);
    let item_budget = reliable_path_command_writer_run_budget_items(context.mux_limits);
    let mut next_command = Some(first_command);
    pending_frames.clear();
    let mut sent_bytes = 0usize;
    let mut sent_items = 0usize;

    loop {
        let Some(command) = next_command
            .take()
            .or_else(|| try_recv_reliable_path_command(commands))
        else {
            if try_coalesce_reliable_path_writer_run(
                commands,
                &mut next_command,
                sent_items,
                sent_bytes,
                byte_budget,
                item_budget,
            )
            .await
            {
                continue;
            }
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(false);
        };
        let pending_bytes = reliable_path_command_pending_bytes(&command);
        let writer_run_bytes = reliable_path_command_writer_run_bytes(&command);
        let should_close = match command {
            ReliablePathCommand::SendFrame(frame)
                if reliable_path_frame_requires_capacity_command(&frame) =>
            {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC datagram writer received capacity data",
                ));
            }
            ReliablePathCommand::SendFrame(frame) => {
                if let Frame::DatagramClose { flow_id } = frame {
                    flows.retain(|flow| flow.flow_id != flow_id);
                }
                pending_frames.push(frame);
                commands.release_pending_command_bytes(pending_bytes);
                sent_bytes = sent_bytes.saturating_add(writer_run_bytes);
                sent_items = sent_items.saturating_add(1);
                if sent_bytes >= byte_budget || sent_items >= item_budget {
                    flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
                    #[cfg(feature = "lab-diagnostics")]
                    lab_diagnostic(
                        "path_writer_drain",
                        format_args!(
                            "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                            sent_items,
                            sent_bytes,
                            byte_budget,
                            item_budget,
                            commands.pending_bytes(),
                            drain_started.elapsed().as_micros(),
                            true,
                            sent_items >= item_budget,
                        ),
                    );
                    return Ok(false);
                }
                continue;
            }
            ReliablePathCommand::SendQuicCapacityProbe(probe) => {
                probe.ticket.cancel();
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC datagram writer received reliable capacity command",
                ));
            }
            ReliablePathCommand::SendTcpCapacityProbe(_) => {
                commands.release_pending_command_bytes(pending_bytes);
                return Err(RuntimeError::Protocol(
                    "server QUIC datagram writer received TCP capacity command",
                ));
            }
            ReliablePathCommand::CloseStream(_) => {
                flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
                let _ = udp_path_finish_stream(send);
                true
            }
            ReliablePathCommand::OpenStream { .. } => {
                return Err(RuntimeError::Protocol(
                    "server QUIC UDP path datagram stream received open command",
                ));
            }
        };
        commands.release_pending_command_bytes(pending_bytes);
        if should_close {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    false,
                    false,
                ),
            );
            return Ok(true);
        }
        sent_items = sent_items.saturating_add(1);
        if sent_bytes >= byte_budget || sent_items >= item_budget {
            flush_udp_frame_batch(send, pending_frames, context.codec_limits).await?;
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "path_writer_drain",
                format_args!(
                    "role=server underlay=Udp datagram=true sent_items={} sent_bytes={} byte_budget={} item_budget={} pending_bytes_after={} elapsed_us={} hit_byte_budget={} hit_item_budget={}",
                    sent_items,
                    sent_bytes,
                    byte_budget,
                    item_budget,
                    commands.pending_bytes(),
                    drain_started.elapsed().as_micros(),
                    true,
                    sent_items >= item_budget,
                ),
            );
            return Ok(false);
        }
    }
}

async fn open_server_udp_datagram_flow(
    context: &ServerPathContext,
    commands_tx: &ReliablePathCommandSender,
    send: &mut UdpPathSendStream,
    flows: &mut Vec<ServerUdpDatagramFlow>,
    session_id: SessionId,
    flow_id: DatagramFlowId,
    target: TargetAddr,
    _lane: FlowLane,
) -> Result<(), RuntimeError> {
    if flows.iter().any(|flow| flow.flow_id == flow_id) {
        return Err(RuntimeError::Protocol(
            "duplicate QUIC UDP path datagram flow",
        ));
    }
    if flows.len() >= context.max_udp_flows_per_session {
        udp_path_write_frame(
            send,
            &Frame::DatagramClose { flow_id },
            context.codec_limits,
        )
        .await?;
        return Ok(());
    }
    outbound::validate_target(&target)?;
    context.outbound.ensure_supports(TargetProtocol::Udp)?;
    let realtime_registration = context.reliable_streams.register_realtime_flow(session_id);
    let outbound_socket = match outbound::connect_udp(
        &context.outbound,
        &context.outbound_dns,
        &target,
        context.outbound_connect_timeout,
    )
    .await
    {
        Ok(socket) => socket,
        Err(err) => {
            udp_path_write_frame(
                send,
                &Frame::DatagramClose { flow_id },
                context.codec_limits,
            )
            .await?;
            return Err(RuntimeError::OutboundConnect(err));
        }
    };
    let requests = spawn_server_udp_datagram_flow_worker(
        flow_id,
        outbound_socket,
        commands_tx.clone(),
        context.mux_limits,
    );
    flows.push(ServerUdpDatagramFlow {
        flow_id,
        requests,
        _realtime_registration: realtime_registration,
    });
    Ok(())
}

fn udp_path_frame_finished(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::UnexpectedEnd) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Connection(_)) => true,
        _ => false,
    }
}

fn udp_runtime_error_is_expected_shutdown(err: &RuntimeError) -> bool {
    match err {
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Read(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::ReadExact(_)) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::UnexpectedEnd) => true,
        RuntimeError::QuicCarrier(quic_carrier::QuicCarrierError::Connection(_)) => true,
        RuntimeError::RemoteClosed(CloseReason::Normal) => true,
        _ => false,
    }
}

fn warn_unexpected_udp_runtime_error(message: &str, err: &RuntimeError) {
    if !udp_runtime_error_is_expected_shutdown(err) {
        eprintln!("warning: {message}: {err}");
    }
}

fn quic_path_open_error_is_retryable(err: &RuntimeError) -> bool {
    matches!(
        err,
        RuntimeError::Io(_)
            | RuntimeError::Udp(_)
            | RuntimeError::QuicCarrier(_)
            | RuntimeError::RemoteClosed(_)
            | RuntimeError::Protocol(_)
            | RuntimeError::ReliablePathSessionClosed
    )
}

fn udp_path_command_queue(mux_limits: MuxLimits, _codec_limits: CodecLimits) -> usize {
    // This queue is a sender-service work queue, not a QUIC record-buffer queue.
    // QUIC reliable streams may split OwnerData into smaller records to reduce
    // stream head-of-line burst size, but that packetization detail must not
    // multiply the number of commands admitted above the carrier. Otherwise a
    // 12--32 KiB QUIC record cap would inflate the queue from the logical
    // product-flight budget to thousands of commands and recreate the hidden
    // backlog that caused zero-goodput bursts.  Keep queue capacity tied to the
    // logical sender quantum; the QUIC writer/flow-control path performs the
    // lower-level pacing.
    reliable_path_command_queue(mux_limits)
}

async fn resolve_first_socket_addr(path: &PathSpec) -> Result<SocketAddr, RuntimeError> {
    let mut addrs = lookup_host((path.endpoint.host.as_str(), path.endpoint.port)).await?;
    addrs.next().ok_or(RuntimeError::Protocol(
        "QUIC UDP path endpoint resolved no socket addresses",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nonblocking_udp_open_uses_zero_initial_window_without_accept() {
        let options = UdpStreamOpenOptions {
            wait_for_accept: false,
            role: StreamOpenRole::Validation,
        };

        assert_eq!(udp_stream_open_initial_max_offset(options, None), 0);
    }

    #[test]
    fn blocking_udp_open_uses_accepted_initial_window() {
        assert_eq!(
            udp_stream_open_initial_max_offset(UdpStreamOpenOptions::ACTIVE_WAIT, Some(8192)),
            8192
        );
    }

    #[test]
    fn reliable_output_guard_detaches_on_abnormal_stream_exit() {
        let registry = Arc::new(ServerReliableStreamRegistry::new(
            ResourceLimits::default().max_streams,
        ));
        let session_id = SessionId(201);
        let stream_id = StreamId(301);
        let path_id = PathId(0);
        let path_registration =
            registry.register_carrier_path(session_id, UnderlayProtocol::Udp, path_id);
        let target = TargetAddr::Ip(SocketAddr::from(([127, 0, 0, 1], 80)));
        let (commands, _receivers) = reliable_path_command_channels(8);
        let commands_for_guard = commands.clone();
        let stream = match registry
            .open_or_attach(
                ServerReliableStreamOpenRequest {
                    session_id,
                    stream_id,
                    target: &target,
                    lane: FlowLane::Throughput,
                    attachment: ServerReliablePathAttachment {
                        path_registration: path_registration.clone(),
                        commands,
                        max_frame_payload_bytes: udp_path_max_stream_payload_bytes(
                            CodecLimits::default(),
                            MuxLimits::default(),
                        ),
                        role: StreamOpenRole::Active,
                        initial_metrics: None,
                    },
                },
                MuxLimits::default(),
                ResourceLimits::default().max_streams,
            )
            .expect("open UDP response stream")
        {
            ServerReliableStreamOpen::New(stream) => stream,
            _ => panic!("expected new UDP response stream"),
        };
        let ReliablePathStreamOutput::Switchable(binding) = &stream.output else {
            panic!("expected switchable response output");
        };
        assert_eq!(
            binding
                .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
                .len(),
            1
        );

        drop(ServerUdpReliableOutputDetachGuard {
            registry,
            session_id,
            stream_id,
            path_id,
            commands: commands_for_guard,
        });

        assert!(
            binding
                .sender_path_targets(FlowLane::Throughput, BBR_MAX_SEND_QUANTUM_BYTES)
                .is_empty(),
            "every server QUIC stream exit must detach its response output"
        );
    }

    fn quic_congestion(
        congestion_window: u64,
        pacing_rate_bps: Option<u64>,
    ) -> quic_carrier::CongestionMetrics {
        quic_carrier::CongestionMetrics {
            congestion_window,
            bytes_in_flight: Some(0),
            pending_bytes: 0,
            pacing_rate_bps,
            loss_ppm: None,
            ecn_ppm: None,
            newly_acked_bytes: None,
            non_app_limited_acked_bytes: None,
            timed_non_app_limited_acked_bytes: None,
            non_app_limited_ack_elapsed: None,
            delivery_evidence_written_bytes: 0,
            delivery_sample_count: 0,
            non_app_limited_delivery_sample_count: 0,
            timed_non_app_limited_delivery_sample_count: 0,
            app_limited: true,
            capacity_probe: None,
        }
    }

    fn with_delivery_evidence_written(
        mut metrics: quic_carrier::CongestionMetrics,
        bytes: u64,
    ) -> quic_carrier::CongestionMetrics {
        metrics.delivery_evidence_written_bytes = bytes;
        metrics
    }

    fn with_acked_bytes(
        metrics: quic_carrier::CongestionMetrics,
        bytes: u64,
        sample_count: u64,
    ) -> quic_carrier::CongestionMetrics {
        with_acked_bytes_elapsed(metrics, bytes, sample_count, Duration::from_millis(100))
    }

    fn with_acked_bytes_elapsed(
        mut metrics: quic_carrier::CongestionMetrics,
        bytes: u64,
        sample_count: u64,
        elapsed: Duration,
    ) -> quic_carrier::CongestionMetrics {
        metrics.newly_acked_bytes = Some(bytes);
        metrics.non_app_limited_acked_bytes = Some(bytes);
        metrics.timed_non_app_limited_acked_bytes = (!elapsed.is_zero()).then_some(bytes);
        metrics.non_app_limited_ack_elapsed = (!elapsed.is_zero()).then_some(elapsed);
        metrics.delivery_sample_count = sample_count;
        metrics.non_app_limited_delivery_sample_count = sample_count;
        metrics.timed_non_app_limited_delivery_sample_count =
            if elapsed.is_zero() { 0 } else { sample_count };
        metrics.app_limited = false;
        metrics
    }

    fn capacity_probe_metrics(
        token: u64,
        now: Instant,
        warmup_bytes: u64,
        required_bytes: u64,
        timed_bytes: u64,
        timed_count: u64,
        timed_elapsed: Option<Duration>,
    ) -> quic_carrier::CapacityProbeMetrics {
        let sample_floor_bytes = required_bytes.saturating_add(PATH_OPEN_SCORE_BYTES as u64);
        let train_payload_bytes = warmup_bytes
            .saturating_add(required_bytes)
            .max(sample_floor_bytes);
        let receipt_elapsed = Duration::from_millis(80);
        quic_carrier::CapacityProbeMetrics {
            token,
            train_payload_bytes,
            sample_floor_bytes,
            warmup_carrier_bytes: warmup_bytes,
            required_timed_carrier_bytes: required_bytes,
            expires_at: now + Duration::from_secs(5),
            phase: quic_carrier::CapacityProbePhase::Proven,
            started_clean: false,
            write_committed: true,
            written_payload_bytes: train_payload_bytes,
            written_data_frame_count: train_payload_bytes.div_ceil(64 * 1024),
            total_acked_carrier_bytes: train_payload_bytes,
            total_ack_sample_count: timed_count.saturating_add(u64::from(warmup_bytes > 0)),
            warmup_acked_carrier_bytes: warmup_bytes,
            warmup_ack_sample_count: u64::from(warmup_bytes > 0),
            measurement_acked_carrier_bytes: train_payload_bytes.saturating_sub(warmup_bytes),
            measurement_ack_sample_count: timed_count,
            timed_measurement_acked_carrier_bytes: timed_bytes,
            timed_measurement_ack_sample_count: timed_count,
            app_limited_acked_carrier_bytes: timed_bytes,
            app_limited_ack_sample_count: timed_count,
            timed_measurement_ack_elapsed: timed_elapsed,
            native_proved_at: timed_elapsed.map(|_| now),
            proved_at: Some(now),
            proof_validity: Duration::from_secs(3),
            receipt_received_payload_bytes: train_payload_bytes,
            receipt_elapsed: Some(receipt_elapsed),
            receipt_rtt: Some(Duration::from_millis(20)),
            receipt_at: Some(now),
            last_authoritative_in_flight: Some(0),
            last_authoritative_in_flight_at: Some(now),
            last_authoritative_sent_watermark: Some(train_payload_bytes),
            receipt_frozen_sent_watermark: Some(train_payload_bytes),
            current_sent_watermark: train_payload_bytes,
        }
    }

    fn with_capacity_probe(
        mut metrics: quic_carrier::CongestionMetrics,
        probe: quic_carrier::CapacityProbeMetrics,
    ) -> quic_carrier::CongestionMetrics {
        metrics.capacity_probe = Some(probe);
        metrics
    }

    #[test]
    fn quic_product_payload_uses_sender_quantum_not_packet_train_cap() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let payload_cap = udp_path_max_stream_payload_bytes(codec_limits, mux_limits);

        assert!(
            payload_cap >= BBR_MAX_SEND_QUANTUM_BYTES,
            "QUIC product dispatch must stay BDP/service-quantum sized; only carrier serialization may split records"
        );
    }

    #[test]
    fn quic_reliable_stream_reader_queue_stays_logical_product_queue() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let queue = udp_reliable_stream_frame_queue(codec_limits, mux_limits);

        assert_eq!(
            queue,
            reliable_stream_frame_queue(mux_limits),
            "carrier recordization must not multiply the product reader queue or hide backlog"
        );
    }

    #[test]
    fn quic_stats_feed_sender_side_udp_path_metrics() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;

        let startup = tracker.quic.observe(stats, congestion, 2);
        assert_eq!(startup.direction, 2);
        assert_eq!(startup.delivery_sample_count, 0);
        assert_eq!(startup.delivery_rate_bps.round() as u64, 500_000_000);
        assert_eq!(startup.inflight_hi, 4 * 1024 * 1024);
        stats.frame_rx.acks = 4;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
                8 * 1024 * 1024,
                4,
            ),
            2,
        );
        assert_eq!(measured.direction, 2);
        assert_eq!(measured.delivery_sample_count, 4);
        assert!(measured.delivery_rate_bps > 0.0);
        assert!(measured.last_delivery_sample_at.is_some());
        assert!(!measured.app_limited);
    }

    #[test]
    fn quic_delivery_rate_uses_carrier_ack_elapsed_not_metrics_poll_phase() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let mut fast_poll = QuicPathMetricTracker::default();
        let mut slow_poll = QuicPathMetricTracker::default();
        let _ = fast_poll.observe_at(stats, congestion, 2, base);
        let _ = slow_poll.observe_at(stats, congestion, 2, base);
        let ack = with_acked_bytes_elapsed(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
            Duration::from_millis(20),
        );

        let fast = fast_poll.observe_at(stats, ack, 2, base + Duration::from_millis(10));
        let slow = slow_poll.observe_at(stats, ack, 2, base + Duration::from_millis(500));

        assert_eq!(
            fast.delivery_rate_bps.round() as u64,
            slow.delivery_rate_bps.round() as u64,
            "scheduler poll phase must not enter the carrier delivery-rate denominator"
        );
        assert_eq!(
            fast.delivery_rate_bps.round() as u64,
            (sample_bytes as f64 * 8.0 / 0.020).round() as u64
        );
    }

    #[test]
    fn quic_zero_span_ack_batch_proves_reachability_without_rate() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(200);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let mut tracker = QuicPathMetricTracker::default();
        let startup = tracker.observe(stats, congestion, 2);

        let untimed = tracker.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
                Duration::ZERO,
            ),
            2,
        );

        assert!(untimed.ack_derived_data_seen);
        assert_eq!(untimed.delivery_sample_bytes, 0);
        assert_eq!(untimed.delivery_sample_count, 0);
        assert_eq!(untimed.delivery_rate_bps, startup.delivery_rate_bps);
        assert!(untimed.app_limited);
    }

    #[test]
    fn quic_combined_poll_excludes_untimed_ack_bytes_from_rate() {
        let timed_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let total_bytes = timed_bytes * 2;
        let congestion = quic_congestion(timed_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = timed_bytes;
        stats.path.current_mtu = 1400;
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe(stats, congestion, 2);

        let mut combined = with_delivery_evidence_written(congestion, total_bytes);
        combined.newly_acked_bytes = Some(total_bytes);
        combined.non_app_limited_acked_bytes = Some(total_bytes);
        combined.timed_non_app_limited_acked_bytes = Some(timed_bytes);
        combined.non_app_limited_ack_elapsed = Some(Duration::from_millis(20));
        combined.delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
        combined.non_app_limited_delivery_sample_count = (QUIC_INITIAL_WINDOW_PACKETS * 2) as u64;
        combined.timed_non_app_limited_delivery_sample_count = QUIC_INITIAL_WINDOW_PACKETS as u64;
        combined.app_limited = false;

        let measured = tracker.observe(stats, combined, 2);

        assert!(measured.ack_derived_data_seen);
        assert_eq!(measured.delivery_sample_bytes, timed_bytes);
        assert_eq!(
            measured.delivery_rate_bps.round() as u64,
            (timed_bytes as f64 * 8.0 / 0.020).round() as u64,
            "untimed reachability ACKs must not enter a timed rate numerator"
        );
    }

    #[test]
    fn quic_split_ack_polls_sum_carrier_elapsed_before_one_timer_clamp() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let chunk_bytes = sample_bytes / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe(stats, congestion, 2);

        let first = tracker.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(congestion, sample_bytes),
                chunk_bytes,
                (QUIC_INITIAL_WINDOW_PACKETS / 2) as u64,
                Duration::from_millis(20),
            ),
            2,
        );
        assert_eq!(first.delivery_sample_count, 0);
        let measured = tracker.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(congestion, sample_bytes),
                chunk_bytes,
                (QUIC_INITIAL_WINDOW_PACKETS / 2) as u64,
                Duration::from_millis(30),
            ),
            2,
        );

        assert_eq!(measured.delivery_sample_bytes, sample_bytes);
        assert_eq!(
            measured.delivery_rate_bps.round() as u64,
            (sample_bytes as f64 * 8.0 / 0.050).round() as u64
        );
    }

    #[test]
    fn quic_ack_only_stats_do_not_create_delivery_rate_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(1);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 1);

        stats.frame_rx.acks = 1;
        let ack_only = tracker.quic.observe(stats, congestion, 1);
        assert_eq!(ack_only.delivery_sample_count, 0);
        assert!(ack_only.last_delivery_sample_at.is_none());
        assert_eq!(ack_only.delivery_rate_bps.round() as u64, 500_000_000);
    }

    #[test]
    fn quic_tx_bytes_without_newly_acked_bytes_do_not_create_delivery_rate_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);
        let tx_only = tracker.quic.observe(
            stats,
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            2,
        );

        assert_eq!(tx_only.delivery_sample_count, 0);
        assert!(tx_only.last_delivery_sample_at.is_none());
        assert_eq!(tx_only.delivery_rate_bps.round() as u64, 500_000_000);
    }

    #[test]
    fn quic_product_data_accepted_by_quinn_counts_as_queue_until_ack() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        let queued = tracker.quic.observe(
            stats,
            with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
            2,
        );
        assert_eq!(queued.bytes_in_flight, 0);
        assert_eq!(queued.pending_bytes, 8 * 1024 * 1024);
        let product_metrics = path_metrics_from_quic_path(PathId(7), queued);
        assert_eq!(product_metrics.queue_bytes, 8 * 1024 * 1024);

        let partially_acked = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
                2 * 1024 * 1024,
                1,
            ),
            2,
        );
        assert_eq!(partially_acked.pending_bytes, 6 * 1024 * 1024);
    }

    #[test]
    fn quic_loss_unknown_is_not_reported_as_observed_zero() {
        let metrics = UdpPathMetrics {
            direction: 2,
            srtt: Duration::from_millis(20),
            rttvar: Duration::from_millis(2),
            min_rtt: Duration::from_millis(18),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 128 * 1024,
            pending_bytes: 256 * 1024,
            loss_ppm: None,
            ecn_ppm: None,
            app_limited: true,
            ack_derived_data_seen: false,
            delivery_sample_count: 0,
            delivery_sample_bytes: 0,
            last_delivery_sample_at: None,
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 0,
            latest_delivery_sample_count: 0,
            latest_carrier_ack_elapsed: None,
            latest_rate_sample_elapsed: None,
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        };

        let path_metrics = path_metrics_from_quic_path(PathId(7), metrics);

        assert_eq!(path_metrics.loss_ppm, 0);
        assert!(!path_metrics.loss_observed);
        assert_eq!(path_metrics.ecn_ppm, 0);
        assert!(!path_metrics.ecn_observed);
        assert_eq!(path_metrics.bytes_in_flight, 128 * 1024);
        assert_eq!(path_metrics.queue_bytes, 128 * 1024);
    }

    #[test]
    fn quic_unknown_capacity_ack_sample_does_not_create_bulk_evidence() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(0, None);
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);

        let _ = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 1;
        let unknown_capacity = tracker.quic.observe(
            stats,
            with_acked_bytes(with_delivery_evidence_written(congestion, 4096), 4096, 1),
            2,
        );

        assert_eq!(unknown_capacity.delivery_sample_count, 0);
        assert!(unknown_capacity.last_delivery_sample_at.is_none());
        assert_eq!(
            unknown_capacity.delivery_rate_bps.round() as u64,
            default_path_rate_bps(UnderlayProtocol::Udp).round() as u64
        );
        assert!(unknown_capacity.app_limited);
    }

    #[test]
    fn quic_tiny_startup_pacing_does_not_poison_product_scheduler_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(0, Some(4));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);

        let startup = tracker.quic.observe(stats, congestion, 2);
        let udp_startup_rate = default_path_rate_bps(UnderlayProtocol::Udp).round() as u64;

        assert_eq!(startup.delivery_sample_count, 0);
        assert!(startup.last_delivery_sample_at.is_none());
        assert_eq!(startup.delivery_rate_bps.round() as u64, udp_startup_rate);
        assert_eq!(startup.pacing_rate_bps.round() as u64, udp_startup_rate);
        stats.frame_rx.acks = 1;
        let app_limited =
            tracker
                .quic
                .observe(stats, with_delivery_evidence_written(congestion, 4096), 2);

        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.last_delivery_sample_at.is_none());
        assert_eq!(
            app_limited.delivery_rate_bps.round() as u64,
            udp_startup_rate
        );
        assert_eq!(app_limited.pacing_rate_bps.round() as u64, udp_startup_rate);
        assert!(app_limited.app_limited);
    }

    #[test]
    fn quic_udp_command_queue_tracks_sender_quantum_not_record_size() {
        let mux_limits = MuxLimits::default();
        let codec_limits = CodecLimits::default();
        let product_queue = reliable_path_command_queue(mux_limits);
        let quic_udp_queue = udp_path_command_queue(mux_limits, codec_limits);
        let sender_quantum =
            reliable_relay_scheduler_quantum_cap(None, FlowLane::Throughput, mux_limits);
        let record_sized_queue = reliable_path_command_queue_for_payload(
            mux_limits,
            sender_quantum.min(UDP_DEFAULT_MTU_PAYLOAD_BYTES).max(1),
        );

        assert_eq!(
            quic_udp_queue, product_queue,
            "command queue capacity must stay tied to the logical sender quantum"
        );
        assert_ne!(
            quic_udp_queue, record_sized_queue,
            "carrier packet/record sizing must not inflate the command queue"
        );
    }

    #[test]
    fn quic_app_limited_low_ack_sample_does_not_poison_delivery_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 1;
        let app_limited = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );

        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.last_delivery_sample_at.is_none());
        assert_eq!(app_limited.delivery_rate_bps.round() as u64, 500_000_000);
        assert!(app_limited.app_limited);

        let mut changed_pacing = congestion;
        changed_pacing.pacing_rate_bps = Some(750_000_000);
        let refreshed_prior = tracker.quic.observe(stats, changed_pacing, 2);
        assert_eq!(refreshed_prior.delivery_sample_count, 0);
        assert_eq!(
            refreshed_prior.delivery_rate_bps.round() as u64,
            750_000_000,
            "a rejected app-limited ACK must not freeze the live pacing prior in the measured-rate slot"
        );
    }

    #[test]
    fn quic_initial_full_quantum_sample_does_not_seed_tiny_bulk_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 1;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
                PATH_OPEN_SCORE_BYTES as u64,
                1,
                Duration::from_millis(1000),
            ),
            2,
        );

        assert_eq!(measured.delivery_sample_count, 1);
        assert_eq!(
            measured.delivery_rate_bps.round() as u64,
            startup.delivery_rate_bps.round() as u64,
            "a single underfed validation quantum must not replace the startup/pacing fallback with a tiny rate"
        );
    }

    #[test]
    fn quic_poll_retains_non_app_limited_ack_bytes_after_later_idle_ack() {
        let mut tracker = UdpPathMetricTracker::default();
        let sample_bytes = 256 * 1024_u64;
        let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);
        let mut polled = with_acked_bytes(
            with_delivery_evidence_written(congestion, sample_bytes),
            sample_bytes,
            QUIC_INITIAL_WINDOW_PACKETS as u64,
        );
        polled.app_limited = true;
        let measured = tracker.quic.observe(stats, polled, 2);

        assert_eq!(measured.delivery_sample_bytes, sample_bytes);
        assert!(measured.delivery_sample_count >= QUIC_INITIAL_WINDOW_PACKETS as u64);
        assert!(
            !measured.app_limited,
            "a later idle ACK flag must not erase non-app-limited bytes accumulated before the metrics poll"
        );
    }

    #[test]
    fn quic_capacity_evidence_accumulates_across_small_ack_polls() {
        let mut tracker = UdpPathMetricTracker::default();
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let chunk_bytes = sample_bytes / 8;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        let mut measured = None;
        for _ in 0..8 {
            measured = Some(tracker.quic.observe(
                stats,
                with_acked_bytes(
                    with_delivery_evidence_written(congestion, sample_bytes),
                    chunk_bytes,
                    2,
                ),
                2,
            ));
        }
        let measured = measured.expect("split calibration sample");
        assert_eq!(measured.delivery_sample_bytes, sample_bytes);
        assert!(!measured.app_limited);

        let idle = tracker.quic.observe(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
        );
        assert!(
            !idle.app_limited,
            "an idle metrics poll inside the 3-PTO horizon must preserve capacity evidence"
        );
    }

    #[test]
    fn quic_app_limited_capacity_probe_emits_candidate_without_generic_proof() {
        let now = Instant::now();
        let required_bytes = 240 * 1024_u64;
        let mut congestion = quic_congestion(256 * 1024, Some(100_000_000));
        congestion.app_limited = true;
        congestion = with_capacity_probe(
            congestion,
            capacity_probe_metrics(
                41,
                now,
                0,
                required_bytes,
                required_bytes,
                32,
                Some(Duration::from_millis(40)),
            ),
        );
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = 256 * 1024;
        stats.path.current_mtu = 1400;
        let mut tracker = QuicPathMetricTracker::default();

        let observed = tracker.observe_at(stats, congestion, 2, now);
        let candidate = observed
            .capacity_proof_candidate
            .expect("receiver-confirmed capacity token");

        assert_eq!(candidate.token, 41);
        assert!(candidate.receipt_confirmed);
        assert_eq!(candidate.received_bytes, candidate.train_bytes);
        assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
        assert!(candidate.written_data_frame_count > 0);
        assert!(observed.app_limited);
        assert_eq!(observed.delivery_sample_count, 0);
        assert_eq!(observed.delivery_sample_bytes, 0);
        assert!(observed.bulk_proof_expires_at.is_none());
    }

    #[test]
    fn quic_capacity_receipt_publishes_after_terminalization_and_freezes_rate() {
        let now = Instant::now();
        let required_bytes = 240 * 1024_u64;
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = 256 * 1024;
        stats.path.current_mtu = 1400;
        let base = quic_congestion(256 * 1024, Some(100_000_000));
        let mut probe = capacity_probe_metrics(
            42,
            now,
            0,
            required_bytes,
            required_bytes,
            32,
            Some(Duration::from_millis(40)),
        );
        probe.phase = quic_carrier::CapacityProbePhase::Proven;
        probe.last_authoritative_in_flight = Some(0);
        probe.last_authoritative_sent_watermark = Some(10_000);
        probe.receipt_frozen_sent_watermark = Some(11_200);
        probe.current_sent_watermark = 11_200;
        let mut tracker = QuicPathMetricTracker::default();

        let measured = tracker.observe_at(stats, with_capacity_probe(base, probe), 2, now);
        let candidate = measured
            .capacity_proof_candidate
            .expect("terminal exact receipt publishes independently of native cleanup");
        assert_eq!(candidate.proof_elapsed, Duration::from_millis(80));
        assert_eq!(candidate.accepted_at, now);
        assert_eq!(candidate.expires_at, now + candidate.proof_validity);
        assert_eq!(
            candidate.rate_bps,
            quic_capacity_receipt_rate_bps(candidate.train_bytes, candidate.proof_elapsed)
                .expect("receipt rate")
        );

        probe.phase = quic_carrier::CapacityProbePhase::Proven;
        probe.timed_measurement_ack_elapsed = Some(Duration::from_secs(2));
        probe.current_sent_watermark = 12_400;
        let later = tracker.observe_at(
            stats,
            with_capacity_probe(base, probe),
            2,
            now + Duration::from_millis(10),
        );
        assert_eq!(later.capacity_proof_candidate, Some(candidate));

        let mut late_tracker = QuicPathMetricTracker::default();
        let independently_observed = late_tracker.observe_at(
            stats,
            with_capacity_probe(base, probe),
            2,
            now + Duration::from_millis(20),
        );
        assert_eq!(
            independently_observed.capacity_proof_candidate,
            Some(candidate)
        );
    }

    #[test]
    fn quic_capacity_candidate_accepts_only_receipted_publishable_phases() {
        let now = Instant::now();
        let required_bytes = 240 * 1024_u64;
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = 256 * 1024;
        stats.path.current_mtu = 1400;
        let base = quic_congestion(256 * 1024, Some(100_000_000));

        let proven = QuicPathMetricTracker::default().observe_at(
            stats,
            with_capacity_probe(
                base,
                capacity_probe_metrics(43, now, 0, required_bytes, 0, 0, None),
            ),
            2,
            now,
        );
        assert!(proven.capacity_proof_candidate.is_some());
        for phase in [
            quic_carrier::CapacityProbePhase::Writing,
            quic_carrier::CapacityProbePhase::Measuring,
            quic_carrier::CapacityProbePhase::ProvenDraining,
            quic_carrier::CapacityProbePhase::Expired,
            quic_carrier::CapacityProbePhase::Aborted,
        ] {
            let mut probe = capacity_probe_metrics(44, now, 0, required_bytes, 0, 0, None);
            probe.phase = phase;
            let observed = QuicPathMetricTracker::default().observe_at(
                stats,
                with_capacity_probe(base, probe),
                2,
                now,
            );
            assert!(
                observed.capacity_proof_candidate.is_none(),
                "phase {phase:?} cannot publish receipt authority"
            );
        }
    }

    #[test]
    fn quic_active_capacity_probe_uses_bounded_quarter_rtt_poll_cadence() {
        let now = Instant::now();
        let required_bytes = 240 * 1024_u64;
        let metrics_for = |phase, rtt: Duration| {
            let mut stats = quinn::ConnectionStats::default();
            stats.path.rtt = rtt;
            stats.path.cwnd = 256 * 1024;
            stats.path.current_mtu = 1400;
            let mut probe = capacity_probe_metrics(45, now, 0, required_bytes, 0, 0, None);
            probe.phase = phase;
            QuicPathMetricTracker::default().observe_at(
                stats,
                with_capacity_probe(quic_congestion(256 * 1024, None), probe),
                2,
                now,
            )
        };

        for phase in [
            quic_carrier::CapacityProbePhase::Writing,
            quic_carrier::CapacityProbePhase::Measuring,
            quic_carrier::CapacityProbePhase::ProvenDraining,
            quic_carrier::CapacityProbePhase::Proven,
        ] {
            assert_eq!(
                quic_path_metrics_poll_interval(metrics_for(phase, Duration::from_millis(80))),
                Duration::from_millis(20),
                "phase {phase:?} must be polled faster than idle PTO cadence"
            );
        }
        assert_eq!(
            quic_path_metrics_poll_interval(metrics_for(
                quic_carrier::CapacityProbePhase::Proven,
                Duration::from_millis(400),
            )),
            QUIC_MAX_ACK_DELAY
        );
        assert_eq!(
            quic_path_metrics_poll_interval(metrics_for(
                quic_carrier::CapacityProbePhase::Measuring,
                Duration::from_millis(2),
            )),
            QUIC_TIMER_GRANULARITY
        );
        let expired = metrics_for(
            quic_carrier::CapacityProbePhase::Expired,
            Duration::from_millis(80),
        );
        assert!(quic_path_metrics_poll_interval(expired) > Duration::from_millis(20));
    }

    #[test]
    fn quic_capacity_probe_requires_exact_full_train_receipt() {
        let now = Instant::now();
        let warmup_bytes = 384 * 1024_u64;
        let required_bytes = 240 * 1024_u64;
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = 512 * 1024;
        stats.path.current_mtu = 1400;
        let base = quic_congestion(512 * 1024, Some(100_000_000));
        let mut tracker = QuicPathMetricTracker::default();

        let mut incomplete_receipt =
            capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None);
        incomplete_receipt.receipt_received_payload_bytes =
            incomplete_receipt.train_payload_bytes - 1;
        let below_floor =
            tracker.observe_at(stats, with_capacity_probe(base, incomplete_receipt), 2, now);
        assert!(below_floor.capacity_proof_candidate.is_none());

        let proven = tracker.observe_at(
            stats,
            with_capacity_probe(
                base,
                capacity_probe_metrics(51, now, warmup_bytes, required_bytes, 0, 0, None),
            ),
            2,
            now + Duration::from_millis(1),
        );
        let candidate = proven
            .capacity_proof_candidate
            .expect("exact receiver-confirmed train");
        assert_eq!(candidate.warmup_bytes, warmup_bytes);
        assert_eq!(candidate.received_bytes, candidate.train_bytes);
        assert_eq!(candidate.required_proof_bytes, required_bytes);
    }

    #[test]
    fn quic_capacity_receipt_candidate_is_sticky_and_frozen() {
        let now = Instant::now();
        let required_bytes = 240 * 1024_u64;
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = 256 * 1024;
        stats.path.current_mtu = 1400;
        let base = quic_congestion(256 * 1024, Some(100_000_000));
        let mut tracker = QuicPathMetricTracker::default();
        let probe = |token, elapsed| {
            with_capacity_probe(
                base,
                capacity_probe_metrics(token, now, 0, required_bytes, required_bytes, 32, elapsed),
            )
        };

        let received = tracker.observe_at(stats, probe(61, None), 2, now);
        let accepted = received
            .capacity_proof_candidate
            .expect("receipt does not depend on a native ACK span");
        let mut retried = tracker.observe_at(
            stats,
            probe(61, Some(Duration::from_millis(40))),
            2,
            now + Duration::from_millis(2),
        );
        let retried_candidate = retried
            .capacity_proof_candidate
            .expect("transient rejection must retain sticky token");
        assert_eq!(retried_candidate.token, accepted.token);
        tracker.accept_capacity_proof(&mut retried, retried_candidate);
        let frozen_deadline = retried_candidate.expires_at;
        assert_eq!(
            frozen_deadline,
            retried_candidate.accepted_at + retried_candidate.proof_validity
        );
        let sticky = tracker.observe_at(
            stats,
            probe(61, Some(Duration::from_millis(40))),
            2,
            now + Duration::from_millis(3),
        );
        assert!(sticky.capacity_proof_candidate.is_none());
        assert!(sticky.bulk_proof_expires_at.is_none());
        let expired_sticky = tracker.observe_at(
            stats,
            probe(61, Some(Duration::from_millis(40))),
            2,
            frozen_deadline,
        );
        assert!(expired_sticky.app_limited);
        assert!(expired_sticky.capacity_proof_candidate.is_none());
        let rollover_at = frozen_deadline + Duration::from_millis(1);
        let rollover = tracker.observe_at(
            stats,
            with_capacity_probe(
                base,
                capacity_probe_metrics(
                    62,
                    rollover_at,
                    0,
                    required_bytes,
                    required_bytes,
                    32,
                    Some(Duration::from_millis(40)),
                ),
            ),
            2,
            rollover_at,
        );
        assert_eq!(
            rollover.capacity_proof_candidate.map(|proof| proof.token),
            Some(62)
        );
    }

    #[test]
    fn quic_bulk_proof_deadline_does_not_shrink_with_falling_rtt() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(400);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let proof_at = base + Duration::from_millis(1);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, 2, base);
        let proven = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            proof_at,
        );
        let frozen_deadline = proven
            .bulk_proof_expires_at
            .expect("accepted proof deadline");

        stats.path.rtt = Duration::from_millis(20);
        let smaller_horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
        assert!(proof_at + smaller_horizon < frozen_deadline);
        let still_fresh = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
            proof_at + smaller_horizon,
        );
        assert!(!still_fresh.app_limited);
        assert_eq!(still_fresh.bulk_proof_expires_at, Some(frozen_deadline));

        let expired = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
            frozen_deadline,
        );
        assert!(expired.app_limited);
        assert!(expired.bulk_proof_expires_at.is_none());
    }

    #[test]
    fn quic_expired_proof_preserves_new_pending_sample() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let fragment_bytes = sample_bytes / 8;
        let congestion = quic_congestion(sample_bytes, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let proof_at = base + Duration::from_millis(1);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, 2, base);
        let proven = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            proof_at,
        );
        let deadline = proven.bulk_proof_expires_at.expect("proof deadline");
        let written_bytes = sample_bytes.saturating_mul(3);
        let _ = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, written_bytes),
                fragment_bytes,
                2,
            ),
            2,
            deadline - QUIC_TIMER_GRANULARITY,
        );
        assert_eq!(tracker.pending_non_app_limited_sample_bytes, fragment_bytes);

        let expired = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, written_bytes),
            2,
            deadline,
        );
        assert!(expired.app_limited);
        assert_eq!(tracker.pending_non_app_limited_sample_bytes, fragment_bytes);
        assert_eq!(tracker.pending_non_app_limited_sample_count, 2);
        assert!(!tracker.pending_non_app_limited_sample_elapsed.is_zero());
    }

    #[test]
    fn quic_bulk_proof_is_fresh_inside_persistent_congestion_horizon() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let proof_at = base + Duration::from_millis(1);
        let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, 2, base);
        let proven = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            proof_at,
        );

        assert!(!proven.app_limited);
        let fresh = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
            proof_at + horizon - QUIC_TIMER_GRANULARITY,
        );
        assert_eq!(fresh.delivery_sample_count, proven.delivery_sample_count);
        assert_eq!(fresh.delivery_sample_bytes, proven.delivery_sample_bytes);
        assert!(!fresh.app_limited);
    }

    #[test]
    fn quic_aged_bulk_proof_expires_without_erasing_ack_reachability() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let proof_at = base + Duration::from_millis(1);
        let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, 2, base);
        let proven = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            proof_at,
        );
        assert!(proven.ack_derived_data_seen);

        let aged = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
            proof_at + horizon,
        );
        assert!(aged.ack_derived_data_seen);
        assert_eq!(aged.delivery_rate_bps, proven.delivery_rate_bps);
        assert_eq!(aged.delivery_sample_count, proven.delivery_sample_count);
        assert_eq!(aged.delivery_sample_bytes, proven.delivery_sample_bytes);
        assert_eq!(aged.last_delivery_sample_at, proven.last_delivery_sample_at);
        assert!(aged.bulk_proof_expires_at.is_none());
        assert!(aged.app_limited);
    }

    #[test]
    fn quic_reproved_bulk_rights_are_not_permanently_sticky() {
        let sample_bytes = RELIABLE_STREAM_STARTUP_PRODUCT_WINDOW_BYTES / 2;
        let congestion = quic_congestion(sample_bytes, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(20);
        stats.path.cwnd = sample_bytes;
        stats.path.current_mtu = 1400;
        let base = Instant::now();
        let first_proof_at = base + Duration::from_millis(1);
        let horizon = quic_bulk_proof_freshness_horizon(stats.path.rtt, stats.path.rtt / 4);
        let mut tracker = QuicPathMetricTracker::default();
        let _ = tracker.observe_at(stats, congestion, 2, base);
        let _ = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            first_proof_at,
        );
        let _ = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes),
            2,
            first_proof_at + horizon,
        );

        let second_proof_at = first_proof_at + horizon + QUIC_TIMER_GRANULARITY;
        let reproved = tracker.observe_at(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, sample_bytes * 2),
                sample_bytes,
                QUIC_INITIAL_WINDOW_PACKETS as u64,
            ),
            2,
            second_proof_at,
        );
        assert!(!reproved.app_limited);
        assert!(reproved.delivery_sample_count > 0);

        let aged_again = tracker.observe_at(
            stats,
            with_delivery_evidence_written(congestion, sample_bytes * 2),
            2,
            second_proof_at + horizon,
        );
        assert!(aged_again.app_limited);
        assert_eq!(aged_again.delivery_rate_bps, reproved.delivery_rate_bps);
        assert_eq!(
            aged_again.delivery_sample_count,
            reproved.delivery_sample_count
        );
        assert_eq!(
            aged_again.delivery_sample_bytes,
            reproved.delivery_sample_bytes
        );
        assert_eq!(
            aged_again.last_delivery_sample_at,
            reproved.last_delivery_sample_at
        );
        assert!(aged_again.bulk_proof_expires_at.is_none());
        assert!(aged_again.ack_derived_data_seen);
    }

    #[test]
    fn quic_first_confident_sample_replaces_optimistic_startup_prior() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 1;
        let first_quantum = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, PATH_OPEN_SCORE_BYTES as u64),
                PATH_OPEN_SCORE_BYTES as u64,
                1,
            ),
            2,
        );
        assert_eq!(first_quantum.delivery_sample_count, 1);
        assert_eq!(first_quantum.delivery_rate_bps, startup.delivery_rate_bps);

        let measured_bytes = 2 * 1024 * 1024_u64;
        stats.frame_rx.acks += 9;
        let confident = tracker.quic.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(
                    congestion,
                    PATH_OPEN_SCORE_BYTES as u64 + measured_bytes,
                ),
                measured_bytes,
                9,
                Duration::from_millis(200),
            ),
            2,
        );

        assert_eq!(
            confident.delivery_sample_count,
            QUIC_INITIAL_WINDOW_PACKETS as u64
        );
        assert!(confident.delivery_rate_bps < startup.delivery_rate_bps);
        let expected_rate = measured_bytes as f64 * 8.0 / 0.2;
        assert!(
            confident.delivery_rate_bps >= expected_rate * 0.95
                && confident.delivery_rate_bps <= expected_rate,
            "the first confident rate must replace, not maximize against, the unmeasured pacing prior: expected~{expected_rate} actual={}",
            confident.delivery_rate_bps,
        );
    }

    #[test]
    fn quic_confidence_boundary_discards_inflated_preconfidence_sample() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(PATH_OPEN_SCORE_BYTES as u64, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = PATH_OPEN_SCORE_BYTES as u64;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);

        let fast_sample_bytes = 64 * 1024_u64;
        stats.frame_rx.acks = 1;
        let preconfidence = tracker.quic.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(congestion, fast_sample_bytes),
                fast_sample_bytes,
                1,
                Duration::from_millis(1),
            ),
            2,
        );
        assert_eq!(preconfidence.delivery_sample_count, 1);
        assert!(
            preconfidence.delivery_rate_bps > startup.delivery_rate_bps,
            "the setup must retain an inflated provisional sample before confidence"
        );

        let measured_bytes = 2 * 1024 * 1024_u64;
        stats.frame_rx.acks += 9;
        let confident = tracker.quic.observe(
            stats,
            with_acked_bytes_elapsed(
                with_delivery_evidence_written(
                    congestion,
                    fast_sample_bytes.saturating_add(measured_bytes),
                ),
                measured_bytes,
                9,
                Duration::from_millis(200),
            ),
            2,
        );

        let expected_rate = measured_bytes as f64 * 8.0 / 0.2;
        assert_eq!(
            confident.delivery_sample_count,
            QUIC_INITIAL_WINDOW_PACKETS as u64
        );
        assert!(
            confident.delivery_rate_bps >= expected_rate * 0.95
                && confident.delivery_rate_bps <= expected_rate,
            "confidence graduation must use the establishing sample, not retain a faster preconfidence outlier: expected~{expected_rate} actual={}",
            confident.delivery_rate_bps,
        );
    }

    #[test]
    fn quic_confidence_requires_ack_samples_and_current_flight_volume() {
        let mut tracker = UdpPathMetricTracker::default();
        let startup_cwnd = PATH_OPEN_SCORE_BYTES as u64;
        let startup_congestion = quic_congestion(startup_cwnd, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = startup_cwnd;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, startup_congestion, 2);
        let first = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(startup_congestion, startup_cwnd),
                startup_cwnd,
                1,
            ),
            2,
        );
        assert_eq!(first.delivery_sample_count, 1);

        let grown_cwnd = 4 * 1024 * 1024_u64;
        let tiny_followup = 9 * 1024_u64;
        let grown_congestion = quic_congestion(grown_cwnd, Some(500_000_000));
        stats.path.cwnd = grown_cwnd;
        let count_only = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(
                    grown_congestion,
                    startup_cwnd.saturating_add(tiny_followup),
                ),
                tiny_followup,
                9,
            ),
            2,
        );
        assert_eq!(
            count_only.delivery_sample_count,
            QUIC_INITIAL_WINDOW_PACKETS.saturating_sub(1) as u64,
            "sample count alone cannot graduate below the current carrier flight evidence floor"
        );
        assert_eq!(count_only.delivery_rate_bps, startup.delivery_rate_bps);
        let byte_confident = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(
                    grown_congestion,
                    startup_cwnd
                        .saturating_add(tiny_followup)
                        .saturating_add(grown_cwnd),
                ),
                grown_cwnd,
                1,
            ),
            2,
        );
        assert_eq!(
            byte_confident.delivery_sample_count,
            QUIC_INITIAL_WINDOW_PACKETS as u64
        );
        assert!(byte_confident.delivery_rate_bps < startup.delivery_rate_bps);
    }

    #[test]
    fn quic_app_limited_duplicate_ack_counts_as_ack_data_seen_not_bulk_rate() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 1;
        let app_limited = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );
        let product_metrics = path_metrics_from_quic_path(PathId(7), app_limited);

        assert!(app_limited.ack_derived_data_seen);
        assert_eq!(app_limited.delivery_sample_count, 0);
        assert!(app_limited.app_limited);
        assert!(product_metrics.has_ack_derived_data_sample);
        assert_eq!(product_metrics.data_sample_count, 0);
    }

    #[test]
    fn quic_server_metrics_publish_ack_data_seen_even_when_app_limited() {
        let metrics = UdpPathMetrics {
            direction: 2,
            srtt: Duration::from_millis(50),
            rttvar: Duration::from_millis(5),
            min_rtt: Duration::from_millis(45),
            min_rtt_observed: true,
            delivery_rate_bps: 500_000_000.0,
            pacing_rate_bps: 500_000_000.0,
            inflight_hi: 4 * 1024 * 1024,
            bytes_in_flight: 0,
            pending_bytes: 0,
            loss_ppm: None,
            ecn_ppm: None,
            app_limited: true,
            ack_derived_data_seen: true,
            delivery_sample_count: 0,
            delivery_sample_bytes: 0,
            last_delivery_sample_at: None,
            bulk_proof_expires_at: None,
            latest_delivery_sample_bytes: 0,
            latest_delivery_sample_count: 0,
            latest_carrier_ack_elapsed: None,
            latest_rate_sample_elapsed: None,
            capacity_proof_candidate: None,
            capacity_probe: None,
            #[cfg(feature = "lab-diagnostics")]
            ack_poll: QuicAckPollDiagnostics::default(),
        };

        assert!(quic_path_metrics_should_publish_local_sender(metrics));
        let product_metrics = path_metrics_from_quic_path(PathId(7), metrics);
        assert!(product_metrics.has_ack_derived_data_sample);
        assert_eq!(product_metrics.data_sample_count, 0);
        assert!(product_metrics.app_limited);
    }

    #[test]
    fn quic_ack_after_prior_data_send_counts_as_ack_data_seen() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);

        let sent_without_ack = tracker.quic.observe(
            stats,
            with_delivery_evidence_written(congestion, 32 * 1024),
            2,
        );
        assert!(!sent_without_ack.ack_derived_data_seen);
        let ack_after_send = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 32 * 1024),
                32 * 1024,
                1,
            ),
            2,
        );

        assert!(
            ack_after_send.ack_derived_data_seen,
            "QUIC ACK-derived data evidence must survive normal TX/ACK timing; it cannot require TX and ACK in the same metrics poll"
        );
        assert_eq!(ack_after_send.delivery_sample_count, 0);
        assert!(ack_after_send.app_limited);
    }

    #[test]
    fn quic_compressed_ack_sample_cannot_jump_beyond_startup_gain() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(4 * 1024 * 1024, Some(500_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 4 * 1024 * 1024;
        stats.path.current_mtu = 1400;
        let startup = tracker.quic.observe(stats, congestion, 2);
        stats.frame_rx.acks = 64;
        let measured = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 64 * 1024 * 1024),
                64 * 1024 * 1024,
                64,
            ),
            2,
        );

        assert_eq!(measured.delivery_sample_count, 64);
        assert!(measured.delivery_rate_bps <= startup.delivery_rate_bps * BBR_DEFAULT_CWND_GAIN);
    }

    #[test]
    fn quic_lower_full_sample_smoothly_reduces_bulk_rate_model() {
        let mut tracker = UdpPathMetricTracker::default();
        let congestion = quic_congestion(512 * 1024, Some(100_000_000));
        let mut stats = quinn::ConnectionStats::default();
        stats.path.rtt = Duration::from_millis(50);
        stats.path.cwnd = 512 * 1024;
        stats.path.current_mtu = 1400;
        let _ = tracker.quic.observe(stats, congestion, 2);
        stats.udp_tx.bytes = 8 * 1024 * 1024;
        stats.frame_tx.stream = 512;
        stats.frame_rx.acks = 16;
        let raised = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 8 * 1024 * 1024),
                8 * 1024 * 1024,
                16,
            ),
            2,
        );
        stats.udp_tx.bytes += 512 * 1024;
        stats.frame_tx.stream += 512;
        stats.frame_rx.acks += 16;
        let after_low = tracker.quic.observe(
            stats,
            with_acked_bytes(
                with_delivery_evidence_written(congestion, 8 * 1024 * 1024 + 512 * 1024),
                512 * 1024,
                16,
            ),
            2,
        );

        assert_eq!(after_low.delivery_sample_count, 32);
        let low_sample_rate = 512.0 * 1024.0 * 8.0 / 0.100;
        assert!(after_low.delivery_rate_bps < raised.delivery_rate_bps);
        assert!(after_low.delivery_rate_bps > low_sample_rate);
        assert!(after_low.delivery_rate_bps <= raised.delivery_rate_bps * 0.5);
        assert_eq!(
            after_low.delivery_rate_bps,
            raised
                .delivery_rate_bps
                .mul_add(0.25, low_sample_rate * 0.75)
        );
    }
}

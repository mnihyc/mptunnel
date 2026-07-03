use super::*;
/// Session-wide registry for server-side product reliable streams.
///
/// The registry owns stream lookup, target consistency, recent closed-stream
/// filtering, and peer/local path metrics for response scheduling. It does not
/// own target sockets or carrier packet state.
pub(in crate::runtime) struct ServerReliableStreamRegistry {
    streams: Mutex<HashMap<(SessionId, StreamId), ServerReliableStreamEntry>>,
    path_metrics: Mutex<HashMap<(SessionId, UnderlayProtocol, PathId), ServerPathMetricsEntry>>,
    closed_streams: Mutex<RecentIdCache<(SessionId, StreamId)>>,
    lane_tracker: Arc<ServerPathLaneTracker>,
}

impl std::fmt::Debug for ServerReliableStreamRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerReliableStreamRegistry")
            .finish_non_exhaustive()
    }
}

struct ServerReliableStreamEntry {
    target: TargetAddr,
    lane: FlowLane,
    frames: mpsc::Sender<Result<Frame, RuntimeError>>,
    binding: Arc<ResponseStreamBinding>,
}

pub(in crate::runtime) struct ServerReliablePathAttachment {
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) commands: ReliablePathCommandSender,
    pub(in crate::runtime) max_frame_payload_bytes: usize,
    pub(in crate::runtime) role: StreamOpenRole,
    pub(in crate::runtime) initial_metrics: Option<PathMetrics>,
}

/// Request to open or attach a carrier path to a product reliable stream.
///
/// The attachment carries carrier command access; the registry decides whether
/// this is a new product stream or an additional path for an existing stream.
pub(in crate::runtime) struct ServerReliableStreamOpenRequest<'a> {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) stream_id: StreamId,
    pub(in crate::runtime) target: &'a TargetAddr,
    pub(in crate::runtime) lane: FlowLane,
    pub(in crate::runtime) attachment: ServerReliablePathAttachment,
}

pub(in crate::runtime) enum ServerReliableStreamOpen {
    New(ReliablePathStream),
    Existing,
    DuplicateLiveIgnored,
    Rejected,
}

pub(in crate::runtime) struct ServerReliableRegistryManagementSnapshot {
    pub(in crate::runtime) active_streams: usize,
    pub(in crate::runtime) path_metrics: Vec<ServerCarrierPathMetricSnapshot>,
}

pub(in crate::runtime) struct ServerCarrierPathMetricSnapshot {
    pub(in crate::runtime) session_id: SessionId,
    pub(in crate::runtime) underlay: UnderlayProtocol,
    pub(in crate::runtime) path_id: PathId,
    pub(in crate::runtime) metrics: PathMetrics,
    pub(in crate::runtime) source: &'static str,
}

impl ServerReliableStreamRegistry {
    pub(in crate::runtime) fn new(max_streams: usize) -> Self {
        Self {
            streams: Mutex::new(HashMap::new()),
            path_metrics: Mutex::new(HashMap::new()),
            closed_streams: Mutex::new(RecentIdCache::new(reliable_closed_stream_cache_capacity(
                max_streams,
            ))),
            lane_tracker: Arc::new(ServerPathLaneTracker::default()),
        }
    }

    pub(in crate::runtime) fn management_snapshot(
        &self,
    ) -> ServerReliableRegistryManagementSnapshot {
        let active_streams = self.streams.lock().expect("server stream lock").len();
        let path_metrics = self
            .path_metrics
            .lock()
            .expect("server path metrics lock")
            .iter()
            .map(
                |((session_id, underlay, path_id), entry)| ServerCarrierPathMetricSnapshot {
                    session_id: *session_id,
                    underlay: *underlay,
                    path_id: *path_id,
                    metrics: entry.metrics,
                    source: match entry.source {
                        ServerPathMetricsSource::PeerHint => "peer_hint",
                        ServerPathMetricsSource::LocalSender => "local_sender",
                    },
                },
            )
            .collect();
        ServerReliableRegistryManagementSnapshot {
            active_streams,
            path_metrics,
        }
    }

    pub(in crate::runtime) fn open_or_attach(
        &self,
        request: ServerReliableStreamOpenRequest<'_>,
        mux_limits: MuxLimits,
        max_streams: usize,
    ) -> Result<ServerReliableStreamOpen, RuntimeError> {
        let ServerReliableStreamOpenRequest {
            session_id,
            stream_id,
            target,
            lane,
            attachment,
        } = request;
        let max_frame_payload_bytes = attachment.max_frame_payload_bytes;
        let underlay = attachment.underlay;
        let path_id = attachment.path_id;
        let role = attachment.role;
        let initial_metrics = attachment
            .initial_metrics
            .map(|metrics| ServerPathMetricsEntry {
                metrics,
                source: ServerPathMetricsSource::LocalSender,
            })
            .or_else(|| self.stored_path_metrics(session_id, underlay, path_id));
        let mut streams = self
            .streams
            .lock()
            .expect("server reliable stream registry lock");
        if let Some(entry) = streams.get_mut(&(session_id, stream_id)) {
            if entry.target != *target {
                return Err(RuntimeError::Protocol(
                    "reliable stream migration target does not match original stream",
                ));
            }
            entry.lane = lane;
            let attach_outcome = entry.binding.attach(
                underlay,
                path_id,
                attachment.commands,
                lane,
                role,
                max_frame_payload_bytes,
            );
            if matches!(
                attach_outcome,
                ResponseStreamAttachOutcome::RejectedDuplicateLiveOutput
            ) {
                #[cfg(feature = "lab-diagnostics")]
                lab_diagnostic(
                    "server_stream_open",
                    format_args!(
                        "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_duplicate_live_output",
                        session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                    ),
                );
                return Ok(ServerReliableStreamOpen::DuplicateLiveIgnored);
            }
            if let Some(metrics) = initial_metrics {
                entry.binding.update_path_metrics(
                    CarrierPathKey { underlay, path_id },
                    metrics.metrics,
                    metrics.source,
                );
            }
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=existing",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Existing);
        }

        if self
            .closed_streams
            .lock()
            .expect("server reliable stream closed cache lock")
            .contains(&(session_id, stream_id))
        {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_closed_stream",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Rejected);
        }

        if role != StreamOpenRole::Active {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_open",
                format_args!(
                    "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=rejected_attach_only_unknown",
                    session_id.0, stream_id.0, underlay, path_id.0, role, lane,
                ),
            );
            return Ok(ServerReliableStreamOpen::Rejected);
        }

        if streams.len() >= max_streams {
            return Err(RuntimeError::Protocol(
                "server reliable stream limit reached",
            ));
        }

        let (frames_tx, frames_rx) = mpsc::channel(reliable_stream_frame_queue_for_payload(
            mux_limits,
            max_frame_payload_bytes,
        ));
        let binding = ResponseStreamBinding::new_with_limits_and_tracker(
            session_id,
            underlay,
            path_id,
            attachment.commands,
            lane,
            mux_limits,
            self.lane_tracker.clone(),
        );
        if let Some(metrics) = initial_metrics {
            binding.update_path_metrics(
                CarrierPathKey { underlay, path_id },
                metrics.metrics,
                metrics.source,
            );
        }
        streams.insert(
            (session_id, stream_id),
            ServerReliableStreamEntry {
                target: target.clone(),
                lane,
                frames: frames_tx,
                binding: binding.clone(),
            },
        );
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_stream_open",
            format_args!(
                "session_id={} stream_id={} path_underlay={:?} path_id={} role={:?} lane={:?} result=new",
                session_id.0, stream_id.0, underlay, path_id.0, role, lane,
            ),
        );
        Ok(ServerReliableStreamOpen::New(ReliablePathStream {
            stream_id,
            max_offset: mux_limits.max_stream_window_bytes,
            lane,
            underlay,
            max_frame_payload_bytes,
            output: ReliablePathStreamOutput::Switchable(binding),
            frames: frames_rx,
        }))
    }

    pub(in crate::runtime) fn record_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            session_id,
            underlay,
            path_id,
            metrics,
            ServerPathMetricsSource::PeerHint,
        );
    }

    pub(in crate::runtime) fn record_local_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
    ) {
        self.record_path_metrics_with_source(
            session_id,
            underlay,
            path_id,
            metrics,
            ServerPathMetricsSource::LocalSender,
        );
    }

    fn record_path_metrics_with_source(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        metrics: PathMetrics,
        source: ServerPathMetricsSource,
    ) {
        let metrics = PathMetrics { path_id, ..metrics };
        let entry = ServerPathMetricsEntry { metrics, source };
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .insert((session_id, underlay, path_id), entry);
        #[cfg(feature = "lab-diagnostics")]
        lab_diagnostic(
            "server_path_metrics_recorded",
            format_args!(
                "session_id={} underlay={:?} path_id={} source={:?} direction={:?} rate_mbps={:.3} pacing_mbps={:.3} srtt_ms={:.3} confidence_ppm={} app_limited={} ack_sample={} sample_count={}",
                session_id.0,
                underlay,
                path_id.0,
                source,
                metrics.direction,
                metrics.delivery_rate_bps as f64 / 1_000_000.0,
                metrics.pacing_rate_bps as f64 / 1_000_000.0,
                metrics.srtt_us as f64 / 1000.0,
                metrics.confidence_ppm,
                metrics.app_limited,
                metrics.has_ack_derived_data_sample,
                metrics.data_sample_count,
            ),
        );
        let bindings = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .iter()
                .filter_map(|((entry_session_id, _), entry)| {
                    (*entry_session_id == session_id).then_some(entry.binding.clone())
                })
                .collect::<Vec<_>>()
        };
        let key = CarrierPathKey { underlay, path_id };
        for binding in bindings {
            binding.update_path_metrics(key, metrics, source);
        }
    }

    fn stored_path_metrics(
        &self,
        session_id: SessionId,
        underlay: UnderlayProtocol,
        path_id: PathId,
    ) -> Option<ServerPathMetricsEntry> {
        self.path_metrics
            .lock()
            .expect("server path metrics lock")
            .get(&(session_id, underlay, path_id))
            .copied()
    }

    pub(in crate::runtime) fn detach_path(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        underlay: UnderlayProtocol,
        path_id: PathId,
        commands: &ReliablePathCommandSender,
    ) {
        if let Some(binding) = self
            .streams
            .lock()
            .expect("server reliable stream registry lock")
            .get(&(session_id, stream_id))
            .map(|entry| entry.binding.clone())
        {
            binding.detach(CarrierPathKey { underlay, path_id }, commands);
        }
    }

    pub(in crate::runtime) async fn route_frame(
        &self,
        session_id: SessionId,
        stream_id: StreamId,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        #[cfg(feature = "lab-diagnostics")]
        let bytes = frame_pacing_bytes(&frame);
        let stream = {
            let streams = self
                .streams
                .lock()
                .expect("server reliable stream registry lock");
            streams
                .get(&(session_id, stream_id))
                .map(|entry| entry.frames.clone())
        };
        let Some(stream) = stream else {
            #[cfg(feature = "lab-diagnostics")]
            lab_diagnostic(
                "server_stream_unknown_frame_drop",
                format_args!(
                    "session_id={} stream_id={} frame_kind={}",
                    session_id.0,
                    stream_id.0,
                    frame_kind_name(&frame),
                ),
            );
            return Ok(());
        };
        #[cfg(feature = "lab-diagnostics")]
        let started = Instant::now();
        let result = stream
            .send(Ok(frame))
            .await
            .map_err(|_| RuntimeError::ReliablePathSessionClosed);
        #[cfg(feature = "lab-diagnostics")]
        lab_perf_record(
            "runtime.server_stream.route_frame",
            started.elapsed(),
            bytes,
        );
        result
    }

    pub(in crate::runtime) fn close(&self, session_id: SessionId, stream_id: StreamId) {
        let removed = self
            .streams
            .lock()
            .expect("server reliable stream registry lock")
            .remove(&(session_id, stream_id))
            .is_some();
        if removed {
            self.closed_streams
                .lock()
                .expect("server reliable stream closed cache lock")
                .insert((session_id, stream_id));
        }
    }
}

impl Default for ServerReliableStreamRegistry {
    fn default() -> Self {
        Self::new(ResourceLimits::default().max_streams)
    }
}

use super::*;

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

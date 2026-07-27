use super::*;

#[test]
fn defaults_preserve_the_deployed_resource_envelope() {
    let limits = ResourceLimits::default();

    assert_eq!(limits.max_frame_bytes, 1_048_576);
    assert_eq!(limits.max_payload_bytes, 1_048_512);
    assert_eq!(limits.max_streams, 65_536);
    assert_eq!(limits.max_repair_bytes, 64 * 1024 * 1024);
    assert_eq!(limits.max_reinjection_cache_chunks, 65_536);
    assert_eq!(limits.max_reorder_buffer_chunks, 65_536);
    assert_eq!(limits.max_retained_receive_ranges, 65_536);
    assert_eq!(limits.validate(), Ok(()));
}

#[test]
fn performance_default_preserves_the_deployed_overhead_hint() {
    assert_eq!(
        MppPerformanceConfig::default().extra_traffic_hint_percent,
        5
    );
}

#[test]
fn validation_is_owned_by_carrier_neutral_performance_policy() {
    let limits = ResourceLimits {
        quic_path_idle_timeout: DEFAULT_QUIC_PATH_KEEP_ALIVE_INTERVAL,
        ..ResourceLimits::default()
    };

    assert_eq!(
        limits.validate(),
        Err(ResourceLimitError::QuicPathIdleTimeoutTooSmall)
    );
}

#[test]
fn sparse_node_limits_must_be_nonzero() {
    for (limits, expected) in [
        (
            ResourceLimits {
                max_reinjection_cache_chunks: 0,
                ..ResourceLimits::default()
            },
            ResourceLimitError::ReinjectionCacheChunkLimitZero,
        ),
        (
            ResourceLimits {
                max_reorder_buffer_chunks: 0,
                ..ResourceLimits::default()
            },
            ResourceLimitError::ReorderBufferChunkLimitZero,
        ),
        (
            ResourceLimits {
                max_retained_receive_ranges: 0,
                ..ResourceLimits::default()
            },
            ResourceLimitError::RetainedReceiveRangeLimitZero,
        ),
    ] {
        assert_eq!(limits.validate(), Err(expected));
    }
}

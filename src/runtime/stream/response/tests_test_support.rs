//! Shared response-stream test fixtures.

use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
use crate::model::capacity::{reliable_path_startup_sample_limit_bytes, reliable_relay_buffer_len};
use crate::model::carrier_rate_authority::CarrierRateAuthorityScope;
use crate::model::path::CarrierPathKey;
use crate::mux::MuxLimits;
use crate::protocol::{
    Frame, OffsetRange, PathId, PathMetricDirection, SessionId, StreamId, UnderlayProtocol,
};
use crate::runtime::path::authority::NativeCarrierRateAuthorityHandle;
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, ReliablePathCommandSender, reliable_path_command_channels,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Duration;

pub(super) struct NativeResponseBindingFixture {
    pub(super) binding: Arc<ResponseStreamBinding>,
    pub(super) key: CarrierPathKey,
    pub(super) commands: ReliablePathCommandSender,
    pub(super) receivers: ReliablePathCommandReceivers,
    pub(super) authority: Arc<NativeCarrierRateAuthorityHandle>,
    pub(super) scope: CarrierRateAuthorityScope,
}

pub(super) fn binding_for_underlay(
    underlay: UnderlayProtocol,
) -> (
    Arc<ResponseStreamBinding>,
    CarrierPathKey,
    ReliablePathCommandReceivers,
) {
    let (commands, receivers) = reliable_path_command_channels(8);
    let key = CarrierPathKey {
        underlay,
        path_id: PathId(0),
    };
    let binding = ResponseStreamBinding::new(
        SessionId(42),
        underlay,
        key.path_id,
        commands,
        TrafficClass::Throughput,
    );
    (binding, key, receivers)
}

pub(super) fn native_response_binding_fixture(
    queue_capacity: usize,
    operational_rate_bps: Option<u128>,
) -> NativeResponseBindingFixture {
    let (commands, receivers) = reliable_path_command_channels(queue_capacity);
    let key = CarrierPathKey {
        underlay: UnderlayProtocol::Udp,
        path_id: PathId(31),
    };
    let binding = ResponseStreamBinding::new(
        SessionId(43),
        key.underlay,
        key.path_id,
        commands.clone(),
        TrafficClass::Throughput,
    );
    let path_instance_id = with_output_entry_for_key(&binding, key, |entry| entry.path_instance_id);
    let scope =
        CarrierRateAuthorityScope::new(path_instance_id, PathMetricDirection::ServerToClient);
    let authority = NativeCarrierRateAuthorityHandle::from_observation_for_test(
        scope,
        25_000_000,
        1,
        7,
        operational_rate_bps,
    )
    .expect("checked server Native authority fixture");
    let shape = authority
        .refresh_scheduling_shape_for_test(
            scope,
            1,
            7,
            operational_rate_bps,
            Duration::from_millis(80),
            Duration::from_millis(12),
            2 * 1024 * 1024,
            256 * 1024,
            1_400,
            Some(100_000_000),
            false,
        )
        .expect("activation-coherent server Native shape fixture");
    with_output_entry_for_key_mut(&binding, key, |entry| {
        entry.commands = entry
            .commands
            .clone()
            .with_native_rate_authority(authority.clone());
        entry.native_scheduling_shape = Some(shape);
    });

    NativeResponseBindingFixture {
        binding,
        key,
        commands,
        receivers,
        authority,
        scope,
    }
}

pub(super) fn stream_data_frame(payload_len: usize) -> Frame {
    stream_data_frame_at(0, payload_len)
}

pub(super) fn stream_data_frame_at(offset: u64, payload_len: usize) -> Frame {
    Frame::StreamData {
        stream_id: StreamId(7),
        offset,
        payload: Bytes::from(vec![0x5a; payload_len]),
    }
}

pub(super) fn with_output_entry_for_key<R>(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
    inspect: impl FnOnce(&ResponseStreamOutputEntry) -> R,
) -> R {
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let mut matching = outputs.entries.iter().filter(|entry| entry.key == key);
    let entry = matching
        .next()
        .expect("test response output key is attached");
    assert!(
        matching.next().is_none(),
        "test response output key must identify exactly one attachment"
    );
    inspect(entry)
}

pub(super) fn with_output_entry_for_key_mut<R>(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
    inspect: impl FnOnce(&mut ResponseStreamOutputEntry) -> R,
) -> R {
    let mut outputs = binding.outputs.lock().expect("test response outputs lock");
    let index = outputs
        .entries
        .iter()
        .position(|entry| entry.key == key)
        .expect("test response output key is attached");
    assert!(
        outputs.entries[index + 1..]
            .iter()
            .all(|entry| entry.key != key),
        "test response output key must identify exactly one attachment"
    );
    inspect(&mut outputs.entries[index])
}

pub(super) fn qualify_product_assignment(
    entry: &mut ResponseStreamOutputEntry,
    mux_limits: MuxLimits,
) {
    let floor = reliable_path_startup_sample_limit_bytes(mux_limits);
    let range = OffsetRange {
        start: 0,
        end: floor,
    };
    let receipt = entry
        .product_qualification
        .tag_admitted_original(
            floor,
            u64::try_from(reliable_relay_buffer_len(mux_limits))
                .expect("relay buffer length is u64-addressable"),
            range,
        )
        .expect("valid qualification fixture")
        .expect("qualification fixture receipt");
    assert_eq!(
        entry.product_qualification.release_exact(receipt, range),
        floor
    );
    assert!(entry.product_qualification.qualified());
}

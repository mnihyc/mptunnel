//! Shared response-stream test fixtures.

use super::ResponseStreamBinding;
use super::attachment::ResponseStreamOutputEntry;
use crate::model::path::CarrierPathKey;
use crate::protocol::{Frame, PathId, SessionId, StreamId, UnderlayProtocol};
use crate::runtime::path::commands::{
    ReliablePathCommandReceivers, reliable_path_command_channels,
};
use crate::scheduler::TrafficClass;
use bytes::Bytes;
use std::sync::Arc;

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

pub(super) fn output_entry_for_key(
    binding: &ResponseStreamBinding,
    key: CarrierPathKey,
) -> ResponseStreamOutputEntry {
    let outputs = binding.outputs.lock().expect("test response outputs lock");
    let mut matching = outputs.entries.iter().filter(|entry| entry.key == key);
    let entry = matching
        .next()
        .expect("test response output key is attached");
    assert!(
        matching.next().is_none(),
        "test response output key must identify exactly one attachment"
    );
    entry.clone()
}

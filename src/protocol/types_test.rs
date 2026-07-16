use super::*;

#[test]
fn offset_ranges_must_be_non_empty() {
    assert_eq!(OffsetRange::new(10, 10), None);
    assert_eq!(OffsetRange::new(11, 10), None);
    assert_eq!(OffsetRange::new(10, 12).map(OffsetRange::len), Some(2));
    assert!(OffsetRange { start: 10, end: 10 }.is_empty());
}

#[test]
fn peer_status_frames_are_ordinary_control_traffic() {
    let frames = [
        Frame::PeerStatusRequest { request_id: 1 },
        Frame::PeerStatusResponse {
            request_id: 1,
            code: PeerStatusCode::Disabled,
            paths: Vec::new(),
        },
    ];

    assert_eq!(frames[0].kind_name(), "PEER_STATUS_REQUEST");
    assert_eq!(frames[1].kind_name(), "PEER_STATUS_RESPONSE");
    for frame in frames {
        assert_eq!(
            frame.write_class(),
            FrameWriteClass::Ordinary {
                delivery_evidence_bytes: 0,
            }
        );
    }
}

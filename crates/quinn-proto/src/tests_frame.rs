use super::*;
use crate::coding::Codec;
use assert_matches::assert_matches;

fn frames(buf: Vec<u8>) -> Vec<Frame> {
    Iter::new(Bytes::from(buf))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn ack_coding() {
    const PACKETS: &[u64] = &[1, 2, 3, 5, 10, 11, 14];
    let mut ranges = ArrayRangeSet::new();
    for &packet in PACKETS {
        ranges.insert(packet..packet + 1);
    }
    let mut buf = Vec::new();
    const ECN: EcnCounts = EcnCounts {
        ect0: 42,
        ect1: 24,
        ce: 12,
    };
    Ack::encode(42, &ranges, Some(&ECN), &mut buf);
    let frames = frames(buf);
    assert_eq!(frames.len(), 1);
    match frames[0] {
        Frame::Ack(ref ack) => {
            let mut packets = ack.iter().flatten().collect::<Vec<_>>();
            packets.sort_unstable();
            assert_eq!(&packets[..], PACKETS);
            assert_eq!(ack.ecn, Some(ECN));
        }
        ref x => panic!("incorrect frame {x:?}"),
    }
}

#[test]
fn ack_frequency_coding() {
    let mut buf = Vec::new();
    let original = AckFrequency {
        sequence: VarInt(42),
        ack_eliciting_threshold: VarInt(20),
        request_max_ack_delay: VarInt(50_000),
        reordering_threshold: VarInt(1),
    };
    original.encode(&mut buf);
    let frames = frames(buf);
    assert_eq!(frames.len(), 1);
    match &frames[0] {
        Frame::AckFrequency(decoded) => assert_eq!(decoded, &original),
        x => panic!("incorrect frame {x:?}"),
    }
}

#[test]
fn immediate_ack_coding() {
    let mut buf = Vec::new();
    FrameType::IMMEDIATE_ACK.encode(&mut buf);
    let frames = frames(buf);
    assert_eq!(frames.len(), 1);
    assert_matches!(&frames[0], Frame::ImmediateAck);
}

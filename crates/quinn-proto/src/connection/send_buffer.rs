use std::{collections::VecDeque, ops::Range};

use bytes::{Buf, Bytes};

use crate::{range_set::RangeSet, VarInt};

/// Buffer of outgoing retransmittable stream data
#[derive(Default, Debug)]
pub(super) struct SendBuffer {
    /// Data queued by the application but not yet acknowledged. May or may not have been sent.
    unacked_segments: VecDeque<Segment>,
    /// Total size of `unacked_segments`
    unacked_len: usize,
    /// The first offset that hasn't been written by the application, i.e. the offset past the end of `unacked`
    offset: u64,
    /// The first offset that hasn't been sent
    ///
    /// Always lies in (offset - unacked.len())..offset
    unsent: u64,
    /// Acknowledged ranges which couldn't be discarded yet as they don't include the earliest
    /// offset in `unacked`
    // TODO: Recover storage from these by compacting (#700)
    acks: RangeSet,
    /// Previously transmitted ranges deemed lost
    retransmits: RangeSet,
}

/// One owned, contiguous range of unacknowledged stream bytes.
///
/// `start` is the absolute stream offset represented by `data[0]`. The starts are ordered and
/// may be equal when empty writes precede a non-empty write. This adds one 8-byte start per
/// retained segment.
#[derive(Debug)]
struct Segment {
    start: u64,
    data: Bytes,
}

impl SendBuffer {
    /// Construct an empty buffer at the initial offset
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Append application data to the end of the stream
    pub(super) fn write(&mut self, data: Bytes) {
        let start = self.offset;
        self.unacked_len += data.len();
        self.offset += data.len() as u64;
        self.unacked_segments.push_back(Segment { start, data });
    }

    /// Discard a range of acknowledged stream data
    pub(super) fn ack(&mut self, mut range: Range<u64>) {
        // Clamp the range to data which is still tracked
        let base_offset = self.offset - self.unacked_len as u64;
        range.start = base_offset.max(range.start);
        range.end = base_offset.max(range.end);

        self.acks.insert(range);

        while self.acks.min() == Some(self.offset - self.unacked_len as u64) {
            let prefix = self.acks.pop_min().unwrap();
            let mut to_advance = (prefix.end - prefix.start) as usize;

            self.unacked_len -= to_advance;
            while to_advance > 0 {
                let front = self
                    .unacked_segments
                    .front_mut()
                    .expect("Expected buffered data");

                if front.data.len() <= to_advance {
                    to_advance -= front.data.len();
                    self.unacked_segments.pop_front();

                    if self.unacked_segments.len() * 4 < self.unacked_segments.capacity() {
                        self.unacked_segments.shrink_to_fit();
                    }
                } else {
                    front.data.advance(to_advance);
                    front.start += to_advance as u64;
                    to_advance = 0;
                }
            }
        }
    }

    /// Compute the next range to transmit on this stream and update state to account for that
    /// transmission.
    ///
    /// `max_len` here includes the space which is available to transmit the
    /// offset and length of the data to send. The caller has to guarantee that
    /// there is at least enough space available to write maximum-sized metadata
    /// (8 byte offset + 8 byte length).
    ///
    /// The method returns a tuple:
    /// - The first return value indicates the range of data to send
    /// - The second return value indicates whether the length needs to be encoded
    ///   in the STREAM frames metadata (`true`), or whether it can be omitted
    ///   since the selected range will fill the whole packet.
    pub(super) fn poll_transmit(&mut self, mut max_len: usize) -> (Range<u64>, bool) {
        debug_assert!(max_len >= 8 + 8);
        let mut encode_length = false;

        if let Some(range) = self.retransmits.pop_min() {
            // Retransmit sent data

            // When the offset is known, we know how many bytes are required to encode it.
            // Offset 0 requires no space
            if range.start != 0 {
                max_len -= VarInt::size(unsafe { VarInt::from_u64_unchecked(range.start) });
            }
            if range.end - range.start < max_len as u64 {
                encode_length = true;
                max_len -= 8;
            }

            let end = range.end.min((max_len as u64).saturating_add(range.start));
            if end != range.end {
                self.retransmits.insert(end..range.end);
            }
            return (range.start..end, encode_length);
        }

        // Transmit new data

        // When the offset is known, we know how many bytes are required to encode it.
        // Offset 0 requires no space
        if self.unsent != 0 {
            max_len -= VarInt::size(unsafe { VarInt::from_u64_unchecked(self.unsent) });
        }
        if self.offset - self.unsent < max_len as u64 {
            encode_length = true;
            max_len -= 8;
        }

        let end = self
            .offset
            .min((max_len as u64).saturating_add(self.unsent));
        let result = self.unsent..end;
        self.unsent = end;
        (result, encode_length)
    }

    /// Returns data which is associated with a range
    ///
    /// This function can return a subset of the range, if the data is stored
    /// in noncontiguous fashion in the send buffer. In this case callers
    /// should call the function again with an incremented start offset to
    /// retrieve more data.
    pub(super) fn get(&self, offsets: Range<u64>) -> &[u8] {
        let mut low = 0;
        let mut high = self.unacked_segments.len();

        // Find the rightmost start <= the requested offset. There can be duplicate starts when
        // an empty write is followed by another write, so an arbitrary equal-key match could
        // select an empty segment and hide the following non-empty one.
        while low < high {
            let mid = low + (high - low) / 2;
            if self.unacked_segments[mid].start <= offsets.start {
                low = mid + 1;
            } else {
                high = mid;
            }
        }

        let Some(index) = low.checked_sub(1) else {
            return &[];
        };
        let segment = &self.unacked_segments[index];
        if offsets.start >= segment.start + segment.data.len() as u64 {
            return &[];
        }

        let start = (offsets.start - segment.start) as usize;
        let end = (offsets.end - segment.start) as usize;
        &segment.data[start..end.min(segment.data.len())]
    }

    /// Queue a range of sent but unacknowledged data to be retransmitted
    pub(super) fn retransmit(&mut self, range: Range<u64>) {
        debug_assert!(range.end <= self.unsent, "unsent data can't be lost");
        self.retransmits.insert(range);
    }

    pub(super) fn retransmit_all_for_0rtt(&mut self) {
        debug_assert_eq!(self.offset, self.unacked_len as u64);
        self.unsent = 0;
    }

    /// First stream offset unwritten by the application, i.e. the offset that the next write will
    /// begin at
    pub(super) fn offset(&self) -> u64 {
        self.offset
    }

    /// First offset not yet selected for its initial STREAM frame construction.
    pub(super) fn first_unpacketized(&self) -> u64 {
        self.unsent
    }

    /// Whether all sent data has been acknowledged
    pub(super) fn is_fully_acked(&self) -> bool {
        self.unacked_len == 0
    }

    /// Whether there's data to send
    ///
    /// There may be sent unacknowledged data even when this is false.
    pub(super) fn has_unsent_data(&self) -> bool {
        self.unsent != self.offset || !self.retransmits.is_empty()
    }

    /// Compute the amount of data that hasn't been acknowledged
    pub(super) fn unacked(&self) -> u64 {
        self.unacked_len as u64 - self.acks.iter().map(|x| x.end - x.start).sum::<u64>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Weak};
    use std::time::{Duration, Instant};

    struct TrackedBytesOwner {
        bytes: Vec<u8>,
        _lifetime: Arc<()>,
    }

    impl AsRef<[u8]> for TrackedBytesOwner {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    #[test]
    fn fragment_with_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => 13 byte data isn't enough
        // with 8 bytes reserved for length 11 payload bytes will fit
        assert_eq!(buf.poll_transmit(19), (0..11, true));
        assert_eq!(
            buf.poll_transmit(MSG.len() + 16 - 11),
            (11..MSG.len() as u64, true)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn fragment_without_length() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with some extra data!";
        buf.write(MSG.into());
        // 0 byte offset => 19 bytes left => can be filled by 34 bytes payload
        assert_eq!(buf.poll_transmit(19), (0..19, false));
        assert_eq!(
            buf.poll_transmit(MSG.len() - 19 + 1),
            (19..MSG.len() as u64, false)
        );
        assert_eq!(
            buf.poll_transmit(58),
            (MSG.len() as u64..MSG.len() as u64, true)
        );
    }

    #[test]
    fn reserves_encoded_offset() {
        let mut buf = SendBuffer::new();

        // Pretend we have more than 1 GB of data in the buffer
        let chunk: Bytes = Bytes::from_static(&[0; 1024 * 1024]);
        for _ in 0..1025 {
            buf.write(chunk.clone());
        }

        const SIZE1: u64 = 64;
        const SIZE2: u64 = 16 * 1024;
        const SIZE3: u64 = 1024 * 1024 * 1024;

        // Offset 0 requires no space
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        buf.retransmit(0..16);
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        let mut transmitted = 16u64;

        // Offset 16 requires 1 byte
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        buf.retransmit(transmitted..SIZE1);
        assert_eq!(
            buf.poll_transmit((SIZE1 - transmitted + 1) as usize),
            (transmitted..SIZE1, false)
        );
        transmitted = SIZE1;

        // Offset 64 requires 2 bytes
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        buf.retransmit(transmitted..SIZE2);
        assert_eq!(
            buf.poll_transmit((SIZE2 - transmitted + 2) as usize),
            (transmitted..SIZE2, false)
        );
        transmitted = SIZE2;

        // Offset 16384 requires requires 4 bytes
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        buf.retransmit(transmitted..SIZE3);
        assert_eq!(
            buf.poll_transmit((SIZE3 - transmitted + 4) as usize),
            (transmitted..SIZE3, false)
        );
        transmitted = SIZE3;

        // Offset 1GB requires 8 bytes
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
        buf.retransmit(transmitted..transmitted + chunk.len() as u64);
        assert_eq!(
            buf.poll_transmit(chunk.len() + 8),
            (transmitted..transmitted + chunk.len() as u64, false)
        );
    }

    #[test]
    fn multiple_segments() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        const MSG_LEN: u64 = MSG.len() as u64;

        const SEG1: &[u8] = b"He";
        buf.write(SEG1.into());
        const SEG2: &[u8] = b"llo,";
        buf.write(SEG2.into());
        const SEG3: &[u8] = b" w";
        buf.write(SEG3.into());
        const SEG4: &[u8] = b"o";
        buf.write(SEG4.into());
        const SEG5: &[u8] = b"rld!";
        buf.write(SEG5.into());

        assert_eq!(aggregate_unacked(&buf), MSG);

        assert_eq!(buf.poll_transmit(16), (0..8, true));
        assert_eq!(buf.get(0..5), SEG1);
        assert_eq!(buf.get(2..8), SEG2);
        assert_eq!(buf.get(6..8), SEG3);

        assert_eq!(buf.poll_transmit(16), (8..MSG_LEN, true));
        assert_eq!(buf.get(8..MSG_LEN), SEG4);
        assert_eq!(buf.get(9..MSG_LEN), SEG5);

        assert_eq!(buf.poll_transmit(42), (MSG_LEN..MSG_LEN, true));

        // Now drain the segments
        buf.ack(0..1);
        assert_eq!(aggregate_unacked(&buf), &MSG[1..]);
        buf.ack(0..3);
        assert_eq!(aggregate_unacked(&buf), &MSG[3..]);
        buf.ack(3..5);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(7..9);
        assert_eq!(aggregate_unacked(&buf), &MSG[5..]);
        buf.ack(4..7);
        assert_eq!(aggregate_unacked(&buf), &MSG[9..]);
        buf.ack(0..MSG_LEN);
        assert_eq!(aggregate_unacked(&buf), &[] as &[u8]);
    }

    #[test]
    fn retransmit() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        // Transmit two frames
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        // Lose the first, but not the second
        buf.retransmit(0..16);
        // Ensure we only retransmit the lost frame, then continue sending fresh data
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (23..MSG.len() as u64, true));
        // Lose the second frame
        buf.retransmit(16..23);
        assert_eq!(buf.poll_transmit(16), (16..23, true));
    }

    #[test]
    fn ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..8, true));
        buf.ack(0..8);
        assert_eq!(aggregate_unacked(&buf), &MSG[8..]);
    }

    #[test]
    fn reordered_ack() {
        let mut buf = SendBuffer::new();
        const MSG: &[u8] = b"Hello, world with extra data!";
        buf.write(MSG.into());
        assert_eq!(buf.poll_transmit(16), (0..16, false));
        assert_eq!(buf.poll_transmit(16), (16..23, true));
        buf.ack(16..23);
        assert_eq!(aggregate_unacked(&buf), MSG);
        buf.ack(0..16);
        assert_eq!(aggregate_unacked(&buf), &MSG[23..]);
        assert!(buf.acks.is_empty());
    }

    #[test]
    fn indexed_get_matches_legacy_through_empty_and_reordered_acks() {
        let mut indexed = SendBuffer::new();
        let mut legacy = LegacySendBuffer::default();
        let writes: [&[u8]; 7] = [b"", b"ab", b"", b"cde", b"", b"fg", b""];

        for bytes in writes {
            let bytes = Bytes::copy_from_slice(bytes);
            indexed.write(bytes.clone());
            legacy.write(bytes);
        }
        assert_matches_legacy(&indexed, &legacy);

        // Reordered ACKs first retain later data, then partially advance the head, then close
        // the gap. This also leaves an empty descriptor at the old end offset.
        for range in [5..7, 0..1, 3..4, 1..3, 4..5] {
            indexed.ack(range.clone());
            legacy.ack(range);
            assert_matches_legacy(&indexed, &legacy);
        }
        assert!(indexed.is_fully_acked());

        // The retained trailing empty write and this new non-empty write have the same start.
        // Lookup must choose the rightmost equal start.
        let appended = Bytes::from_static(b"hi");
        indexed.write(appended.clone());
        legacy.write(appended);
        assert_matches_legacy(&indexed, &legacy);
        assert_eq!(indexed.get(7..8), b"h");
        assert_eq!(indexed.get(8..9), b"i");
    }

    #[test]
    fn indexed_offsets_survive_deque_wrap_and_repeated_ack_write_cycles() {
        let mut indexed = SendBuffer::new();
        let mut legacy = LegacySendBuffer::default();

        for byte in 0..32u8 {
            let bytes = Bytes::copy_from_slice(&[byte]);
            indexed.write(bytes.clone());
            legacy.write(bytes);
        }
        let capacity = indexed.unacked_segments.capacity();
        while indexed.unacked_segments.len() < capacity {
            let byte = indexed.unacked_segments.len() as u8;
            let bytes = Bytes::copy_from_slice(&[byte]);
            indexed.write(bytes.clone());
            legacy.write(bytes);
        }
        assert_eq!(indexed.unacked_segments.len(), capacity);

        // Keep the deque full while moving its head. Each append reuses the freed slot, forcing
        // the VecDeque's physical storage to wrap while absolute stream starts keep increasing.
        for cycle in 0..4u8 {
            let base = indexed.offset - indexed.unacked_len as u64;
            indexed.ack(base..base + 1);
            legacy.ack(base..base + 1);

            let byte = (capacity as u8).wrapping_add(32).wrapping_add(cycle);
            let bytes = Bytes::copy_from_slice(&[byte]);
            indexed.write(bytes.clone());
            legacy.write(bytes);
        }

        assert!(
            !indexed.unacked_segments.as_slices().1.is_empty(),
            "fixture should exercise a wrapped VecDeque"
        );
        assert_matches_legacy(&indexed, &legacy);
    }

    #[test]
    fn zero_rtt_retransmission_restarts_across_segments() {
        let mut buf = SendBuffer::new();
        buf.write(Bytes::from_static(b"abcdef"));
        buf.write(Bytes::from_static(b"ghij"));

        let first_transmit = buf.poll_transmit(18);
        assert_eq!(first_transmit, (0..10, true));
        assert_eq!(buf.first_unpacketized(), 10);

        buf.retransmit_all_for_0rtt();
        assert_eq!(buf.first_unpacketized(), 0);
        assert_eq!(buf.poll_transmit(18), first_transmit);
    }

    #[test]
    fn owned_backing_lives_until_contiguous_ack_frontier_passes_it() {
        let lifetime = Arc::new(());
        let lifetime_observer: Weak<()> = Arc::downgrade(&lifetime);
        let bytes = Bytes::from_owner(TrackedBytesOwner {
            bytes: b"abcdefghijkl".to_vec(),
            _lifetime: Arc::clone(&lifetime),
        });
        drop(lifetime);

        let mut buf = SendBuffer::new();
        buf.write(bytes);

        // A later ACK cannot release bytes while the native contiguous ACK
        // frontier is still at zero.
        buf.ack(8..12);
        assert!(lifetime_observer.upgrade().is_some());
        assert_eq!(aggregate_unacked(&buf), b"abcdefghijkl");

        // Advancing only part of the first segment still leaves a live suffix
        // sharing the same owner.
        buf.ack(0..4);
        assert!(lifetime_observer.upgrade().is_some());
        assert_eq!(aggregate_unacked(&buf), b"efghijkl");

        // This fills the gap; the contiguous ACK frontier now consumes the
        // remaining segment and releases its owned backing.
        buf.ack(4..8);
        assert!(buf.is_fully_acked());
        assert!(lifetime_observer.upgrade().is_none());
    }

    fn aggregate_unacked(buf: &SendBuffer) -> Vec<u8> {
        let mut result = Vec::new();
        for segment in buf.unacked_segments.iter() {
            result.extend_from_slice(&segment.data[..]);
        }
        result
    }

    fn assert_matches_legacy(indexed: &SendBuffer, legacy: &LegacySendBuffer) {
        assert_eq!(indexed.offset(), legacy.offset);
        assert_eq!(indexed.is_fully_acked(), legacy.unacked_len == 0);
        assert_eq!(indexed.unacked(), legacy.unacked());

        // Includes zero-length queries, every segment boundary, end offsets, and starts beyond
        // the retained data. End is never less than start, keeping the old API defined.
        for start in 0..=legacy.offset + 2 {
            for end in start..=legacy.offset + 3 {
                assert_eq!(
                    indexed.get(start..end),
                    legacy.get(start..end),
                    "different bytes for {start}..{end}"
                );
            }
        }
    }

    /// Test-only copy of the former prefix-scanning representation, used as a differential oracle.
    #[derive(Default)]
    struct LegacySendBuffer {
        unacked_segments: VecDeque<Bytes>,
        unacked_len: usize,
        offset: u64,
        acks: RangeSet,
    }

    impl LegacySendBuffer {
        fn write(&mut self, data: Bytes) {
            self.unacked_len += data.len();
            self.offset += data.len() as u64;
            self.unacked_segments.push_back(data);
        }

        fn ack(&mut self, mut range: Range<u64>) {
            let base_offset = self.offset - self.unacked_len as u64;
            range.start = base_offset.max(range.start);
            range.end = base_offset.max(range.end);
            self.acks.insert(range);

            while self.acks.min() == Some(self.offset - self.unacked_len as u64) {
                let prefix = self.acks.pop_min().unwrap();
                let mut to_advance = (prefix.end - prefix.start) as usize;
                self.unacked_len -= to_advance;
                while to_advance > 0 {
                    let front = self
                        .unacked_segments
                        .front_mut()
                        .expect("Expected buffered data");
                    if front.len() <= to_advance {
                        to_advance -= front.len();
                        self.unacked_segments.pop_front();
                    } else {
                        front.advance(to_advance);
                        to_advance = 0;
                    }
                }
            }
        }

        fn get(&self, offsets: Range<u64>) -> &[u8] {
            let base_offset = self.offset - self.unacked_len as u64;
            let mut segment_offset = base_offset;
            for segment in self.unacked_segments.iter() {
                if offsets.start >= segment_offset
                    && offsets.start < segment_offset + segment.len() as u64
                {
                    let start = (offsets.start - segment_offset) as usize;
                    let end = (offsets.end - segment_offset) as usize;
                    return &segment[start..end.min(segment.len())];
                }
                segment_offset += segment.len() as u64;
            }
            &[]
        }

        fn unacked(&self) -> u64 {
            self.unacked_len as u64
                - self
                    .acks
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum::<u64>()
        }
    }

    /// Manual, deterministic lookup-cost check. Run explicitly in release mode; normal tests
    /// never include this timing experiment. Sequential and permuted query orders and bytes are
    /// fixed, and both paths must produce the same checksum before their times are reported.
    #[test]
    #[ignore = "manual local SendBuffer lookup-cost check; run in release mode"]
    fn send_buffer_lookup_cost_small_and_large_retained_sets() {
        const SEGMENT_BYTES: usize = 64;
        const TARGET_QUERIES_PER_REPETITION: usize = 32_768;
        const REPETITIONS: usize = 3;

        for segment_count in [1usize, 2, 88, 1024] {
            let mut indexed = SendBuffer::new();
            let mut legacy = LegacySendBuffer::default();
            let bytes = Bytes::from(vec![0x5a; SEGMENT_BYTES]);
            for _ in 0..segment_count {
                indexed.write(bytes.clone());
                legacy.write(bytes.clone());
            }

            // Ordered starts model packetization advancing through the stream. The second order
            // visits every segment once in a fixed permutation and probes different interiors.
            let sequential_offsets: Vec<u64> = (0..segment_count)
                .map(|index| (index * SEGMENT_BYTES) as u64)
                .collect();
            let permuted_offsets: Vec<u64> = (0..segment_count)
                .map(|index| {
                    (((index * 37) % segment_count) * SEGMENT_BYTES + (index % SEGMENT_BYTES))
                        as u64
                })
                .collect();
            let rounds = TARGET_QUERIES_PER_REPETITION.div_ceil(segment_count);

            for (order, offsets) in [
                ("sequential", sequential_offsets.as_slice()),
                ("permuted", permuted_offsets.as_slice()),
            ] {
                for &offset in offsets {
                    assert_eq!(
                        indexed.get(offset..offset + 1),
                        legacy.get(offset..offset + 1)
                    );
                }

                let mut indexed_times = Vec::with_capacity(REPETITIONS);
                let mut legacy_times = Vec::with_capacity(REPETITIONS);
                let mut indexed_checksum = 0u64;
                let mut legacy_checksum = 0u64;

                for repetition in 0..REPETITIONS {
                    if repetition % 2 == 0 {
                        let (elapsed, checksum) = measure_indexed(&indexed, offsets, rounds);
                        indexed_times.push(elapsed);
                        indexed_checksum = indexed_checksum.wrapping_add(checksum);

                        let (elapsed, checksum) = measure_legacy(&legacy, offsets, rounds);
                        legacy_times.push(elapsed);
                        legacy_checksum = legacy_checksum.wrapping_add(checksum);
                    } else {
                        let (elapsed, checksum) = measure_legacy(&legacy, offsets, rounds);
                        legacy_times.push(elapsed);
                        legacy_checksum = legacy_checksum.wrapping_add(checksum);

                        let (elapsed, checksum) = measure_indexed(&indexed, offsets, rounds);
                        indexed_times.push(elapsed);
                        indexed_checksum = indexed_checksum.wrapping_add(checksum);
                    }
                }

                assert_eq!(indexed_checksum, legacy_checksum);
                indexed_times.sort_unstable();
                legacy_times.sort_unstable();
                let indexed_median: Duration = indexed_times[REPETITIONS / 2];
                let legacy_median: Duration = legacy_times[REPETITIONS / 2];
                let queries_per_repetition = offsets.len() * rounds;
                println!(
                    "segments={segment_count} order={order} queries_per_repetition={queries_per_repetition} timed_queries_per_method={} total_across_both_methods={} indexed_median_of_3={indexed_median:?} legacy_linear_median_of_3={legacy_median:?} ratio={:.2}x checksum={indexed_checksum}",
                    queries_per_repetition * REPETITIONS,
                    queries_per_repetition * REPETITIONS * 2,
                    legacy_median.as_secs_f64() / indexed_median.as_secs_f64()
                );
            }
        }
    }

    fn measure_indexed(buf: &SendBuffer, offsets: &[u64], rounds: usize) -> (Duration, u64) {
        let started = Instant::now();
        let mut checksum = 0u64;
        for _ in 0..rounds {
            for &offset in offsets {
                let offset = std::hint::black_box(offset);
                checksum = checksum
                    .wrapping_add(std::hint::black_box(buf.get(offset..offset + 1)[0] as u64));
            }
        }
        (started.elapsed(), checksum)
    }

    fn measure_legacy(buf: &LegacySendBuffer, offsets: &[u64], rounds: usize) -> (Duration, u64) {
        let started = Instant::now();
        let mut checksum = 0u64;
        for _ in 0..rounds {
            for &offset in offsets {
                let offset = std::hint::black_box(offset);
                checksum = checksum
                    .wrapping_add(std::hint::black_box(buf.get(offset..offset + 1)[0] as u64));
            }
        }
        (started.elapsed(), checksum)
    }
}

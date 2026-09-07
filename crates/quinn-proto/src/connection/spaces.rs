use std::{
    collections::{BTreeMap, VecDeque},
    mem,
    ops::{Bound, Index, IndexMut, Range},
};

use rand::{Rng, RngExt};
use rustc_hash::FxHashSet;
use tracing::trace;

use super::assembler::Assembler;
use crate::{
    cid_queue::CidQueue,
    congestion::{PacketDeliveryState, RecoveryTransactionId}, connection::StreamsState,
    crypto::Keys, frame,
    packet::SpaceId, range_set::ArrayRangeSet, shared::IssuedCid, Dir, Duration, Instant,
    SocketAddr, StreamId, TransportError, VarInt,
};

pub(super) struct PacketSpace {
    pub(super) crypto: Option<Keys>,
    pub(super) dedup: Dedup,
    /// Highest received packet number
    pub(super) rx_packet: u64,

    /// Data to send
    pub(super) pending: Retransmits,
    /// Packet numbers to acknowledge
    pub(super) pending_acks: PendingAcks,

    /// The packet number of the next packet that will be sent, if any. In the Data space, the
    /// packet number stored here is sometimes skipped by [`PacketNumberFilter`] logic.
    pub(super) next_packet_number: u64,
    /// The largest packet number the remote peer acknowledged in an ACK frame.
    pub(super) largest_acked_packet: Option<u64>,
    /// The highest-numbered ACK-eliciting packet we've sent
    pub(super) largest_ack_eliciting_sent: u64,
    /// Number of packets in `sent_packets` with numbers above `largest_ack_eliciting_sent`
    pub(super) unacked_non_ack_eliciting_tail: u64,
    /// Transmitted but not acked
    // We use a BTreeMap here so we can efficiently query by range on ACK and for loss detection
    pub(super) sent_packets: BTreeMap<u64, SentPacket>,
    /// Packets retained after loss declaration so late ACKs can identify a spurious episode.
    pub(super) lost_packets: BTreeMap<u64, LostPacket>,
    /// Number of explicit congestion notification codepoints seen on incoming packets
    pub(super) ecn_counters: frame::EcnCounts,
    /// Recent ECN counters sent by the peer in ACK frames
    ///
    /// Updated (and inspected) whenever we receive an ACK with a new highest acked packet
    /// number. Stored per-space to simplify verification, which would otherwise have difficulty
    /// distinguishing between ECN bleaching and counts having been updated by a near-simultaneous
    /// ACK already processed in another space.
    pub(super) ecn_feedback: frame::EcnCounts,

    /// Incoming cryptographic handshake stream
    pub(super) crypto_stream: Assembler,
    /// Current offset of outgoing cryptographic handshake stream
    pub(super) crypto_offset: u64,

    /// The time the most recently sent retransmittable packet was sent.
    pub(super) time_of_last_ack_eliciting_packet: Option<Instant>,
    /// The time at which the earliest sent packet in this space will be considered lost based on
    /// exceeding the reordering window in time. Only set for packets numbered prior to a packet
    /// that has been acknowledged.
    pub(super) loss_time: Option<Instant>,
    /// Number of tail loss probes to send
    pub(super) loss_probes: u32,
    pub(super) ping_pending: bool,
    pub(super) immediate_ack_pending: bool,
    /// Number of packets sent in the current key phase
    pub(super) sent_with_keys: u64,
}

/// Result of validating a cumulative ACK_ECN block against the previous in-order block.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) struct EcnValidation {
    pub(super) congestion_experienced: bool,
    /// Total ECT(0), ECT(1), and CE counter increase since the prior validation point.
    pub(super) total_increase: u64,
}

impl PacketSpace {
    pub(super) fn new(_now: Instant) -> Self {
        Self {
            crypto: None,
            dedup: Dedup::new(),
            rx_packet: 0,

            pending: Retransmits::default(),
            pending_acks: PendingAcks::new(),

            next_packet_number: 0,
            largest_acked_packet: None,
            largest_ack_eliciting_sent: 0,
            unacked_non_ack_eliciting_tail: 0,
            sent_packets: BTreeMap::new(),
            lost_packets: BTreeMap::new(),
            ecn_counters: frame::EcnCounts::ZERO,
            ecn_feedback: frame::EcnCounts::ZERO,

            crypto_stream: Assembler::new(),
            crypto_offset: 0,

            time_of_last_ack_eliciting_packet: None,
            loss_time: None,
            loss_probes: 0,
            ping_pending: false,
            immediate_ack_pending: false,
            sent_with_keys: 0,
        }
    }

    /// Queue data for a tail loss probe (or anti-amplification deadlock prevention) packet
    ///
    /// Probes are sent similarly to normal packets when an expected ACK has not arrived. We never
    /// deem a packet lost until we receive an ACK that should have included it, but if a trailing
    /// run of packets (or their ACKs) are lost, this might not happen in a timely fashion. We send
    /// probe packets to force an ACK, and exempt them from congestion control to prevent a deadlock
    /// when the congestion window is filled with lost tail packets.
    ///
    /// We prefer to send new data, to make the most efficient use of bandwidth. If there's no data
    /// waiting to be sent, then we retransmit in-flight data to reduce odds of loss. If there's no
    /// in-flight data either, we're probably a client guarding against a handshake
    /// anti-amplification deadlock and we just make something up.
    pub(super) fn maybe_queue_probe(
        &mut self,
        request_immediate_ack: bool,
        streams: &StreamsState,
    ) {
        if self.loss_probes == 0 {
            return;
        }

        if request_immediate_ack {
            // The probe should be ACKed without delay (should only be used in the Data space and
            // when the peer supports the acknowledgement frequency extension)
            self.immediate_ack_pending = true;
        }

        if !self.pending.is_empty(streams) {
            // There's real data to send here, no need to make something up
            return;
        }

        // Retransmit the data of the oldest in-flight packet
        for packet in self.sent_packets.values_mut() {
            if !packet.retransmits.is_empty(streams) {
                // Remove retransmitted data from the old packet so we don't end up retransmitting
                // it *again* even if the copy we're sending now gets acknowledged.
                self.pending |= mem::take(&mut packet.retransmits);
                return;
            }
        }

        // Nothing new to send and nothing to retransmit, so fall back on a ping. This should only
        // happen in rare cases during the handshake when the server becomes blocked by
        // anti-amplification.
        if !self.immediate_ack_pending {
            self.ping_pending = true;
        }
    }

    /// Get the next outgoing packet number in this space
    ///
    /// In the Data space, the connection's [`PacketNumberFilter`] must be used rather than calling
    /// this directly.
    pub(super) fn get_tx_number(&mut self) -> u64 {
        // TODO: Handle packet number overflow gracefully
        assert!(self.next_packet_number < 2u64.pow(62));
        let x = self.next_packet_number;
        self.next_packet_number += 1;
        self.sent_with_keys += 1;
        x
    }

    pub(super) fn can_send(&self, streams: &StreamsState) -> SendableFrames {
        let acks = self.pending_acks.can_send();
        let other =
            !self.pending.is_empty(streams) || self.ping_pending || self.immediate_ack_pending;

        SendableFrames { acks, other }
    }

    /// Verifies sanity of an ECN block and returns whether congestion was encountered.
    pub(super) fn detect_ecn(
        &mut self,
        accounted_ect0: u64,
        ecn: frame::EcnCounts,
    ) -> Result<EcnValidation, &'static str> {
        let ect0_increase = ecn
            .ect0
            .checked_sub(self.ecn_feedback.ect0)
            .ok_or("peer ECT(0) count regression")?;
        let ect1_increase = ecn
            .ect1
            .checked_sub(self.ecn_feedback.ect1)
            .ok_or("peer ECT(1) count regression")?;
        let ce_increase = ecn
            .ce
            .checked_sub(self.ecn_feedback.ce)
            .ok_or("peer CE count regression")?;
        let total_increase = ect0_increase + ect1_increase + ce_increase;
        if total_increase < accounted_ect0 {
            return Err("ECN bleaching");
        }
        if (ect0_increase + ce_increase) < accounted_ect0 || ect1_increase != 0 {
            return Err("ECN corruption");
        }
        // If total_increase > accounted_ect0 (which happens when ACK feedback is lost or
        // reordered), this is required by
        // the draft so that long-term drift does not occur. If =, then the only question is whether
        // to count CE packets as CE or ECT0. Recording them as CE is more consistent and keeps the
        // congestion check obvious.
        self.ecn_feedback = ecn;
        Ok(EcnValidation {
            congestion_experienced: ce_increase != 0,
            total_increase,
        })
    }

    /// Stop tracking sent packet `number`, and return what we knew about it
    pub(super) fn take(&mut self, number: u64) -> Option<SentPacket> {
        let packet = self.sent_packets.remove(&number)?;
        if !packet.ack_eliciting && number > self.largest_ack_eliciting_sent {
            self.unacked_non_ack_eliciting_tail =
                self.unacked_non_ack_eliciting_tail.checked_sub(1).unwrap();
        }
        Some(packet)
    }

    /// May return a packet that should be forgotten
    pub(super) fn sent(&mut self, number: u64, packet: SentPacket) -> Option<SentPacket> {
        // Retain state for at most this many non-ACK-eliciting packets sent after the most recently
        // sent ACK-eliciting packet. We're never guaranteed to receive an ACK for those, and we
        // can't judge them as lost without an ACK, so to limit memory in applications which receive
        // packets but don't send ACK-eliciting data for long periods use we must eventually start
        // forgetting about them, although it might also be reasonable to just kill the connection
        // due to weird peer behavior.
        const MAX_UNACKED_NON_ACK_ELICTING_TAIL: u64 = 1_000;

        let mut forgotten = None;
        if packet.ack_eliciting {
            self.unacked_non_ack_eliciting_tail = 0;
            self.largest_ack_eliciting_sent = number;
        } else if self.unacked_non_ack_eliciting_tail > MAX_UNACKED_NON_ACK_ELICTING_TAIL {
            let oldest_after_ack_eliciting = *self
                .sent_packets
                .range((
                    Bound::Excluded(self.largest_ack_eliciting_sent),
                    Bound::Unbounded,
                ))
                .next()
                .unwrap()
                .0;
            // Per https://www.rfc-editor.org/rfc/rfc9000.html#name-frames-and-frame-types,
            // non-ACK-eliciting packets must only contain PADDING, ACK, and CONNECTION_CLOSE
            // frames, which require no special handling on ACK or loss beyond removal from
            // in-flight counters if padded.
            let packet = self
                .sent_packets
                .remove(&oldest_after_ack_eliciting)
                .unwrap();
            debug_assert!(!packet.ack_eliciting);
            forgotten = Some(packet);
        } else {
            self.unacked_non_ack_eliciting_tail += 1;
        }

        self.sent_packets.insert(number, packet);
        forgotten
    }

    /// Whether any congestion-controlled packets in this space are not yet acknowledged or lost
    pub(super) fn has_in_flight(&self) -> bool {
        // The number of non-congestion-controlled (i.e. size == 0) packets in flight at a time
        // should be small, since otherwise congestion control wouldn't be effective. Therefore,
        // this shouldn't need to visit many packets before finishing one way or another.
        self.sent_packets.values().any(|x| x.size != 0)
    }
}

impl Index<SpaceId> for [PacketSpace; 3] {
    type Output = PacketSpace;
    fn index(&self, space: SpaceId) -> &PacketSpace {
        &self.as_ref()[space as usize]
    }
}

impl IndexMut<SpaceId> for [PacketSpace; 3] {
    fn index_mut(&mut self, space: SpaceId) -> &mut PacketSpace {
        &mut self.as_mut()[space as usize]
    }
}

/// Represents one or more packets subject to retransmission
#[derive(Debug, Clone)]
pub(super) struct SentPacket {
    /// [`PathData::generation`](super::PathData::generation) of the path on which this packet was sent
    pub(super) path_generation: u64,
    /// Congestion-controller model that owns this packet's delivery/loss callbacks.
    pub(super) controller_epoch: u64,
    /// Whether this packet was transmitted with an ECN-capable codepoint.
    pub(super) ecn_marked: bool,
    /// The time the packet was sent.
    pub(super) time_sent: Instant,
    /// Compact congestion-controller delivery state captured when the packet entered flight.
    /// Read only through [`Self::delivery_state`] because controllers may return no payload.
    pub(super) delivery_state_payload: StoredPacketDeliveryState,
    /// Whether [`Self::delivery_state_payload`] contains controller-provided state.
    pub(super) has_delivery_state: bool,
    /// Whether the sender lacked application data when this packet entered flight.
    pub(super) app_limited: bool,
    /// The number of bytes sent in the packet, not including UDP or IP overhead, but including QUIC
    /// framing overhead. Zero if this packet is not counted towards congestion control, i.e. not an
    /// "in flight" packet.
    pub(super) size: u16,
    /// Whether an acknowledgement is expected directly in response to this packet.
    pub(super) ack_eliciting: bool,
    /// The largest packet number acknowledged by this packet
    pub(super) largest_acked: Option<u64>,
    /// Data which needs to be retransmitted in case the packet is lost.
    /// The data is boxed to minimize `SentPacket` size for the typical case of
    /// packets only containing ACKs and STREAM frames.
    pub(super) retransmits: ThinRetransmits,
    /// Metadata for stream frames in a packet
    ///
    /// The actual application data is stored with the stream state.
    pub(super) stream_frames: frame::StreamMetaVec,
}

impl SentPacket {
    pub(super) fn store_delivery_state(
        time_sent: Instant,
        state: Option<PacketDeliveryState>,
    ) -> (StoredPacketDeliveryState, bool) {
        let Some(state) = state else {
            return (StoredPacketDeliveryState::EMPTY, false);
        };
        let delivered_before_send_ns = u64::try_from(
            time_sent
                .saturating_duration_since(state.delivered_time)
                .as_nanos(),
        )
        .unwrap_or(u64::MAX);
        (
            StoredPacketDeliveryState {
                delivered: state.delivered,
                delivered_before_send_ns,
                send_elapsed_ns: state.send_elapsed_ns,
            },
            true,
        )
    }

    pub(super) fn delivery_state(&self) -> Option<PacketDeliveryState> {
        self.has_delivery_state.then(|| PacketDeliveryState {
            delivered: self.delivery_state_payload.delivered,
            delivered_time: self
                .time_sent
                .checked_sub(Duration::from_nanos(
                    self.delivery_state_payload.delivered_before_send_ns,
                ))
                .unwrap_or(self.time_sent),
            send_elapsed_ns: self.delivery_state_payload.send_elapsed_ns,
        })
    }
}

/// Storage form of [`PacketDeliveryState`] relative to [`SentPacket::time_sent`].
///
/// The controller contract makes `delivered_time` a prior-delivery timestamp, so storing its
/// distance from the send time retains the full useful range in 24 bytes. A `u64` nanosecond
/// duration spans roughly 584 years, matching the existing compact send-elapsed representation.
#[derive(Debug, Clone, Copy)]
pub(super) struct StoredPacketDeliveryState {
    delivered: u64,
    delivered_before_send_ns: u64,
    send_elapsed_ns: u64,
}

impl StoredPacketDeliveryState {
    const EMPTY: Self = Self {
        delivered: 0,
        delivered_before_send_ns: 0,
        send_elapsed_ns: 0,
    };
}

/// Minimal retained evidence for a packet declared lost.
#[derive(Debug)]
pub(super) struct LostPacket {
    /// Original transmission time, used to expire evidence after two PTOs.
    pub(super) time_sent: Instant,
    /// Congestion-controller model whose loss episode retained this evidence.
    pub(super) controller_epoch: u64,
    /// Exact controller undo transaction, if this loss can contribute proof of spurious recovery.
    pub(super) recovery_transaction: Option<RecoveryTransactionId>,
    /// Whether the original transmission carried an ECN-capable codepoint.
    pub(super) ecn_marked: bool,
}

/// Retransmittable data queue
#[allow(unreachable_pub)] // fuzzing only
#[derive(Debug, Default, Clone)]
pub struct Retransmits {
    pub(super) max_data: bool,
    pub(super) max_stream_id: [bool; 2],
    pub(super) reset_stream: Vec<(StreamId, VarInt)>,
    pub(super) stop_sending: Vec<frame::StopSending>,
    pub(super) max_stream_data: FxHashSet<StreamId>,
    pub(super) crypto: VecDeque<frame::Crypto>,
    pub(super) new_cids: Vec<IssuedCid>,
    pub(super) retire_cids: Vec<u64>,
    pub(super) ack_frequency: bool,
    pub(super) handshake_done: bool,
    /// For each enqueued NEW_TOKEN frame, a copy of the path's remote address
    ///
    /// There are 2 reasons this is unusual:
    ///
    /// - If the path changes, NEW_TOKEN frames bound for the old path are not retransmitted on the
    ///   new path. That is why this field stores the remote address: so that ones for old paths
    ///   can be filtered out.
    /// - If a token is lost, a new randomly generated token is re-transmitted, rather than the
    ///   original. This is so that if both transmissions are received, the client won't risk
    ///   sending the same token twice. That is why this field does _not_ store any actual token.
    ///
    /// It is true that a QUIC endpoint will only want to effectively have NEW_TOKEN frames
    /// enqueued for its current path at a given point in time. Based on that, we could conceivably
    /// change this from a vector to an `Option<(SocketAddr, usize)>` or just a `usize` or
    /// something. However, due to the architecture of Quinn, it is considerably simpler to not do
    /// that; consider what such a change would mean for implementing `BitOrAssign` on Self.
    pub(super) new_tokens: Vec<SocketAddr>,
}

impl Retransmits {
    pub(super) fn retire_cids(&mut self, cids: Range<u64>) -> Result<(), TransportError> {
        // We don't bother counting in-flight frames because those are bounded by congestion control.
        let num = cids.end.saturating_sub(cids.start);
        if (self.retire_cids.len() as u64).saturating_add(num) > Self::MAX_PENDING_RETIRED_CIDS {
            return Err(TransportError::CONNECTION_ID_LIMIT_ERROR(
                "queued too many retired CIDs",
            ));
        }

        self.retire_cids.extend(cids);
        Ok(())
    }

    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        !self.max_data
            && !self.max_stream_id.into_iter().any(|x| x)
            && self.reset_stream.is_empty()
            && self.stop_sending.is_empty()
            && self
                .max_stream_data
                .iter()
                .all(|&id| !streams.can_send_flow_control(id))
            && self.crypto.is_empty()
            && self.new_cids.is_empty()
            && self.retire_cids.is_empty()
            && !self.ack_frequency
            && !self.handshake_done
            && self.new_tokens.is_empty()
    }

    /// Ensure `pending_retired` cannot grow without bound
    ///
    /// Limit is somewhat arbitrary but very permissive.
    const MAX_PENDING_RETIRED_CIDS: u64 = CidQueue::LEN as u64 * 10;
}

impl ::std::ops::BitOrAssign for Retransmits {
    fn bitor_assign(&mut self, rhs: Self) {
        // We reduce in-stream head-of-line blocking by queueing retransmits before other data for
        // STREAM and CRYPTO frames.
        self.max_data |= rhs.max_data;
        for dir in Dir::iter() {
            self.max_stream_id[dir as usize] |= rhs.max_stream_id[dir as usize];
        }
        self.reset_stream.extend_from_slice(&rhs.reset_stream);
        self.stop_sending.extend_from_slice(&rhs.stop_sending);
        self.max_stream_data.extend(&rhs.max_stream_data);
        for crypto in rhs.crypto.into_iter().rev() {
            self.crypto.push_front(crypto);
        }
        self.new_cids.extend(&rhs.new_cids);
        self.retire_cids.extend(rhs.retire_cids);
        self.ack_frequency |= rhs.ack_frequency;
        self.handshake_done |= rhs.handshake_done;
        self.new_tokens.extend_from_slice(&rhs.new_tokens);
    }
}

impl ::std::ops::BitOrAssign<ThinRetransmits> for Retransmits {
    fn bitor_assign(&mut self, rhs: ThinRetransmits) {
        if let Some(retransmits) = rhs.retransmits {
            self.bitor_assign(*retransmits)
        }
    }
}

impl ::std::iter::FromIterator<Self> for Retransmits {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = Self>,
    {
        let mut result = Self::default();
        for packet in iter {
            result |= packet;
        }
        result
    }
}

/// A variant of `Retransmits` which only allocates storage when required
#[derive(Debug, Default, Clone)]
pub(super) struct ThinRetransmits {
    retransmits: Option<Box<Retransmits>>,
}

impl ThinRetransmits {
    /// Returns `true` if no retransmits are necessary
    pub(super) fn is_empty(&self, streams: &StreamsState) -> bool {
        match &self.retransmits {
            Some(retransmits) => retransmits.is_empty(streams),
            None => true,
        }
    }

    /// Returns a reference to the retransmits stored in this box
    pub(super) fn get(&self) -> Option<&Retransmits> {
        self.retransmits.as_deref()
    }

    /// Returns a mutable reference to the stored retransmits
    ///
    /// This function will allocate a backing storage if required.
    pub(super) fn get_or_create(&mut self) -> &mut Retransmits {
        if self.retransmits.is_none() {
            self.retransmits = Some(Box::default());
        }
        self.retransmits.as_deref_mut().unwrap()
    }
}

/// Exact, bounded receive history matching our minimum packet-number encoding.
///
/// The monotone floor is `next - WINDOW_SIZE`. Packets below it are always
/// rejected; retained packet numbers are accepted exactly once. Ring positions
/// are cleared only when they enter the window, never when an old packet arrives.
pub(super) struct Dedup {
    window: Box<[u64; WINDOW_WORDS]>,
    /// Lowest packet number higher than all yet authenticated.
    next: u64,
}

const WINDOW_SIZE: u64 = crate::packet::PACKET_NUMBER_REORDER_WINDOW;
const WINDOW_WORDS: usize = (WINDOW_SIZE as usize).div_ceil(64);
const STORAGE_BITS: u64 = WINDOW_WORDS as u64 * 64;

impl Dedup {
    pub(super) fn new() -> Self {
        Self { window: Box::new([0; WINDOW_WORDS]), next: 0 }
    }

    /// Highest packet number authenticated.
    fn highest(&self) -> u64 {
        self.next - 1
    }

    /// Record an authenticated packet. True means duplicate or retired.
    pub(super) fn insert(&mut self, packet: u64) -> bool {
        if packet < self.next.saturating_sub(WINDOW_SIZE) {
            return true;
        }
        if packet >= self.next {
            let end = packet + 1;
            if end - self.next >= STORAGE_BITS {
                self.window.fill(0);
            } else {
                let mut cursor = self.next;
                while cursor < end {
                    let word_end = ((cursor / 64 + 1) * 64).min(end);
                    self.window[Self::word(cursor)] &= !Self::mask(cursor, word_end);
                    cursor = word_end;
                }
            }
            self.next = end;
        }
        let mask = 1 << (packet % 64);
        let word = &mut self.window[Self::word(packet)];
        let duplicate = *word & mask != 0;
        *word |= mask;
        duplicate
    }

    fn word(packet: u64) -> usize {
        ((packet / 64) % WINDOW_WORDS as u64) as usize
    }

    /// Bits for a nonempty interval within one word.
    fn mask(start: u64, end: u64) -> u64 {
        let width = end - start;
        (u64::MAX >> (64 - width)) << (start % 64)
    }

    /// Find the first retained, missing packet strictly between two received
    /// endpoints. Retired history is not evidence of a current missing packet.
    fn smallest_missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> Option<u64> {
        debug_assert!(lower_bound <= upper_bound);
        debug_assert!(upper_bound <= self.highest());
        let mut cursor = (lower_bound + 1).max(self.next.saturating_sub(WINDOW_SIZE));
        while cursor < upper_bound {
            let end = ((cursor / 64 + 1) * 64).min(upper_bound);
            let gaps = !self.window[Self::word(cursor)] & Self::mask(cursor, end);
            if gaps != 0 {
                return Some(cursor / 64 * 64 + u64::from(gaps.trailing_zeros()));
            }
            cursor = end;
        }
        None
    }

    fn missing_in_interval(&self, lower_bound: u64, upper_bound: u64) -> bool {
        self.smallest_missing_in_interval(lower_bound, upper_bound).is_some()
    }
}

/// Indicates which data is available for sending
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) struct SendableFrames {
    pub(super) acks: bool,
    pub(super) other: bool,
}

impl SendableFrames {
    /// Returns that no data is available for sending
    pub(super) fn empty() -> Self {
        Self {
            acks: false,
            other: false,
        }
    }

    /// Whether no data is sendable
    pub(super) fn is_empty(&self) -> bool {
        !self.acks && !self.other
    }
}

#[derive(Debug)]
pub(super) struct PendingAcks {
    /// Whether we should send an ACK immediately, even if that means sending an ACK-only packet
    ///
    /// When `immediate_ack_required` is false, the normal behavior is to send ACK frames only when
    /// there is other data to send, or when the `MaxAckDelay` timer expires.
    immediate_ack_required: bool,
    /// The number of ack-eliciting packets received since the last ACK frame was sent
    ///
    /// Once the count _exceeds_ `ack_eliciting_threshold`, an immediate ACK is required
    ack_eliciting_since_last_ack_sent: u64,
    non_ack_eliciting_since_last_ack_sent: u64,
    ack_eliciting_threshold: u64,
    /// The reordering threshold, controlling how we respond to out-of-order ack-eliciting packets
    ///
    /// Different values enable different behavior:
    ///
    /// * `0`: no special action is taken
    /// * `1`: an ACK is immediately sent if it is out-of-order according to RFC 9000
    /// * `>1`: an ACK is immediately sent if it is out-of-order according to the ACK frequency draft
    reordering_threshold: u64,
    /// The earliest ack-eliciting packet since the last ACK was sent, used to calculate the moment
    /// upon which `max_ack_delay` elapses
    earliest_ack_eliciting_since_last_ack_sent: Option<Instant>,
    /// The packet number ranges of ack-eliciting packets the peer hasn't confirmed receipt of ACKs
    /// for
    ranges: ArrayRangeSet,
    /// The packet with the largest packet number, and the time upon which it was received (used to
    /// calculate ACK delay in [`PendingAcks::ack_delay`])
    largest_packet: Option<(u64, Instant)>,
    /// The ack-eliciting packet we have received with the largest packet number
    largest_ack_eliciting_packet: Option<u64>,
    /// The largest acknowledged packet number sent in an ACK frame
    largest_acked: Option<u64>,
}

impl PendingAcks {
    fn new() -> Self {
        Self {
            immediate_ack_required: false,
            ack_eliciting_since_last_ack_sent: 0,
            non_ack_eliciting_since_last_ack_sent: 0,
            ack_eliciting_threshold: 1,
            reordering_threshold: 1,
            earliest_ack_eliciting_since_last_ack_sent: None,
            ranges: ArrayRangeSet::default(),
            largest_packet: None,
            largest_ack_eliciting_packet: None,
            largest_acked: None,
        }
    }

    pub(super) fn set_ack_frequency_params(&mut self, frame: &frame::AckFrequency) {
        self.ack_eliciting_threshold = frame.ack_eliciting_threshold.into_inner();
        self.reordering_threshold = frame.reordering_threshold.into_inner();
    }

    pub(super) fn set_immediate_ack_required(&mut self) {
        self.immediate_ack_required = true;
    }

    pub(super) fn on_max_ack_delay_timeout(&mut self) {
        self.immediate_ack_required = self.ack_eliciting_since_last_ack_sent > 0;
    }

    pub(super) fn max_ack_delay_timeout(&self, max_ack_delay: Duration) -> Option<Instant> {
        self.earliest_ack_eliciting_since_last_ack_sent
            .map(|earliest_unacked| earliest_unacked + max_ack_delay)
    }

    /// Whether any ACK frames can be sent
    pub(super) fn can_send(&self) -> bool {
        self.immediate_ack_required && !self.ranges.is_empty()
    }

    /// Returns the delay since the packet with the largest packet number was received
    pub(super) fn ack_delay(&self, now: Instant) -> Duration {
        self.largest_packet
            .map_or(Duration::default(), |(_, received)| now - received)
    }

    /// Handle receipt of a new packet
    ///
    /// Returns true if the max ack delay timer should be armed
    pub(super) fn packet_received(
        &mut self,
        now: Instant,
        packet_number: u64,
        ack_eliciting: bool,
        dedup: &Dedup,
    ) -> bool {
        if !ack_eliciting {
            self.non_ack_eliciting_since_last_ack_sent += 1;
            return false;
        }

        let prev_largest_ack_eliciting = self.largest_ack_eliciting_packet.unwrap_or(0);

        // Track largest ack-eliciting packet
        self.largest_ack_eliciting_packet = self
            .largest_ack_eliciting_packet
            .map(|pn| pn.max(packet_number))
            .or(Some(packet_number));

        // Handle ack_eliciting_threshold
        self.ack_eliciting_since_last_ack_sent += 1;
        self.immediate_ack_required |=
            self.ack_eliciting_since_last_ack_sent > self.ack_eliciting_threshold;

        // Handle out-of-order packets
        self.immediate_ack_required |=
            self.is_out_of_order(packet_number, prev_largest_ack_eliciting, dedup);

        // Arm max_ack_delay timer if necessary
        if self.earliest_ack_eliciting_since_last_ack_sent.is_none() && !self.can_send() {
            self.earliest_ack_eliciting_since_last_ack_sent = Some(now);
            return true;
        }

        false
    }

    fn is_out_of_order(
        &self,
        packet_number: u64,
        prev_largest_ack_eliciting: u64,
        dedup: &Dedup,
    ) -> bool {
        match self.reordering_threshold {
            0 => false,
            1 => {
                // From https://www.rfc-editor.org/rfc/rfc9000#section-13.2.1-7
                packet_number < prev_largest_ack_eliciting
                    || dedup.missing_in_interval(prev_largest_ack_eliciting, packet_number)
            }
            _ => {
                // From acknowledgement frequency draft, section 6.1: send an ACK immediately if
                // doing so would cause the sender to detect a new packet loss
                let Some((largest_acked, largest_unacked)) =
                    self.largest_acked.zip(self.largest_ack_eliciting_packet)
                else {
                    return false;
                };
                if self.reordering_threshold > largest_acked {
                    return false;
                }
                // The largest packet number that could be declared lost without a new ACK being
                // sent
                let largest_reported = largest_acked - self.reordering_threshold + 1;
                let Some(smallest_missing_unreported) =
                    dedup.smallest_missing_in_interval(largest_reported, largest_unacked)
                else {
                    return false;
                };
                largest_unacked - smallest_missing_unreported >= self.reordering_threshold
            }
        }
    }

    /// Should be called whenever ACKs have been sent
    ///
    /// This will suppress sending further ACKs until additional ACK eliciting frames arrive
    pub(super) fn acks_sent(&mut self) {
        // It is possible (though unlikely) that the ACKs we just sent do not cover all the
        // ACK-eliciting packets we have received (e.g. if there is not enough room in the packet to
        // fit all the ranges). To keep things simple, however, we assume they do. If there are
        // indeed some ACKs that weren't covered, the packets might be ACKed later anyway, because
        // they are still contained in `self.ranges`. If we somehow fail to send the ACKs at a later
        // moment, the peer will assume the packets got lost and will retransmit their frames in a
        // new packet, which is suboptimal, because we already received them. Our assumption here is
        // that simplicity results in code that is more performant, even in the presence of
        // occasional redundant retransmits.
        self.immediate_ack_required = false;
        self.ack_eliciting_since_last_ack_sent = 0;
        self.non_ack_eliciting_since_last_ack_sent = 0;
        self.earliest_ack_eliciting_since_last_ack_sent = None;
        self.largest_acked = self.largest_ack_eliciting_packet;
    }

    /// Insert one packet that needs to be acknowledged
    pub(super) fn insert_one(&mut self, packet: u64, now: Instant) {
        self.ranges.insert_one(packet);

        if self.largest_packet.is_none_or(|(pn, _)| packet > pn) {
            self.largest_packet = Some((packet, now));
        }

        if self.ranges.len() > MAX_ACK_BLOCKS {
            self.ranges.pop_min();
        }
    }

    /// Remove ACKs of packets numbered at or below `max` from the set of pending ACKs
    pub(super) fn subtract_below(&mut self, max: u64) {
        self.ranges.remove(0..(max + 1));
    }

    /// Returns the set of currently pending ACK ranges
    pub(super) fn ranges(&self) -> &ArrayRangeSet {
        &self.ranges
    }

    /// Queue an ACK if a significant number of non-ACK-eliciting packets have not yet been
    /// acknowledged
    ///
    /// Should be called immediately before a non-probing packet is composed, when we've already
    /// committed to sending a packet regardless.
    pub(super) fn maybe_ack_non_eliciting(&mut self) {
        // If we're going to send a packet anyway, and we've received a significant number of
        // non-ACK-eliciting packets, then include an ACK to help the peer perform timely loss
        // detection even if they're not sending any ACK-eliciting packets themselves. Exact
        // threshold chosen somewhat arbitrarily.
        const LAZY_ACK_THRESHOLD: u64 = 10;
        if self.non_ack_eliciting_since_last_ack_sent > LAZY_ACK_THRESHOLD {
            self.immediate_ack_required = true;
        }
    }
}

/// Helper for mitigating [optimistic ACK attacks]
///
/// A malicious peer could prompt the local application to begin a large data transfer, and then
/// send ACKs without first waiting for data to be received. This could defeat congestion control,
/// allowing the connection to consume disproportionate resources. We therefore occasionally skip
/// packet numbers, and classify any ACK referencing a skipped packet number as a transport error.
///
/// Skipped packet numbers occur only in the application data space (where costly transfers might
/// take place) and are distributed exponentially to reflect the reduced likelihood and impact of
/// bad behavior from a peer that has been well-behaved for an extended period.
///
/// ACKs for packet numbers that have not yet been allocated are also a transport error, but an
/// attacker with knowledge of the congestion control algorithm in use could time falsified ACKs to
/// arrive after the packets they reference are sent.
///
/// [optimistic ACK attacks]: https://www.rfc-editor.org/rfc/rfc9000.html#name-optimistic-ack-attack
pub(super) struct PacketNumberFilter {
    /// Next outgoing packet number to skip
    next_skipped_packet_number: u64,
    /// Most recently skipped packet number
    prev_skipped_packet_number: Option<u64>,
    /// Next packet number to skip is randomly selected from 2^n..2^n+1
    exponent: u32,
}

impl PacketNumberFilter {
    pub(super) fn new(rng: &mut (impl Rng + ?Sized)) -> Self {
        // First skipped PN is in 0..64
        let exponent = 6;
        Self {
            next_skipped_packet_number: rng.random_range(0..2u64.saturating_pow(exponent)),
            prev_skipped_packet_number: None,
            exponent,
        }
    }

    #[cfg(test)]
    pub(super) fn disabled() -> Self {
        Self {
            next_skipped_packet_number: u64::MAX,
            prev_skipped_packet_number: None,
            exponent: u32::MAX,
        }
    }

    pub(super) fn peek(&self, space: &PacketSpace) -> u64 {
        let n = space.next_packet_number;
        if n != self.next_skipped_packet_number {
            return n;
        }
        n + 1
    }

    pub(super) fn allocate(
        &mut self,
        rng: &mut (impl Rng + ?Sized),
        space: &mut PacketSpace,
    ) -> u64 {
        let n = space.get_tx_number();
        if n != self.next_skipped_packet_number {
            return n;
        }

        trace!("skipping pn {n}");
        // Skip this packet number, and choose the next one to skip
        self.prev_skipped_packet_number = Some(self.next_skipped_packet_number);
        let next_exponent = self.exponent.saturating_add(1);
        self.next_skipped_packet_number = rng
            .random_range(2u64.saturating_pow(self.exponent)..2u64.saturating_pow(next_exponent));
        self.exponent = next_exponent;

        space.get_tx_number()
    }

    pub(super) fn check_ack(
        &self,
        space_id: SpaceId,
        range: std::ops::RangeInclusive<u64>,
    ) -> Result<(), TransportError> {
        if space_id == SpaceId::Data
            && self
                .prev_skipped_packet_number
                .is_some_and(|x| range.contains(&x))
        {
            return Err(TransportError::PROTOCOL_VIOLATION("unsent packet acked"));
        }
        Ok(())
    }
}

/// Ensures we can always fit all our ACKs in a single minimum-MTU packet with room to spare
const MAX_ACK_BLOCKS: usize = 64;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn sanity() {
        let mut dedup = Dedup::new();
        for packet in [0, 1, 2, 4, 7, 3, 6, 5] {
            assert!(!dedup.insert(packet));
            assert!(dedup.insert(packet));
        }
        assert_eq!(dedup.next, 8);
        assert!(!dedup.missing_in_interval(0, 7));
    }

    #[test]
    fn happypath() {
        let mut dedup = Dedup::new();
        for i in 0..(2 * STORAGE_BITS) {
            assert!(!dedup.insert(i));
            // Recent, boundary and already-retired duplicates. Linear work,
            // rather than a quadratic test of a much larger history.
            for j in [0, i / 2, i.saturating_sub(WINDOW_SIZE - 1), i] {
                assert!(dedup.insert(j));
            }
        }
        assert_eq!(mem::size_of_val(dedup.window.as_ref()), 4096);
    }

    #[test]
    fn jump() {
        let mut dedup = Dedup::new();
        for highest in [2 * WINDOW_SIZE, 1 << 40, (1 << 62) - 1] {
            assert!(!dedup.insert(highest));
            assert!(dedup.insert(highest - WINDOW_SIZE));
            assert!(!dedup.insert(highest - WINDOW_SIZE + 1));
            assert!(dedup.insert(highest - WINDOW_SIZE + 1));
            assert_eq!(dedup.next, highest + 1);
        }
    }

    #[test]
    fn dedup_has_missing() {
        let mut dedup = Dedup::new();
        dedup.insert(0);
        assert!(!dedup.missing_in_interval(0, 0));
        dedup.insert(1);
        assert!(!dedup.missing_in_interval(0, 1));
        dedup.insert(3);
        assert!(dedup.missing_in_interval(1, 3));
        dedup.insert(4);
        assert!(!dedup.missing_in_interval(3, 4));
        assert!(dedup.missing_in_interval(0, 4));
        dedup.insert(2);
        assert!(!dedup.missing_in_interval(0, 4));
    }

    #[test]
    fn dedup_outside_of_window_has_missing() {
        let mut dedup = Dedup::new();
        dedup.insert(0);
        dedup.insert(4);
        dedup.insert(WINDOW_SIZE + 10);
        // Retired holes never become newly unseen packets.
        assert!(!dedup.missing_in_interval(0, 4));
        assert_eq!(dedup.smallest_missing_in_interval(0, WINDOW_SIZE + 10), Some(11));
        assert!(dedup.insert(10));
        assert!(!dedup.insert(11));
        assert_eq!(dedup.smallest_missing_in_interval(0, WINDOW_SIZE + 10), Some(12));
    }

    #[test]
    fn dedup_smallest_missing() {
        let mut dedup = Dedup::new();
        dedup.insert(0);
        assert_eq!(dedup.smallest_missing_in_interval(0, 0), None);
        dedup.insert(1);
        assert_eq!(dedup.smallest_missing_in_interval(0, 1), None);
        dedup.insert(5);
        dedup.insert(7);
        assert_eq!(dedup.smallest_missing_in_interval(0, 7), Some(2));
        assert_eq!(dedup.smallest_missing_in_interval(5, 7), Some(6));
        dedup.insert(2);
        assert_eq!(dedup.smallest_missing_in_interval(1, 7), Some(3));
        dedup.insert(170);
        dedup.insert(172);
        dedup.insert(300);
        assert_eq!(dedup.smallest_missing_in_interval(170, 172), Some(171));
        dedup.insert(2 * WINDOW_SIZE);
        let floor = WINDOW_SIZE + 1;
        assert_eq!(dedup.smallest_missing_in_interval(0, 2 * WINDOW_SIZE), Some(floor));
        assert_eq!(dedup.smallest_missing_in_interval(0, floor + 1), Some(floor));
        assert_eq!(dedup.smallest_missing_in_interval(0, floor), None);
    }

    #[test]
    fn dedup_ring_wrap_matches_exact_reference() {
        use std::collections::BTreeSet;
        let mut received = BTreeSet::new();
        let mut dedup = Dedup::new();
        // Include word and storage wrap, skipped packets and reverse delivery.
        for block in 0..600 {
            let base = block * 131;
            for offset in (0..131).rev().filter(|n| n % 5 != 0) {
                let packet = base + offset;
                let floor = dedup.next.saturating_sub(WINDOW_SIZE);
                let duplicate = packet < floor || !received.insert(packet);
                assert_eq!(dedup.insert(packet), duplicate);
                assert!(dedup.insert(packet));
            }
            let highest = dedup.highest();
            let start = highest.saturating_sub(190);
            let first_missing = (start + 1..highest)
                .filter(|n| *n >= dedup.next.saturating_sub(WINDOW_SIZE))
                .find(|n| !received.contains(n));
            assert_eq!(dedup.smallest_missing_in_interval(start, highest), first_missing);
        }
    }

    #[test]
    fn pending_acks_first_packet_is_not_considered_reordered() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();
        dedup.insert(0);
        acks.packet_received(Instant::now(), 0, true, &dedup);
        assert!(!acks.immediate_ack_required);
    }

    #[test]
    fn pending_acks_after_immediate_ack_set() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();

        // Receive ack-eliciting packet
        dedup.insert(0);
        let now = Instant::now();
        acks.insert_one(0, now);
        acks.packet_received(now, 0, true, &dedup);

        // Sanity check
        assert!(!acks.ranges.is_empty());
        assert!(!acks.can_send());

        // Can send ACK after max_ack_delay exceeded
        acks.set_immediate_ack_required();
        assert!(acks.can_send());
    }

    #[test]
    fn pending_acks_ack_delay() {
        let mut acks = PendingAcks::new();
        let mut dedup = Dedup::new();

        let t1 = Instant::now();
        let t2 = t1 + Duration::from_millis(2);
        let t3 = t2 + Duration::from_millis(5);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(0));

        // In-order packet
        dedup.insert(0);
        acks.insert_one(0, t1);
        acks.packet_received(t1, 0, true, &dedup);
        assert_eq!(acks.ack_delay(t1), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(2));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(7));

        // Out of order (higher than expected)
        dedup.insert(3);
        acks.insert_one(3, t2);
        acks.packet_received(t2, 3, true, &dedup);
        assert_eq!(acks.ack_delay(t2), Duration::from_millis(0));
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));

        // Out of order (lower than expected, so previous instant is kept)
        dedup.insert(2);
        acks.insert_one(2, t3);
        acks.packet_received(t3, 2, true, &dedup);
        assert_eq!(acks.ack_delay(t3), Duration::from_millis(5));
    }

    #[test]
    fn sent_packet_size() {
        // The tracking state of sent packets should be minimal, and not grow
        // over time.
        let size = std::mem::size_of::<SentPacket>();
        assert!(size <= 128, "SentPacket grew to {size} bytes");
    }

    #[test]
    fn sent_packet_delivery_state_accessor_preserves_some_and_none() {
        fn packet(now: Instant, state: Option<PacketDeliveryState>) -> SentPacket {
            let (delivery_state_payload, has_delivery_state) =
                SentPacket::store_delivery_state(now, state);
            SentPacket {
                path_generation: 0,
                controller_epoch: 0,
                ecn_marked: false,
                time_sent: now,
                delivery_state_payload,
                has_delivery_state,
                app_limited: false,
                size: 0,
                ack_eliciting: false,
                largest_acked: None,
                retransmits: ThinRetransmits::default(),
                stream_frames: frame::StreamMetaVec::default(),
            }
        }

        let now = Instant::now();
        let expected = PacketDeliveryState {
            delivered: 41,
            delivered_time: now.checked_sub(Duration::from_millis(7)).unwrap(),
            send_elapsed_ns: 17,
        };
        let actual = packet(now, Some(expected))
            .delivery_state()
            .expect("present controller state must round-trip");
        assert_eq!(actual.delivered, expected.delivered);
        assert_eq!(actual.delivered_time, expected.delivered_time);
        assert_eq!(actual.send_elapsed_ns, expected.send_elapsed_ns);
        assert!(packet(now, None).delivery_state().is_none());
    }
}

use crate::mux::MuxLimits;
use crate::protocol::{DatagramFlowId, DatagramId, Frame};
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatagramFlow {
    flow_id: DatagramFlowId,
    next_datagram_id: u64,
    queued_bytes: usize,
    queue: VecDeque<QueuedDatagram>,
    dropped_expired: u64,
    dropped_queue_full: u64,
    dropped_oversize: u64,
    limits: MuxLimits,
}

impl DatagramFlow {
    pub fn new(flow_id: DatagramFlowId, limits: MuxLimits) -> Self {
        Self {
            flow_id,
            next_datagram_id: 0,
            queued_bytes: 0,
            queue: VecDeque::new(),
            dropped_expired: 0,
            dropped_queue_full: 0,
            dropped_oversize: 0,
            limits,
        }
    }

    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes
    }

    pub fn dropped_expired(&self) -> u64 {
        self.dropped_expired
    }

    pub fn dropped_queue_full(&self) -> u64 {
        self.dropped_queue_full
    }

    pub fn dropped_oversize(&self) -> u64 {
        self.dropped_oversize
    }

    pub fn enqueue(
        &mut self,
        now_ms: u64,
        ttl_ms: u32,
        payload: Bytes,
    ) -> Result<DatagramId, DatagramError> {
        if payload.is_empty() {
            return Err(DatagramError::EmptyPayload);
        }
        if payload.len() > self.limits.max_payload_bytes {
            self.dropped_oversize = self.dropped_oversize.saturating_add(1);
            return Err(DatagramError::PayloadTooLarge {
                actual: payload.len(),
                limit: self.limits.max_payload_bytes,
            });
        }
        let new_queued =
            self.queued_bytes
                .checked_add(payload.len())
                .ok_or(DatagramError::QueueFull {
                    actual: usize::MAX,
                    limit: self.limits.max_datagram_queue_bytes,
                })?;
        if new_queued > self.limits.max_datagram_queue_bytes {
            self.dropped_queue_full = self.dropped_queue_full.saturating_add(1);
            return Err(DatagramError::QueueFull {
                actual: new_queued,
                limit: self.limits.max_datagram_queue_bytes,
            });
        }

        let datagram_id = DatagramId(self.next_datagram_id);
        self.next_datagram_id = self
            .next_datagram_id
            .checked_add(1)
            .ok_or(DatagramError::DatagramIdOverflow)?;
        self.queued_bytes = new_queued;
        self.queue.push_back(QueuedDatagram {
            datagram_id,
            enqueued_at_ms: now_ms,
            ttl_ms,
            payload,
        });
        Ok(datagram_id)
    }

    pub fn pop_frame(&mut self, now_ms: u64) -> Option<Frame> {
        self.drop_expired(now_ms);
        let item = self.queue.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(item.payload.len());
        Some(Frame::DatagramData {
            flow_id: self.flow_id,
            datagram_id: item.datagram_id,
            ttl_ms: remaining_ttl_ms(item.enqueued_at_ms, item.ttl_ms, now_ms),
            payload: item.payload,
        })
    }

    pub fn drop_expired(&mut self, now_ms: u64) {
        while self
            .queue
            .front()
            .is_some_and(|item| is_expired(item.enqueued_at_ms, item.ttl_ms, now_ms))
        {
            let item = self.queue.pop_front().expect("front exists");
            self.queued_bytes = self.queued_bytes.saturating_sub(item.payload.len());
            self.dropped_expired = self.dropped_expired.saturating_add(1);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueuedDatagram {
    datagram_id: DatagramId,
    enqueued_at_ms: u64,
    ttl_ms: u32,
    payload: Bytes,
}

fn is_expired(enqueued_at_ms: u64, ttl_ms: u32, now_ms: u64) -> bool {
    now_ms.saturating_sub(enqueued_at_ms) >= ttl_ms as u64
}

fn remaining_ttl_ms(enqueued_at_ms: u64, ttl_ms: u32, now_ms: u64) -> u32 {
    let elapsed = now_ms.saturating_sub(enqueued_at_ms);
    (ttl_ms as u64).saturating_sub(elapsed).min(u32::MAX as u64) as u32
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramError {
    EmptyPayload,
    PayloadTooLarge { actual: usize, limit: usize },
    QueueFull { actual: usize, limit: usize },
    FlowLimitExceeded { limit: usize },
    DatagramIdOverflow,
}

impl std::fmt::Display for DatagramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyPayload => write!(f, "datagram payload must not be empty"),
            Self::PayloadTooLarge { actual, limit } => {
                write!(f, "datagram payload is {actual} bytes, limit is {limit}")
            }
            Self::QueueFull { actual, limit } => {
                write!(
                    f,
                    "datagram queue would hold {actual} bytes, limit is {limit}"
                )
            }
            Self::FlowLimitExceeded { limit } => {
                write!(f, "datagram flow limit of {limit} reached")
            }
            Self::DatagramIdOverflow => write!(f, "datagram id space is exhausted"),
        }
    }
}

impl std::error::Error for DatagramError {}

#[cfg(test)]
#[path = "datagram_test.rs"]
mod tests;

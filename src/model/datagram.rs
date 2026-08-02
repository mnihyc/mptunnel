//! Direction-local datagram replay protection.
//!
//! Datagram IDs are monotonic within one sender direction. A bounded sliding
//! window accepts limited reordering while preventing a replayed ID from being
//! delivered twice after its retained payload identity is evicted.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DatagramPayloadIdentity {
    len: usize,
    digest: [u8; 32],
}

impl DatagramPayloadIdentity {
    pub(crate) fn new(payload: &[u8]) -> Self {
        Self {
            len: payload.len(),
            digest: Sha256::digest(payload).into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatagramAdmission {
    Fresh,
    Duplicate,
}

#[derive(Debug)]
pub(crate) struct DatagramReceiveWindow {
    width: u64,
    largest: Option<u64>,
    retained: BTreeMap<u64, DatagramPayloadIdentity>,
}

impl DatagramReceiveWindow {
    pub(crate) fn new(width: usize) -> Self {
        Self {
            width: u64::try_from(width.max(1)).unwrap_or(u64::MAX),
            largest: None,
            retained: BTreeMap::new(),
        }
    }

    pub(crate) fn admit(
        &mut self,
        datagram_id: u64,
        payload: DatagramPayloadIdentity,
    ) -> Result<DatagramAdmission, ()> {
        let admission = self.classify(datagram_id, payload)?;
        if admission == DatagramAdmission::Fresh {
            self.record_fresh(datagram_id, payload);
        }
        Ok(admission)
    }

    pub(crate) fn classify(
        &self,
        datagram_id: u64,
        payload: DatagramPayloadIdentity,
    ) -> Result<DatagramAdmission, ()> {
        let lower_bound = self.lower_bound();
        if self.largest.is_some() && datagram_id < lower_bound {
            return Ok(DatagramAdmission::Duplicate);
        }
        if let Some(retained) = self.retained.get(&datagram_id) {
            return if *retained == payload {
                Ok(DatagramAdmission::Duplicate)
            } else {
                Err(())
            };
        }

        Ok(DatagramAdmission::Fresh)
    }

    pub(crate) fn record_fresh(&mut self, datagram_id: u64, payload: DatagramPayloadIdentity) {
        debug_assert_eq!(
            self.classify(datagram_id, payload),
            Ok(DatagramAdmission::Fresh)
        );
        if self.largest.is_none_or(|largest| datagram_id > largest) {
            self.largest = Some(datagram_id);
            let lower_bound = self.lower_bound();
            self.retained.retain(|id, _| *id >= lower_bound);
        }
        self.retained.insert(datagram_id, payload);
    }

    fn lower_bound(&self) -> u64 {
        self.largest
            .map(|largest| largest.saturating_sub(self.width.saturating_sub(1)))
            .unwrap_or(0)
    }
}

#[cfg(test)]
#[path = "tests_datagram.rs"]
mod tests;

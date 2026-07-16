//! Stateful validation for bounded path-capacity wire records.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PathCapacityReceiveError {
    InvalidData,
    ReceiptOverflow,
    SessionEnvelopeExceeded,
    InterleavedToken,
    FinishWithoutData,
    FinishMismatch,
}

impl PathCapacityReceiveError {
    pub(crate) fn message(self) -> &'static str {
        match self {
            Self::InvalidData => "invalid path capacity data",
            Self::ReceiptOverflow => "path capacity receipt overflow",
            Self::SessionEnvelopeExceeded => "path capacity data exceeds the session envelope",
            Self::InterleavedToken => "interleaved path capacity measurement tokens",
            Self::FinishWithoutData => "path capacity finish has no data epoch",
            Self::FinishMismatch => "path capacity finish does not match received data",
        }
    }
}

// Capacity records share one ordered carrier stream. Tracking one bounded
// epoch makes Finish an exact receipt boundary without retaining the train.
#[derive(Debug)]
pub(crate) struct CapacityReceiveTracker {
    active: Option<(u64, u64)>,
    limit_bytes: u64,
}

impl CapacityReceiveTracker {
    pub(crate) fn new(limit_bytes: u64) -> Self {
        Self {
            active: None,
            limit_bytes: limit_bytes.max(1),
        }
    }

    pub(crate) fn record_data(
        &mut self,
        token: u64,
        payload_bytes: usize,
    ) -> Result<(), PathCapacityReceiveError> {
        if token == 0 || payload_bytes == 0 {
            return Err(PathCapacityReceiveError::InvalidData);
        }
        let payload_bytes = payload_bytes as u64;
        match self.active {
            Some((active_token, received_bytes)) if active_token == token => {
                let received_bytes = received_bytes
                    .checked_add(payload_bytes)
                    .ok_or(PathCapacityReceiveError::ReceiptOverflow)?;
                if received_bytes > self.limit_bytes {
                    return Err(PathCapacityReceiveError::SessionEnvelopeExceeded);
                }
                self.active = Some((token, received_bytes));
            }
            None if payload_bytes <= self.limit_bytes => self.active = Some((token, payload_bytes)),
            None => {
                return Err(PathCapacityReceiveError::SessionEnvelopeExceeded);
            }
            Some(_) => {
                return Err(PathCapacityReceiveError::InterleavedToken);
            }
        }
        Ok(())
    }

    pub(crate) fn finish(
        &mut self,
        token: u64,
        declared_bytes: u64,
    ) -> Result<u64, PathCapacityReceiveError> {
        let Some((active_token, received_bytes)) = self.active.take() else {
            return Err(PathCapacityReceiveError::FinishWithoutData);
        };
        if token == 0
            || token != active_token
            || declared_bytes == 0
            || declared_bytes != received_bytes
        {
            return Err(PathCapacityReceiveError::FinishMismatch);
        }
        Ok(received_bytes)
    }
}

#[cfg(test)]
#[path = "path_capacity_test.rs"]
mod tests;

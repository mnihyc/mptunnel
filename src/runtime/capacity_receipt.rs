use super::RuntimeError;

// Capacity records share one ordered carrier stream. Tracking one bounded
// epoch makes Finish an exact receipt boundary without retaining the train.
#[derive(Debug)]
pub(super) struct CapacityReceiveTracker {
    active: Option<(u64, u64)>,
    limit_bytes: u64,
}

impl CapacityReceiveTracker {
    pub(super) fn new(limit_bytes: u64) -> Self {
        Self {
            active: None,
            limit_bytes: limit_bytes.max(1),
        }
    }

    pub(super) fn record_data(
        &mut self,
        token: u64,
        payload_bytes: usize,
    ) -> Result<(), RuntimeError> {
        if token == 0 || payload_bytes == 0 {
            return Err(RuntimeError::Protocol("invalid path capacity data"));
        }
        let payload_bytes = payload_bytes as u64;
        match self.active {
            Some((active_token, received_bytes)) if active_token == token => {
                let received_bytes = received_bytes
                    .checked_add(payload_bytes)
                    .ok_or(RuntimeError::Protocol("path capacity receipt overflow"))?;
                if received_bytes > self.limit_bytes {
                    return Err(RuntimeError::Protocol(
                        "path capacity data exceeds the session envelope",
                    ));
                }
                self.active = Some((token, received_bytes));
            }
            None if payload_bytes <= self.limit_bytes => self.active = Some((token, payload_bytes)),
            None => {
                return Err(RuntimeError::Protocol(
                    "path capacity data exceeds the session envelope",
                ));
            }
            Some(_) => {
                return Err(RuntimeError::Protocol(
                    "interleaved path capacity calibration tokens",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn finish(&mut self, token: u64, declared_bytes: u64) -> Result<u64, RuntimeError> {
        let Some((active_token, received_bytes)) = self.active.take() else {
            return Err(RuntimeError::Protocol(
                "path capacity finish has no data epoch",
            ));
        };
        if token == 0
            || token != active_token
            || declared_bytes == 0
            || declared_bytes != received_bytes
        {
            return Err(RuntimeError::Protocol(
                "path capacity finish does not match received data",
            ));
        }
        Ok(received_bytes)
    }
}

#[cfg(test)]
mod tests;

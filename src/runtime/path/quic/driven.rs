//! Native-owned polling and cancellation of the ordinary QUIC actor.
//!
//! Execution ownership preserves framing, Product authority, first-packetization
//! admission and Native congestion policy.

use crate::runtime::RuntimeError;
use crate::transport::quic::{NativeSourceRegistration, QuicCarrierError};
use std::future::Future;

pub(super) async fn run_ordinary_source<R: Send + 'static>(
    registration: NativeSourceRegistration,
    actor: impl Future<Output = R> + Send + 'static,
) -> Result<R, RuntimeError> {
    let handle = registration
        .register(actor)
        .map_err(QuicCarrierError::Connection)?;
    // The owning handle synchronously destroys the actual actor on select
    // cancellation. No Native or Product lock may be held at that boundary.
    // Actor panics resume in this original Tokio parent, not the Native driver.
    handle.await.map_err(|stopped| match stopped.cause {
        Some(cause) => QuicCarrierError::Connection(cause).into(),
        None => QuicCarrierError::NativeDriverStopped.into(),
    })
}

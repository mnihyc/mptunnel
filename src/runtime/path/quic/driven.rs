//! Diagnostic comparison of existing actor polling and Native-owned handoff.
//!
//! The switch selects execution ownership only. Framing, Product authority,
//! first-packetization admission and Native congestion policy remain unchanged.

use crate::runtime::RuntimeError;
use crate::transport::quic::{NativeSourceRegistration, QuicCarrierError};
use std::future::Future;
use std::sync::OnceLock;

pub(super) async fn run_ordinary_source<R: Send + 'static>(
    registration: NativeSourceRegistration,
    actor: impl Future<Output = R> + Send + 'static,
) -> Result<R, RuntimeError> {
    static DRIVEN: OnceLock<bool> = OnceLock::new();
    let driven = *DRIVEN
        .get_or_init(|| std::env::var("MPTUNNEL_NATIVE_SOURCE_DRIVEN").as_deref() == Ok("1"));
    if !driven {
        return Ok(actor.await);
    }
    let handle = registration
        .register(actor)
        .map_err(QuicCarrierError::Connection)?;
    if let Ok(role) = std::env::var("MPTUNNEL_NATIVE_STATE_TRACE_ROLE") {
        eprintln!("native_product_source_registered role={role}");
    }
    // The owning handle synchronously destroys the actual actor on select
    // cancellation. No Native or Product lock may be held at that boundary.
    // Actor panics resume in this original Tokio parent, not the Native driver.
    handle.await.map_err(|stopped| match stopped.cause {
        Some(cause) => QuicCarrierError::Connection(cause).into(),
        None => QuicCarrierError::NativeDriverStopped.into(),
    })
}

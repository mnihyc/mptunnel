//! Targeted observation of native initial STREAM frame construction.

use std::{future::Future, pin::pin, sync::Arc};

use proto::{ClosedStream, ConnectionError, Dir, SendStreamProgress, StreamId, VarInt};
use thiserror::Error;
use tokio::sync::Notify;

use crate::connection::{ConnectionRef, State};

/// Read-only native progress for one established send-stream lifetime.
///
/// Clones and independently created observers of the same native stream share
/// one notification cell. Proto holds only its earliest armed end; after that
/// end crosses, concurrent later waiters recheck and rearm. No per-write registry
/// or polling task is created. Dropping a waiter cannot withdraw another's arm;
/// the last observer removes the cell and any remaining arm.
#[derive(Clone, Debug)]
pub struct SendStreamObserver {
    conn: ConnectionRef,
    stream: StreamId,
    notify: Arc<Notify>,
}

impl SendStreamObserver {
    pub(crate) fn new(
        conn: ConnectionRef,
        stream: StreamId,
    ) -> Result<Self, SendStreamObservationError> {
        let notify = {
            let mut state = conn.state.lock("observe_send_stream");
            progress(&mut state, stream)?;
            state
                .packetization_observers
                .entry(stream)
                .or_default()
                .clone()
        };
        Ok(Self {
            conn,
            stream,
            notify,
        })
    }

    /// Return one coherent native-stream snapshot, including adapter headers.
    ///
    /// Initial packet construction can precede UDP socket emission. This is
    /// neither a native ACK nor Product delivery/credit observation.
    pub fn snapshot(&self) -> Result<SendStreamProgress, SendStreamObservationError> {
        progress(
            &mut self.conn.state.lock("send_stream_progress"),
            self.stream,
        )
    }

    /// Wait for this send half to stop, reset, finish, or lose its connection.
    ///
    /// This works with no accepted bytes or pending packetization target. It
    /// uses the same stream-local notification cell without arming a native
    /// offset. Cancelling the owned future leaves all native service and other
    /// observers unchanged. The returned error identifies the ended lifetime.
    pub fn wait_until_terminated(
        &self,
    ) -> impl Future<Output = SendStreamObservationError> + Send + 'static {
        let observer = self.clone();
        async move {
            loop {
                // Register before checking under the same connection lock used
                // to publish terminal events. A stop cannot fall between them.
                let mut changed = pin!(observer.notify.notified());
                changed.as_mut().enable();
                if let Err(error) = observer.snapshot() {
                    return error;
                }
                changed.await;
            }
        }
    }

    /// Wait until the already accepted `end` has been initially packetized.
    ///
    /// The target is validated when first polled. A past crossing returns
    /// immediately; reset, peer stop, or connection termination returns an error.
    /// Retransmission does not advance this cursor. Cancelling this future leaves
    /// stream transmission and other observers untouched. An earlier cancelled
    /// target may cause one same-stream recheck, but never false completion.
    pub fn wait_until_packetized(
        &self,
        end: u64,
    ) -> impl Future<Output = Result<SendStreamProgress, SendStreamObservationError>> + Send + 'static
    {
        let observer = self.clone();
        async move {
            loop {
                // Register before examining/arming under the connection lock.
                // The packetizer and event forwarding use that same lock, so a
                // crossing cannot disappear between the check and this wait.
                let mut changed = pin!(observer.notify.notified());
                changed.as_mut().enable();
                {
                    let mut state = observer.conn.state.lock("wait_stream_packetized");
                    let snapshot = progress(&mut state, observer.stream)?;
                    if end > snapshot.accepted_end {
                        return Err(SendStreamObservationError::UnacceptedEnd {
                            accepted_end: snapshot.accepted_end,
                        });
                    }
                    if snapshot.first_unpacketized >= end {
                        return Ok(snapshot);
                    }
                    state
                        .inner
                        .send_stream(observer.stream)
                        .request_packetization_notification(end)?;
                }
                changed.await;
            }
        }
    }
}

impl Drop for SendStreamObserver {
    fn drop(&mut self) {
        let mut state = self.conn.state.lock("drop_send_stream_observer");
        // Only this handle and the map retain the cell. Pending wait futures
        // each own a cloned observer and therefore prevent this last-drop path.
        if Arc::strong_count(&self.notify) == 2
            && state
                .packetization_observers
                .get(&self.stream)
                .is_some_and(|cell| Arc::ptr_eq(cell, &self.notify))
        {
            state.packetization_observers.remove(&self.stream);
            state
                .inner
                .send_stream(self.stream)
                .cancel_packetization_notification();
        }
    }
}

fn progress(
    state: &mut State,
    stream: StreamId,
) -> Result<SendStreamProgress, SendStreamObservationError> {
    if let Some(error) = &state.error {
        return Err(SendStreamObservationError::ConnectionLost(error.clone()));
    }
    if state.inner.is_handshaking() {
        return Err(SendStreamObservationError::NotEstablished);
    }
    if stream.dir() == Dir::Uni && stream.initiator() != state.inner.side() {
        return Err(SendStreamObservationError::ClosedStream);
    }
    let sender = state.inner.send_stream(stream);
    if let Some(reason) = sender.stopped()? {
        return Err(SendStreamObservationError::Stopped(reason));
    }
    Ok(sender.packetization_progress()?)
}

/// Failure to observe one established native send-stream lifetime.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SendStreamObservationError {
    /// Early data can rewind native offsets; observation is unavailable then.
    #[error("send-stream observation requires an established connection")]
    NotEstablished,
    /// The native stream was reset, removed, or does not exist.
    #[error("native send stream is reset or closed")]
    ClosedStream,
    /// The peer asked that transmission stop.
    #[error("native send stream stopped by peer: {0}")]
    Stopped(VarInt),
    /// The connection lifetime ended.
    #[error("native connection lost: {0}")]
    ConnectionLost(ConnectionError),
    /// The requested end had not been accepted when the wait was checked.
    #[error("packetization target exceeds accepted end {accepted_end}")]
    UnacceptedEnd {
        /// Current end of native accepted bytes.
        accepted_end: u64,
    },
}

impl From<ClosedStream> for SendStreamObservationError {
    fn from(_: ClosedStream) -> Self {
        Self::ClosedStream
    }
}

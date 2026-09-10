//! One retained carrier frame and the exact resource that can accept it.
//!
//! A mailbox wait is independent of a pinned native write. An actor barrier is
//! not: it may need that writer's completion or a lifecycle transition first.

use crate::protocol::Frame;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;

pub(in crate::runtime) enum CarrierInputRoute {
    Routed,
    Barrier(Frame),
    Mailbox(PendingMailboxFrame),
}

type FramePermit = Box<dyn FnOnce(Frame) + Send>;

/// Holds one frame until its exact recipient grants a real slot. Erasing only
/// the permit keeps server event types out of the carrier-facing contract.
pub(in crate::runtime) struct PendingMailboxFrame {
    frame: Option<Frame>,
    permit: Pin<Box<dyn Future<Output = Option<FramePermit>> + Send>>,
}

impl PendingMailboxFrame {
    pub(in crate::runtime) fn new<T: Send + 'static>(
        frame: Frame,
        recipient: mpsc::Sender<T>,
        wrap: impl FnOnce(Frame) -> T + Send + 'static,
    ) -> Self {
        let permit = Box::pin(async move {
            recipient.reserve_owned().await.ok().map(|permit| {
                Box::new(move |frame| {
                    permit.send(wrap(frame));
                }) as FramePermit
            })
        });
        Self {
            frame: Some(frame),
            permit,
        }
    }

    /// Cancellation before transfer leaves the frame here. Once the permit is
    /// ready, taking the frame and sending have no intervening await.
    /// False means the Product recipient retired, not carrier failure or delivery.
    pub(in crate::runtime) async fn deliver(&mut self) -> bool {
        let permit = self.permit.as_mut().await;
        let frame = self.frame.take().expect("pending mailbox owns one frame");
        match permit {
            Some(send) => {
                send(frame);
                true
            }
            None => false,
        }
    }

    /// Used only when the native write wins before mailbox transfer.
    pub(in crate::runtime) fn into_frame(mut self) -> Frame {
        self.frame
            .take()
            .expect("unfinished mailbox retains its frame")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;

    #[tokio::test]
    async fn mailbox_wait_cancellation_retains_frame_and_releases_reservation() {
        let (sender, mut receiver) = mpsc::channel(1);
        sender.try_send(Frame::Ping { nonce: 1 }).unwrap();
        let frame = Frame::Ping { nonce: 2 };
        let mut pending = PendingMailboxFrame::new(frame.clone(), sender.clone(), |frame| frame);
        let mut delivery = Box::pin(pending.deliver());
        std::future::poll_fn(|cx| {
            assert!(delivery.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(delivery);
        assert_eq!(pending.into_frame(), frame);
        assert!(matches!(
            receiver.recv().await,
            Some(Frame::Ping { nonce: 1 })
        ));
        sender
            .try_send(Frame::Ping { nonce: 3 })
            .expect("cancelled reservation must not hold capacity");
        assert!(matches!(
            receiver.recv().await,
            Some(Frame::Ping { nonce: 3 })
        ));
        assert!(
            receiver.try_recv().is_err(),
            "cancelled frame was not duplicated"
        );
    }

    #[tokio::test]
    async fn mailbox_wait_closed_recipient_retires_without_delivery() {
        let (sender, receiver) = mpsc::channel(1);
        let mut pending = PendingMailboxFrame::new(Frame::Ping { nonce: 4 }, sender, |frame| frame);
        drop(receiver);
        assert!(!pending.deliver().await);
    }
}

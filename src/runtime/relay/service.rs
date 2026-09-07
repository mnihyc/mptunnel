//! Cancellation-safe arbitration at the Product actor, above carrier writers.

use std::future::{Future, poll_fn};
use std::pin::pin;
use std::task::Poll;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum ServiceClass {
    #[default]
    Input,
    Dispatch,
    Read,
}

impl ServiceClass {
    fn next(self) -> Self {
        match self {
            Self::Input => Self::Dispatch,
            Self::Dispatch => Self::Read,
            Self::Read => Self::Input,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum RelayServiceEvent<R, I> {
    Input(I),
    Dispatch,
    Read(R),
}

#[derive(Default)]
pub(super) struct RelayServiceTurn {
    next: ServiceClass,
}

impl RelayServiceTurn {
    /// Select ready Product work without letting a replenished input tail own
    /// the actor. Each result uses its existing bounded handler quantum. A
    /// pending source read is skipped, not awaited ahead of input; cursor
    /// state changes only when an event transfers to the actor.
    pub(super) async fn select<R, I>(
        &mut self,
        dispatch_ready: bool,
        read_enabled: bool,
        read: impl Future<Output = R>,
        input_enabled: bool,
        input: impl Future<Output = I>,
    ) -> RelayServiceEvent<R, I> {
        let mut read = pin!(read);
        let mut input = pin!(input);
        // Dispatch can be synchronously ready even after an input channel
        // exhausted this task's executor budget. Preserve that yield boundary
        // for the entire Product turn, including zero-progress dispatches.
        tokio::task::coop::cooperative(poll_fn(|cx| {
            let mut class = self.next;
            for _ in 0..3 {
                let event = match class {
                    ServiceClass::Input if input_enabled => {
                        input.as_mut().poll(cx).map(RelayServiceEvent::Input)
                    }
                    ServiceClass::Dispatch if dispatch_ready => {
                        Poll::Ready(RelayServiceEvent::Dispatch)
                    }
                    ServiceClass::Read if read_enabled => {
                        read.as_mut().poll(cx).map(RelayServiceEvent::Read)
                    }
                    _ => Poll::Pending,
                };
                if event.is_ready() {
                    self.next = class.next();
                    return event;
                }
                class = class.next();
            }
            Poll::Pending
        }))
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};

    #[tokio::test]
    async fn continuously_ready_input_cannot_starve_source_or_dispatch() {
        let mut turn = RelayServiceTurn::default();
        let mut events = Vec::new();
        for _ in 0..6 {
            events.push(turn.select(true, true, ready(7), true, ready(11)).await);
        }
        assert_eq!(
            events,
            vec![
                RelayServiceEvent::Input(11),
                RelayServiceEvent::Dispatch,
                RelayServiceEvent::Read(7),
                RelayServiceEvent::Input(11),
                RelayServiceEvent::Dispatch,
                RelayServiceEvent::Read(7),
            ],
        );
    }

    #[tokio::test]
    async fn pending_source_never_blocks_input() {
        let mut turn = RelayServiceTurn {
            next: ServiceClass::Read,
        };
        assert_eq!(
            turn.select(false, true, pending::<()>(), true, ready(11))
                .await,
            RelayServiceEvent::Input(11),
        );
    }

    #[tokio::test]
    async fn exhausted_executor_budget_cannot_be_bypassed_by_dispatch() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.try_send(11).unwrap();
        let mut turn = RelayServiceTurn::default();
        while tokio::task::coop::has_budget_remaining() {
            tokio::task::coop::consume_budget().await;
        }
        let future = turn.select(true, false, pending::<()>(), true, rx.recv());
        let mut future = pin!(future);
        assert!(
            matches!(futures::poll!(&mut future), Poll::Pending),
            "ready dispatch must not bypass the executor yield required by buffered input",
        );
    }

    #[tokio::test]
    async fn disabled_source_is_not_polled_and_cancelled_selection_keeps_cursor() {
        let mut turn = RelayServiceTurn {
            next: ServiceClass::Read,
        };
        {
            let future = turn.select(
                false,
                false,
                async { panic!("disabled source") },
                true,
                pending::<()>(),
            );
            let mut future = pin!(future);
            assert!(matches!(futures::poll!(&mut future), Poll::Pending));
        }
        assert_eq!(turn.next, ServiceClass::Read);
    }

    #[tokio::test]
    async fn productive_turn_does_not_take_or_reorder_unselected_input() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(2);
        tx.try_send(11).unwrap();
        tx.try_send(13).unwrap();
        let mut turn = RelayServiceTurn {
            next: ServiceClass::Read,
        };
        assert_eq!(
            turn.select(false, true, ready(7), true, rx.recv()).await,
            RelayServiceEvent::Read(7),
        );
        assert_eq!(rx.len(), 2);
        assert_eq!(
            turn.select(true, true, ready(9), true, rx.recv()).await,
            RelayServiceEvent::Input(Some(11)),
        );
        assert_eq!(
            turn.select(true, true, ready(9), true, rx.recv()).await,
            RelayServiceEvent::Dispatch,
        );
        assert_eq!(rx.try_recv().unwrap(), 13);
    }
}

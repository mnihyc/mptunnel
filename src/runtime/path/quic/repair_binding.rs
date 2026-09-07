//! One companion claim per accepted native parent, within one QUIC connection.
//!
//! Only parent acceptance publishes entries. A child never allocates Product
//! state, waits for an unknown parent, or owns that parent's detach authority.

use crate::protocol::StreamId;
use crate::runtime::error::RuntimeError;
use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;

type Entries<T> = HashMap<u64, (StreamId, oneshot::Sender<T>)>;

pub(super) struct RepairBindings<T> {
    entries: Arc<Mutex<Entries<T>>>,
}

impl<T> Clone for RepairBindings<T> {
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
        }
    }
}

impl<T> Default for RepairBindings<T> {
    fn default() -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<T> RepairBindings<T> {
    pub(super) fn register(
        &self,
        parent_id: u64,
        stream_id: StreamId,
    ) -> Result<(RepairParent<T>, oneshot::Receiver<T>), RuntimeError> {
        let (sender, receiver) = oneshot::channel();
        match self
            .entries
            .lock()
            .expect("repair binding lock")
            .entry(parent_id)
        {
            Entry::Vacant(entry) => {
                entry.insert((stream_id, sender));
            }
            Entry::Occupied(_) => {
                return Err(RuntimeError::Protocol("duplicate native repair parent"));
            }
        }
        Ok((
            RepairParent {
                bindings: self.clone(),
                parent_id,
            },
            receiver,
        ))
    }

    pub(super) fn claim(&self, parent_id: u64, stream_id: StreamId, child: T) -> Result<(), T> {
        let sender = {
            let mut entries = self.entries.lock().expect("repair binding lock");
            if entries
                .get(&parent_id)
                .is_none_or(|(owner, _)| *owner != stream_id)
            {
                return Err(child);
            }
            entries.remove(&parent_id).expect("checked repair parent").1
        };
        // Cancellation can race claim after removal. Ownership either moves
        // once to the parent's receiver or returns here for local disposal.
        sender.send(child)
    }
}

pub(super) struct RepairParent<T> {
    bindings: RepairBindings<T>,
    parent_id: u64,
}

impl<T> Drop for RepairParent<T> {
    fn drop(&mut self) {
        self.bindings
            .entries
            .lock()
            .expect("repair binding lock")
            .remove(&self.parent_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn repair_binding_claim_is_exact_once_and_connection_local() {
        let bindings = RepairBindings::default();
        let other_connection = RepairBindings::default();
        assert_eq!(
            bindings.claim(4, StreamId(1), 10),
            Err(10),
            "child cannot create a parent"
        );
        let (parent, receiver) = bindings.register(4, StreamId(1)).unwrap();
        assert!(bindings.register(4, StreamId(1)).is_err());
        assert_eq!(bindings.claim(4, StreamId(2), 11), Err(11));
        assert_eq!(other_connection.claim(4, StreamId(1), 12), Err(12));
        assert_eq!(bindings.claim(4, StreamId(1), 13), Ok(()));
        assert_eq!(bindings.claim(4, StreamId(1), 14), Err(14));
        assert_eq!(receiver.await.unwrap(), 13);
        drop(parent);
        assert!(bindings.entries.lock().unwrap().is_empty());
    }

    #[test]
    fn repair_binding_cancellation_cannot_reach_a_replacement() {
        let bindings = RepairBindings::default();
        let (old, mut old_rx) = bindings.register(4, StreamId(1)).unwrap();
        drop(old);
        assert!(matches!(
            old_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        let (_new, mut new_rx) = bindings.register(12, StreamId(1)).unwrap();
        assert_eq!(bindings.claim(4, StreamId(1), 10), Err(10));
        assert!(matches!(
            new_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(bindings.claim(12, StreamId(1), 11), Ok(()));
        assert_eq!(new_rx.try_recv(), Ok(11));
    }

    #[test]
    fn repair_binding_cancelled_claim_returns_child_without_leaking_entry() {
        let bindings = RepairBindings::default();
        let (_parent, receiver) = bindings.register(4, StreamId(1)).unwrap();
        drop(receiver);
        assert_eq!(bindings.claim(4, StreamId(1), 10), Err(10));
        assert!(bindings.entries.lock().unwrap().is_empty());
    }
}

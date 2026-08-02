//! Generation-owned authenticated carrier inventory for Product admission.
//!
//! Transport actors publish one exact registration after authenticated
//! readiness and retain it for the physical connection lifetime. Product reads
//! one atomic snapshot only when selecting a new outbound flow; established
//! flows and Core scheduling never consult this inventory.

use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) enum AuthenticatedCarrierAvailability {
    AwaitingFirstCarrier,
    Available,
    Offline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::runtime) struct AuthenticatedCarrierSnapshot {
    pub(in crate::runtime) live_count: usize,
    pub(in crate::runtime) ever_authenticated: bool,
}

impl AuthenticatedCarrierSnapshot {
    pub(in crate::runtime) const fn availability(self) -> AuthenticatedCarrierAvailability {
        if self.live_count > 0 {
            AuthenticatedCarrierAvailability::Available
        } else if self.ever_authenticated {
            AuthenticatedCarrierAvailability::Offline
        } else {
            AuthenticatedCarrierAvailability::AwaitingFirstCarrier
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(in crate::runtime) struct AuthenticatedCarrierInventory {
    state: Arc<Mutex<AuthenticatedCarrierInventoryState>>,
}

#[derive(Debug, Default)]
struct AuthenticatedCarrierInventoryState {
    live_count: usize,
    ever_authenticated: bool,
}

impl AuthenticatedCarrierInventory {
    pub(in crate::runtime) fn register(&self) -> AuthenticatedCarrierRegistration {
        let mut state = self
            .state
            .lock()
            .expect("authenticated carrier inventory lock");
        state.live_count = state
            .live_count
            .checked_add(1)
            .expect("authenticated carrier inventory overflow");
        state.ever_authenticated = true;
        AuthenticatedCarrierRegistration {
            inventory: self.clone(),
        }
    }

    pub(in crate::runtime) fn snapshot(&self) -> AuthenticatedCarrierSnapshot {
        let state = self
            .state
            .lock()
            .expect("authenticated carrier inventory lock");
        AuthenticatedCarrierSnapshot {
            live_count: state.live_count,
            ever_authenticated: state.ever_authenticated,
        }
    }
}

#[derive(Debug)]
pub(in crate::runtime) struct AuthenticatedCarrierRegistration {
    inventory: AuthenticatedCarrierInventory,
}

impl Drop for AuthenticatedCarrierRegistration {
    fn drop(&mut self) {
        let mut state = self
            .inventory
            .state
            .lock()
            .expect("authenticated carrier inventory lock");
        state.live_count = state
            .live_count
            .checked_sub(1)
            .expect("authenticated carrier registration dropped more than once");
    }
}

#[cfg(test)]
#[path = "tests_carrier_inventory.rs"]
mod tests;

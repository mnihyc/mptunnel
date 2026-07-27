//! Control-plane handle for one canonical configuration generation.
//!
//! Data-plane owners never receive this handle. The local management service
//! validates and persists a complete document through the configuration store,
//! then requests a clean generation replacement through this signal.

use crate::config::{CanonicalConfigStore, ConfigRevision};
#[cfg(test)]
use crate::runtime::readiness::RuntimeGenerationStopReason;
use crate::runtime::readiness::{
    RuntimeGenerationControl, RuntimeGenerationReadinessError, RuntimeGenerationStatus,
};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub(crate) struct RuntimeConfigControl {
    store: Arc<CanonicalConfigStore>,
    runtime_revision: Arc<Mutex<ConfigRevision>>,
    generation: RuntimeGenerationControl,
}

impl RuntimeConfigControl {
    pub(crate) fn new(store: Arc<CanonicalConfigStore>) -> Self {
        let runtime_revision = store.revision();
        Self {
            store,
            runtime_revision: Arc::new(Mutex::new(runtime_revision)),
            generation: RuntimeGenerationControl::new(),
        }
    }

    pub(crate) fn next_generation(&self) -> Self {
        Self {
            runtime_revision: Arc::new(Mutex::new(self.store.revision())),
            store: self.store.clone(),
            generation: RuntimeGenerationControl::new(),
        }
    }

    pub(crate) fn store(&self) -> &CanonicalConfigStore {
        &self.store
    }

    pub(crate) fn runtime_revision(&self) -> ConfigRevision {
        *self
            .runtime_revision
            .lock()
            .expect("runtime configuration revision lock")
    }

    pub(crate) fn publish_runtime_revision(&self, revision: ConfigRevision) {
        *self
            .runtime_revision
            .lock()
            .expect("runtime configuration revision lock") = revision;
    }

    pub(crate) fn generation(&self) -> RuntimeGenerationControl {
        self.generation.clone()
    }

    pub(crate) fn generation_status(&self) -> RuntimeGenerationStatus {
        self.generation.status()
    }

    pub(crate) async fn wait_until_ready(&self) -> Result<(), RuntimeGenerationReadinessError> {
        self.generation.wait_until_ready().await
    }

    pub(crate) fn is_ready(&self) -> bool {
        self.generation.is_ready()
    }

    pub(crate) fn mark_stopping(&self) {
        self.generation.mark_stopping();
    }

    pub(crate) fn request_reload(&self) {
        self.generation.request_reload();
    }

    pub(crate) fn request_shutdown(&self) {
        self.generation.request_shutdown();
    }

    pub(crate) fn defer_retirement(&self) {
        self.generation.defer_retirement();
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_stop(&self) -> RuntimeGenerationStopReason {
        self.generation.wait_for_stop().await
    }

    #[cfg(test)]
    pub(crate) async fn wait_for_reload(&self) {
        assert_eq!(
            self.wait_for_stop().await,
            RuntimeGenerationStopReason::ReloadRequested
        );
    }

    #[cfg(test)]
    pub(crate) fn signal_ready_for_test(&self) {
        self.generation.mark_ready();
    }
}

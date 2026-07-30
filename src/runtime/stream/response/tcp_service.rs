//! Validation-only response writer ordering and exact flight provenance.
//!
//! The observer is installed only for one frozen RFC TCP service validation.
//! Normal response flights and release records remain unchanged.

use super::ResponseStreamBinding;
use super::attachment::{ResponseStreamOutputEntry, ResponseStreamOutputs};
use crate::model::path::CarrierPathKey;
use crate::model::tcp_service::{
    TcpServiceAckRelease, TcpServiceCarrierFence, TcpServiceReleaseKind, TcpServiceStreamFence,
    TcpServiceWriterLifecycle, TcpServiceWriterPoint,
};
use crate::model::work::CarrierWorkKind;
use crate::protocol::frame::reliable_stream_frame_extent;
use crate::protocol::{Frame, OffsetRange, UnderlayProtocol};
use crate::runtime::tcp_service::{
    TcpServiceFlightSidecarError, TcpServiceObserverRemoval, TcpServicePreparedAck,
    TcpServiceWriterCoordinator, TcpServiceWriterObserver, TcpServiceWriterTransaction,
};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseTcpServiceOutputIdentity {
    pub(super) key: CarrierPathKey,
    pub(super) output_incarnation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseTcpServiceFlightIdentity {
    output: ResponseTcpServiceOutputIdentity,
    kind: CarrierWorkKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ResponseTcpServiceRecordedCommit {
    pub(super) stream: TcpServiceStreamFence,
    pub(super) carrier: TcpServiceCarrierFence,
    pub(super) range: OffsetRange,
    pub(super) committed_at: TcpServiceWriterPoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResponseTcpServiceCarrierBinding {
    fence: TcpServiceCarrierFence,
    output: Option<ResponseTcpServiceOutputIdentity>,
}

#[derive(Debug)]
pub(in crate::runtime) struct ResponseTcpServiceObserverInstall {
    pub(in crate::runtime) stream: TcpServiceStreamFence,
    pub(in crate::runtime) accepted: Vec<TcpServiceCarrierFence>,
    pub(in crate::runtime) candidate: TcpServiceCarrierFence,
    pub(in crate::runtime) coordinator: Arc<TcpServiceWriterCoordinator>,
    pub(in crate::runtime) max_flight_records: usize,
    pub(in crate::runtime) max_ack_release_records: usize,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct ResponseObservedPathRelease {
    pub(super) output: ResponseTcpServiceOutputIdentity,
    pub(super) range: OffsetRange,
    pub(super) kind: CarrierWorkKind,
    pub(super) unambiguous: bool,
}

#[derive(Debug)]
pub(super) enum ResponseTcpServiceCompletion {
    Ack(TcpServicePreparedAck),
    Failure(TcpServiceFlightSidecarError),
}

#[derive(Debug)]
pub(super) struct ResponseTcpServiceObserver {
    stream: TcpServiceStreamFence,
    carriers: Vec<ResponseTcpServiceCarrierBinding>,
    writer: TcpServiceWriterObserver<ResponseTcpServiceFlightIdentity>,
    coordinator: Arc<TcpServiceWriterCoordinator>,
    max_flight_records: usize,
    max_ack_release_records: usize,
    failure: Option<TcpServiceFlightSidecarError>,
    pending_ack: Option<TcpServicePreparedAck>,
}

impl ResponseTcpServiceObserver {
    fn new(
        outputs: &ResponseStreamOutputs,
        install: ResponseTcpServiceObserverInstall,
    ) -> Result<Self, TcpServiceFlightSidecarError> {
        if install.accepted.is_empty()
            || install.max_flight_records == 0
            || install.max_ack_release_records == 0
            || install.accepted.contains(&install.candidate)
            || install
                .accepted
                .iter()
                .enumerate()
                .any(|(index, carrier)| install.accepted[..index].contains(carrier))
        {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }

        let mut carriers = Vec::new();
        carriers
            .try_reserve(install.accepted.len().saturating_add(1))
            .map_err(|_| TcpServiceFlightSidecarError::ResourceLimit)?;
        for fence in &install.accepted {
            let output = exact_live_output(outputs, *fence)
                .ok_or(TcpServiceFlightSidecarError::InvalidRelease)?;
            if carriers
                .iter()
                .any(|binding: &ResponseTcpServiceCarrierBinding| binding.output == Some(output))
            {
                return Err(TcpServiceFlightSidecarError::InvalidRelease);
            }
            carriers.push(ResponseTcpServiceCarrierBinding {
                fence: *fence,
                output: Some(output),
            });
        }
        if exact_live_output(outputs, install.candidate).is_some() {
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        carriers.push(ResponseTcpServiceCarrierBinding {
            fence: install.candidate,
            output: None,
        });

        let lifecycle = install.coordinator.lifecycle();
        Ok(Self {
            stream: install.stream,
            carriers,
            writer: TcpServiceWriterObserver::new(lifecycle, install.max_flight_records)?,
            coordinator: install.coordinator,
            max_flight_records: install.max_flight_records,
            max_ack_release_records: install.max_ack_release_records,
            failure: None,
            pending_ack: None,
        })
    }

    fn lifecycle(&self) -> TcpServiceWriterLifecycle {
        self.writer.lifecycle()
    }

    fn same_install(
        &self,
        outputs: &ResponseStreamOutputs,
        install: &ResponseTcpServiceObserverInstall,
    ) -> bool {
        self.stream == install.stream
            && self.max_flight_records == install.max_flight_records
            && self.max_ack_release_records == install.max_ack_release_records
            && Arc::ptr_eq(&self.coordinator, &install.coordinator)
            && self.failure.is_none()
            && self.writer.is_observing()
            && self.carriers.len() == install.accepted.len().saturating_add(1)
            && self
                .carriers
                .iter()
                .take(install.accepted.len())
                .zip(&install.accepted)
                .all(|(binding, fence)| {
                    binding.fence == *fence && binding.output == exact_live_output(outputs, *fence)
                })
            && self.carriers.last().is_some_and(|binding| {
                binding.fence == install.candidate
                    && binding.output.is_none()
                    && exact_live_output(outputs, install.candidate).is_none()
            })
    }

    fn fail(&mut self, error: TcpServiceFlightSidecarError) {
        self.failure.get_or_insert(error);
        self.pending_ack = None;
        self.writer.stop(self.lifecycle());
    }

    fn observe_commit(
        &mut self,
        output: ResponseTcpServiceOutputIdentity,
        frame: &Frame,
        kind: CarrierWorkKind,
        transaction: &mut TcpServiceWriterTransaction<'_>,
    ) -> Result<Option<ResponseTcpServiceRecordedCommit>, TcpServiceFlightSidecarError> {
        if self.failure.is_some() || !kind.is_original_transmission() {
            return Ok(None);
        }
        let Some(binding) = self
            .carriers
            .iter()
            .find(|binding| binding.output == Some(output))
        else {
            return Ok(None);
        };
        let carrier = binding.fence;
        let Some((start, end, _)) = reliable_stream_frame_extent(frame) else {
            return Ok(None);
        };
        if transaction.lifecycle() != self.lifecycle() {
            let error = TcpServiceFlightSidecarError::InvalidRelease;
            self.fail(error);
            transaction.fail(error);
            return Err(error);
        }
        let range = OffsetRange { start, end };
        let committed_at = match self.writer.record_commit(
            ResponseTcpServiceFlightIdentity { output, kind },
            range,
            transaction,
        ) {
            Ok(point) => point,
            Err(error) => {
                self.fail(error);
                transaction.fail(error);
                return Err(error);
            }
        };
        Ok(Some(ResponseTcpServiceRecordedCommit {
            stream: self.stream,
            carrier,
            range,
            committed_at,
        }))
    }

    fn observe_ack(
        &mut self,
        releases: &[ResponseObservedPathRelease],
        assigned_end: u64,
        lifecycle: Option<TcpServiceWriterLifecycle>,
    ) {
        if self.failure.is_some() {
            return;
        }
        if lifecycle != Some(self.lifecycle()) || self.pending_ack.is_some() {
            self.fail(TcpServiceFlightSidecarError::InvalidRelease);
            return;
        }
        let mapped_release_count = releases
            .iter()
            .filter(|release| {
                self.carriers
                    .iter()
                    .any(|binding| binding.output == Some(release.output))
            })
            .count();
        if mapped_release_count > self.max_ack_release_records {
            self.fail(TcpServiceFlightSidecarError::ResourceLimit);
            return;
        }
        let mut observed = Vec::new();
        if observed.try_reserve(mapped_release_count).is_err() {
            self.fail(TcpServiceFlightSidecarError::ResourceLimit);
            return;
        }
        for release in releases {
            let Some(carrier) = self.carriers.iter().find_map(|binding| {
                (binding.output == Some(release.output)).then_some(binding.fence)
            }) else {
                continue;
            };
            let committed_at = match self.writer.release(
                ResponseTcpServiceFlightIdentity {
                    output: release.output,
                    kind: release.kind,
                },
                release.range,
            ) {
                Ok(point) => point,
                Err(error) => {
                    self.fail(error);
                    return;
                }
            };
            observed.push(TcpServiceAckRelease {
                carrier,
                range: release.range,
                committed_at,
                kind: if release.kind.is_original_transmission() {
                    TcpServiceReleaseKind::Original
                } else {
                    TcpServiceReleaseKind::Duplicate
                },
                unambiguous: release.unambiguous,
            });
        }
        if !observed.is_empty() {
            self.pending_ack = Some(TcpServicePreparedAck {
                lifecycle: self.lifecycle(),
                stream: self.stream,
                assigned_end,
                releases: observed,
            });
        }
    }

    fn observe_attachment(&mut self, entry: &ResponseStreamOutputEntry) {
        if self.failure.is_some() || entry.key.underlay != UnderlayProtocol::Tcp {
            return;
        }
        let output = ResponseTcpServiceOutputIdentity {
            key: entry.key,
            output_incarnation: entry.incarnation,
        };
        let Some(binding) = self.carriers.iter_mut().find(|binding| {
            binding.fence.local_instance_id == entry.path_instance_id
                && binding.fence.accepted.path_id == entry.key.path_id
        }) else {
            return;
        };
        match binding.output {
            None => binding.output = Some(output),
            Some(current) if current == output => {}
            Some(_) => self.fail(TcpServiceFlightSidecarError::InvalidRelease),
        }
    }

    fn observe_detach(&mut self, output: ResponseTcpServiceOutputIdentity) {
        if self
            .carriers
            .iter()
            .any(|binding| binding.output == Some(output))
        {
            self.fail(TcpServiceFlightSidecarError::InvalidRelease);
        }
    }

    fn observes_output(&self, output: ResponseTcpServiceOutputIdentity) -> bool {
        self.carriers
            .iter()
            .any(|binding| binding.output == Some(output))
    }

    fn take_completion(&mut self) -> Option<ResponseTcpServiceCompletion> {
        if let Some(error) = self.failure {
            self.pending_ack = None;
            return Some(ResponseTcpServiceCompletion::Failure(error));
        }
        self.pending_ack
            .take()
            .map(ResponseTcpServiceCompletion::Ack)
    }
}

fn exact_live_output(
    outputs: &ResponseStreamOutputs,
    fence: TcpServiceCarrierFence,
) -> Option<ResponseTcpServiceOutputIdentity> {
    outputs
        .entries
        .iter()
        .filter(|entry| {
            entry.key.underlay == UnderlayProtocol::Tcp
                && entry.key.path_id == fence.accepted.path_id
                && entry.path_instance_id == fence.local_instance_id
                && !entry.commands.is_closed()
        })
        .map(|entry| ResponseTcpServiceOutputIdentity {
            key: entry.key,
            output_incarnation: entry.incarnation,
        })
        .next()
}

impl ResponseStreamOutputs {
    pub(super) fn install_tcp_service_observer(
        &mut self,
        install: ResponseTcpServiceObserverInstall,
    ) -> Result<bool, TcpServiceFlightSidecarError> {
        if let Some(current) = self.tcp_service.as_ref() {
            if current.lifecycle() == install.coordinator.lifecycle()
                && current.same_install(self, &install)
            {
                return Ok(false);
            }
            return Err(TcpServiceFlightSidecarError::InvalidRelease);
        }
        self.tcp_service = Some(Box::new(ResponseTcpServiceObserver::new(self, install)?));
        Ok(true)
    }

    pub(super) fn remove_tcp_service_observer(
        &mut self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> TcpServiceObserverRemoval {
        let Some(observer) = self.tcp_service.as_ref() else {
            return TcpServiceObserverRemoval::AlreadyAbsent;
        };
        if observer.lifecycle() != lifecycle {
            return TcpServiceObserverRemoval::DifferentLifecycle;
        }
        self.tcp_service = None;
        TcpServiceObserverRemoval::Removed
    }

    pub(super) fn observe_tcp_service_commit(
        &mut self,
        target_index: usize,
        frame: &Frame,
        transaction: Option<&mut TcpServiceWriterTransaction<'_>>,
    ) -> Result<Option<ResponseTcpServiceRecordedCommit>, TcpServiceFlightSidecarError> {
        let Some(observer) = self.tcp_service.as_mut() else {
            return Ok(None);
        };
        let Some(transaction) = transaction else {
            let error = TcpServiceFlightSidecarError::InvalidRelease;
            observer.fail(error);
            return Err(error);
        };
        let Some(entry) = self.entries.get(target_index) else {
            let error = TcpServiceFlightSidecarError::InvalidRelease;
            observer.fail(error);
            transaction.fail(error);
            return Err(error);
        };
        observer.observe_commit(
            ResponseTcpServiceOutputIdentity {
                key: entry.key,
                output_incarnation: entry.incarnation,
            },
            frame,
            CarrierWorkKind::OriginalData,
            transaction,
        )
    }

    pub(super) fn observe_tcp_service_ack(
        &mut self,
        releases: &[ResponseObservedPathRelease],
        assigned_end: u64,
        lifecycle: Option<TcpServiceWriterLifecycle>,
    ) {
        if let Some(observer) = self.tcp_service.as_mut() {
            observer.observe_ack(releases, assigned_end, lifecycle);
        }
    }

    pub(super) fn tcp_service_ack_release_limit(&self) -> Option<usize> {
        self.tcp_service
            .as_ref()
            .map(|observer| observer.max_ack_release_records)
    }

    pub(super) fn tcp_service_observes_output(
        &self,
        output: ResponseTcpServiceOutputIdentity,
    ) -> bool {
        self.tcp_service
            .as_ref()
            .is_some_and(|observer| observer.observes_output(output))
    }

    pub(super) fn fail_tcp_service_observer(&mut self, error: TcpServiceFlightSidecarError) {
        if let Some(observer) = self.tcp_service.as_mut() {
            observer.fail(error);
        }
    }

    pub(super) fn take_tcp_service_completion(
        &mut self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> Option<ResponseTcpServiceCompletion> {
        let observer = self.tcp_service.as_mut()?;
        if observer.lifecycle() != lifecycle {
            return Some(ResponseTcpServiceCompletion::Failure(
                TcpServiceFlightSidecarError::InvalidRelease,
            ));
        }
        observer.take_completion()
    }

    pub(super) fn observe_tcp_service_attachment(&mut self, entry_index: usize) {
        let Some(observer) = self.tcp_service.as_mut() else {
            return;
        };
        let Some(entry) = self.entries.get(entry_index) else {
            observer.fail(TcpServiceFlightSidecarError::InvalidRelease);
            return;
        };
        observer.observe_attachment(entry);
    }

    pub(super) fn observe_tcp_service_detach(
        &mut self,
        key: CarrierPathKey,
        output_incarnation: u64,
    ) {
        if let Some(observer) = self.tcp_service.as_mut() {
            observer.observe_detach(ResponseTcpServiceOutputIdentity {
                key,
                output_incarnation,
            });
        }
    }

    pub(super) fn stop_tcp_service_observer(&mut self) {
        if let Some(observer) = self.tcp_service.as_mut() {
            observer.fail(TcpServiceFlightSidecarError::ObserverStopped);
        }
    }
}

impl ResponseStreamBinding {
    pub(in crate::runtime) fn install_tcp_service_observer(
        &self,
        install: ResponseTcpServiceObserverInstall,
    ) -> Result<bool, TcpServiceFlightSidecarError> {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .install_tcp_service_observer(install)
    }

    pub(in crate::runtime) fn remove_tcp_service_observer(
        &self,
        lifecycle: TcpServiceWriterLifecycle,
    ) -> TcpServiceObserverRemoval {
        self.outputs
            .lock()
            .expect("server reliable stream binding lock")
            .remove_tcp_service_observer(lifecycle)
    }

    pub(in crate::runtime) fn finish_tcp_service_ack(
        &self,
        transaction: &mut TcpServiceWriterTransaction<'_>,
    ) {
        let completion = self
            .outputs
            .lock()
            .expect("server reliable stream binding lock")
            .take_tcp_service_completion(transaction.lifecycle());
        match completion {
            Some(ResponseTcpServiceCompletion::Ack(ack)) => {
                if let Err(error) = transaction.commit_ack(ack) {
                    self.outputs
                        .lock()
                        .expect("server reliable stream binding lock")
                        .fail_tcp_service_observer(error);
                }
            }
            Some(ResponseTcpServiceCompletion::Failure(error)) => {
                transaction.fail(error);
            }
            None => {}
        }
    }
}

//! Serialized output primitives for a server TCP carrier.
//!
//! One owner preserves frame order, proof timestamps, batching, and the
//! request-capacity receipts on TCP's ordered byte stream. The session actor
//! retains command and lifecycle orchestration.

use super::super::io::{EncryptedTcpWriter, encrypted_framed_peer_closed};
use super::evidence::ServerTcpEvidenceState;
use crate::protocol::Frame;
use crate::runtime::error::RuntimeError;
use crate::transport::encrypted::EncryptedFramedTransportError;
use crate::transport::tcp_write_admission::TcpWriteAdmission;

pub(in crate::runtime::path::tcp) struct ServerTcpWriter {
    pending_frames: Vec<Frame>,
    framed: EncryptedTcpWriter,
    write_admission: Option<TcpWriteAdmission>,
    #[cfg(test)]
    original_handoff_permission: Option<bool>,
}

impl ServerTcpWriter {
    #[cfg(feature = "lab-diagnostics")]
    pub(super) fn lab_wire_bytes_written(&self) -> u64 {
        self.framed.wire_bytes_written()
    }

    pub(in crate::runtime::path::tcp) fn new(framed: EncryptedTcpWriter) -> Self {
        Self {
            pending_frames: Vec::new(),
            framed,
            write_admission: None,
            #[cfg(test)]
            original_handoff_permission: None,
        }
    }

    pub(super) fn set_write_admission(&mut self, admission: Option<TcpWriteAdmission>) {
        self.write_admission = admission;
    }

    /// Unsupported adapters preserve structural-only writer readiness; this
    /// fallback is not a measured native capacity observation.
    pub(super) fn allows_original_handoff(&self) -> Result<bool, RuntimeError> {
        #[cfg(test)]
        if let Some(permission) = self.original_handoff_permission {
            return Ok(permission);
        }
        self.write_admission
            .as_ref()
            .map_or(Ok(true), TcpWriteAdmission::is_ready)
            .map_err(|error| RuntimeError::Encrypted(EncryptedFramedTransportError::Io(error)))
    }

    /// Model permission only: native socket negative/wake behavior is tested
    /// separately by the actual socket adapter, never by invented telemetry.
    #[cfg(test)]
    pub(super) fn set_original_handoff_permission_for_test(&mut self, permission: bool) {
        self.original_handoff_permission = Some(permission);
    }

    pub(super) async fn native_writable(&self) -> Result<(), RuntimeError> {
        match &self.write_admission {
            Some(admission) => admission
                .writable()
                .await
                .map_err(|error| RuntimeError::Encrypted(EncryptedFramedTransportError::Io(error))),
            None => std::future::pending().await,
        }
    }

    pub(in crate::runtime::path::tcp) fn begin_transaction(&self) -> Result<(), RuntimeError> {
        if self.pending_frames.is_empty() {
            Ok(())
        } else {
            Err(RuntimeError::Protocol(
                "server TCP writer cannot replace an uncommitted transaction",
            ))
        }
    }

    /// Freezes exactly one already-arbitrated ordinary frame into the current
    /// transaction. A second head must return through actor arbitration.
    pub(in crate::runtime::path::tcp) fn stage_transaction_frame(
        &mut self,
        frame: Frame,
    ) -> Result<(), RuntimeError> {
        if !self.pending_frames.is_empty() {
            return Err(RuntimeError::Protocol(
                "server TCP transaction received more than one ordinary frame",
            ));
        }
        self.pending_frames.push(frame);
        Ok(())
    }

    /// Flushes one structural transaction and only then publishes sent-frame
    /// evidence. An ambiguous write or flush leaves the ledger intact for the
    /// terminal actor drop; it must never be retried on this carrier.
    pub(in crate::runtime::path::tcp) async fn commit_transaction(
        &mut self,
        evidence: &mut ServerTcpEvidenceState,
    ) -> Result<bool, RuntimeError> {
        if self.pending_frames.is_empty() {
            return Ok(true);
        }
        #[cfg(feature = "lab-diagnostics")]
        let lab_wire_start = self
            .pending_frames
            .first()
            .filter(|frame| {
                self.pending_frames.len() == 1 && evidence.lab_can_track_wire_frame(frame)
            })
            .map(|_| (self.framed.wire_bytes_written(), std::time::Instant::now()));
        let commit = async {
            self.framed.write_frames(&self.pending_frames).await?;
            self.framed.flush().await
        }
        .await;
        match commit {
            Ok(()) => {
                #[cfg(feature = "lab-diagnostics")]
                if let Some((wire_start, started)) = lab_wire_start {
                    evidence.lab_track_wire_frame(
                        &self.pending_frames[0],
                        wire_start,
                        self.framed.wire_bytes_written(),
                        started,
                    );
                }
                for frame in &self.pending_frames {
                    evidence.record_sent_frame(frame);
                }
                self.pending_frames.clear();
                Ok(true)
            }
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    }

    pub(in crate::runtime::path::tcp) async fn write_frame(
        &mut self,
        frame: &Frame,
    ) -> Result<bool, RuntimeError> {
        if !self.write_frame_unflushed(frame).await? {
            return Ok(false);
        }
        self.flush().await
    }

    pub(in crate::runtime::path::tcp) async fn write_frame_unflushed(
        &mut self,
        frame: &Frame,
    ) -> Result<bool, RuntimeError> {
        match self.framed.write_frame(frame).await {
            Ok(()) => Ok(true),
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    }

    pub(in crate::runtime::path::tcp) async fn flush(&mut self) -> Result<bool, RuntimeError> {
        match self.framed.flush().await {
            Ok(()) => Ok(true),
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    }
}

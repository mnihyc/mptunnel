//! Serialized output primitives for a server TCP carrier.
//!
//! One owner preserves frame order, proof timestamps, batching, and the
//! request-capacity receipts on TCP's ordered byte stream. The session actor
//! retains command and lifecycle orchestration.

use super::io::{EncryptedTcpWriter, encrypted_framed_peer_closed};
use super::server_evidence::ServerTcpEvidenceState;
use crate::protocol::Frame;
use crate::runtime::error::RuntimeError;

pub(super) struct ServerTcpWriter {
    pending_frames: Vec<Frame>,
    framed: EncryptedTcpWriter,
}

impl ServerTcpWriter {
    pub(super) fn new(framed: EncryptedTcpWriter) -> Self {
        Self {
            pending_frames: Vec::new(),
            framed,
        }
    }

    pub(super) fn clear_batch(&mut self) {
        self.pending_frames.clear();
    }

    pub(super) fn push_frame(&mut self, frame: Frame) {
        self.pending_frames.push(frame);
    }

    pub(super) async fn write_batch(
        &mut self,
        evidence: &mut ServerTcpEvidenceState,
    ) -> Result<bool, RuntimeError> {
        if self.pending_frames.is_empty() {
            return Ok(true);
        }
        match self.framed.write_frames(&self.pending_frames).await {
            Ok(()) => {
                for frame in &self.pending_frames {
                    evidence.record_sent_frame(frame);
                }
                self.pending_frames.clear();
                Ok(true)
            }
            Err(err) if encrypted_framed_peer_closed(&err) => {
                self.pending_frames.clear();
                Ok(false)
            }
            Err(err) => {
                self.pending_frames.clear();
                Err(RuntimeError::Encrypted(err))
            }
        }
    }

    pub(super) async fn write_frame(&mut self, frame: &Frame) -> Result<bool, RuntimeError> {
        if !self.write_frame_unflushed(frame).await? {
            return Ok(false);
        }
        self.flush().await
    }

    pub(super) async fn write_frame_unflushed(
        &mut self,
        frame: &Frame,
    ) -> Result<bool, RuntimeError> {
        match self.framed.write_frame(frame).await {
            Ok(()) => Ok(true),
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    }

    pub(super) async fn flush(&mut self) -> Result<bool, RuntimeError> {
        match self.framed.flush().await {
            Ok(()) => Ok(true),
            Err(err) if encrypted_framed_peer_closed(&err) => Ok(false),
            Err(err) => Err(RuntimeError::Encrypted(err)),
        }
    }
}

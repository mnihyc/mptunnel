//! Serialized output primitives for a server TCP carrier.
//!
//! One owner preserves frame order, proof timestamps, batching, and the
//! exclusive typed-capacity write on TCP's ordered byte stream. The session
//! actor retains command and lifecycle orchestration.

use super::io::{EncryptedTcpWriter, encrypted_framed_peer_closed};
use super::server_evidence::ServerTcpEvidenceState;
use crate::protocol::Frame;
use crate::runtime::error::RuntimeError;
use crate::runtime::path::commands::TcpCapacityProbeCommand;
use bytes::Bytes;

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

    async fn write_frame_unflushed(&mut self, frame: &Frame) -> Result<bool, RuntimeError> {
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

    pub(super) async fn write_capacity_probe(
        &mut self,
        probe: &TcpCapacityProbeCommand,
        max_payload_bytes: usize,
    ) -> Result<bool, RuntimeError> {
        let frame_payload_bytes = max_payload_bytes.max(1) as u64;
        let mut remaining = probe.train_payload_bytes;
        while remaining > 0 {
            let payload_bytes = remaining.min(frame_payload_bytes) as usize;
            if !self
                .write_frame_unflushed(&Frame::PathCapacityData {
                    path_id: probe.path_id,
                    calibration_id: probe.calibration_id,
                    payload: Bytes::from(vec![0u8; payload_bytes]),
                })
                .await?
            {
                return Ok(false);
            }
            remaining = remaining.saturating_sub(payload_bytes as u64);
        }
        self.write_frame(&Frame::PathCapacityFinish {
            path_id: probe.path_id,
            calibration_id: probe.calibration_id,
            payload_bytes: probe.train_payload_bytes,
        })
        .await
    }
}

//! Exclusive record serialization for one QUIC measurement epoch.

use super::{
    InstrumentedController, MeasurementSpec, QuicCarrierError, QuicCarrierTelemetry,
    QuicWriteTransaction, SendStream, encode_quic_length_prefixed_frame,
    quic_encoded_frame_capacity_hint,
};
#[cfg(feature = "lab-diagnostics")]
use crate::lab_diagnostics::lab_perf_record;
use crate::protocol::codec::CodecLimits;
use crate::protocol::{Frame, FrameWriteClass};
use quinn::VarInt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
#[cfg(feature = "lab-diagnostics")]
use std::time::Duration;
use std::time::Instant;

/// Owns the connection-wide writer gate for one measurement transaction.
///
/// Runtime chooses the protocol records; their protocol-owned write class
/// supplies measured bytes while transport preserves exclusivity and ACK
/// attribution.
pub struct MeasurementEpoch<'a> {
    send: &'a mut SendStream,
    spec: MeasurementSpec,
    start_guard: MeasurementStartGuard,
    #[cfg(feature = "lab-diagnostics")]
    write_started: Instant,
    #[cfg(feature = "lab-diagnostics")]
    encode_elapsed: Duration,
    #[cfg(feature = "lab-diagnostics")]
    encoded_bytes: u64,
}

pub async fn begin_measurement(
    send: &mut SendStream,
    spec: MeasurementSpec,
) -> Result<MeasurementEpoch<'_>, QuicCarrierError> {
    verify_controller_ownership(send)?;
    ensure_measurement_writable(send)?;
    if spec.token == 0 || spec.train_payload_bytes == 0 {
        return Err(QuicCarrierError::InvalidMeasurement);
    }

    let reservation = send
        .telemetry
        .reserve_measurement_token(spec.token, spec.expires_at)
        .await?;
    send.telemetry
        .install_measurement(spec, send.write_backlog.load(Ordering::Acquire))?;
    reservation.commit();
    let start_guard = MeasurementStartGuard {
        telemetry: send.telemetry.clone(),
        token: spec.token,
        keep_epoch: false,
    };
    spawn_measurement_expiry(send, spec);

    if !send
        .telemetry
        .mark_measurement_write_started(spec.token, Instant::now())
    {
        return Err(QuicCarrierError::MeasurementExpired);
    }

    Ok(MeasurementEpoch {
        send,
        spec,
        start_guard,
        #[cfg(feature = "lab-diagnostics")]
        write_started: Instant::now(),
        #[cfg(feature = "lab-diagnostics")]
        encode_elapsed: Duration::ZERO,
        #[cfg(feature = "lab-diagnostics")]
        encoded_bytes: 0,
    })
}

impl MeasurementEpoch<'_> {
    pub async fn write_data(
        &mut self,
        frame: &Frame,
        limits: CodecLimits,
    ) -> Result<(), QuicCarrierError> {
        let FrameWriteClass::MeasurementData { payload_bytes } = frame.write_class() else {
            return Err(QuicCarrierError::MeasurementRecordRequiresDedicatedWrite);
        };
        if payload_bytes == 0 {
            return Err(QuicCarrierError::InvalidMeasurement);
        }
        self.write_record(frame, limits).await?;
        if !self
            .send
            .telemetry
            .record_measurement_data_written(self.spec.token, payload_bytes)
        {
            return Err(QuicCarrierError::MeasurementExpired);
        }
        Ok(())
    }

    pub async fn finish(
        mut self,
        frame: &Frame,
        limits: CodecLimits,
    ) -> Result<(), QuicCarrierError> {
        if frame.write_class() != FrameWriteClass::MeasurementFinish {
            return Err(QuicCarrierError::MeasurementRecordRequiresDedicatedWrite);
        }
        self.write_record(frame, limits).await?;
        if !self
            .send
            .telemetry
            .commit_measurement_write(self.spec.token, Instant::now())
        {
            return Err(QuicCarrierError::MeasurementExpired);
        }
        self.start_guard.keep_epoch = true;
        #[cfg(feature = "lab-diagnostics")]
        {
            lab_perf_record(
                "transport.quic.encode_measurement",
                self.encode_elapsed,
                usize::try_from(self.encoded_bytes).unwrap_or(usize::MAX),
            );
            lab_perf_record(
                "transport.quic.write_measurement_wait",
                self.write_started.elapsed(),
                usize::try_from(self.encoded_bytes).unwrap_or(usize::MAX),
            );
        }
        Ok(())
    }

    async fn write_record(
        &mut self,
        frame: &Frame,
        limits: CodecLimits,
    ) -> Result<(), QuicCarrierError> {
        #[cfg(feature = "lab-diagnostics")]
        let encode_started = Instant::now();
        let record_bytes = encode_measurement_record(self.send, frame, limits)?;
        #[cfg(feature = "lab-diagnostics")]
        {
            self.encode_elapsed = self.encode_elapsed.saturating_add(encode_started.elapsed());
            self.encoded_bytes = self.encoded_bytes.saturating_add(record_bytes);
        }
        write_measurement_record(self.send, record_bytes).await
    }
}

pub async fn write_measurement_control(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<(), QuicCarrierError> {
    if frame.write_class() != FrameWriteClass::MeasurementControl {
        return Err(QuicCarrierError::MeasurementRecordRequiresDedicatedWrite);
    }
    ensure_measurement_writable(send)?;
    let record_bytes = encode_measurement_record(send, frame, limits)?;
    write_measurement_record(send, record_bytes).await
}

fn verify_controller_ownership(send: &SendStream) -> Result<(), QuicCarrierError> {
    let current_telemetry = send
        .connection
        .congestion_state()
        .into_any()
        .downcast::<InstrumentedController>()
        .expect("QUIC carrier must use the instrumented congestion controller")
        .telemetry
        .clone();
    if Arc::ptr_eq(&current_telemetry, &send.telemetry) {
        return Ok(());
    }
    send.connection.close(
        VarInt::from_u32(1),
        b"QUIC congestion controller ownership changed",
    );
    Err(QuicCarrierError::MeasurementExpired)
}

fn ensure_measurement_writable(send: &SendStream) -> Result<(), QuicCarrierError> {
    if !send.telemetry.measurement_failed_closed() {
        return Ok(());
    }
    send.connection
        .close(VarInt::from_u32(1), b"measurement epoch failed closed");
    Err(QuicCarrierError::MeasurementExpired)
}

fn spawn_measurement_expiry(send: &SendStream, spec: MeasurementSpec) {
    let expiry_telemetry = send.telemetry.clone();
    let expiry_connection = send.connection.clone();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(spec.expires_at)) => {
                if expiry_telemetry.expire_measurement(spec.token, Instant::now()) {
                    expiry_connection.close(VarInt::from_u32(1), b"measurement epoch expired");
                }
            }
            _ = expiry_telemetry.wait_for_measurement_fail_close() => {
                expiry_connection.close(VarInt::from_u32(1), b"measurement epoch failed closed");
            }
            _ = expiry_connection.closed() => {}
        }
    });
}

fn encode_measurement_record(
    send: &mut SendStream,
    frame: &Frame,
    limits: CodecLimits,
) -> Result<u64, QuicCarrierError> {
    let packet = &mut send.encode_buffer;
    packet.clear();
    packet.reserve(quic_encoded_frame_capacity_hint(frame));
    encode_quic_length_prefixed_frame(frame, limits, packet)?;
    Ok(packet.len() as u64)
}

async fn write_measurement_record(
    send: &mut SendStream,
    packet_len: u64,
) -> Result<(), QuicCarrierError> {
    let transaction_connection = send.connection.clone();
    let transaction_backlog = send.write_backlog.clone();
    send.write_backlog.fetch_add(packet_len, Ordering::Relaxed);
    let write_transaction =
        QuicWriteTransaction::new(transaction_connection, transaction_backlog, packet_len);
    send.stream.write_all(&send.encode_buffer).await?;
    write_transaction.commit();
    Ok(())
}

struct MeasurementStartGuard {
    telemetry: Arc<QuicCarrierTelemetry>,
    token: u64,
    keep_epoch: bool,
}

impl Drop for MeasurementStartGuard {
    fn drop(&mut self) {
        if !self.keep_epoch {
            self.telemetry.abort_measurement(self.token);
        }
    }
}

#[cfg(test)]
#[path = "measurement_writer_test.rs"]
mod tests;

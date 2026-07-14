//! QUIC path capacity-probe framing and receipt validation.
//!
//! MPTUN proof geometry remains above the generic QUIC transport; this module
//! is the only runtime owner that translates capacity commands into carrier
//! measurement epochs.

use super::io::UdpPathSendStream;
use super::*;
use crate::model::path::CarrierPathInstanceId;

const QUIC_CAPACITY_RECORD_PAYLOAD_BYTES: usize = 64 * 1024;

pub(super) async fn udp_path_write_capacity_probe(
    send: &mut UdpPathSendStream,
    probe: &QuicCapacityProbeCommand,
    codec_limits: CodecLimits,
    mux_limits: MuxLimits,
) -> Result<(), RuntimeError> {
    // Only this entry point can turn a carrier-neutral command into a QUIC ACK
    // epoch; ordinary frame batching must never absorb capacity payloads.
    let chunk_bytes = mux_limits
        .max_payload_bytes
        .min(codec_limits.max_payload_bytes.max(1))
        .min(QUIC_CAPACITY_RECORD_PAYLOAD_BYTES);
    let train_payload_bytes = usize::try_from(probe.train_payload_bytes).map_err(|_| {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::InvalidMeasurement)
    })?;
    if chunk_bytes == 0 || train_payload_bytes == 0 {
        return Err(RuntimeError::QuicCarrier(
            quic_transport::QuicCarrierError::InvalidMeasurement,
        ));
    }

    let mut epoch = quic_transport::begin_measurement(
        send.transport_stream_mut(),
        quic_transport::MeasurementSpec {
            token: probe.calibration_id,
            train_payload_bytes: probe.train_payload_bytes,
            sample_floor_bytes: probe.sample_floor_bytes,
            warmup_carrier_bytes: probe.warmup_carrier_bytes,
            required_timed_carrier_bytes: probe.required_timed_carrier_bytes,
            retention: probe.proof_validity,
            expires_at: probe.expires_at,
        },
    )
    .await?;
    let zero_block = bytes::Bytes::from(vec![0_u8; chunk_bytes.min(train_payload_bytes)]);
    let mut remaining = train_payload_bytes;
    while remaining > 0 {
        let payload_bytes = remaining.min(zero_block.len());
        epoch
            .write_data(
                &Frame::PathCapacityData {
                    path_id: probe.path_id,
                    calibration_id: probe.calibration_id,
                    payload: zero_block.slice(..payload_bytes),
                },
                codec_limits,
            )
            .await?;
        remaining -= payload_bytes;
    }
    epoch
        .finish(
            &Frame::PathCapacityFinish {
                path_id: probe.path_id,
                calibration_id: probe.calibration_id,
                payload_bytes: probe.train_payload_bytes,
            },
            codec_limits,
        )
        .await?;
    Ok(())
}

pub(super) async fn udp_path_write_capacity_receipt(
    send: &mut UdpPathSendStream,
    path_id: PathId,
    calibration_id: u64,
    received_payload_bytes: u64,
    codec_limits: CodecLimits,
) -> Result<(), RuntimeError> {
    // Let QUIC emit the delayed terminal ACK before the application receipt.
    // Otherwise the receipt may overtake transport telemetry and make identical
    // probes alternate between native rate and cold-start average.
    tokio::time::sleep(QUIC_MAX_ACK_DELAY.saturating_add(QUIC_TIMER_GRANULARITY)).await;
    quic_transport::write_measurement_control(
        send.transport_stream_mut(),
        &Frame::PathCapacityReceipt {
            path_id,
            calibration_id,
            received_payload_bytes,
        },
        codec_limits,
    )
    .await?;
    Ok(())
}

pub(super) fn quic_capacity_command_drop_reason(
    probe: &QuicCapacityProbeCommand,
    now: Instant,
) -> Option<&'static str> {
    if !probe.ticket.is_current() {
        Some("ownership_invalidated")
    } else if now >= probe.expires_at {
        Some("deadline_elapsed_before_start")
    } else {
        None
    }
}

pub(super) fn quic_capacity_start_rejection_reason(err: &RuntimeError) -> Option<&'static str> {
    match err {
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::InvalidMeasurement) => {
            Some("invalid_specification")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementBusy) => {
            Some("carrier_epoch_busy")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementNotIdle) => {
            Some("carrier_not_idle")
        }
        RuntimeError::QuicCarrier(quic_transport::QuicCarrierError::MeasurementExpired) => {
            Some("carrier_deadline_elapsed")
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn confirm_server_quic_capacity_receipt(
    send: &UdpPathSendStream,
    _session_id: SessionId,
    path_id: PathId,
    _path_instance_id: CarrierPathInstanceId,
    _stream_id: StreamId,
    receipt_path_id: PathId,
    calibration_id: u64,
    received_payload_bytes: u64,
) -> Result<(), RuntimeError> {
    if receipt_path_id != path_id
        || calibration_id == 0
        || received_payload_bytes == 0
        || !send.connection.confirm_capacity_probe_receipt(
            calibration_id,
            received_payload_bytes,
            Instant::now(),
        )
    {
        return Err(RuntimeError::Protocol("invalid QUIC capacity receipt"));
    }
    #[cfg(feature = "lab-diagnostics")]
    lab_diagnostic(
        "quic_capacity_receipt",
        format_args!(
            "role=server phase=confirmed session_id={} path_id={} path_instance_id={} stream_id={} calibration_id={} received_payload_bytes={}",
            _session_id.0,
            path_id.0,
            _path_instance_id.as_u64(),
            _stream_id.0,
            calibration_id,
            received_payload_bytes,
        ),
    );
    Ok(())
}

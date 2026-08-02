use super::*;

#[test]
fn quic_path_lifetime_failures_are_migratable_but_protocol_shape_errors_are_not() {
    assert!(reliable_path_error_is_migratable(
        &RuntimeError::QuicCarrier(QuicCarrierError::H3DriverClosed)
    ));
    assert!(reliable_path_error_is_migratable(
        &RuntimeError::QuicCarrier(QuicCarrierError::UnexpectedEnd)
    ));
    assert!(!reliable_path_error_is_migratable(
        &RuntimeError::QuicCarrier(QuicCarrierError::FrameTooLarge)
    ));
    assert!(!reliable_path_error_is_migratable(
        &RuntimeError::QuicCarrier(QuicCarrierError::H3Role("invalid carrier role"))
    ));
    assert!(!reliable_path_error_is_migratable(
        &RuntimeError::ReliablePathAttachmentRefused
    ));
}

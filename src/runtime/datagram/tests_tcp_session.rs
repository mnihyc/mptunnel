use super::received_tcp_datagram_expires_at;
use std::time::Duration;

#[test]
fn attachment_queue_delay_does_not_renew_datagram_ttl() {
    let received_at = tokio::time::Instant::now() - Duration::from_millis(10);

    assert_eq!(received_tcp_datagram_expires_at(received_at, 5), None);
}

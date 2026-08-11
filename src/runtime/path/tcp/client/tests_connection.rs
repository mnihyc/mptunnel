use super::connection::heartbeat_renewal_delay;
use std::time::Duration;

#[test]
fn heartbeat_renewal_preserves_the_rfc_bounds_and_distribution() {
    let maximum = Duration::from_secs(10);
    let minimum = Duration::from_secs(8);
    assert_eq!(heartbeat_renewal_delay(maximum, 0), minimum);
    assert_eq!(heartbeat_renewal_delay(maximum, u64::MAX), maximum);

    let samples = 65_536_u64;
    let mut total_nanos = 0_u128;
    let mut previous = Duration::ZERO;
    for index in 0..samples {
        let sample = index.saturating_mul(u64::MAX / (samples - 1));
        let delay = heartbeat_renewal_delay(maximum, sample);
        assert!((minimum..=maximum).contains(&delay));
        assert!(delay >= previous);
        previous = delay;
        total_nanos += delay.as_nanos();
    }
    let mean = Duration::from_nanos((total_nanos / u128::from(samples)) as u64);
    assert!(mean.abs_diff(Duration::from_secs(9)) < Duration::from_millis(1));
}

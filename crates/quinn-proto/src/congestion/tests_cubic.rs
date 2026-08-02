use super::*;

#[test]
fn fast_convergence_reduces_w_max_without_double_reducing_window() {
    let now = Instant::now();
    let config = Arc::new(CubicConfig::default());
    let mut cubic = Cubic::new(config, now, BASE_DATAGRAM_SIZE as u16);
    let window = 8 * BASE_DATAGRAM_SIZE;

    cubic.window = window;
    cubic.ssthresh = window;
    cubic.cubic_state.w_max = 12.0 * BASE_DATAGRAM_SIZE as f64;

    cubic.on_congestion_event(now, now + Duration::from_millis(1), false, 0);

    assert_eq!(
        cubic.cubic_state.w_max,
        window as f64 * (1.0 + BETA_CUBIC) / 2.0
    );
    assert_eq!(cubic.ssthresh, (window as f64 * BETA_CUBIC) as u64);
    assert_eq!(cubic.window, cubic.ssthresh);
}

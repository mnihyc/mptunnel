// Non-acceptance diagnostic. Insert inside the existing BBR3 tests module,
// beside Sim. It asserts the observed defect, not desired production behavior.
// Run the release-mode test by its diagnosis_deep_queue name, then remove it.
    /// Diagnosis only: a lossless deep queue with one directional service-rate
    /// drop. Uses the existing always-backlogged simulator and real callbacks.
    #[test]
    fn diagnosis_deep_queue_rate_drop_retains_capacity_model() {
        for operational_rtt_enabled in [true, false] {
            diagnosis_deep_queue_rate_drop(operational_rtt_enabled);
        }
    }

    fn diagnosis_deep_queue_rate_drop(operational_rtt_enabled: bool) {
        let config = Bbr3Config {
            probe_rng_seed: Some([1; 16]),
            ..Bbr3Config::default()
        };
        let mut sim = Sim::new(config, 1200, 50_000_000.0, 80_000_000);
        sim.fwd_ns = 70_000_000;
        sim.ret_ns = 10_000_000;
        let mut next_sample = 0;
        for (until_ns, service_ns) in [(20_000_000_000, 24_000), (80_000_000_000, 960_000)] {
            sim.btl_service_ns = service_ns;
            sim.run(
                5_000_000,
                |_| ControlFlow::Continue(()),
                |bbr, now_ns, inflight, _| {
                    if now_ns >= next_sample {
                        let rs = bbr.rs.unwrap();
                        eprintln!(
                            "operational={} t={:.1} state={:?} raw_mbps={:.3} model_mbps={:.3} max_mbps={:.3} min_rtt_ms={:.1} operational_rtt={:?} cwnd={} flight={} cycle={} pending={:?} app={} round={}",
                            operational_rtt_enabled,
                            now_ns as f64 / 1e9,
                            bbr.state,
                            rs.delivery_rate * 8.0 / 1e6,
                            bbr.bw * 8.0 / 1e6,
                            bbr.max_bw * 8.0 / 1e6,
                            bbr.min_rtt.as_secs_f64() * 1e3,
                            bbr.inflight_rtt,
                            bbr.cwnd,
                            inflight,
                            bbr.cycle_count,
                            bbr.max_bw_advance_pending,
                            rs.is_app_limited,
                            bbr.round_count,
                        );
                        next_sample = now_ns + 5_000_000_000;
                    }
                    if !operational_rtt_enabled {
                        // Attribution-only ablation: retain raw propagation-RTT
                        // flight sizing. No native ACK/rate/state event is removed.
                        bbr.inflight_rtt = Duration::from_secs(u64::MAX);
                        bbr.inflight_rtt_filter.clear();
                    }
                    if now_ns >= until_ns {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                },
            );
        }
        assert!((sim.bbr.rs.unwrap().delivery_rate * 8.0 / 1e6 - 10.0).abs() < 0.1);
        assert!(sim.bbr.bw * 8.0 / 1e6 > 300.0);
        assert!(sim.bbr.min_rtt > Duration::from_secs(1));
    }

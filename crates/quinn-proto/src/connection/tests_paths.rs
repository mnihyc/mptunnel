use super::*;
use crate::congestion::{Controller, ControllerFactory};
use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Clone)]
struct EpochController {
    lineage: u64,
    epoch: u64,
    supports_fresh_path: bool,
}

impl Controller for EpochController {
    fn on_congestion_event(
        &mut self,
        _now: Instant,
        _sent: Instant,
        _is_persistent_congestion: bool,
        _lost_bytes: u64,
    ) {
    }

    fn on_mtu_update(&mut self, _new_mtu: u16) {}

    fn window(&self) -> u64 {
        12_000
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn fresh_path_box(&self, _now: Instant, _current_mtu: u16) -> Option<Box<dyn Controller>> {
        self.supports_fresh_path.then(|| {
            Box::new(Self {
                lineage: self.lineage,
                epoch: self.epoch + 1,
                supports_fresh_path: true,
            }) as Box<dyn Controller>
        })
    }

    fn initial_window(&self) -> u64 {
        self.window()
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct EpochControllerFactory {
    builds: AtomicU64,
    supports_fresh_path: bool,
}

impl ControllerFactory for EpochControllerFactory {
    fn build(self: Arc<Self>, _now: Instant, _current_mtu: u16) -> Box<dyn Controller> {
        Box::new(EpochController {
            lineage: self.builds.fetch_add(1, Ordering::Relaxed) + 1,
            epoch: 1,
            supports_fresh_path: self.supports_fresh_path,
        })
    }
}

fn controller(path: &PathData) -> EpochController {
    *path
        .congestion
        .clone_box()
        .into_any()
        .downcast::<EpochController>()
        .expect("epoch controller")
}

fn path(config: &TransportConfig, generation: u64, now: Instant) -> PathData {
    PathData::new(
        "127.0.0.1:443".parse().expect("test address"),
        false,
        None,
        generation,
        now,
        config,
    )
}

#[test]
fn nat_clone_keeps_epoch_but_new_network_path_and_reset_advance_it() {
    let factory = Arc::new(EpochControllerFactory {
        builds: AtomicU64::new(0),
        supports_fresh_path: true,
    });
    let mut config = TransportConfig::default();
    config.congestion_controller_factory(factory.clone());
    let now = Instant::now();
    let initial = path(&config, 0, now);

    let rebound = PathData::from_previous(
        "127.0.0.1:8443".parse().expect("rebound address"),
        &initial,
        1,
        now,
    );
    assert_eq!(controller(&rebound).lineage, controller(&initial).lineage);
    assert_eq!(controller(&rebound).epoch, controller(&initial).epoch);

    let mut migrated = PathData::for_new_network_path(
        "[::1]:443".parse().expect("new network address"),
        &rebound,
        false,
        None,
        2,
        now,
        &config,
    );
    assert_eq!(controller(&migrated).lineage, controller(&initial).lineage);
    assert_eq!(controller(&migrated).epoch, controller(&initial).epoch + 1);
    assert_eq!(factory.builds.load(Ordering::Relaxed), 1);

    migrated.reset(now, &config);
    assert_eq!(controller(&migrated).lineage, controller(&initial).lineage);
    assert_eq!(controller(&migrated).epoch, controller(&initial).epoch + 2);
    assert_eq!(factory.builds.load(Ordering::Relaxed), 1);
}

#[test]
fn controllers_without_fresh_path_hook_keep_factory_replacement() {
    let factory = Arc::new(EpochControllerFactory {
        builds: AtomicU64::new(0),
        supports_fresh_path: false,
    });
    let mut config = TransportConfig::default();
    config.congestion_controller_factory(factory.clone());
    let now = Instant::now();
    let initial = path(&config, 0, now);
    let migrated = PathData::for_new_network_path(
        "[::1]:443".parse().expect("new network address"),
        &initial,
        false,
        None,
        1,
        now,
        &config,
    );

    assert_ne!(controller(&migrated).lineage, controller(&initial).lineage);
    assert_eq!(factory.builds.load(Ordering::Relaxed), 2);
}

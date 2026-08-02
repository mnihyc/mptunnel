use super::ExpiringReplayCache;
use std::sync::{Arc, Barrier, Mutex};

#[test]
fn expiring_replay_cache_keeps_the_inclusive_freshness_boundary() {
    let mut cache = ExpiringReplayCache::new(2);

    assert!(cache.try_insert(1_u8, 110, 100));
    assert!(cache.try_insert(2_u8, 200, 100));
    assert!(!cache.try_insert(1_u8, 110, 110));
    assert_eq!(cache.len(), 2);

    assert!(cache.try_insert(3_u8, 200, 111));
    assert_eq!(cache.len(), 2);
    assert!(!cache.try_insert(1_u8, 110, 111));
}

#[test]
fn expiring_replay_cache_fails_closed_without_evicting_live_entries() {
    let mut cache = ExpiringReplayCache::new(2);

    assert!(cache.try_insert(1_u8, 200, 100));
    assert!(cache.try_insert(2_u8, 200, 100));
    assert!(!cache.try_insert(3_u8, 200, 100));
    assert!(!cache.try_insert(1_u8, 200, 100));
    assert_eq!(cache.len(), 2);
}

#[test]
fn wall_clock_rollback_does_not_reopen_a_pruned_replay_identity() {
    let mut cache = ExpiringReplayCache::new(2);

    assert!(cache.try_insert(1_u8, 110, 100));
    assert!(cache.try_insert(2_u8, 200, 111));
    assert!(!cache.try_insert(1_u8, 110, 105));
    assert_eq!(cache.len(), 1);
}

#[test]
fn expiring_replay_admission_is_atomic_under_the_runtime_mutex() {
    const CONTENDERS: usize = 16;

    let cache = Arc::new(Mutex::new(ExpiringReplayCache::new(CONTENDERS)));
    let start = Arc::new(Barrier::new(CONTENDERS));
    let mut threads = Vec::with_capacity(CONTENDERS);
    for _ in 0..CONTENDERS {
        let cache = Arc::clone(&cache);
        let start = Arc::clone(&start);
        threads.push(std::thread::spawn(move || {
            start.wait();
            cache
                .lock()
                .expect("replay cache lock")
                .try_insert(7_u8, 200, 100)
        }));
    }

    let accepted = threads
        .into_iter()
        .map(|thread| thread.join().expect("replay admission thread"))
        .filter(|accepted| *accepted)
        .count();
    assert_eq!(accepted, 1);
}

#[test]
fn a_fresh_process_cache_starts_a_new_documented_replay_boundary() {
    let mut before_restart = ExpiringReplayCache::new(1);
    assert!(before_restart.try_insert(9_u8, 200, 100));

    let mut after_restart = ExpiringReplayCache::new(1);
    assert!(after_restart.try_insert(9_u8, 200, 100));
}

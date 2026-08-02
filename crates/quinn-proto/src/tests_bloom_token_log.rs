use super::*;
use rand::prelude::*;
use rand_pcg::Pcg32;

fn new_rng() -> impl Rng {
    Pcg32::from_seed(0xdeadbeefdeadbeefdeadbeefdeadbeef_u128.to_le_bytes())
}

#[test]
fn identity_hash_test() {
    let mut rng = new_rng();
    let builder = IdentityBuildHasher;
    for _ in 0..100 {
        let n = rng.random::<u64>();
        let hash = builder.hash_one(n);
        assert_eq!(hash, n);
    }
}

#[test]
fn optimal_k_num_test() {
    assert_eq!(optimal_k_num(10 << 20, 1_000_000), 58);
    assert_eq!(optimal_k_num(10 << 20, 1_000_000_000_000_000), 1);
    // assert that these don't panic:
    optimal_k_num(10 << 20, 0);
    optimal_k_num(usize::MAX, 1_000_000);
}

#[test]
fn bloom_token_log_conversion() {
    let mut rng = new_rng();
    let mut log = BloomTokenLog::new_expected_items(800, 200);

    let issued = SystemTime::now();
    let lifetime = Duration::from_secs(1_000_000);

    for i in 0..200 {
        let token = rng.random::<u128>();
        let result = log.check_and_insert(token, issued, lifetime);
        {
            let filter = &log.0.lock().unwrap().filter_1;
            if let Filter::Set(ref hset) = *filter {
                assert!(hset.capacity() * size_of::<u64>() <= 800);
                assert_eq!(hset.len(), i + 1);
                assert!(result.is_ok());
            } else {
                assert!(i > 10, "definitely bloomed too early");
            }
        }
        assert!(log.check_and_insert(token, issued, lifetime).is_err());
    }

    assert!(
        matches!(log.0.get_mut().unwrap().filter_1, Filter::Bloom { .. }),
        "didn't bloom"
    );
}

#[test]
fn turn_over() {
    let mut rng = new_rng();
    let log = BloomTokenLog::new_expected_items(800, 200);
    let lifetime = Duration::from_secs(1_000);
    let mut old = Vec::default();
    let mut accepted = 0;

    for i in 0..200 {
        let token = rng.random::<u128>();
        let now = UNIX_EPOCH + lifetime * 10 + lifetime * i / 10;
        let issued = now - lifetime.mul_f32(rng.random_range(0.0..3.0));
        let result = log.check_and_insert(token, issued, lifetime);
        if result.is_ok() {
            accepted += 1;
        }
        old.push((token, issued));
        let old_idx = rng.random_range(0..old.len());
        let (old_token, old_issued) = old[old_idx];
        assert!(
            log.check_and_insert(old_token, old_issued, lifetime)
                .is_err()
        );
    }
    assert!(accepted > 0);
}

fn test_doesnt_panic(log: BloomTokenLog) {
    let mut rng = new_rng();

    let issued = SystemTime::now();
    let lifetime = Duration::from_secs(1_000_000);

    for _ in 0..200 {
        let _ = log.check_and_insert(rng.random::<u128>(), issued, lifetime);
    }
}

#[test]
fn max_bytes_zero() {
    // "max bytes" is documented to be approximate. but make sure it doesn't panic.
    test_doesnt_panic(BloomTokenLog::new_expected_items(0, 200));
}

#[test]
fn expected_hits_zero() {
    test_doesnt_panic(BloomTokenLog::new_expected_items(100, 0));
}

#[test]
fn k_num_zero() {
    test_doesnt_panic(BloomTokenLog::new(100, 0));
}

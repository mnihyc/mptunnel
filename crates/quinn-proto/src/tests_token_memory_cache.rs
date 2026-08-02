use std::collections::VecDeque;

use super::*;
use rand::prelude::*;
use rand_pcg::Pcg32;

fn new_rng() -> impl Rng {
    Pcg32::new(0xdeadbeefdeadbeef, 0xdeadbeefdeadbeef)
}

#[test]
fn cache_test() {
    let mut rng = new_rng();
    const N: usize = 2;

    for _ in 0..10 {
        let mut cache_1: Vec<(u32, VecDeque<Bytes>)> = Vec::new(); // keep it sorted oldest to newest
        let cache_2 = TokenMemoryCache::new(20, 2);

        for i in 0..200 {
            let server_name = rng.random::<u32>() % 10;
            if rng.random_bool(0.666) {
                // store
                let token = Bytes::from(vec![i]);
                println!("STORE {server_name} {token:?}");
                if let Some((j, _)) = cache_1
                    .iter()
                    .enumerate()
                    .find(|&(_, &(server_name_2, _))| server_name_2 == server_name)
                {
                    let (_, mut queue) = cache_1.remove(j);
                    queue.push_back(token.clone());
                    if queue.len() > N {
                        queue.pop_front();
                    }
                    cache_1.push((server_name, queue));
                } else {
                    let mut queue = VecDeque::new();
                    queue.push_back(token.clone());
                    cache_1.push((server_name, queue));
                    if cache_1.len() > 20 {
                        cache_1.remove(0);
                    }
                }
                cache_2.insert(&server_name.to_string(), token);
            } else {
                // take
                println!("TAKE {server_name}");
                let expecting = cache_1
                    .iter()
                    .enumerate()
                    .find(|&(_, &(server_name_2, _))| server_name_2 == server_name)
                    .map(|(j, _)| j)
                    .map(|j| {
                        let (_, mut queue) = cache_1.remove(j);
                        let token = queue.pop_front().unwrap();
                        if !queue.is_empty() {
                            cache_1.push((server_name, queue));
                        }
                        token
                    });
                println!("EXPECTING {expecting:?}");
                assert_eq!(cache_2.take(&server_name.to_string()), expecting);
            }
        }
    }
}

#[test]
fn zero_max_server_names() {
    // test that this edge case doesn't panic
    let cache = TokenMemoryCache::new(0, 2);
    for i in 0..10 {
        cache.insert(&i.to_string(), Bytes::from(vec![i]));
        for j in 0..10 {
            assert!(cache.take(&j.to_string()).is_none());
        }
    }
}

#[test]
fn zero_queue_length() {
    // test that this edge case doesn't panic
    let cache = TokenMemoryCache::new(256, 0);
    for i in 0..10 {
        cache.insert(&i.to_string(), Bytes::from(vec![i]));
        for j in 0..10 {
            assert!(cache.take(&j.to_string()).is_none());
        }
    }
}

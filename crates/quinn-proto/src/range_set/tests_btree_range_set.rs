#![allow(clippy::single_range_in_vec_init)] // https://github.com/rust-lang/rust-clippy/issues/11086
use super::*;

#[test]
fn replace_contained() {
    let mut set = RangeSet::new();
    set.insert(2..4);
    assert_eq!(set.replace(1..5).collect::<Vec<_>>(), &[2..4]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 1..5);
}

#[test]
fn replace_contains() {
    let mut set = RangeSet::new();
    set.insert(1..5);
    assert_eq!(set.replace(2..4).collect::<Vec<_>>(), &[2..4]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 1..5);
}

#[test]
fn replace_pred() {
    let mut set = RangeSet::new();
    set.insert(2..4);
    assert_eq!(set.replace(3..5).collect::<Vec<_>>(), &[3..4]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 2..5);
}

#[test]
fn replace_succ() {
    let mut set = RangeSet::new();
    set.insert(2..4);
    assert_eq!(set.replace(1..3).collect::<Vec<_>>(), &[2..3]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 1..4);
}

#[test]
fn replace_exact_pred() {
    let mut set = RangeSet::new();
    set.insert(2..4);
    assert_eq!(set.replace(4..6).collect::<Vec<_>>(), &[]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 2..6);
}

#[test]
fn replace_exact_succ() {
    let mut set = RangeSet::new();
    set.insert(2..4);
    assert_eq!(set.replace(0..2).collect::<Vec<_>>(), &[]);
    assert_eq!(set.len(), 1);
    assert_eq!(set.peek_min().unwrap(), 0..4);
}

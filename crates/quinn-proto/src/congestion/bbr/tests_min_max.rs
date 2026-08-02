use super::*;

#[test]
fn test() {
    let round = 25;
    let mut min_max = MinMax::default();
    min_max.update_max(round + 1, 100);
    assert_eq!(100, min_max.get());
    min_max.update_max(round + 3, 120);
    assert_eq!(120, min_max.get());
    min_max.update_max(round + 5, 160);
    assert_eq!(160, min_max.get());
    min_max.update_max(round + 7, 100);
    assert_eq!(160, min_max.get());
    min_max.update_max(round + 10, 100);
    assert_eq!(160, min_max.get());
    min_max.update_max(round + 14, 100);
    assert_eq!(160, min_max.get());
    min_max.update_max(round + 16, 100);
    assert_eq!(100, min_max.get());
    min_max.update_max(round + 18, 130);
    assert_eq!(130, min_max.get());
}

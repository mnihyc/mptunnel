use super::cyclic_cursor_distance;

#[test]
fn cyclic_cursor_distance_handles_empty_wrap_and_order() {
    assert_eq!(cyclic_cursor_distance(usize::MAX, usize::MAX, 0), 0);
    assert_eq!(cyclic_cursor_distance(2, 2, 4), 0);
    assert_eq!(cyclic_cursor_distance(3, 2, 4), 1);
    assert_eq!(cyclic_cursor_distance(0, 2, 4), 2);
    assert_eq!(cyclic_cursor_distance(1, 2, 4), 3);
    assert_eq!(cyclic_cursor_distance(1, 6, 4), 3);
}

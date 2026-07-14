
use super::*;

#[test]
fn offset_ranges_must_be_non_empty() {
    assert_eq!(OffsetRange::new(10, 10), None);
    assert_eq!(OffsetRange::new(11, 10), None);
    assert_eq!(OffsetRange::new(10, 12).map(OffsetRange::len), Some(2));
    assert!(OffsetRange { start: 10, end: 10 }.is_empty());
}

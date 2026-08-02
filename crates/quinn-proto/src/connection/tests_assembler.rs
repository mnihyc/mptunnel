use super::*;
use assert_matches::assert_matches;

#[test]
fn assemble_ordered() {
    let mut x = Assembler::new();
    assert_matches!(next(&mut x, 32), None);
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    assert_matches!(next(&mut x, 1), Some(ref y) if &y[..] == b"1");
    assert_matches!(next(&mut x, 3), Some(ref y) if &y[..] == b"23");
    x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
    x.insert(6, Bytes::from_static(b"789"), 3).unwrap();
    x.insert(9, Bytes::from_static(b"10"), 2).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"789");
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"10");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_unordered() {
    let mut x = Assembler::new();
    x.ensure_ordering(false).unwrap();
    x.insert(3, Bytes::from_static(b"456"), 3).unwrap();
    assert_matches!(next(&mut x, 32), None);
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"456");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_duplicate() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_duplicate_compact() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    x.defragment();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_contained() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
    x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_contained_compact() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
    x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
    x.defragment();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_contains() {
    let mut x = Assembler::new();
    x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
    x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_contains_compact() {
    let mut x = Assembler::new();
    x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
    x.insert(0, Bytes::from_static(b"12345"), 5).unwrap();
    x.defragment();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"12345");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_overlapping() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"123"), 3).unwrap();
    x.insert(1, Bytes::from_static(b"234"), 3).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123");
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"4");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_overlapping_compact() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"123"), 4).unwrap();
    x.insert(1, Bytes::from_static(b"234"), 4).unwrap();
    x.defragment();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_complex() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
    x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
    x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
    x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_complex_compact() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"1"), 1).unwrap();
    x.insert(2, Bytes::from_static(b"3"), 1).unwrap();
    x.insert(4, Bytes::from_static(b"5"), 1).unwrap();
    x.insert(0, Bytes::from_static(b"123456"), 6).unwrap();
    x.defragment();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"123456");
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn assemble_old() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
    assert_matches!(next(&mut x, 32), Some(ref y) if &y[..] == b"1234");
    x.insert(0, Bytes::from_static(b"1234"), 4).unwrap();
    assert_matches!(next(&mut x, 32), None);
}

#[test]
fn compact() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
    x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
    x.insert(9, Bytes::from_static(b"jkl"), 4).unwrap();
    x.insert(12, Bytes::from_static(b"mno"), 4).unwrap();
    x.defragment();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(0, Bytes::from_static(b"abcdef"))
    );
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(9, Bytes::from_static(b"jklmno"))
    );
}

#[test]
fn defrag_with_missing_prefix() {
    let mut x = Assembler::new();
    x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
    x.defragment();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(3, Bytes::from_static(b"def"))
    );
}

#[test]
fn defrag_read_chunk() {
    let mut x = Assembler::new();
    x.insert(3, Bytes::from_static(b"def"), 4).unwrap();
    x.insert(0, Bytes::from_static(b"abc"), 4).unwrap();
    x.insert(7, Bytes::from_static(b"hij"), 4).unwrap();
    x.insert(11, Bytes::from_static(b"lmn"), 4).unwrap();
    x.defragment();
    assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"abcdef");
    x.insert(5, Bytes::from_static(b"fghijklmn"), 9).unwrap();
    assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"ghijklmn");
    x.insert(13, Bytes::from_static(b"nopq"), 4).unwrap();
    assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"opq");
    x.insert(15, Bytes::from_static(b"pqrs"), 4).unwrap();
    assert_matches!(x.read(usize::MAX, true), Some(ref y) if &y.bytes[..] == b"rs");
    assert_matches!(x.read(usize::MAX, true), None);
}

#[test]
fn unordered_happy_path() {
    let mut x = Assembler::new();
    x.ensure_ordering(false).unwrap();
    x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(0, Bytes::from_static(b"abc"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
    x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(3, Bytes::from_static(b"def"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
}

#[test]
fn unordered_dedup() {
    let mut x = Assembler::new();
    x.ensure_ordering(false).unwrap();
    x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(3, Bytes::from_static(b"def"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
    x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
    x.insert(0, Bytes::from_static(b"abcdefghi"), 9).unwrap();
    x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(0, Bytes::from_static(b"a"))
    );
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(1, Bytes::from_static(b"bc"))
    );
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(6, Bytes::from_static(b"ghi"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
    x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(9, Bytes::from_static(b"jkl"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
    x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
    assert_eq!(
        next_unordered(&mut x),
        Chunk::new(12, Bytes::from_static(b"mno"))
    );
    assert_eq!(x.read(usize::MAX, false), None);
    x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
    assert_eq!(x.read(usize::MAX, false), None);
}

#[test]
fn chunks_dedup() {
    let mut x = Assembler::new();
    x.insert(3, Bytes::from_static(b"def"), 3).unwrap();
    assert_eq!(x.read(usize::MAX, true), None);
    x.insert(0, Bytes::from_static(b"a"), 1).unwrap();
    x.insert(1, Bytes::from_static(b"bcdefghi"), 9).unwrap();
    x.insert(0, Bytes::from_static(b"abcd"), 4).unwrap();
    assert_eq!(
        x.read(usize::MAX, true),
        Some(Chunk::new(0, Bytes::from_static(b"abcd")))
    );
    assert_eq!(
        x.read(usize::MAX, true),
        Some(Chunk::new(4, Bytes::from_static(b"efghi")))
    );
    assert_eq!(x.read(usize::MAX, true), None);
    x.insert(8, Bytes::from_static(b"ijkl"), 4).unwrap();
    assert_eq!(
        x.read(usize::MAX, true),
        Some(Chunk::new(9, Bytes::from_static(b"jkl")))
    );
    assert_eq!(x.read(usize::MAX, true), None);
    x.insert(12, Bytes::from_static(b"mno"), 3).unwrap();
    assert_eq!(
        x.read(usize::MAX, true),
        Some(Chunk::new(12, Bytes::from_static(b"mno")))
    );
    assert_eq!(x.read(usize::MAX, true), None);
    x.insert(2, Bytes::from_static(b"cde"), 3).unwrap();
    assert_eq!(x.read(usize::MAX, true), None);
}

#[test]
fn ordered_eager_discard() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
    assert_eq!(x.data.len(), 1);
    assert_eq!(
        x.read(usize::MAX, true),
        Some(Chunk::new(0, Bytes::from_static(b"abc")))
    );
    x.insert(0, Bytes::from_static(b"ab"), 2).unwrap();
    assert_eq!(x.data.len(), 0);
    x.insert(2, Bytes::from_static(b"cd"), 2).unwrap();
    assert_eq!(
        x.data.peek(),
        Some(&Buffer::new(3, Bytes::from_static(b"d"), 2))
    );
}

#[test]
fn ordered_insert_unordered_read() {
    let mut x = Assembler::new();
    x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
    x.insert(0, Bytes::from_static(b"abc"), 3).unwrap();
    x.ensure_ordering(false).unwrap();
    assert_eq!(
        x.read(3, false),
        Some(Chunk::new(0, Bytes::from_static(b"abc")))
    );
    assert_eq!(x.read(3, false), None);
}

fn next_unordered(x: &mut Assembler) -> Chunk {
    x.read(usize::MAX, false).unwrap()
}

fn next(x: &mut Assembler, size: usize) -> Option<Bytes> {
    x.read(size, true).map(|chunk| chunk.bytes)
}

use super::*;

#[test]
fn bytes_array() {
    let full = b"Hello World 123456789 ABCDEFGHJIJKLMNOPQRSTUVWXYZ".to_owned();
    for limit in 0..full.len() {
        let mut chunks = [
            Bytes::from_static(b""),
            Bytes::from_static(b"Hello "),
            Bytes::from_static(b"Wo"),
            Bytes::from_static(b""),
            Bytes::from_static(b"r"),
            Bytes::from_static(b"ld"),
            Bytes::from_static(b""),
            Bytes::from_static(b" 12345678"),
            Bytes::from_static(b"9 ABCDE"),
            Bytes::from_static(b"F"),
            Bytes::from_static(b"GHJIJKLMNOPQRSTUVWXYZ"),
        ];
        let num_chunks = chunks.len();
        let last_chunk_len = chunks[chunks.len() - 1].len();

        let mut array = BytesArray::from_chunks(&mut chunks);

        let mut buf = Vec::new();
        let mut chunks_popped = 0;
        let mut chunks_consumed = 0;
        let mut remaining = limit;
        loop {
            let (chunk, consumed) = array.pop_chunk(remaining);
            chunks_consumed += consumed;

            if !chunk.is_empty() {
                buf.extend_from_slice(&chunk);
                remaining -= chunk.len();
                chunks_popped += 1;
            } else {
                break;
            }
        }

        assert_eq!(&buf[..], &full[..limit]);

        if limit == full.len() {
            // Full consumption of the last chunk
            assert_eq!(chunks_consumed, num_chunks);
            // Since there are empty chunks, we consume more than there are popped
            assert_eq!(chunks_consumed, chunks_popped + 3);
        } else if limit > full.len() - last_chunk_len {
            // Partial consumption of the last chunk
            assert_eq!(chunks_consumed, num_chunks - 1);
            assert_eq!(chunks_consumed, chunks_popped + 2);
        }
    }
}

#[test]
fn byte_slice() {
    let full = b"Hello World 123456789 ABCDEFGHJIJKLMNOPQRSTUVWXYZ".to_owned();
    for limit in 0..full.len() {
        let mut array = ByteSlice::from_slice(&full[..]);

        let mut buf = Vec::new();
        let mut chunks_popped = 0;
        let mut chunks_consumed = 0;
        let mut remaining = limit;
        loop {
            let (chunk, consumed) = array.pop_chunk(remaining);
            chunks_consumed += consumed;

            if !chunk.is_empty() {
                buf.extend_from_slice(&chunk);
                remaining -= chunk.len();
                chunks_popped += 1;
            } else {
                break;
            }
        }

        assert_eq!(&buf[..], &full[..limit]);
        if limit != 0 {
            assert_eq!(chunks_popped, 1);
        } else {
            assert_eq!(chunks_popped, 0);
        }

        if limit == full.len() {
            assert_eq!(chunks_consumed, 1);
        } else {
            assert_eq!(chunks_consumed, 0);
        }
    }
}

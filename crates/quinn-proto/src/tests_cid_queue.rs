use super::*;

fn cid(sequence: u64, retire_prior_to: u64) -> NewConnectionId {
    NewConnectionId {
        sequence,
        id: ConnectionId::new(&[0xAB; 8]),
        reset_token: ResetToken::from([0xCD; crate::RESET_TOKEN_SIZE]),
        retire_prior_to,
    }
}

fn initial_cid() -> ConnectionId {
    ConnectionId::new(&[0xFF; 8])
}

#[test]
fn next_dense() {
    let mut q = CidQueue::new(initial_cid());
    assert!(q.next().is_none());
    assert!(q.next().is_none());

    for i in 1..CidQueue::LEN as u64 {
        q.insert(cid(i, 0)).unwrap();
    }
    for i in 1..CidQueue::LEN as u64 {
        let (_, retire) = q.next().unwrap();
        assert_eq!(q.active_seq(), i);
        assert_eq!(retire.end - retire.start, 1);
    }
    assert!(q.next().is_none());
}
#[test]
fn next_sparse() {
    let mut q = CidQueue::new(initial_cid());
    let seqs = (1..CidQueue::LEN as u64).filter(|x| x % 2 == 0);
    for i in seqs.clone() {
        q.insert(cid(i, 0)).unwrap();
    }
    for i in seqs {
        let (_, retire) = q.next().unwrap();
        dbg!(&retire);
        assert_eq!(q.active_seq(), i);
        assert_eq!(retire, (q.active_seq().saturating_sub(2))..q.active_seq());
    }
    assert!(q.next().is_none());
}

#[test]
fn wrap() {
    let mut q = CidQueue::new(initial_cid());

    for i in 1..CidQueue::LEN as u64 {
        q.insert(cid(i, 0)).unwrap();
    }
    for _ in 1..(CidQueue::LEN as u64 - 1) {
        q.next().unwrap();
    }
    for i in CidQueue::LEN as u64..(CidQueue::LEN as u64 + 3) {
        q.insert(cid(i, 0)).unwrap();
    }
    for i in (CidQueue::LEN as u64 - 1)..(CidQueue::LEN as u64 + 3) {
        q.next().unwrap();
        assert_eq!(q.active_seq(), i);
    }
    assert!(q.next().is_none());
}

#[test]
fn retire_dense() {
    let mut q = CidQueue::new(initial_cid());

    for i in 1..CidQueue::LEN as u64 {
        q.insert(cid(i, 0)).unwrap();
    }
    assert_eq!(q.active_seq(), 0);

    assert_eq!(q.insert(cid(4, 2)).unwrap().unwrap().0, 0..2);
    assert_eq!(q.active_seq(), 2);
    assert_eq!(q.insert(cid(4, 2)), Ok(None));

    for i in 2..(CidQueue::LEN as u64 - 1) {
        let _ = q.next().unwrap();
        assert_eq!(q.active_seq(), i + 1);
        assert_eq!(q.insert(cid(i + 1, i + 1)), Ok(None));
    }

    assert!(q.next().is_none());
}

#[test]
fn retire_sparse() {
    // Retiring CID 0 when CID 1 is not known should retire CID 1 as we move to CID 2
    let mut q = CidQueue::new(initial_cid());
    q.insert(cid(2, 0)).unwrap();
    assert_eq!(q.insert(cid(3, 1)).unwrap().unwrap().0, 0..2,);
    assert_eq!(q.active_seq(), 2);
}

#[test]
fn retire_many() {
    let mut q = CidQueue::new(initial_cid());
    q.insert(cid(2, 0)).unwrap();
    assert_eq!(
        q.insert(cid(1_000_000, 1_000_000)).unwrap().unwrap().0,
        0..CidQueue::LEN as u64,
    );
    assert_eq!(q.active_seq(), 1_000_000);
}

#[test]
fn insert_limit() {
    let mut q = CidQueue::new(initial_cid());
    assert_eq!(q.insert(cid(CidQueue::LEN as u64 - 1, 0)), Ok(None));
    assert_eq!(
        q.insert(cid(CidQueue::LEN as u64, 0)),
        Err(InsertError::ExceedsLimit)
    );
}

#[test]
fn insert_duplicate() {
    let mut q = CidQueue::new(initial_cid());
    q.insert(cid(0, 0)).unwrap();
    q.insert(cid(0, 0)).unwrap();
}

#[test]
fn insert_retired() {
    let mut q = CidQueue::new(initial_cid());
    assert_eq!(
        q.insert(cid(0, 0)),
        Ok(None),
        "reinserting active CID succeeds"
    );
    assert!(q.next().is_none(), "active CID isn't requeued");
    q.insert(cid(1, 0)).unwrap();
    q.next().unwrap();
    assert_eq!(
        q.insert(cid(0, 0)),
        Err(InsertError::Retired),
        "previous active CID is already retired"
    );
}

#[test]
fn retire_then_insert_next() {
    let mut q = CidQueue::new(initial_cid());
    for i in 1..CidQueue::LEN as u64 {
        q.insert(cid(i, 0)).unwrap();
    }
    q.next().unwrap();
    q.insert(cid(CidQueue::LEN as u64, 0)).unwrap();
    assert_eq!(
        q.insert(cid(CidQueue::LEN as u64 + 1, 0)),
        Err(InsertError::ExceedsLimit)
    );
}

#[test]
fn always_valid() {
    let mut q = CidQueue::new(initial_cid());
    assert!(q.next().is_none());
    assert_eq!(q.active(), initial_cid());
    assert_eq!(q.active_seq(), 0);
}

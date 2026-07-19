use super::*;

fn identity(payload: &'static [u8]) -> DatagramPayloadIdentity {
    DatagramPayloadIdentity::new(payload)
}

#[test]
fn accepts_reordering_within_the_window_once() {
    let mut window = DatagramReceiveWindow::new(4);

    assert_eq!(
        window.admit(3, identity(b"three")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(1, identity(b"one")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(1, identity(b"one")),
        Ok(DatagramAdmission::Duplicate)
    );
}

#[test]
fn rejects_payload_changes_while_an_id_is_retained() {
    let mut window = DatagramReceiveWindow::new(4);
    assert_eq!(
        window.admit(2, identity(b"original")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(window.admit(2, identity(b"changed")), Err(()));
}

#[test]
fn suppresses_ids_retired_by_a_newer_window() {
    let mut window = DatagramReceiveWindow::new(2);
    assert_eq!(
        window.admit(1, identity(b"one")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(3, identity(b"three")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(1, identity(b"different after retirement")),
        Ok(DatagramAdmission::Duplicate)
    );
}

#[test]
fn one_slot_window_tracks_only_the_newest_id() {
    let mut window = DatagramReceiveWindow::new(1);
    assert_eq!(
        window.admit(7, identity(b"seven")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(8, identity(b"eight")),
        Ok(DatagramAdmission::Fresh)
    );
    assert_eq!(
        window.admit(7, identity(b"seven")),
        Ok(DatagramAdmission::Duplicate)
    );
}

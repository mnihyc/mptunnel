//! Loopback echo targets shared by packaged daily-use acceptance scenarios.

use super::IO_TIMEOUT;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, UdpSocket};
use std::thread;

pub fn spawn_tcp_echo() -> (SocketAddr, thread::JoinHandle<()>) {
    spawn_tcp_echo_connections(1)
}

pub fn spawn_tcp_echo_connections(connection_count: usize) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind echo target");
    let address = listener.local_addr().expect("echo target address");
    let task = thread::spawn(move || {
        for _ in 0..connection_count {
            let (mut stream, _) = listener.accept().expect("accept echo connection");
            stream
                .set_read_timeout(Some(IO_TIMEOUT))
                .expect("set echo read timeout");
            stream
                .set_write_timeout(Some(IO_TIMEOUT))
                .expect("set echo write timeout");
            let mut request = [0_u8; 4];
            stream.read_exact(&mut request).expect("echo request");
            assert_eq!(&request, b"ping");
            stream.write_all(b"pong").expect("echo response");
        }
    });
    (address, task)
}

pub fn spawn_udp_echo_packets(packet_count: usize) -> (SocketAddr, thread::JoinHandle<()>) {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind UDP echo target");
    socket
        .set_read_timeout(Some(IO_TIMEOUT))
        .expect("set UDP echo read timeout");
    socket
        .set_write_timeout(Some(IO_TIMEOUT))
        .expect("set UDP echo write timeout");
    let address = socket.local_addr().expect("UDP echo target address");
    let task = thread::spawn(move || {
        let mut payload = [0_u8; 65_535];
        for _ in 0..packet_count {
            let (length, peer) = socket.recv_from(&mut payload).expect("UDP echo request");
            assert_eq!(&payload[..length], b"ping");
            socket.send_to(b"pong", peer).expect("UDP echo response");
        }
    });
    (address, task)
}

pub fn join_thread(task: thread::JoinHandle<()>, context: &str) {
    task.join()
        .unwrap_or_else(|_| panic!("{context} thread panicked"));
}

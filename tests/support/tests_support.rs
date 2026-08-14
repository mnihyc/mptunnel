use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{
    IpAddr, Ipv4Addr, Ipv6Addr, Shutdown, SocketAddr, TcpListener, TcpStream, UdpSocket,
};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static ISSUED_LOOPBACK_ADDRESSES: Mutex<Vec<SocketAddr>> = Mutex::new(Vec::new());
static NETWORK_ACCEPTANCE: Mutex<()> = Mutex::new(());

const IO_TIMEOUT: Duration = Duration::from_secs(5);

mod echo;
pub use echo::{join_thread, spawn_tcp_echo, spawn_tcp_echo_connections, spawn_udp_echo_packets};

pub struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    pub fn new(label: &str) -> Self {
        let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let target_tmp = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
        fs::create_dir_all(&target_tmp).expect("create Cargo integration-test scratch root");
        let path = target_tmp.join(format!(
            "mptunnel-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create isolated acceptance-test directory");
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn write(&self, name: &str, contents: &str) -> PathBuf {
        let path = self.path.join(name);
        fs::write(&path, contents).expect("write acceptance-test fixture");
        path
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub fn mptunnel_binary() -> &'static str {
    env!("CARGO_BIN_EXE_mptunnel")
}

pub fn check_config(path: &Path) -> Output {
    Command::new(mptunnel_binary())
        .args(["--config", path.to_str().expect("UTF-8 config path")])
        .arg("--check-config")
        .env("MPTUNNEL_WORKER_THREADS", "2")
        .output()
        .expect("run packaged mptunnel config check")
}

pub struct MptunnelProcess {
    child: Option<Child>,
    log_path: PathBuf,
}

impl MptunnelProcess {
    pub fn spawn(config_path: &Path, log_path: PathBuf) -> Self {
        let mut command = Command::new(mptunnel_binary());
        command
            .args(["--config", config_path.to_str().expect("UTF-8 config path")])
            .env("MPTUNNEL_WORKER_THREADS", "2");
        Self::spawn_command(command, log_path)
    }

    pub fn spawn_without_args(working_directory: &Path, log_path: PathBuf) -> Self {
        let mut command = Command::new(mptunnel_binary());
        command
            .current_dir(working_directory)
            .env("MPTUNNEL_WORKER_THREADS", "2");
        Self::spawn_command(command, log_path)
    }

    fn spawn_command(mut command: Command, log_path: PathBuf) -> Self {
        let log = File::create(&log_path).expect("create mptunnel process log");
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn packaged mptunnel process");
        Self {
            child: Some(child),
            log_path,
        }
    }

    pub fn stop(&mut self) {
        let mut child = self.child.take().expect("process exists");
        if child
            .try_wait()
            .expect("inspect process before stop")
            .is_none()
        {
            child.kill().expect("stop mptunnel process");
        }
        child.wait().expect("join stopped mptunnel process");
    }

    pub fn assert_running(&mut self, context: &str) {
        let status = self
            .child
            .as_mut()
            .expect("process exists")
            .try_wait()
            .expect("inspect process");
        if let Some(status) = status {
            panic!(
                "mptunnel exited during {context} with {status}; stderr:\n{}",
                self.log()
            );
        }
    }

    pub fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_else(|error| format!("<unreadable: {error}>"))
    }
}

impl Drop for MptunnelProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            if child.try_wait().ok().flatten().is_none() {
                let _ = child.kill();
            }
            let _ = child.wait();
        }
    }
}

pub fn unused_loopback_addr() -> SocketAddr {
    let mut duplicate_reservations = Vec::new();
    loop {
        let reservation =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve loopback port");
        let address = reservation.local_addr().expect("reserved loopback address");
        let mut issued = ISSUED_LOOPBACK_ADDRESSES
            .lock()
            .expect("acceptance-test port ledger");
        if !issued.contains(&address) {
            issued.push(address);
            return address;
        }
        drop(issued);
        // Keep a recycled port occupied until the kernel selects one that this
        // acceptance-test process has not already assigned.
        duplicate_reservations.push(reservation);
    }
}

pub fn unused_loopback_udp_addr() -> SocketAddr {
    let mut duplicate_reservations = Vec::new();
    loop {
        let reservation =
            UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve loopback UDP port");
        let address = reservation
            .local_addr()
            .expect("reserved loopback UDP address");
        let mut issued = ISSUED_LOOPBACK_ADDRESSES
            .lock()
            .expect("acceptance-test port ledger");
        if !issued.contains(&address) {
            issued.push(address);
            return address;
        }
        drop(issued);
        duplicate_reservations.push(reservation);
    }
}

pub fn network_test_guard() -> MutexGuard<'static, ()> {
    // Released port-0 reservations cannot be handed atomically to a child
    // process, so socket-owning acceptance scenarios must not race each other.
    NETWORK_ACCEPTANCE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub fn wait_for_tcp(
    process: &mut MptunnelProcess,
    address: SocketAddr,
    timeout: Duration,
    context: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        process.assert_running(context);
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {context} at {address}; stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

pub fn wait_for_tcp_closed(
    process: &mut MptunnelProcess,
    address: SocketAddr,
    timeout: Duration,
    context: &str,
) {
    let deadline = Instant::now() + timeout;
    loop {
        process.assert_running(context);
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_err() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for retirement of {context} at {address}; stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[derive(Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).expect("JSON management response")
    }
}

pub fn http_request(
    address: SocketAddr,
    method: &str,
    path: &str,
    token: Option<&str>,
    headers: &[(&str, &str)],
    body: &[u8],
) -> io::Result<HttpResponse> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(token) = token {
        request.push_str("Authorization: Bearer ");
        request.push_str(token);
        request.push_str("\r\n");
    }
    for (name, value) in headers {
        request.push_str(name);
        request.push_str(": ");
        request.push_str(value);
        request.push_str("\r\n");
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes())?;
    stream.write_all(body)?;
    stream.shutdown(Shutdown::Write)?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response)?;
    parse_http_response(&response)
}

fn parse_http_response(response: &[u8]) -> io::Result<HttpResponse> {
    let header_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP headers incomplete"))?;
    let header = std::str::from_utf8(&response[..header_end])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let status = header
        .lines()
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "HTTP status missing"))?;
    Ok(HttpResponse {
        status,
        body: response[header_end..].to_vec(),
    })
}

pub fn wait_for_ready_management(
    process: &mut MptunnelProcess,
    address: SocketAddr,
    token: &str,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    loop {
        process.assert_running("management readiness");
        if let Ok(response) = http_request(
            address,
            "GET",
            "/api/v4/health/ready",
            Some(token),
            &[],
            &[],
        ) && response.status == 200
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for ready management API at {address}; stderr:\n{}",
            process.log()
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[derive(Clone, Copy)]
pub enum SocksTarget<'a> {
    Domain(&'a str, u16),
    Ipv4(Ipv4Addr, u16),
}

fn protocol_target(target: SocksTarget<'_>) -> mptunnel::protocol::TargetAddr {
    match target {
        SocksTarget::Domain(host, port) => mptunnel::protocol::TargetAddr::Domain {
            host: host.to_string(),
            port,
        },
        SocksTarget::Ipv4(address, port) => {
            mptunnel::protocol::TargetAddr::Ip(SocketAddr::new(IpAddr::V4(address), port))
        }
    }
}

fn read_socks5_reply(stream: &mut TcpStream) -> io::Result<(u8, Option<SocketAddr>)> {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix)?;
    if prefix[0] != 0x05 || prefix[2] != 0x00 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid SOCKS5 reply prefix: {prefix:?}"),
        ));
    }
    let address = match prefix[3] {
        0x01 => {
            let mut encoded = [0_u8; 6];
            stream.read_exact(&mut encoded)?;
            Some(SocketAddr::from((
                [encoded[0], encoded[1], encoded[2], encoded[3]],
                u16::from_be_bytes([encoded[4], encoded[5]]),
            )))
        }
        0x04 => {
            let mut encoded = [0_u8; 18];
            stream.read_exact(&mut encoded)?;
            let octets: [u8; 16] = encoded[..16].try_into().expect("IPv6 reply length");
            Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(octets)),
                u16::from_be_bytes([encoded[16], encoded[17]]),
            ))
        }
        0x03 => {
            let mut length = [0_u8; 1];
            stream.read_exact(&mut length)?;
            let mut encoded = vec![0_u8; usize::from(length[0]) + 2];
            stream.read_exact(&mut encoded)?;
            None
        }
        atyp => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid SOCKS5 reply address type: {atyp}"),
            ));
        }
    };
    Ok((prefix[1], address))
}

pub fn socks5_connect(proxy: SocketAddr, target: SocksTarget<'_>) -> io::Result<(TcpStream, u8)> {
    let mut stream = TcpStream::connect_timeout(&proxy, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    stream.write_all(&[0x05, 0x01, 0x00])?;
    let mut method = [0_u8; 2];
    stream.read_exact(&mut method)?;
    if method != [0x05, 0x00] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected SOCKS5 method reply: {method:?}"),
        ));
    }
    let mut request = vec![0x05, 0x01, 0x00];
    match target {
        SocksTarget::Domain(host, port) => {
            let host_len = u8::try_from(host.len())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "domain too long"))?;
            request.extend_from_slice(&[0x03, host_len]);
            request.extend_from_slice(host.as_bytes());
            request.extend_from_slice(&port.to_be_bytes());
        }
        SocksTarget::Ipv4(address, port) => {
            request.push(0x01);
            request.extend_from_slice(&address.octets());
            request.extend_from_slice(&port.to_be_bytes());
        }
    }
    stream.write_all(&request)?;
    let (reply, _) = read_socks5_reply(&mut stream)?;
    Ok((stream, reply))
}

pub fn http_connect(proxy: SocketAddr, authority: &str) -> io::Result<(TcpStream, u16)> {
    let mut stream = TcpStream::connect_timeout(&proxy, Duration::from_millis(500))?;
    stream.set_read_timeout(Some(IO_TIMEOUT))?;
    stream.set_write_timeout(Some(IO_TIMEOUT))?;
    write!(
        stream,
        "CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n"
    )?;
    let mut response = Vec::new();
    while !response.ends_with(b"\r\n\r\n") {
        if response.len() >= 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "HTTP CONNECT response headers exceed 64 KiB",
            ));
        }
        let mut byte = [0_u8; 1];
        stream.read_exact(&mut byte)?;
        response.push(byte[0]);
    }
    let parsed = mptunnel::outbound::http_connect::parse_connect_response(&response)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok((stream, parsed.status))
}

pub fn socks5_udp_round_trip(
    proxy: SocketAddr,
    target: SocksTarget<'_>,
    request: &[u8],
    response: &[u8],
) -> io::Result<()> {
    let mut control = TcpStream::connect_timeout(&proxy, Duration::from_millis(500))?;
    control.set_read_timeout(Some(IO_TIMEOUT))?;
    control.set_write_timeout(Some(IO_TIMEOUT))?;
    control.write_all(&[0x05, 0x01, 0x00])?;
    let mut method = [0_u8; 2];
    control.read_exact(&mut method)?;
    if method != [0x05, 0x00] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected SOCKS5 method reply: {method:?}"),
        ));
    }
    control.write_all(&[0x05, 0x03, 0x00, 0x01, 0, 0, 0, 0, 0, 0])?;
    let (reply, relay) = read_socks5_reply(&mut control)?;
    if reply != 0x00 {
        return Err(io::Error::other(format!(
            "SOCKS5 UDP ASSOCIATE failed with status {reply:#04x}"
        )));
    }
    let mut relay = relay.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "SOCKS5 UDP relay reply used a domain address",
        )
    })?;
    if relay.ip().is_unspecified() {
        relay.set_ip(proxy.ip());
    }

    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    socket.set_read_timeout(Some(IO_TIMEOUT))?;
    socket.set_write_timeout(Some(IO_TIMEOUT))?;
    let target = protocol_target(target);
    let packet = mptunnel::ingress::socks5::udp_datagram(&target, request)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    socket.send_to(&packet, relay)?;
    let mut received = [0_u8; 65_535];
    let (received_len, source) = socket.recv_from(&mut received)?;
    if source != relay {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("SOCKS5 UDP response came from {source}, expected {relay}"),
        ));
    }
    let (datagram, consumed) =
        mptunnel::ingress::socks5::parse_udp_datagram(&received[..received_len])
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    if consumed != received_len
        || datagram.target != target
        || datagram.payload.as_ref() != response
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected SOCKS5 UDP response: {datagram:?}"),
        ));
    }
    Ok(())
}

pub fn udp_round_trip(address: SocketAddr, request: &[u8], response: &[u8]) -> io::Result<()> {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?;
    socket.set_read_timeout(Some(IO_TIMEOUT))?;
    socket.set_write_timeout(Some(IO_TIMEOUT))?;
    socket.send_to(request, address)?;
    let mut received = vec![0_u8; response.len().max(1)];
    let (length, source) = socket.recv_from(&mut received)?;
    if source != address || length != response.len() || &received[..length] != response {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unexpected UDP response from {source}: {:?}",
                &received[..length]
            ),
        ));
    }
    Ok(())
}

pub fn socks5_round_trip(
    proxy: SocketAddr,
    target: SocksTarget<'_>,
    request: &[u8],
    response: &[u8],
) -> io::Result<()> {
    let (mut stream, status) = socks5_connect(proxy, target)?;
    if status != 0 {
        return Err(io::Error::other(format!(
            "SOCKS5 connect failed with status {status:#04x}"
        )));
    }
    stream.write_all(request)?;
    let mut received = vec![0_u8; response.len()];
    stream.read_exact(&mut received)?;
    if received != response {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected proxied response: {received:?}"),
        ));
    }
    Ok(())
}

pub fn spawn_blackhole_proxy() -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind blackhole proxy");
    let address = listener.local_addr().expect("blackhole proxy address");
    let task = thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("accept blackhole proxy connection");
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .expect("set blackhole proxy read timeout");
        let mut received = Vec::new();
        stream
            .read_to_end(&mut received)
            .expect("timed-out proxy attempt closes");
        assert!(
            !received.is_empty(),
            "blackhole proxy should receive the opening handshake"
        );
    });
    (address, task)
}

pub fn spawn_echo_socks5_proxy(
    expected_target: Ipv4Addr,
    expected_port: u16,
) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind working proxy");
    let address = listener.local_addr().expect("working proxy address");
    let task = thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept working proxy connection");
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .expect("set proxy read timeout");
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .expect("set proxy write timeout");
        let mut greeting = [0_u8; 2];
        stream.read_exact(&mut greeting).expect("proxy greeting");
        assert_eq!(greeting[0], 0x05);
        let mut methods = vec![0_u8; usize::from(greeting[1])];
        stream.read_exact(&mut methods).expect("proxy methods");
        assert!(methods.contains(&0x00));
        stream
            .write_all(&[0x05, 0x00])
            .expect("proxy method selection");

        let mut request = [0_u8; 4];
        stream
            .read_exact(&mut request)
            .expect("proxy connect request");
        assert_eq!(&request[..3], &[0x05, 0x01, 0x00]);
        assert_eq!(request[3], 0x01, "target should be pre-resolved");
        let mut target = [0_u8; 6];
        stream.read_exact(&mut target).expect("proxy target");
        assert_eq!(
            Ipv4Addr::new(target[0], target[1], target[2], target[3]),
            expected_target
        );
        assert_eq!(u16::from_be_bytes([target[4], target[5]]), expected_port);
        stream
            .write_all(&[0x05, 0x00, 0x00, 0x01, 127, 0, 0, 1, 0, 0])
            .expect("proxy success reply");

        let mut payload = [0_u8; 4];
        stream.read_exact(&mut payload).expect("proxied payload");
        assert_eq!(&payload, b"ping");
        stream.write_all(b"pong").expect("proxied response");
    });
    (address, task)
}

pub fn spawn_echo_http_connect_proxy(
    expected_authority: &str,
) -> (SocketAddr, thread::JoinHandle<()>) {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind HTTP CONNECT proxy");
    let address = listener.local_addr().expect("HTTP CONNECT proxy address");
    let expected_authority = expected_authority.to_string();
    let task = thread::spawn(move || {
        let (mut stream, _) = listener
            .accept()
            .expect("accept HTTP CONNECT proxy connection");
        stream
            .set_read_timeout(Some(IO_TIMEOUT))
            .expect("set HTTP CONNECT proxy read timeout");
        stream
            .set_write_timeout(Some(IO_TIMEOUT))
            .expect("set HTTP CONNECT proxy write timeout");
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            assert!(
                request.len() < 16 * 1024,
                "HTTP CONNECT request exceeded the Product request bound"
            );
            let mut byte = [0_u8; 1];
            stream
                .read_exact(&mut byte)
                .expect("HTTP CONNECT proxy request");
            request.push(byte[0]);
        }
        let request = std::str::from_utf8(&request).expect("ASCII HTTP CONNECT request");
        assert!(
            request.starts_with(&format!("CONNECT {expected_authority} HTTP/1.1\r\n")),
            "HTTP CONNECT outbound changed the delegated authority: {request:?}"
        );
        assert!(
            request
                .lines()
                .any(|line| line == format!("Host: {expected_authority}")),
            "HTTP CONNECT outbound changed the delegated Host authority: {request:?}"
        );
        stream
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .expect("HTTP CONNECT proxy success reply");
        let mut payload = [0_u8; 4];
        stream
            .read_exact(&mut payload)
            .expect("HTTP CONNECT proxied payload");
        assert_eq!(&payload, b"ping");
        stream
            .write_all(b"pong")
            .expect("HTTP CONNECT proxied response");
    });
    (address, task)
}

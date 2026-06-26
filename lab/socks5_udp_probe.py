#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import time


def parse_host_port(value):
    if value.startswith("["):
        host, rest = value[1:].split("]", 1)
        if not rest.startswith(":"):
            raise ValueError(f"missing port in {value!r}")
        return host, int(rest[1:])
    host, port = value.rsplit(":", 1)
    return host, int(port)


def recv_exact(sock, length):
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise OSError("connection closed")
        data.extend(chunk)
    return bytes(data)


def encode_socks_addr(host, port):
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        encoded = host.encode("idna")
        if len(encoded) > 255:
            raise ValueError("SOCKS5 domain is too long")
        return b"\x03" + bytes([len(encoded)]) + encoded + struct.pack("!H", port)
    if ip.version == 4:
        return b"\x01" + ip.packed + struct.pack("!H", port)
    return b"\x04" + ip.packed + struct.pack("!H", port)


def decode_socks_addr(sock):
    atyp = recv_exact(sock, 1)[0]
    if atyp == 1:
        host = socket.inet_ntop(socket.AF_INET, recv_exact(sock, 4))
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, recv_exact(sock, 16))
    elif atyp == 3:
        length = recv_exact(sock, 1)[0]
        host = recv_exact(sock, length).decode("idna")
    else:
        raise OSError(f"unsupported SOCKS5 address type {atyp}")
    port = struct.unpack("!H", recv_exact(sock, 2))[0]
    return host, port


def parse_udp_packet(data):
    if len(data) < 4 or data[0:2] != b"\x00\x00" or data[2] != 0:
        raise ValueError("invalid SOCKS5 UDP header")
    offset = 3
    atyp = data[offset]
    offset += 1
    if atyp == 1:
        host = socket.inet_ntop(socket.AF_INET, data[offset : offset + 4])
        offset += 4
    elif atyp == 4:
        host = socket.inet_ntop(socket.AF_INET6, data[offset : offset + 16])
        offset += 16
    elif atyp == 3:
        length = data[offset]
        offset += 1
        host = data[offset : offset + length].decode("idna")
        offset += length
    else:
        raise ValueError(f"unsupported SOCKS5 UDP address type {atyp}")
    port = struct.unpack("!H", data[offset : offset + 2])[0]
    offset += 2
    return host, port, data[offset:]


def percentile(sorted_values, rank):
    if not sorted_values:
        return None
    index = int(round((len(sorted_values) - 1) * rank))
    return sorted_values[index]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="mptunnel_udp")
    parser.add_argument("--proxy", default="127.0.0.1:1080")
    parser.add_argument("--target", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--payload-bytes", type=int, default=512)
    parser.add_argument("--timeout-ms", type=int, default=1500)
    parser.add_argument("--interval-ms", type=int, default=20)
    args = parser.parse_args()

    proxy_host, proxy_port = parse_host_port(args.proxy)
    target_host, target_port = parse_host_port(args.target)
    timeout = args.timeout_ms / 1000.0
    interval = args.interval_ms / 1000.0

    control = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    control.settimeout(timeout)
    control.sendall(b"\x05\x01\x00")
    if recv_exact(control, 2) != b"\x05\x00":
        raise OSError("SOCKS5 proxy did not accept no-auth negotiation")

    control.sendall(b"\x05\x03\x00" + encode_socks_addr("0.0.0.0", 0))
    header = recv_exact(control, 3)
    if header != b"\x05\x00\x00":
        raise OSError(f"SOCKS5 UDP ASSOCIATE failed: {header!r}")
    relay_host, relay_port = decode_socks_addr(control)
    if relay_host in ("0.0.0.0", "::"):
        relay_host = proxy_host

    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.settimeout(timeout)
    target_prefix = b"\x00\x00\x00" + encode_socks_addr(target_host, target_port)
    latencies = []
    received = 0

    body_len = max(4, args.payload_bytes)
    for index in range(args.count):
        payload = struct.pack("!I", index) + bytes([index % 251]) * (body_len - 4)
        started = time.monotonic()
        udp.sendto(target_prefix + payload, (relay_host, relay_port))
        deadline = started + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            udp.settimeout(remaining)
            try:
                packet, _ = udp.recvfrom(65535)
            except socket.timeout:
                break
            _, _, response = parse_udp_packet(packet)
            if response == payload:
                received += 1
                latencies.append((time.monotonic() - started) * 1000.0)
                break
        if interval > 0:
            time.sleep(interval)

    latencies.sort()
    result = {
        "case": args.label,
        "protocol": "udp",
        "status": "ok" if received == args.count else "loss",
        "target": args.target,
        "count": args.count,
        "received": received,
        "loss_rate": (args.count - received) / args.count if args.count else 0.0,
        "payload_bytes": args.payload_bytes,
        "min_ms": latencies[0] if latencies else None,
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "max_ms": latencies[-1] if latencies else None,
    }
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()

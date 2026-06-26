#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import sys
import time


def split_host_port(value):
    if value.startswith("["):
        host, _, rest = value[1:].partition("]")
        if not rest.startswith(":"):
            raise ValueError(f"missing port in {value!r}")
        return host, int(rest[1:])
    host, _, port = value.rpartition(":")
    if not host or not port:
        raise ValueError(f"missing host or port in {value!r}")
    return host, int(port)


def socks_target(host, port):
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        encoded = host.encode("idna")
        if len(encoded) > 255:
            raise ValueError("SOCKS target host is too long")
        return b"\x03" + bytes([len(encoded)]) + encoded + struct.pack("!H", port)
    if address.version == 4:
        return b"\x01" + address.packed + struct.pack("!H", port)
    return b"\x04" + address.packed + struct.pack("!H", port)


def read_exact(sock, size):
    chunks = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            raise RuntimeError("unexpected EOF")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def connect_socks5(proxy, target, timeout):
    proxy_host, proxy_port = split_host_port(proxy)
    target_host, target_port = split_host_port(target)
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    sock.settimeout(timeout)
    sock.sendall(b"\x05\x01\x00")
    response = read_exact(sock, 2)
    if response != b"\x05\x00":
        raise RuntimeError(f"SOCKS5 authentication failed: {response!r}")
    request = b"\x05\x01\x00" + socks_target(target_host, target_port)
    sock.sendall(request)
    header = read_exact(sock, 4)
    if header[0] != 5 or header[1] != 0:
        raise RuntimeError(f"SOCKS5 connect failed: {header!r}")
    atyp = header[3]
    if atyp == 1:
        read_exact(sock, 4)
    elif atyp == 3:
        length = read_exact(sock, 1)[0]
        read_exact(sock, length)
    elif atyp == 4:
        read_exact(sock, 16)
    else:
        raise RuntimeError(f"unknown SOCKS5 address type: {atyp}")
    read_exact(sock, 2)
    return sock, target_host, target_port


def parse_headers(buffer):
    head, _, body = buffer.partition(b"\r\n\r\n")
    status_line = head.splitlines()[0].decode("iso-8859-1", errors="replace")
    parts = status_line.split()
    if len(parts) < 2 or not parts[1].isdigit():
        raise RuntimeError(f"invalid HTTP response: {status_line!r}")
    content_length = None
    for line in head.splitlines()[1:]:
        key, sep, value = line.partition(b":")
        if sep and key.strip().lower() == b"content-length":
            content_length = int(value.strip())
    return int(parts[1]), content_length, body


def record_body_chunk(now, state, size):
    if size <= 0:
        return
    if state["first_body_at"] is None:
        state["first_body_at"] = now
    if state["last_body_at"] is not None:
        gap = now - state["last_body_at"]
        state["max_read_gap_s"] = max(state["max_read_gap_s"], gap)
        if now >= state["failover_after_s"] or state["last_body_at"] >= state["failover_after_s"]:
            state["recovery_gap_s"] = max(state["recovery_gap_s"], gap)
    state["last_body_at"] = now
    state["bytes"] += size


def download(args):
    started = time.monotonic()
    sock, target_host, target_port = connect_socks5(args.proxy, args.target, args.timeout)
    with sock:
        request = (
            f"GET {args.path} HTTP/1.1\r\n"
            f"Host: {target_host}:{target_port}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        sock.sendall(request)
        buffer = b""
        while b"\r\n\r\n" not in buffer:
            chunk = sock.recv(args.chunk_bytes)
            if not chunk:
                raise RuntimeError("EOF before HTTP headers")
            buffer += chunk
        status, content_length, body = parse_headers(buffer)
        state = {
            "bytes": 0,
            "first_body_at": None,
            "last_body_at": None,
            "max_read_gap_s": 0.0,
            "recovery_gap_s": 0.0,
            "failover_after_s": args.failover_after,
        }
        record_body_chunk(time.monotonic() - started, state, len(body))
        while content_length is None or state["bytes"] < content_length:
            chunk = sock.recv(args.chunk_bytes)
            if not chunk:
                break
            record_body_chunk(time.monotonic() - started, state, len(chunk))
    elapsed = time.monotonic() - started
    if content_length is not None and state["bytes"] != content_length:
        raise RuntimeError(f"incomplete body: {state['bytes']} of {content_length} bytes")
    return {
        "case": args.label,
        "protocol": "tcp",
        "status": "ok" if 200 <= status < 400 else "fail",
        "exit_code": 0 if 200 <= status < 400 else 22,
        "http_code": status,
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(state["bytes"] * 8 / elapsed / 1_000_000, 3) if elapsed > 0 else 0,
        "bytes": state["bytes"],
        "first_body_s": round(state["first_body_at"], 6) if state["first_body_at"] is not None else None,
        "max_read_gap_s": round(state["max_read_gap_s"], 6),
        "recovery_gap_s": round(state["recovery_gap_s"], 6),
        "failover_after_s": args.failover_after,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--proxy", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--path", default="/large.bin")
    parser.add_argument("--failover-after", type=float, required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    args = parser.parse_args()
    try:
        print(json.dumps(download(args), sort_keys=True))
    except Exception as exc:
        print(
            json.dumps(
                {
                    "case": args.label,
                    "protocol": "tcp",
                    "status": "fail",
                    "exit_code": 1,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

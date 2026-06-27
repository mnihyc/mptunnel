#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import sys
import threading
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


def write_started_file(path):
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{time.time():.9f}\n")


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
        if (
            state["failover_after_s"] >= 0
            and (
                now >= state["failover_after_s"]
                or state["last_body_at"] >= state["failover_after_s"]
            )
        ):
            state["recovery_gap_s"] = max(state["recovery_gap_s"], gap)
    state["last_body_at"] = now
    state["bytes"] += size


def interval_rates_mbps(interval_bytes, interval_seconds):
    if interval_seconds <= 0:
        return []
    if not interval_bytes:
        return []
    last_index = max(interval_bytes)
    rates = []
    for index in range(last_index + 1):
        rates.append(round(interval_bytes.get(index, 0) * 8 / interval_seconds / 1_000_000, 3))
    return rates


def record_interval_chunk(started, state, lock, size):
    if size <= 0:
        return
    now = time.monotonic()
    now_s = now - started
    interval = int(now_s // state["interval_seconds"])
    with lock:
        if state["first_body_at"] is None:
            state["first_body_at"] = now_s
        if state["last_body_at"] is not None:
            gap = now_s - state["last_body_at"]
            state["max_read_gap_s"] = max(state["max_read_gap_s"], gap)
            if (
                state["failover_after_s"] >= 0
                and (
                    now_s >= state["failover_after_s"]
                    or state["last_body_at"] >= state["failover_after_s"]
                )
            ):
                state["recovery_gap_s"] = max(state["recovery_gap_s"], gap)
        state["last_body_at"] = now_s
        state["bytes"] += size
        state["interval_bytes"][interval] = state["interval_bytes"].get(interval, 0) + size


def download_one_request(args, started, deadline, state, lock):
    sock, target_host, target_port = connect_socks5(args.proxy, args.target, args.timeout)
    sock.settimeout(min(args.timeout, 1.0))
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
            if time.monotonic() >= deadline:
                return False, None
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                raise RuntimeError("EOF before HTTP headers")
            buffer += chunk
        status, content_length, body = parse_headers(buffer)
        if not 200 <= status < 400:
            return False, status
        bytes_read = 0
        if body:
            bytes_read += len(body)
            record_interval_chunk(started, state, lock, len(body))
        while content_length is None or bytes_read < content_length:
            if time.monotonic() >= deadline:
                return False, status
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                break
            bytes_read += len(chunk)
            record_interval_chunk(started, state, lock, len(chunk))
    return content_length is None or bytes_read == content_length, status


def interval_download(args):
    started = time.monotonic()
    write_started_file(args.started_file)
    load_duration = args.load_duration if args.load_duration > 0 else args.timeout
    deadline = started + min(load_duration, args.timeout)
    state = {
        "bytes": 0,
        "first_body_at": None,
        "last_body_at": None,
        "max_read_gap_s": 0.0,
        "recovery_gap_s": 0.0,
        "failover_after_s": args.failover_after,
        "interval_seconds": args.interval_seconds,
        "interval_bytes": {},
        "requests": 0,
        "complete_requests": 0,
        "partial_requests": 0,
        "failures": 0,
    }
    lock = threading.Lock()

    def worker():
        while time.monotonic() < deadline:
            try:
                complete, status = download_one_request(args, started, deadline, state, lock)
                with lock:
                    state["requests"] += 1
                    if complete:
                        state["complete_requests"] += 1
                    elif status is not None:
                        state["partial_requests"] += 1
                    else:
                        state["failures"] += 1
            except Exception:
                with lock:
                    state["requests"] += 1
                    state["failures"] += 1
                time.sleep(0.05)

    threads = [
        threading.Thread(target=worker, daemon=True)
        for _ in range(max(1, args.parallel_downloads))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=max(0.1, deadline - time.monotonic() + 2.0))

    elapsed = time.monotonic() - started
    bytes_read = state["bytes"]
    status = "ok" if bytes_read > 0 and state["failures"] == 0 else "loss" if bytes_read > 0 else "fail"
    return {
        "case": args.label,
        "protocol": "tcp",
        "status": status,
        "exit_code": 0 if status != "fail" else 1,
        "http_code": 200 if bytes_read > 0 else None,
        "mode": "duration",
        "load_duration_s": round(load_duration, 6),
        "parallel_downloads": max(1, args.parallel_downloads),
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(bytes_read * 8 / elapsed / 1_000_000, 3) if elapsed > 0 else 0,
        "bytes": bytes_read,
        "content_length": None,
        "complete": bytes_read > 0,
        "requests": state["requests"],
        "complete_requests": state["complete_requests"],
        "partial_requests": state["partial_requests"],
        "failed_requests": state["failures"],
        "first_body_s": round(state["first_body_at"], 6) if state["first_body_at"] is not None else None,
        "max_read_gap_s": round(state["max_read_gap_s"], 6),
        "recovery_gap_s": round(state["recovery_gap_s"], 6),
        "failover_after_s": args.failover_after,
        "interval_seconds": args.interval_seconds,
        "interval_goodput_mbps": interval_rates_mbps(state["interval_bytes"], args.interval_seconds),
    }


def download(args):
    if args.load_duration > 0 or args.parallel_downloads > 1:
        return interval_download(args)

    started = time.monotonic()
    write_started_file(args.started_file)
    sock, target_host, target_port = connect_socks5(args.proxy, args.target, args.timeout)
    sock.settimeout(min(args.timeout, 1.0))
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
            if time.monotonic() - started >= args.timeout:
                raise RuntimeError("timeout before HTTP headers")
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
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
        timed_out = False
        while content_length is None or state["bytes"] < content_length:
            if time.monotonic() - started >= args.timeout:
                timed_out = True
                break
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                break
            record_body_chunk(time.monotonic() - started, state, len(chunk))
    elapsed = time.monotonic() - started
    complete = content_length is None or state["bytes"] == content_length
    ok = 200 <= status < 400 and complete
    error = None
    if timed_out:
        error = f"timeout after {state['bytes']} of {content_length} bytes"
    elif not complete:
        error = f"incomplete body: {state['bytes']} of {content_length} bytes"
    return {
        "case": args.label,
        "protocol": "tcp",
        "status": "ok" if ok else "fail",
        "exit_code": 0 if ok else (124 if timed_out else (18 if not complete else 22)),
        "http_code": status,
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(state["bytes"] * 8 / elapsed / 1_000_000, 3) if elapsed > 0 else 0,
        "bytes": state["bytes"],
        "content_length": content_length,
        "complete": complete,
        "error": error,
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
    parser.add_argument("--load-duration", type=float, default=0.0)
    parser.add_argument("--parallel-downloads", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--started-file")
    args = parser.parse_args()
    try:
        result = download(args)
        print(json.dumps(result, sort_keys=True))
        if result.get("status") == "fail":
            return int(result.get("exit_code") or 1)
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

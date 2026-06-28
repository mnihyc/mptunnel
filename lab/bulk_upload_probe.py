#!/usr/bin/env python3
import argparse
import ipaddress
import json
import select
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
    return sock


def connect_target(args):
    if args.proxy:
        return connect_socks5(args.proxy, args.target, args.timeout)
    target_host, target_port = split_host_port(args.target)
    sock = socket.create_connection((target_host, target_port), timeout=args.timeout)
    sock.settimeout(args.timeout)
    return sock


def interval_rates_mbps(interval_bytes, interval_seconds):
    if interval_seconds <= 0 or not interval_bytes:
        return []
    last_index = max(interval_bytes)
    return [
        round(interval_bytes.get(index, 0) * 8 / interval_seconds / 1_000_000, 3)
        for index in range(last_index + 1)
    ]


def record_interval_chunk(started, state, lock, size):
    if size <= 0:
        return
    now = time.monotonic()
    now_s = now - started
    interval = int(now_s // state["interval_seconds"])
    with lock:
        if state["first_write_at"] is None:
            state["first_write_at"] = now_s
        if state["last_write_at"] is not None:
            gap = now_s - state["last_write_at"]
            state["max_write_gap_s"] = max(state["max_write_gap_s"], gap)
            if (
                state["failover_after_s"] >= 0
                and (
                    now_s >= state["failover_after_s"]
                    or state["last_write_at"] >= state["failover_after_s"]
                )
            ):
                state["recovery_gap_s"] = max(state["recovery_gap_s"], gap)
        state["last_write_at"] = now_s
        state["bytes"] += size
        state["interval_bytes"][interval] = state["interval_bytes"].get(interval, 0) + size


def upload_one_stream(args, started, deadline, state, lock, payload):
    sock = connect_target(args)
    bytes_sent = 0
    with sock:
        sock.setblocking(False)
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            _, writable, _ = select.select([], [sock], [], min(remaining, 0.25))
            if not writable:
                continue
            chunk = memoryview(payload)[: max(1, min(len(payload), args.chunk_bytes))]
            try:
                sent = sock.send(chunk)
            except BlockingIOError:
                continue
            except OSError:
                if time.monotonic() >= deadline and bytes_sent > 0:
                    break
                raise
            if sent <= 0:
                break
            bytes_sent += sent
            record_interval_chunk(started, state, lock, sent)
        sock.setblocking(True)
        sock.settimeout(1.0)
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            return bytes_sent > 0
        try:
            sock.settimeout(1.0)
            sock.recv(128)
        except OSError:
            pass
    return bytes_sent > 0


def interval_upload(args):
    started = time.monotonic()
    write_started_file(args.started_file)
    load_duration = args.load_duration if args.load_duration > 0 else args.timeout
    deadline = started + min(load_duration, args.timeout)
    state = {
        "bytes": 0,
        "first_write_at": None,
        "last_write_at": None,
        "max_write_gap_s": 0.0,
        "recovery_gap_s": 0.0,
        "failover_after_s": args.failover_after,
        "interval_seconds": args.interval_seconds,
        "interval_bytes": {},
        "streams": 0,
        "complete_streams": 0,
        "failures": 0,
    }
    lock = threading.Lock()
    payload = bytes([index % 251 for index in range(max(1, args.chunk_bytes))])

    def worker():
        try:
            complete = upload_one_stream(args, started, deadline, state, lock, payload)
            with lock:
                state["streams"] += 1
                if complete:
                    state["complete_streams"] += 1
                else:
                    state["failures"] += 1
        except Exception:
            with lock:
                state["streams"] += 1
                state["failures"] += 1

    threads = [
        threading.Thread(target=worker, daemon=True)
        for _ in range(max(1, args.parallel_uploads))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=max(0.1, deadline - time.monotonic() + 2.0))

    elapsed = time.monotonic() - started
    bytes_sent = state["bytes"]
    status = (
        "ok"
        if bytes_sent > 0 and state["failures"] == 0
        else "loss"
        if bytes_sent > 0
        else "fail"
    )
    goodput = bytes_sent * 8 / elapsed / 1_000_000 if elapsed > 0 else 0.0
    return {
        "case": args.label,
        "protocol": args.protocol,
        "status": status,
        "exit_code": 0 if status != "fail" else 1,
        "mode": "duration-upload",
        "load_duration_s": round(load_duration, 6),
        "parallel_uploads": max(1, args.parallel_uploads),
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(goodput, 3),
        "upload_goodput_mbps": round(goodput, 3),
        "bytes": bytes_sent,
        "complete": bytes_sent > 0,
        "streams": state["streams"],
        "complete_streams": state["complete_streams"],
        "failed_streams": state["failures"],
        "first_write_s": round(state["first_write_at"], 6)
        if state["first_write_at"] is not None
        else None,
        "max_write_gap_s": round(state["max_write_gap_s"], 6),
        "recovery_gap_s": round(state["recovery_gap_s"], 6),
        "failover_after_s": args.failover_after,
        "interval_seconds": args.interval_seconds,
        "interval_goodput_mbps": interval_rates_mbps(
            state["interval_bytes"], args.interval_seconds
        ),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--protocol", default="tcp-upload")
    parser.add_argument("--proxy")
    parser.add_argument("--target", required=True)
    parser.add_argument("--failover-after", type=float, required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--load-duration", type=float, default=30.0)
    parser.add_argument("--parallel-uploads", type=int, default=1)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--started-file")
    args = parser.parse_args()
    try:
        result = interval_upload(args)
        print(json.dumps(result, sort_keys=True))
        if result.get("status") == "fail":
            return int(result.get("exit_code") or 1)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "case": args.label,
                    "protocol": args.protocol,
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

#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import sys
import threading
import time

DEFAULT_INTERVAL_SECONDS = 0.2
INTERVAL_TRIM_DISCARD_EACH_END = 3


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


def read_failover_marker_elapsed(path, started):
    if not path:
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            marked_at = float(handle.read().strip())
    except (FileNotFoundError, OSError, ValueError):
        return None
    return max(0.0, marked_at - started)


def watch_failover_marker(path, started, deadline, state, lock, interval=0.02):
    while time.monotonic() < deadline:
        elapsed = read_failover_marker_elapsed(path, started)
        if elapsed is not None:
            with lock:
                state["failover_after_s"] = elapsed
                state["failover_trigger_source"] = "marker"
            return
        time.sleep(interval)


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


def connect_http(args):
    if args.proxy:
        return connect_socks5(args.proxy, args.target, args.timeout)
    target_host, target_port = split_host_port(args.target)
    sock = socket.create_connection((target_host, target_port), timeout=args.timeout)
    sock.settimeout(args.timeout)
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


def interval_metric_fields(interval_bytes, interval_seconds):
    if interval_seconds <= 0 or not interval_bytes:
        raw = []
    else:
        raw = [
            round(interval_bytes.get(index, 0) * 8 / interval_seconds / 1_000_000, 3)
            for index in range(max(interval_bytes) + 1)
        ]
    trimmed = (
        raw[INTERVAL_TRIM_DISCARD_EACH_END:-INTERVAL_TRIM_DISCARD_EACH_END]
        if len(raw) > INTERVAL_TRIM_DISCARD_EACH_END * 2
        else []
    )
    fields = {
        "interval_seconds": interval_seconds,
        "interval_trim_discard_each_end": INTERVAL_TRIM_DISCARD_EACH_END,
        "interval_goodput_raw_mbps": raw,
        "interval_goodput_mbps": trimmed,
        "interval_goodput_avg_mbps": None,
        "interval_goodput_max_mbps": None,
        "interval_goodput_min_mbps": None,
    }
    if trimmed:
        fields["interval_goodput_avg_mbps"] = round(sum(trimmed) / len(trimmed), 3)
        fields["interval_goodput_max_mbps"] = max(trimmed)
        fields["interval_goodput_min_mbps"] = min(trimmed)
    return fields


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
    sock, target_host, target_port = connect_http(args)
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
                return False, None, "deadline"
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                raise RuntimeError("EOF before HTTP headers")
            buffer += chunk
        status, content_length, body = parse_headers(buffer)
        if not 200 <= status < 400:
            return False, status, "http_status"
        bytes_read = 0
        if body:
            bytes_read += len(body)
            record_interval_chunk(started, state, lock, len(body))
        while content_length is None or bytes_read < content_length:
            if time.monotonic() >= deadline:
                return False, status, "deadline"
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                break
            bytes_read += len(chunk)
            record_interval_chunk(started, state, lock, len(chunk))
    complete = content_length is None or bytes_read == content_length
    return complete, status, "complete" if complete else "eof"


def run_download_worker(
    args,
    started,
    deadline,
    state,
    lock,
    download_request=download_one_request,
):
    """Run one fixed request or replenish requests for a duration cohort."""
    fixed = args.request_lifecycle == "fixed"
    while time.monotonic() < deadline:
        with lock:
            state["request_attempts_started"] += 1
        try:
            complete, status, termination = download_request(
                args, started, deadline, state, lock
            )
            ended_before_deadline = time.monotonic() < deadline
            with lock:
                state["requests"] += 1
                if complete:
                    state["complete_requests"] += 1
                elif status is not None:
                    state["partial_requests"] += 1
                else:
                    state["failures"] += 1
                if fixed and termination != "deadline" and ended_before_deadline:
                    state["early_terminations"] += 1
        except Exception:
            with lock:
                state["requests"] += 1
                state["failures"] += 1
                if fixed and time.monotonic() < deadline:
                    state["early_terminations"] += 1
            if not fixed:
                time.sleep(0.05)
        if fixed:
            return


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
        "failover_trigger_source": "fixed" if args.failover_after >= 0 else "pending",
        "interval_seconds": args.interval_seconds,
        "interval_bytes": {},
        "requests": 0,
        "request_attempts_started": 0,
        "complete_requests": 0,
        "partial_requests": 0,
        "failures": 0,
        "early_terminations": 0,
    }
    lock = threading.Lock()

    marker_thread = None
    if args.failover_marker_file:
        marker_thread = threading.Thread(
            target=watch_failover_marker,
            args=(args.failover_marker_file, started, deadline, state, lock),
            daemon=True,
        )
        marker_thread.start()

    worker_count = max(1, args.parallel_downloads)
    threads = [
        threading.Thread(
            target=run_download_worker,
            args=(args, started, deadline, state, lock),
            daemon=True,
        )
        for _ in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=max(0.1, deadline - time.monotonic() + 2.0))
    if marker_thread is not None:
        marker_thread.join(timeout=0.1)

    elapsed = time.monotonic() - started
    with lock:
        bytes_read = state["bytes"]
        failover_after_s = state["failover_after_s"]
        failover_trigger_source = state["failover_trigger_source"]
    replacement_requests = max(0, state["request_attempts_started"] - worker_count)
    fixed_contract_ok = (
        state["request_attempts_started"] == worker_count
        and replacement_requests == 0
        and state["failures"] == 0
        and state["early_terminations"] == 0
    )
    successful = state["failures"] == 0
    if args.request_lifecycle == "fixed":
        successful = successful and fixed_contract_ok
    status = "ok" if bytes_read > 0 and successful else "loss" if bytes_read > 0 else "fail"
    result = {
        "case": args.label,
        "protocol": args.protocol,
        "status": status,
        "exit_code": 0 if status != "fail" and successful else 1,
        "http_code": 200 if bytes_read > 0 else None,
        "mode": "duration",
        "request_lifecycle": args.request_lifecycle,
        "load_duration_s": round(load_duration, 6),
        "parallel_downloads": worker_count,
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(bytes_read * 8 / elapsed / 1_000_000, 3) if elapsed > 0 else 0,
        "bytes": bytes_read,
        "content_length": None,
        "complete": bytes_read > 0,
        "requests": state["requests"],
        "request_attempts_started": state["request_attempts_started"],
        "target_request_attempts": worker_count if args.request_lifecycle == "fixed" else None,
        "replacement_requests": replacement_requests,
        "early_terminations": state["early_terminations"],
        "complete_requests": state["complete_requests"],
        "partial_requests": state["partial_requests"],
        "failed_requests": state["failures"],
        "first_body_s": round(state["first_body_at"], 6) if state["first_body_at"] is not None else None,
        "max_read_gap_s": round(state["max_read_gap_s"], 6),
        "recovery_gap_s": round(state["recovery_gap_s"], 6),
        "failover_after_s": round(failover_after_s, 6),
        "failover_trigger_source": failover_trigger_source,
    }
    result.update(interval_metric_fields(state["interval_bytes"], args.interval_seconds))
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--protocol", default="tcp")
    parser.add_argument("--proxy")
    parser.add_argument("--target", required=True)
    parser.add_argument("--path", default="/large.bin")
    parser.add_argument("--failover-after", type=float, required=True)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--load-duration", type=float, default=30.0)
    parser.add_argument("--parallel-downloads", type=int, default=1)
    parser.add_argument(
        "--request-lifecycle",
        choices=("duration", "fixed"),
        default="duration",
        help="replenish requests until the deadline or make one request per worker",
    )
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--started-file")
    parser.add_argument("--failover-marker-file")
    args = parser.parse_args()
    try:
        result = interval_download(args)
        print(json.dumps(result, sort_keys=True))
        if result.get("exit_code"):
            return int(result["exit_code"])
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

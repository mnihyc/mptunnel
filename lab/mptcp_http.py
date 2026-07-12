#!/usr/bin/env python3
import argparse
import json
import os
import socket
import sys
import threading
import time

IPPROTO_MPTCP = getattr(socket, "IPPROTO_MPTCP", 262)
DEFAULT_INTERVAL_SECONDS = 0.2
INTERVAL_TRIM_DISCARD_EACH_END = 3


def split_host_port(value):
    host, _, port = value.rpartition(":")
    if not host or not port:
        raise ValueError(f"missing host or port in {value!r}")
    return host, int(port)


def mptcp_socket():
    return socket.socket(socket.AF_INET, socket.SOCK_STREAM, IPPROTO_MPTCP)


def check_support():
    sock = mptcp_socket()
    sock.close()


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


def serve_client(conn, file_path):
    with conn:
        request = b""
        while b"\r\n\r\n" not in request:
            chunk = conn.recv(4096)
            if not chunk:
                return
            request += chunk
        size = os.path.getsize(file_path)
        headers = (
            "HTTP/1.1 200 OK\r\n"
            f"Content-Length: {size}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        try:
            conn.sendall(headers)
        except OSError:
            return
        with open(file_path, "rb") as handle:
            while True:
                chunk = handle.read(256 * 1024)
                if not chunk:
                    break
                try:
                    conn.sendall(chunk)
                except OSError:
                    return


def serve(args):
    check_support()
    host, port = split_host_port(args.bind)
    sock = mptcp_socket()
    with sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
        sock.listen(64)
        while True:
            conn, _ = sock.accept()
            thread = threading.Thread(target=serve_client, args=(conn, args.file), daemon=True)
            thread.start()


def download_once(args, started, deadline, state, lock):
    host, port = split_host_port(args.target)
    sock = mptcp_socket()
    sock.settimeout(min(args.timeout, 1.0))
    with sock:
        sock.connect((host, port))
        request = (
            f"GET {args.path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
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
        if not 200 <= status < 300:
            return False, status, "http_status"
        request_bytes = 0
        if body:
            request_bytes += len(body)
            record_chunk(started, state, lock, len(body))
        while content_length is None or request_bytes < content_length:
            if time.monotonic() >= deadline:
                return False, status, "deadline"
            try:
                chunk = sock.recv(args.chunk_bytes)
            except socket.timeout:
                continue
            if not chunk:
                break
            request_bytes += len(chunk)
            record_chunk(started, state, lock, len(chunk))
    complete = content_length is None or request_bytes == content_length
    return complete, status, "complete" if complete else "eof"


def record_chunk(started, state, lock, size):
    if size <= 0:
        return
    now_s = time.monotonic() - started
    with lock:
        interval = int(now_s // state["interval_seconds"])
        if state["first_body_s"] is None:
            state["first_body_s"] = now_s
        if state["last_body_s"] is not None:
            gap = now_s - state["last_body_s"]
            state["max_read_gap_s"] = max(state["max_read_gap_s"], gap)
        state["last_body_s"] = now_s
        state["bytes"] += size
        state["interval_bytes"][interval] = (
            state["interval_bytes"].get(interval, 0) + size
        )


def run_download_worker(
    args,
    started,
    deadline,
    state,
    lock,
    download_request=None,
):
    if download_request is None:
        download_request = download_once
    while time.monotonic() < deadline:
        with lock:
            state["requests"] += 1
        try:
            done, status, termination = download_request(
                args, started, deadline, state, lock
            )
        except Exception as exc:
            with lock:
                state["errors"].append(str(exc))
                state["failed"] += 1
            return
        ended_at_deadline = time.monotonic() >= deadline
        with lock:
            state["last_status"] = status
            if done:
                state["complete"] += 1
            elif termination == "http_status":
                state["failed"] += 1
            else:
                state["partial"] += 1
                duration_cutoff = (
                    state["deadline_reason"] == "duration"
                    and (termination == "deadline" or ended_at_deadline)
                )
                if duration_cutoff:
                    state["duration_end_partial"] += 1
                else:
                    state["premature_partial"] += 1
        if not done:
            return


def download(args):
    check_support()
    started = time.monotonic()
    duration = args.load_duration if args.load_duration > 0 else args.timeout
    deadline = started + min(duration, args.timeout)
    deadline_reason = (
        "duration"
        if args.load_duration <= 0 or args.load_duration <= args.timeout
        else "timeout"
    )
    state = {
        "bytes": 0,
        "first_body_s": None,
        "last_body_s": None,
        "max_read_gap_s": 0.0,
        "interval_seconds": args.interval_seconds,
        "interval_bytes": {},
        "requests": 0,
        "complete": 0,
        "partial": 0,
        "duration_end_partial": 0,
        "premature_partial": 0,
        "failed": 0,
        "deadline_reason": deadline_reason,
        "last_status": None,
        "errors": [],
    }
    lock = threading.Lock()
    worker_count = max(1, args.parallel_downloads)
    # Independent MPTCP connections make the baseline's concurrency match the
    # product cohorts without changing how one MPTCP connection uses subflows.
    threads = [
        threading.Thread(
            target=run_download_worker,
            args=(args, started, deadline, state, lock),
            daemon=False,
        )
        for _ in range(worker_count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    elapsed = time.monotonic() - started
    error = state["errors"][0] if state["errors"] else None
    successful = (
        state["bytes"] > 0
        and error is None
        and state["premature_partial"] == 0
        and state["failed"] == 0
    )
    status = "ok" if successful else "loss" if state["bytes"] > 0 else "fail"
    row = {
        "case": args.label,
        "protocol": "mptcp",
        "status": status,
        "exit_code": 0 if status == "ok" else 1,
        "target": args.target,
        "http_code": state["last_status"],
        "bytes": state["bytes"],
        "time_s": elapsed,
        "load_duration_s": args.load_duration,
        "parallel_downloads": worker_count,
        "requests": state["requests"],
        "complete_requests": state["complete"],
        "partial_requests": state["partial"],
        "duration_end_partial_requests": state["duration_end_partial"],
        "premature_partial_requests": state["premature_partial"],
        "failed_requests": state["failed"],
        "deadline_reason": deadline_reason,
        "complete": successful,
        "goodput_mbps": state["bytes"] * 8 / elapsed / 1_000_000 if elapsed > 0 else 0.0,
        "first_body_s": state["first_body_s"],
        "max_read_gap_s": state["max_read_gap_s"],
    }
    row.update(interval_metric_fields(state["interval_bytes"], args.interval_seconds))
    if error:
        row["error"] = error
    print(json.dumps(row, separators=(",", ":")))
    return row["exit_code"]


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check")
    server = sub.add_parser("serve")
    server.add_argument("--bind", required=True)
    server.add_argument("--file", required=True)
    client = sub.add_parser("download")
    client.add_argument("--label", required=True)
    client.add_argument("--target", required=True)
    client.add_argument("--path", default="/large.bin")
    client.add_argument("--timeout", type=float, default=120.0)
    client.add_argument("--load-duration", type=float, default=30.0)
    client.add_argument("--parallel-downloads", type=int, default=1)
    client.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    client.add_argument("--chunk-bytes", type=int, default=256 * 1024)
    args = parser.parse_args()
    if args.command == "check":
        check_support()
        return 0
    if args.command == "serve":
        serve(args)
        return 0
    if args.command == "download":
        return download(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())

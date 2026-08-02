#!/usr/bin/env python3
import argparse
import concurrent.futures
import ipaddress
import json
import os
import socket
import struct
import sys
import threading
import time
from collections import Counter

DEFAULT_INTERVAL_SECONDS = 0.2
INTERVAL_TRIM_DISCARD_EACH_END = 3
FLOAT_TIME_EPSILON_SECONDS = 1e-9


def attempt_has_response_budget(now, workload_deadline_at, timeout):
    return now + timeout <= workload_deadline_at + FLOAT_TIME_EPSILON_SECONDS


def small_http_response_budget_seconds(args):
    return min(args.timeout, args.small_response_budget_ms / 1000.0)


def browser_full_load_response_timeout_seconds(args):
    """Keep saturation completion independent from the periodic batch SLA."""
    return args.timeout


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


def connect_socks5(proxy, target, timeout):
    proxy_host, proxy_port = parse_host_port(proxy)
    target_host, target_port = parse_host_port(target)
    sock = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
    sock.settimeout(timeout)
    sock.sendall(b"\x05\x01\x00")
    response = recv_exact(sock, 2)
    if response != b"\x05\x00":
        raise OSError(f"SOCKS5 auth failed: {response!r}")
    sock.sendall(b"\x05\x01\x00" + encode_socks_addr(target_host, target_port))
    header = recv_exact(sock, 4)
    if header[0] != 5 or header[1] != 0:
        raise OSError(f"SOCKS5 connect failed: {header!r}")
    atyp = header[3]
    if atyp == 1:
        recv_exact(sock, 4)
    elif atyp == 4:
        recv_exact(sock, 16)
    elif atyp == 3:
        recv_exact(sock, recv_exact(sock, 1)[0])
    else:
        raise OSError(f"unsupported SOCKS5 address type {atyp}")
    recv_exact(sock, 2)
    return sock, target_host, target_port


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


def percentile(values, rank):
    if not values:
        return None
    ordered = sorted(values)
    index = int(round((len(ordered) - 1) * rank))
    return ordered[index]


def interval_metric_fields(interval_bytes, interval_seconds, prefix):
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
    name = f"{prefix}_interval"
    fields = {
        f"{name}_seconds": interval_seconds,
        f"{name}_trim_discard_each_end": INTERVAL_TRIM_DISCARD_EACH_END,
        f"{name}_goodput_raw_mbps": raw,
        f"{name}_goodput_mbps": trimmed,
        f"{name}_goodput_avg_mbps": None,
        f"{name}_goodput_max_mbps": None,
        f"{name}_goodput_min_mbps": None,
    }
    if trimmed:
        fields[f"{name}_goodput_avg_mbps"] = round(sum(trimmed) / len(trimmed), 3)
        fields[f"{name}_goodput_max_mbps"] = max(trimmed)
        fields[f"{name}_goodput_min_mbps"] = min(trimmed)
    return fields


def write_started_file(path):
    if not path:
        return
    unix_ns = time.time_ns()
    monotonic_ms = time.monotonic_ns() // 1_000_000
    temporary_path = f"{path}.tmp-{os.getpid()}"
    with open(temporary_path, "w", encoding="utf-8") as handle:
        # Keep the first-line Unix timestamp compatible with existing callers.
        handle.write(f"{unix_ns / 1_000_000_000:.9f}\n")
        handle.write(f"{monotonic_ms}\n")
        handle.write(f"{unix_ns // 1_000_000}\n")
    os.replace(temporary_path, path)


def workload_deadline(started_at, args):
    duration = args.load_duration if args.load_duration > 0 else args.timeout
    return started_at + min(duration, args.timeout)


def parse_http_headers(buffer):
    head, _, body = buffer.partition(b"\r\n\r\n")
    status_line = head.splitlines()[0].decode("iso-8859-1", errors="replace")
    parts = status_line.split()
    if len(parts) < 2 or not parts[1].isdigit():
        raise OSError(f"invalid HTTP response: {status_line!r}")
    content_length = None
    for line in head.splitlines()[1:]:
        key, sep, value = line.partition(b":")
        if sep and key.strip().lower() == b"content-length":
            content_length = int(value.strip())
    return int(parts[1]), content_length, body


def connect_target(args, target, timeout):
    if args.mode == "direct":
        target_host, target_port = parse_host_port(target)
        sock = socket.create_connection((target_host, target_port), timeout=timeout)
        sock.settimeout(timeout)
        return sock, target_host, target_port
    return connect_socks5(args.proxy, target, timeout)


def http_get(args, target, path, timeout, chunk_bytes, connected=None):
    started = time.monotonic()
    sock, target_host, target_port = connect_target(args, target, timeout)
    if connected is not None:
        connected()
    with sock:
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {target_host}:{target_port}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).encode("ascii")
        sock.sendall(request)
        buffer = b""
        while b"\r\n\r\n" not in buffer:
            chunk = sock.recv(chunk_bytes)
            if not chunk:
                raise OSError("EOF before HTTP headers")
            buffer += chunk
        status, content_length, body = parse_http_headers(buffer)
        body_bytes = len(body)
        while content_length is None or body_bytes < content_length:
            chunk = sock.recv(chunk_bytes)
            if not chunk:
                break
            body_bytes += len(chunk)
    elapsed = time.monotonic() - started
    if content_length is not None and body_bytes != content_length:
        raise OSError(f"incomplete HTTP body: {body_bytes} of {content_length}")
    return status, body_bytes, elapsed


def concurrent_http_get(start_barrier, args, target, path, timeout, chunk_bytes):
    start_barrier.wait(timeout=timeout)
    return http_get(args, target, path, timeout, chunk_bytes)


def bulk_worker(args, started_at, interactive_ready, bulk_ready, result):
    try:
        interactive_ready.wait(timeout=min(args.timeout, 5.0))
        bytes_read = 0
        first_body_s = None
        last_body_s = None
        max_read_gap_s = 0.0
        recovery_gap_s = 0.0
        bytes_at_failover = None
        max_gap_start_s = None
        max_gap_end_s = None
        max_gap_start_bytes = None
        max_gap_end_bytes = None
        recovery_gap_start_s = None
        recovery_gap_end_s = None
        recovery_gap_start_bytes = None
        recovery_gap_end_bytes = None
        interval_bytes = {}
        requests = 0
        complete_requests = 0
        partial_requests = 0
        last_status = None
        last_content_length = None
        deadline = workload_deadline(started_at, args)

        def record_chunk(size):
            nonlocal bytes_read, first_body_s, last_body_s, max_read_gap_s, recovery_gap_s
            nonlocal bytes_at_failover
            nonlocal max_gap_start_s, max_gap_end_s, max_gap_start_bytes, max_gap_end_bytes
            nonlocal recovery_gap_start_s, recovery_gap_end_s
            nonlocal recovery_gap_start_bytes, recovery_gap_end_bytes
            if size <= 0:
                return
            now_s = time.monotonic() - started_at
            interval = int(now_s // args.interval_seconds)
            interval_bytes[interval] = interval_bytes.get(interval, 0) + size
            if first_body_s is None:
                first_body_s = now_s
                bulk_ready.set()
            if (
                args.failover_after >= 0
                and bytes_at_failover is None
                and now_s >= args.failover_after
            ):
                bytes_at_failover = bytes_read
            if last_body_s is not None:
                gap = now_s - last_body_s
                if gap > max_read_gap_s:
                    max_read_gap_s = gap
                    max_gap_start_s = last_body_s
                    max_gap_end_s = now_s
                    max_gap_start_bytes = bytes_read
                    max_gap_end_bytes = bytes_read + size
                if (
                    args.failover_after >= 0
                    and (now_s >= args.failover_after or last_body_s >= args.failover_after)
                ):
                    if gap > recovery_gap_s:
                        recovery_gap_s = gap
                        recovery_gap_start_s = last_body_s
                        recovery_gap_end_s = now_s
                        recovery_gap_start_bytes = bytes_read
                        recovery_gap_end_bytes = bytes_read + size
            last_body_s = now_s
            bytes_read += size

        while True:
            if time.monotonic() >= deadline:
                break
            requests += 1
            sock, target_host, target_port = connect_target(
                args, args.http_target, args.timeout
            )
            sock.settimeout(min(args.timeout, 1.0))
            with sock:
                request = (
                    f"GET {args.bulk_path} HTTP/1.1\r\n"
                    f"Host: {target_host}:{target_port}\r\n"
                    "Connection: close\r\n"
                    "\r\n"
                ).encode("ascii")
                sock.sendall(request)
                buffer = b""
                while b"\r\n\r\n" not in buffer:
                    if time.monotonic() >= deadline:
                        partial_requests += 1
                        break
                    try:
                        chunk = sock.recv(args.chunk_bytes)
                    except socket.timeout:
                        continue
                    if not chunk:
                        raise OSError("EOF before HTTP headers")
                    buffer += chunk
                if b"\r\n\r\n" not in buffer:
                    break
                status, content_length, body = parse_http_headers(buffer)
                last_status = status
                last_content_length = content_length
                request_bytes = 0
                record_chunk(len(body))
                request_bytes += len(body)
                while content_length is None or request_bytes < content_length:
                    if time.monotonic() >= deadline:
                        partial_requests += 1
                        break
                    try:
                        chunk = sock.recv(args.chunk_bytes)
                    except socket.timeout:
                        continue
                    if not chunk:
                        break
                    request_bytes += len(chunk)
                    record_chunk(len(chunk))
                else:
                    complete_requests += 1
        elapsed = time.monotonic() - started_at
        if last_body_s is not None:
            terminal_gap = max(0.0, elapsed - last_body_s)
            if terminal_gap > max_read_gap_s:
                max_read_gap_s = terminal_gap
                max_gap_start_s = last_body_s
                max_gap_end_s = elapsed
                max_gap_start_bytes = bytes_read
                max_gap_end_bytes = bytes_read
            if (
                args.failover_after >= 0
                and elapsed >= args.failover_after
                and terminal_gap > recovery_gap_s
            ):
                recovery_gap_s = terminal_gap
                recovery_gap_start_s = last_body_s
                recovery_gap_end_s = elapsed
                recovery_gap_start_bytes = bytes_read
                recovery_gap_end_bytes = bytes_read
        fixed_complete = (
            last_content_length is None
            or complete_requests > 0
            or bytes_read > 0
        )
        result.update(
            {
                "bulk_status": "ok" if bytes_read > 0 and (last_status is None or 200 <= last_status < 400) else "fail",
                "bulk_http_code": last_status,
                "bulk_content_length": last_content_length,
                "bulk_bytes": bytes_read,
                "bulk_time_s": elapsed,
                "bulk_load_duration_s": args.load_duration,
                "bulk_requests": requests,
                "bulk_complete_requests": complete_requests,
                "bulk_partial_requests": partial_requests,
                "bulk_goodput_mbps": bytes_read * 8 / elapsed / 1_000_000
                if elapsed > 0
                else 0.0,
                "bulk_first_body_s": first_body_s,
                "bulk_bytes_at_failover": bytes_at_failover,
                "bulk_max_read_gap_s": max_read_gap_s,
                "bulk_max_gap_start_s": max_gap_start_s,
                "bulk_max_gap_end_s": max_gap_end_s,
                "bulk_max_gap_start_bytes": max_gap_start_bytes,
                "bulk_max_gap_end_bytes": max_gap_end_bytes,
                "bulk_recovery_gap_s": recovery_gap_s,
                "bulk_recovery_gap_start_s": recovery_gap_start_s,
                "bulk_recovery_gap_end_s": recovery_gap_end_s,
                "bulk_recovery_gap_start_bytes": recovery_gap_start_bytes,
                "bulk_recovery_gap_end_bytes": recovery_gap_end_bytes,
            }
        )
        result.update(
            interval_metric_fields(
                interval_bytes,
                args.interval_seconds,
                prefix="bulk",
            )
        )
    except Exception as exc:
        result.update({"bulk_status": "fail", "bulk_error": str(exc)})
    finally:
        bulk_ready.set()


def small_http_worker(args, started_at, bulk_ready, result):
    latencies = []
    failures = 0
    failure_reasons = Counter()
    attempts = 0
    response_bytes = 0
    batch_latencies = []
    batch_deadline_misses = 0
    batch_sizes = []
    batch_start_offsets_ms = []
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    worker_started = time.monotonic()
    deadline = workload_deadline(started_at, args)
    response_budget = small_http_response_budget_seconds(args)
    next_batch_at = worker_started
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.small_batch_size
    ) as executor:
        while time.monotonic() < deadline:
            if args.small_batch_period_ms > 0:
                sleep_s = min(
                    max(0.0, next_batch_at - time.monotonic()),
                    max(0.0, deadline - time.monotonic()),
                )
                if sleep_s > 0:
                    time.sleep(sleep_s)
            batch_started = time.monotonic()
            if not attempt_has_response_budget(
                batch_started, deadline, response_budget
            ):
                break
            batch_size = args.small_batch_size
            batch_sizes.append(batch_size)
            batch_start_offsets_ms.append((batch_started - worker_started) * 1000.0)
            attempts += batch_size
            remaining = max(
                0.1,
                min(args.timeout, response_budget, deadline - batch_started),
            )
            start_barrier = threading.Barrier(batch_size)
            futures = [
                executor.submit(
                    concurrent_http_get,
                    start_barrier,
                    args,
                    args.http_target,
                    args.small_path,
                    remaining,
                    args.chunk_bytes,
                )
                for _ in range(batch_size)
            ]
            for future in futures:
                try:
                    status, body_bytes, elapsed = future.result()
                    if not 200 <= status < 400:
                        failures += 1
                        failure_reasons[f"http_status_{status}"] += 1
                    else:
                        response_bytes += body_bytes
                        latencies.append(elapsed * 1000.0)
                except Exception as exc:
                    failures += 1
                    failure_reasons[f"{type(exc).__name__}: {exc}"] += 1
            batch_elapsed_ms = (time.monotonic() - batch_started) * 1000.0
            batch_latencies.append(batch_elapsed_ms)
            if batch_elapsed_ms > args.small_response_budget_ms:
                batch_deadline_misses += 1

            if args.small_batch_period_ms > 0:
                next_batch_at += args.small_batch_period_ms / 1000.0
                sleep_s = 0.0
            else:
                sleep_s = min(
                    args.small_interval_ms / 1000.0,
                    max(0.0, deadline - time.monotonic()),
                )
            if sleep_s > 0:
                time.sleep(sleep_s)
    result.update(
        {
            "small_start_s": worker_started - started_at,
            "small_time_s": time.monotonic() - worker_started,
            "small_count": attempts,
            "small_ok": len(latencies),
            "small_fail": failures,
            "small_failure_reasons": dict(sorted(failure_reasons.items())),
            "small_response_bytes": response_bytes,
            "small_batch_size": args.small_batch_size,
            "small_batch_period_ms": args.small_batch_period_ms,
            "small_response_budget_ms": args.small_response_budget_ms,
            "small_batch_count": len(batch_latencies),
            "small_batch_sizes": batch_sizes,
            "small_batch_start_offsets_ms": [
                round(value, 3) for value in batch_start_offsets_ms
            ],
            "small_batch_start_intervals_ms": [
                round(right - left, 3)
                for left, right in zip(
                    batch_start_offsets_ms, batch_start_offsets_ms[1:]
                )
            ],
            "small_batch_deadline_misses": batch_deadline_misses,
            "small_batch_p50_ms": percentile(batch_latencies, 0.50),
            "small_batch_p95_ms": percentile(batch_latencies, 0.95),
            "small_batch_max_ms": max(batch_latencies)
            if batch_latencies
            else None,
            "small_p50_ms": percentile(latencies, 0.50),
            "small_p95_ms": percentile(latencies, 0.95),
            "small_max_ms": max(latencies) if latencies else None,
        }
    )


def browser_full_load_worker(args, started_at, bulk_ready, result):
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    worker_started = time.monotonic()
    admission_deadline = workload_deadline(started_at, args)
    # Periodic browser batches have a per-batch deadline. Saturation instead
    # keeps exactly `concurrency` requests in flight for the admission window,
    # then lets every accepted request finish under the general probe timeout.
    # Reusing the batch deadline here would manufacture rejected requests when
    # a healthy, progressing transfer outlives one batch period.
    response_timeout = browser_full_load_response_timeout_seconds(args)
    concurrency = args.small_batch_size
    start_barrier = threading.Barrier(concurrency)
    active_lock = threading.Lock()
    active = 0
    peak_active = 0

    def run_slot():
        nonlocal active, peak_active
        started = 0
        accepted = 0
        completed = 0
        response_bytes = 0
        latencies = []
        failures = Counter()
        start_barrier.wait(timeout=response_timeout)
        while time.monotonic() < admission_deadline:
            started += 1
            with active_lock:
                active += 1
                peak_active = max(peak_active, active)

            def mark_accepted():
                nonlocal accepted
                accepted += 1

            try:
                status, body_bytes, elapsed = http_get(
                    args,
                    args.http_target,
                    args.small_path,
                    response_timeout,
                    args.chunk_bytes,
                    connected=mark_accepted,
                )
                if not 200 <= status < 400:
                    failures[f"http_status_{status}"] += 1
                else:
                    completed += 1
                    response_bytes += body_bytes
                    latencies.append(elapsed * 1000.0)
            except Exception as exc:
                failures[f"{type(exc).__name__}: {exc}"] += 1
            finally:
                with active_lock:
                    active -= 1
        return {
            "started": started,
            "accepted": accepted,
            "completed": completed,
            "response_bytes": response_bytes,
            "latencies": latencies,
            "failures": failures,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
        slots = [executor.submit(run_slot) for _ in range(concurrency)]
        slot_results = [slot.result() for slot in slots]

    elapsed = time.monotonic() - worker_started
    started = sum(slot["started"] for slot in slot_results)
    accepted = sum(slot["accepted"] for slot in slot_results)
    completed = sum(slot["completed"] for slot in slot_results)
    response_bytes = sum(slot["response_bytes"] for slot in slot_results)
    latencies = [
        latency for slot in slot_results for latency in slot["latencies"]
    ]
    failures = Counter()
    for slot in slot_results:
        failures.update(slot["failures"])
    failed = sum(failures.values())
    load_window = args.load_duration if args.load_duration > 0 else args.timeout
    result.update(
        {
            "small_start_s": worker_started - started_at,
            "small_time_s": elapsed,
            "small_count": started,
            "small_ok": completed,
            "small_fail": failed,
            "small_failure_reasons": dict(sorted(failures.items())),
            "small_response_bytes": response_bytes,
            "browser_connections_started": started,
            "browser_connections_accepted": accepted,
            "browser_connections_completed": completed,
            "browser_connections_rejected": started - accepted,
            "browser_connections_incomplete": accepted - completed,
            "browser_peak_concurrency": peak_active,
            "browser_concurrency_limit": concurrency,
            "browser_load_window_s": load_window,
            "browser_completed_connections_per_second": completed / elapsed
            if elapsed > 0
            else 0.0,
            "browser_payload_goodput_mbps": response_bytes * 8 / elapsed / 1_000_000
            if elapsed > 0
            else 0.0,
            "browser_p50_ms": percentile(latencies, 0.50),
            "browser_p95_ms": percentile(latencies, 0.95),
            "browser_max_ms": max(latencies) if latencies else None,
        }
    )


def interactive_tcp_worker(args, started_at, interactive_ready, result):
    if not args.tcp_echo_target:
        interactive_ready.set()
        return

    latencies = []
    failures = 0
    connected = False
    disconnected_at_s = None
    last_success_s = None
    max_success_gap_s = 0.0
    failover_gap_s = 0.0
    first_error = None
    request_bytes = 0
    response_bytes = 0
    timeout = args.tcp_echo_timeout_ms / 1000.0
    payload_len = max(8, args.tcp_echo_payload_bytes)
    deadline = workload_deadline(started_at, args)
    interval_s = max(0.0, args.tcp_echo_interval_ms / 1000.0)
    index = 0
    sock = None
    worker_started = time.monotonic()

    try:
        while time.monotonic() < deadline:
            interval_started = time.monotonic()
            if sock is None and disconnected_at_s is None:
                try:
                    connect_timeout = max(
                        0.1, min(args.timeout, timeout, deadline - time.monotonic())
                    )
                    sock, _, _ = connect_target(args, args.tcp_echo_target, connect_timeout)
                    sock.settimeout(timeout)
                    connected = True
                except Exception as exc:
                    if first_error is None:
                        first_error = str(exc)
                    failures += 1
                    interactive_ready.set()
                    sock = None

            if sock is None:
                if disconnected_at_s is not None:
                    failures += 1
                sleep_s = min(interval_s, max(0.0, deadline - time.monotonic()))
                if sleep_s > 0:
                    time.sleep(sleep_s)
                index += 1
                continue

            payload = (
                struct.pack("!I", index)
                + bytes([index % 251]) * (payload_len - 4)
            )
            started = time.monotonic()
            try:
                sock.sendall(payload)
                request_bytes += len(payload)
                response = recv_exact(sock, len(payload))
                if response != payload:
                    failures += 1
                else:
                    response_bytes += len(response)
                    finished_s = time.monotonic() - started_at
                    latency_ms = (time.monotonic() - started) * 1000.0
                    latencies.append(latency_ms)
                    if len(latencies) == 1:
                        interactive_ready.set()
                    if last_success_s is not None:
                        gap = finished_s - last_success_s
                        max_success_gap_s = max(max_success_gap_s, gap)
                        if (
                            args.failover_after >= 0
                            and (
                                finished_s >= args.failover_after
                                or last_success_s >= args.failover_after
                            )
                        ):
                            failover_gap_s = max(failover_gap_s, gap)
                    last_success_s = finished_s
            except Exception as exc:
                if first_error is None:
                    first_error = str(exc)
                interactive_ready.set()
                failures += 1
                if disconnected_at_s is None:
                    disconnected_at_s = time.monotonic() - started_at
                try:
                    sock.close()
                except Exception:
                    pass
                sock = None

            remaining_interval = interval_s - (time.monotonic() - interval_started)
            if remaining_interval > 0:
                time.sleep(min(remaining_interval, max(0.0, deadline - time.monotonic())))
            index += 1
    finally:
        interactive_ready.set()
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass

    result.update(
        {
            "interactive_connected": connected,
            "interactive_start_s": worker_started - started_at,
            "interactive_time_s": time.monotonic() - worker_started,
            "interactive_count": len(latencies) + failures,
            "interactive_ok": len(latencies),
            "interactive_fail": failures,
            "interactive_request_bytes": request_bytes,
            "interactive_response_bytes": response_bytes,
            "interactive_p50_ms": percentile(latencies, 0.50),
            "interactive_p95_ms": percentile(latencies, 0.95),
            "interactive_max_ms": max(latencies) if latencies else None,
            "interactive_max_success_gap_s": max_success_gap_s,
            "interactive_failover_gap_s": failover_gap_s,
            "interactive_disconnected_at_s": disconnected_at_s,
            "interactive_error": first_error,
        }
    )


def udp_worker(args, started_at, bulk_ready, result):
    latencies = []
    max_latency_ms = None
    max_index = None
    max_start_s = None
    max_end_s = None
    max_after_failover_ms = None
    max_after_failover_index = None
    max_after_failover_start_s = None
    max_after_failover_end_s = None
    received = 0
    attempted = 0
    request_bytes = 0
    response_bytes = 0
    safety_deadline = started_at + args.timeout
    workload_deadline_at = workload_deadline(started_at, args)
    deadline_hit = False
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    worker_started = time.monotonic()
    try:
        if args.mode == "direct":
            target_host, target_port = parse_host_port(args.udp_target)
            timeout = args.udp_timeout_ms / 1000.0
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            with udp:
                udp.settimeout(timeout)
                body_len = max(4, args.udp_payload_bytes)
                index = 0
                while time.monotonic() < workload_deadline_at:
                    now = time.monotonic()
                    if now >= safety_deadline:
                        deadline_hit = True
                        break
                    if not attempt_has_response_budget(now, workload_deadline_at, timeout):
                        break
                    payload = struct.pack("!I", index) + bytes([index % 251]) * (
                        body_len - 4
                    )
                    attempted += 1
                    request_bytes += len(payload)
                    started = time.monotonic()
                    started_s = started - started_at
                    udp.sendto(payload, (target_host, target_port))
                    deadline = min(started + timeout, safety_deadline, workload_deadline_at)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            if time.monotonic() >= safety_deadline:
                                deadline_hit = True
                            break
                        udp.settimeout(remaining)
                        try:
                            response, _ = udp.recvfrom(65535)
                        except socket.timeout:
                            break
                        if response == payload:
                            finished_s = time.monotonic() - started_at
                            latency_ms = (time.monotonic() - started) * 1000.0
                            received += 1
                            response_bytes += len(response)
                            latencies.append(latency_ms)
                            if max_latency_ms is None or latency_ms > max_latency_ms:
                                max_latency_ms = latency_ms
                                max_index = index
                                max_start_s = started_s
                                max_end_s = finished_s
                            if (
                                args.failover_after >= 0
                                and (
                                    finished_s >= args.failover_after
                                    or started_s >= args.failover_after
                                )
                                and (
                                    max_after_failover_ms is None
                                    or latency_ms > max_after_failover_ms
                                )
                            ):
                                max_after_failover_ms = latency_ms
                                max_after_failover_index = index
                                max_after_failover_start_s = started_s
                                max_after_failover_end_s = finished_s
                            break
                    if args.udp_interval_ms > 0:
                        time.sleep(args.udp_interval_ms / 1000.0)
                    if deadline_hit:
                        break
                    index += 1
            result.update(
                {
                    "udp_error": (
                        f"UDP probe deadline exceeded after {args.timeout:.3f}s"
                        if deadline_hit
                        else None
                    ),
                    "udp_start_s": worker_started - started_at,
                    "udp_time_s": time.monotonic() - worker_started,
                    "udp_count": attempted,
                    "udp_received": received,
                    "udp_loss_rate": (attempted - received) / attempted
                    if attempted
                    else 0.0,
                    "udp_p50_ms": percentile(latencies, 0.50),
                    "udp_p95_ms": percentile(latencies, 0.95),
                    "udp_max_ms": max_latency_ms,
                    "udp_max_index": max_index,
                    "udp_max_start_s": max_start_s,
                    "udp_max_end_s": max_end_s,
                    "udp_max_after_failover_ms": max_after_failover_ms,
                    "udp_max_after_failover_index": max_after_failover_index,
                    "udp_max_after_failover_start_s": max_after_failover_start_s,
                    "udp_max_after_failover_end_s": max_after_failover_end_s,
                }
            )
            return

        proxy_host, proxy_port = parse_host_port(args.proxy)
        target_host, target_port = parse_host_port(args.udp_target)
        timeout = args.udp_timeout_ms / 1000.0
        control = socket.create_connection((proxy_host, proxy_port), timeout=timeout)
        with control:
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
            with udp:
                udp.settimeout(timeout)
                target_prefix = b"\x00\x00\x00" + encode_socks_addr(
                    target_host, target_port
                )
                body_len = max(4, args.udp_payload_bytes)
                index = 0
                while time.monotonic() < workload_deadline_at:
                    now = time.monotonic()
                    if now >= safety_deadline:
                        deadline_hit = True
                        break
                    if not attempt_has_response_budget(now, workload_deadline_at, timeout):
                        break
                    payload = struct.pack("!I", index) + bytes([index % 251]) * (
                        body_len - 4
                    )
                    attempted += 1
                    request_bytes += len(payload)
                    started = time.monotonic()
                    started_s = started - started_at
                    udp.sendto(target_prefix + payload, (relay_host, relay_port))
                    deadline = min(
                        started + timeout,
                        safety_deadline,
                        workload_deadline_at,
                    )
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            if time.monotonic() >= safety_deadline:
                                deadline_hit = True
                            break
                        udp.settimeout(remaining)
                        try:
                            packet, _ = udp.recvfrom(65535)
                        except socket.timeout:
                            break
                        _, _, response = parse_udp_packet(packet)
                        if response == payload:
                            finished_s = time.monotonic() - started_at
                            latency_ms = (time.monotonic() - started) * 1000.0
                            received += 1
                            response_bytes += len(response)
                            latencies.append(latency_ms)
                            if max_latency_ms is None or latency_ms > max_latency_ms:
                                max_latency_ms = latency_ms
                                max_index = index
                                max_start_s = started_s
                                max_end_s = finished_s
                            if (
                                args.failover_after >= 0
                                and (
                                    finished_s >= args.failover_after
                                    or started_s >= args.failover_after
                                )
                                and (
                                    max_after_failover_ms is None
                                    or latency_ms > max_after_failover_ms
                                )
                            ):
                                max_after_failover_ms = latency_ms
                                max_after_failover_index = index
                                max_after_failover_start_s = started_s
                                max_after_failover_end_s = finished_s
                            break
                    if args.udp_interval_ms > 0:
                        time.sleep(args.udp_interval_ms / 1000.0)
                    if deadline_hit:
                        break
                    index += 1
        result.update(
            {
                "udp_error": (
                    f"UDP probe deadline exceeded after {args.timeout:.3f}s"
                    if deadline_hit
                    else None
                )
            }
        )
    except Exception as exc:
        result.update({"udp_error": str(exc)})
    result.update(
            {
                "udp_start_s": worker_started - started_at,
                "udp_time_s": time.monotonic() - worker_started,
                "udp_count": attempted,
                "udp_received": received,
                "udp_request_bytes": request_bytes,
                "udp_response_bytes": response_bytes,
                "udp_loss_rate": (attempted - received) / attempted
                if attempted
                else 0.0,
            "udp_p50_ms": percentile(latencies, 0.50),
            "udp_p95_ms": percentile(latencies, 0.95),
            "udp_max_ms": max_latency_ms,
            "udp_max_index": max_index,
            "udp_max_start_s": max_start_s,
            "udp_max_end_s": max_end_s,
            "udp_max_after_failover_ms": max_after_failover_ms,
            "udp_max_after_failover_index": max_after_failover_index,
            "udp_max_after_failover_start_s": max_after_failover_start_s,
            "udp_max_after_failover_end_s": max_after_failover_end_s,
        }
    )


def build_record(args, bulk, small, interactive, udp):
    browser_only = getattr(args, "browser_only", False)
    browser_full_load = getattr(args, "browser_full_load", False)
    bulk_ok = browser_only or bulk.get("bulk_status") == "ok"
    small_attempts = small.get("small_count", 0)
    small_batch_count = small.get("small_batch_count", 0)
    small_batch_sizes = small.get("small_batch_sizes", [])
    small_batch_shape_ok = (
        small_batch_count > 0
        and len(small_batch_sizes) == small_batch_count
        and all(size == args.small_batch_size for size in small_batch_sizes)
    )
    small_ok = (
        small_attempts > 0
        and small.get("small_fail", 0) == 0
        and small_batch_shape_ok
        and (
            not args.require_small_response_budget
            or small.get("small_batch_deadline_misses", 0) == 0
        )
    )
    interactive_expected = interactive.get("interactive_count", 0)
    interactive_ok = (
        browser_only
        or not args.tcp_echo_target
        or (interactive_expected > 0 and interactive.get("interactive_fail", 0) == 0)
    )
    udp_attempts = udp.get("udp_count", 0)
    udp_ok = browser_only or (
        udp_attempts > 0 and udp.get("udp_received", 0) == udp_attempts
    )
    if browser_full_load:
        completed = small.get("browser_connections_completed", 0)
        started = small.get("browser_connections_started", 0)
        accepted = small.get("browser_connections_accepted", 0)
        peak = small.get("browser_peak_concurrency", 0)
        if started <= 0 or accepted <= 0 or completed <= 0 or peak != args.small_batch_size:
            status = "fail"
        elif small.get("small_fail", 0) == 0 and started == accepted == completed:
            status = "ok"
        else:
            status = "fail"
    elif not bulk_ok or (args.require_small_response_budget and not small_ok):
        status = "fail"
    elif small_ok and interactive_ok and udp_ok:
        status = "ok"
    else:
        status = "loss"
    record = {
        "case": args.label,
        "protocol": "browser-load"
        if browser_full_load
        else "browser"
        if browser_only
        else "mixed",
        "status": status,
        "mode": args.mode,
        "target": args.http_target,
        "udp_target": args.udp_target,
        "tcp_echo_target": args.tcp_echo_target,
        "failover_after_s": args.failover_after,
        "test_duration_s": args.load_duration if args.load_duration > 0 else args.timeout,
    }
    record.update(bulk)
    record.update(small)
    record.update(interactive)
    record.update(udp)
    mixed_app_payload_bytes = sum(
        int(record.get(field) or 0)
        for field in (
            "bulk_bytes",
            "small_response_bytes",
            "interactive_request_bytes",
            "interactive_response_bytes",
            "udp_request_bytes",
            "udp_response_bytes",
        )
    )
    if mixed_app_payload_bytes > 0:
        record["mixed_app_payload_bytes"] = mixed_app_payload_bytes
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--mode", choices=("socks5", "direct"), default="socks5")
    parser.add_argument("--proxy", default="127.0.0.1:1080")
    parser.add_argument("--http-target", required=True)
    parser.add_argument("--udp-target")
    parser.add_argument("--tcp-echo-target")
    parser.add_argument("--bulk-path", default="/large.bin")
    parser.add_argument("--small-path", default="/small.bin")
    parser.add_argument("--failover-after", type=float, default=-1.0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--load-duration", type=float, default=30.0)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--small-response-budget-ms", type=int, default=2500)
    parser.add_argument("--small-interval-ms", type=int, default=100)
    parser.add_argument("--small-batch-size", type=int, default=1)
    parser.add_argument("--small-batch-period-ms", type=int, default=0)
    parser.add_argument("--require-small-response-budget", action="store_true")
    parser.add_argument("--browser-only", action="store_true")
    parser.add_argument("--browser-full-load", action="store_true")
    parser.add_argument("--udp-payload-bytes", type=int, default=512)
    parser.add_argument("--udp-timeout-ms", type=int, default=2500)
    parser.add_argument("--udp-interval-ms", type=int, default=20)
    parser.add_argument("--tcp-echo-payload-bytes", type=int, default=64)
    parser.add_argument("--tcp-echo-timeout-ms", type=int, default=5000)
    parser.add_argument("--tcp-echo-interval-ms", type=int, default=500)
    parser.add_argument("--started-file")
    args = parser.parse_args()
    if not args.browser_only and not args.udp_target:
        parser.error("--udp-target is required unless --browser-only is used")
    if args.browser_full_load and not args.browser_only:
        parser.error("--browser-full-load requires --browser-only")
    if args.small_batch_size < 1:
        parser.error("--small-batch-size must be positive")
    if args.small_batch_period_ms < 0:
        parser.error("--small-batch-period-ms must be non-negative")
    if args.small_response_budget_ms < 1:
        parser.error("--small-response-budget-ms must be positive")

    started_at = time.monotonic()
    write_started_file(args.started_file)
    interactive_ready = threading.Event()
    bulk_ready = threading.Event()
    bulk = {}
    small = {}
    interactive = {}
    udp = {}
    if args.browser_only:
        bulk_ready.set()
        interactive_ready.set()
        threads = [
            threading.Thread(
                target=browser_full_load_worker
                if args.browser_full_load
                else small_http_worker,
                args=(args, started_at, bulk_ready, small),
                daemon=True,
            )
        ]
    else:
        threads = [
            threading.Thread(
                target=bulk_worker,
                args=(args, started_at, interactive_ready, bulk_ready, bulk),
                daemon=True,
            ),
            threading.Thread(
                target=small_http_worker,
                args=(args, started_at, bulk_ready, small),
                daemon=True,
            ),
            threading.Thread(
                target=interactive_tcp_worker,
                args=(args, started_at, interactive_ready, interactive),
                daemon=True,
            ),
            threading.Thread(
                target=udp_worker,
                args=(args, started_at, bulk_ready, udp),
                daemon=True,
            ),
        ]
    for thread in threads:
        thread.start()
    join_timeout = args.timeout + 5.0
    if args.browser_full_load:
        load_window = args.load_duration if args.load_duration > 0 else args.timeout
        join_timeout = (
            load_window + browser_full_load_response_timeout_seconds(args) + 5.0
        )
    for thread in threads:
        thread.join(timeout=join_timeout)
    record = build_record(args, bulk, small, interactive, udp)
    print(json.dumps(record, separators=(",", ":")))
    return 0 if record["status"] != "fail" else 1


if __name__ == "__main__":
    sys.exit(main())

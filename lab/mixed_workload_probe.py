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
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{time.time():.9f}\n")


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


def http_get(args, target, path, timeout, chunk_bytes):
    started = time.monotonic()
    sock, target_host, target_port = connect_target(args, target, timeout)
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
    attempts = 0
    response_bytes = 0
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    worker_started = time.monotonic()
    deadline = workload_deadline(started_at, args)
    while time.monotonic() < deadline:
        attempts += 1
        started = time.monotonic()
        remaining = max(0.1, min(args.timeout, deadline - started))
        try:
            status, body_bytes, _ = http_get(
                args,
                args.http_target,
                args.small_path,
                remaining,
                args.chunk_bytes,
            )
            if not 200 <= status < 400:
                failures += 1
            else:
                response_bytes += body_bytes
                latencies.append((time.monotonic() - started) * 1000.0)
        except Exception:
            failures += 1
        if args.small_interval_ms > 0:
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
            "small_response_bytes": response_bytes,
            "small_p50_ms": percentile(latencies, 0.50),
            "small_p95_ms": percentile(latencies, 0.95),
            "small_max_ms": max(latencies) if latencies else None,
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
                    if time.monotonic() >= safety_deadline:
                        deadline_hit = True
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
                    if time.monotonic() >= safety_deadline:
                        deadline_hit = True
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
    bulk_ok = bulk.get("bulk_status") == "ok"
    small_attempts = small.get("small_count", 0)
    small_ok = small_attempts > 0 and small.get("small_fail", 0) == 0
    interactive_expected = interactive.get("interactive_count", 0)
    interactive_ok = (
        not args.tcp_echo_target
        or (interactive_expected > 0 and interactive.get("interactive_fail", 0) == 0)
    )
    udp_attempts = udp.get("udp_count", 0)
    udp_ok = udp_attempts > 0 and udp.get("udp_received", 0) == udp_attempts
    status = (
        "ok"
        if bulk_ok and small_ok and interactive_ok and udp_ok
        else "loss"
        if bulk_ok
        else "fail"
    )
    record = {
        "case": args.label,
        "protocol": "mixed",
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
    parser.add_argument("--udp-target", required=True)
    parser.add_argument("--tcp-echo-target")
    parser.add_argument("--bulk-path", default="/large.bin")
    parser.add_argument("--small-path", default="/small.bin")
    parser.add_argument("--failover-after", type=float, default=-1.0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--load-duration", type=float, default=30.0)
    parser.add_argument("--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS)
    parser.add_argument("--small-interval-ms", type=int, default=100)
    parser.add_argument("--udp-payload-bytes", type=int, default=512)
    parser.add_argument("--udp-timeout-ms", type=int, default=2500)
    parser.add_argument("--udp-interval-ms", type=int, default=20)
    parser.add_argument("--tcp-echo-payload-bytes", type=int, default=64)
    parser.add_argument("--tcp-echo-timeout-ms", type=int, default=5000)
    parser.add_argument("--tcp-echo-interval-ms", type=int, default=500)
    parser.add_argument("--started-file")
    args = parser.parse_args()

    started_at = time.monotonic()
    write_started_file(args.started_file)
    interactive_ready = threading.Event()
    bulk_ready = threading.Event()
    bulk = {}
    small = {}
    interactive = {}
    udp = {}
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
        threading.Thread(target=udp_worker, args=(args, started_at, bulk_ready, udp), daemon=True),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=args.timeout + 5.0)
    print(
        json.dumps(
            build_record(args, bulk, small, interactive, udp), separators=(",", ":")
        )
    )
    return 0 if bulk.get("bulk_status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())

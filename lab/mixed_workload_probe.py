#!/usr/bin/env python3
import argparse
import ipaddress
import json
import socket
import struct
import sys
import threading
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


def write_started_file(path):
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"{time.time():.9f}\n")


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


def http_get_via_socks(proxy, target, path, timeout, chunk_bytes):
    started = time.monotonic()
    sock, target_host, target_port = connect_socks5(proxy, target, timeout)
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
        sock, target_host, target_port = connect_socks5(
            args.proxy, args.http_target, args.timeout
        )
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
                chunk = sock.recv(args.chunk_bytes)
                if not chunk:
                    raise OSError("EOF before HTTP headers")
                buffer += chunk
            status, content_length, body = parse_http_headers(buffer)
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

            def record_chunk(size):
                nonlocal bytes_read, first_body_s, last_body_s, max_read_gap_s, recovery_gap_s
                nonlocal bytes_at_failover
                nonlocal max_gap_start_s, max_gap_end_s, max_gap_start_bytes, max_gap_end_bytes
                nonlocal recovery_gap_start_s, recovery_gap_end_s
                nonlocal recovery_gap_start_bytes, recovery_gap_end_bytes
                if size <= 0:
                    return
                now_s = time.monotonic() - started_at
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

            record_chunk(len(body))
            while content_length is None or bytes_read < content_length:
                chunk = sock.recv(args.chunk_bytes)
                if not chunk:
                    break
                record_chunk(len(chunk))
        elapsed = time.monotonic() - started_at
        if content_length is not None and bytes_read != content_length:
            raise OSError(f"incomplete bulk body: {bytes_read} of {content_length}")
        result.update(
            {
                "bulk_status": "ok" if 200 <= status < 400 else "fail",
                "bulk_http_code": status,
                "bulk_content_length": content_length,
                "bulk_bytes": bytes_read,
                "bulk_time_s": elapsed,
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
    except Exception as exc:
        result.update({"bulk_status": "fail", "bulk_error": str(exc)})
    finally:
        bulk_ready.set()


def small_http_worker(args, bulk_ready, result):
    latencies = []
    failures = 0
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    for _ in range(args.small_count):
        started = time.monotonic()
        try:
            status, _, _ = http_get_via_socks(
                args.proxy,
                args.http_target,
                args.small_path,
                args.timeout,
                args.chunk_bytes,
            )
            if not 200 <= status < 400:
                failures += 1
            else:
                latencies.append((time.monotonic() - started) * 1000.0)
        except Exception:
            failures += 1
        if args.small_interval_ms > 0:
            time.sleep(args.small_interval_ms / 1000.0)
    result.update(
        {
            "small_count": args.small_count,
            "small_ok": len(latencies),
            "small_fail": failures,
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
    timeout = args.tcp_echo_timeout_ms / 1000.0
    payload_len = max(8, args.tcp_echo_payload_bytes)

    try:
        sock, _, _ = connect_socks5(args.proxy, args.tcp_echo_target, args.timeout)
        connected = True
        with sock:
            sock.settimeout(timeout)
            for index in range(args.tcp_echo_count):
                payload = (
                    struct.pack("!I", index)
                    + bytes([index % 251]) * (payload_len - 4)
                )
                started = time.monotonic()
                now_s = started - started_at
                try:
                    sock.sendall(payload)
                    response = recv_exact(sock, len(payload))
                    if response != payload:
                        failures += 1
                    else:
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
                except Exception:
                    interactive_ready.set()
                    failures += 1
                    if disconnected_at_s is None:
                        disconnected_at_s = time.monotonic() - started_at
                    break

                remaining_interval = args.tcp_echo_interval_ms / 1000.0 - (
                    time.monotonic() - started
                )
                if remaining_interval > 0:
                    time.sleep(remaining_interval)
    except Exception as exc:
        result.update({"interactive_error": str(exc)})
    finally:
        interactive_ready.set()

    result.update(
        {
            "interactive_connected": connected,
            "interactive_count": args.tcp_echo_count,
            "interactive_ok": len(latencies),
            "interactive_fail": failures + max(0, args.tcp_echo_count - len(latencies) - failures),
            "interactive_p50_ms": percentile(latencies, 0.50),
            "interactive_p95_ms": percentile(latencies, 0.95),
            "interactive_max_ms": max(latencies) if latencies else None,
            "interactive_max_success_gap_s": max_success_gap_s,
            "interactive_failover_gap_s": failover_gap_s,
            "interactive_disconnected_at_s": disconnected_at_s,
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
    probe_deadline = started_at + args.timeout
    deadline_hit = False
    bulk_ready.wait(timeout=min(args.timeout, 10.0))
    try:
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
                for index in range(args.udp_count):
                    if time.monotonic() >= probe_deadline:
                        deadline_hit = True
                        break
                    payload = struct.pack("!I", index) + bytes([index % 251]) * (
                        body_len - 4
                    )
                    started = time.monotonic()
                    started_s = started - started_at
                    udp.sendto(target_prefix + payload, (relay_host, relay_port))
                    deadline = min(started + timeout, probe_deadline)
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            if time.monotonic() >= probe_deadline:
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
            "udp_count": args.udp_count,
            "udp_received": received,
            "udp_loss_rate": (args.udp_count - received) / args.udp_count
            if args.udp_count
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
    small_ok = small.get("small_ok", 0) == small.get("small_count", args.small_count)
    interactive_expected = interactive.get("interactive_count", 0)
    interactive_ok = interactive.get("interactive_ok", 0) == interactive_expected
    udp_ok = udp.get("udp_received", 0) == udp.get("udp_count", args.udp_count)
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
        "target": args.http_target,
        "udp_target": args.udp_target,
        "tcp_echo_target": args.tcp_echo_target,
        "failover_after_s": args.failover_after,
    }
    record.update(bulk)
    record.update(small)
    record.update(interactive)
    record.update(udp)
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--proxy", default="127.0.0.1:1080")
    parser.add_argument("--http-target", required=True)
    parser.add_argument("--udp-target", required=True)
    parser.add_argument("--tcp-echo-target")
    parser.add_argument("--bulk-path", default="/large.bin")
    parser.add_argument("--small-path", default="/")
    parser.add_argument("--failover-after", type=float, default=-1.0)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--small-count", type=int, default=20)
    parser.add_argument("--small-interval-ms", type=int, default=100)
    parser.add_argument("--udp-count", type=int, default=60)
    parser.add_argument("--udp-payload-bytes", type=int, default=512)
    parser.add_argument("--udp-timeout-ms", type=int, default=2500)
    parser.add_argument("--udp-interval-ms", type=int, default=20)
    parser.add_argument("--tcp-echo-count", type=int, default=40)
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
        threading.Thread(target=small_http_worker, args=(args, bulk_ready, small), daemon=True),
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

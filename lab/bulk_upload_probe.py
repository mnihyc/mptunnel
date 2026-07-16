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

DEFAULT_INTERVAL_SECONDS = 0.2
INTERVAL_TRIM_DISCARD_EACH_END = 3
MAX_ACK_LINE_BYTES = 128
IPPROTO_MPTCP = getattr(socket, "IPPROTO_MPTCP", 262)


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


def remaining_before(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("upload load deadline expired")
    return remaining


def sendall_before(sock, data, deadline):
    sock.settimeout(remaining_before(deadline))
    sock.sendall(data)


def read_exact(sock, size, deadline):
    chunks = []
    remaining = size
    while remaining:
        sock.settimeout(remaining_before(deadline))
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


def watch_failover_marker(path, started, state, lock, stop_event):
    while not stop_event.wait(0.01):
        try:
            with open(path, encoding="utf-8") as handle:
                marker = float(handle.read().strip())
        except (OSError, ValueError):
            # Shell redirection creates the marker before writing its timestamp.
            continue
        with lock:
            state["failover_after_s"] = max(0.0, marker - started)
        return


def connect_socks5(proxy, target, deadline):
    proxy_host, proxy_port = split_host_port(proxy)
    target_host, target_port = split_host_port(target)
    sock = socket.create_connection(
        (proxy_host, proxy_port), timeout=remaining_before(deadline)
    )
    try:
        sendall_before(sock, b"\x05\x01\x00", deadline)
        response = read_exact(sock, 2, deadline)
        if response != b"\x05\x00":
            raise RuntimeError(f"SOCKS5 authentication failed: {response!r}")
        request = b"\x05\x01\x00" + socks_target(target_host, target_port)
        sendall_before(sock, request, deadline)
        header = read_exact(sock, 4, deadline)
        if header[0] != 5 or header[1] != 0:
            raise RuntimeError(f"SOCKS5 connect failed: {header!r}")
        atyp = header[3]
        if atyp == 1:
            read_exact(sock, 4, deadline)
        elif atyp == 3:
            length = read_exact(sock, 1, deadline)[0]
            read_exact(sock, length, deadline)
        elif atyp == 4:
            read_exact(sock, 16, deadline)
        else:
            raise RuntimeError(f"unknown SOCKS5 address type: {atyp}")
        read_exact(sock, 2, deadline)
        return sock
    except Exception:
        sock.close()
        raise


def connect_target(args, deadline):
    if args.proxy:
        if getattr(args, "mptcp", False):
            raise ValueError("MPTCP direct sockets cannot be combined with a proxy")
        return connect_socks5(args.proxy, args.target, deadline)
    target_host, target_port = split_host_port(args.target)
    timeout = remaining_before(deadline)
    if getattr(args, "mptcp", False):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, IPPROTO_MPTCP)
        try:
            sock.settimeout(timeout)
            sock.connect((target_host, target_port))
        except Exception:
            sock.close()
            raise
    else:
        sock = socket.create_connection((target_host, target_port), timeout=timeout)
    try:
        sock.settimeout(remaining_before(deadline))
        return sock
    except Exception:
        sock.close()
        raise


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


def parse_acknowledgement(line):
    try:
        text = line.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError("upload sink acknowledgement is not ASCII") from exc
    parts = text.split()
    if len(parts) != 2 or parts[0] not in {"ACK", "OK"}:
        raise RuntimeError(f"invalid upload sink acknowledgement: {text!r}")
    if not parts[1].isdigit():
        raise RuntimeError(f"invalid upload sink byte count: {parts[1]!r}")
    return parts[0], int(parts[1])


def record_local_chunk(started, state, lock, size):
    if size <= 0:
        return
    now = time.monotonic()
    now_s = now - started
    with lock:
        if state["first_write_at"] is None:
            state["first_write_at"] = now_s
        if state["last_write_at"] is not None:
            gap = now_s - state["last_write_at"]
            state["max_write_gap_s"] = max(state["max_write_gap_s"], gap)
            if state["failover_after_s"] >= 0 and (
                now_s >= state["failover_after_s"]
                or state["last_write_at"] >= state["failover_after_s"]
            ):
                state["local_recovery_gap_s"] = max(state["local_recovery_gap_s"], gap)
        state["last_write_at"] = now_s
        state["local_accepted_bytes"] += size


def record_delivery_progress(started, state, lock, stream_state, total):
    if total < stream_state["confirmed_bytes"]:
        raise RuntimeError("upload sink acknowledgement decreased")
    if total > stream_state["local_accepted_bytes"]:
        raise RuntimeError("upload sink acknowledged bytes not accepted locally")
    delta = total - stream_state["confirmed_bytes"]
    stream_state["confirmed_bytes"] = total
    if delta <= 0:
        return

    now = time.monotonic()
    now_s = now - started
    interval = int(now_s // state["interval_seconds"])
    with lock:
        if not stream_state["delivery_observed"]:
            stream_state["delivery_observed"] = True
            state["streams_with_delivery"] += 1
        if state["first_delivery_at"] is None:
            state["first_delivery_at"] = now_s
        if state["last_delivery_at"] is not None:
            gap = now_s - state["last_delivery_at"]
            state["max_delivery_gap_s"] = max(state["max_delivery_gap_s"], gap)
            if state["failover_after_s"] >= 0 and (
                now_s >= state["failover_after_s"]
                or state["last_delivery_at"] >= state["failover_after_s"]
            ):
                state["recovery_gap_s"] = max(state["recovery_gap_s"], gap)
        state["last_delivery_at"] = now_s
        state["bytes"] += delta
        state["interval_bytes"][interval] = (
            state["interval_bytes"].get(interval, 0) + delta
        )


def consume_acknowledgements(data, started, state, lock, stream_state):
    response_buffer = stream_state["response_buffer"]
    response_buffer.extend(data)
    while True:
        newline = response_buffer.find(b"\n")
        if newline < 0:
            break
        line = bytes(response_buffer[:newline])
        del response_buffer[: newline + 1]
        if stream_state["final_total"] is not None:
            raise RuntimeError("upload sink sent data after final acknowledgement")
        kind, total = parse_acknowledgement(line)
        record_delivery_progress(started, state, lock, stream_state, total)
        if kind == "OK":
            stream_state["final_total"] = total
    if len(response_buffer) > MAX_ACK_LINE_BYTES:
        raise RuntimeError("upload sink acknowledgement line is too long")


def upload_one_stream(
    args, started, load_deadline, drain_deadline, state, lock, payload
):
    sock = connect_target(args, load_deadline)
    stream_state = {
        "local_accepted_bytes": 0,
        "confirmed_bytes": 0,
        "delivery_observed": False,
        "final_total": None,
        "response_buffer": bytearray(),
    }
    with sock:
        sock.setblocking(False)
        while time.monotonic() < load_deadline:
            remaining = load_deadline - time.monotonic()
            if remaining <= 0:
                break
            readable, writable, _ = select.select(
                [sock], [sock], [], min(remaining, 0.25)
            )
            if readable:
                try:
                    response = sock.recv(4096)
                except BlockingIOError:
                    response = None
                if response == b"":
                    break
                if response:
                    consume_acknowledgements(
                        response, started, state, lock, stream_state
                    )
            if writable:
                chunk = memoryview(payload)[
                    : max(1, min(len(payload), args.chunk_bytes))
                ]
                try:
                    sent = sock.send(chunk)
                except BlockingIOError:
                    continue
                except OSError:
                    break
                if sent <= 0:
                    break
                stream_state["local_accepted_bytes"] += sent
                record_local_chunk(started, state, lock, sent)

        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass

        while stream_state["final_total"] is None and time.monotonic() < drain_deadline:
            readable, _, _ = select.select(
                [sock], [], [], max(0.0, drain_deadline - time.monotonic())
            )
            if not readable:
                break
            try:
                response = sock.recv(4096)
            except BlockingIOError:
                continue
            if not response:
                break
            consume_acknowledgements(response, started, state, lock, stream_state)

    if stream_state["response_buffer"]:
        raise RuntimeError("upload sink ended with a partial acknowledgement")
    complete = (
        stream_state["final_total"] is not None
        and stream_state["final_total"] == stream_state["local_accepted_bytes"]
    )
    return {
        "complete": complete,
        "confirmed_bytes": stream_state["confirmed_bytes"],
        "local_accepted_bytes": stream_state["local_accepted_bytes"],
    }


def interval_upload(args):
    started = time.monotonic()
    write_started_file(args.started_file)
    load_duration = args.load_duration if args.load_duration > 0 else args.timeout
    load_deadline = started + min(load_duration, args.timeout)
    drain_timeout = max(0.0, getattr(args, "drain_timeout", 1.0))
    drain_deadline = load_deadline + drain_timeout
    state = {
        "bytes": 0,
        "local_accepted_bytes": 0,
        "first_write_at": None,
        "last_write_at": None,
        "max_write_gap_s": 0.0,
        "local_recovery_gap_s": 0.0,
        "first_delivery_at": None,
        "last_delivery_at": None,
        "max_delivery_gap_s": 0.0,
        "recovery_gap_s": 0.0,
        "failover_after_s": args.failover_after,
        "interval_seconds": args.interval_seconds,
        "interval_bytes": {},
        "streams": 0,
        "streams_with_delivery": 0,
        "complete_streams": 0,
        "failures": 0,
        "probe_errors": [],
    }
    lock = threading.Lock()
    marker_stop = threading.Event()
    marker_thread = None
    if args.failover_marker_file:
        marker_thread = threading.Thread(
            target=watch_failover_marker,
            args=(args.failover_marker_file, started, state, lock, marker_stop),
            name="failover-marker",
            daemon=True,
        )
        marker_thread.start()
    payload = bytes([index % 251 for index in range(max(1, args.chunk_bytes))])

    def worker(stream_index):
        complete = False
        probe_error = None
        try:
            stream_result = upload_one_stream(
                args,
                started,
                load_deadline,
                drain_deadline,
                state,
                lock,
                payload,
            )
            complete = stream_result["complete"]
        except Exception as exc:
            detail = str(exc) or "no error detail"
            probe_error = (stream_index, f"{type(exc).__name__}: {detail}")
        with lock:
            state["streams"] += 1
            if complete:
                state["complete_streams"] += 1
            else:
                state["failures"] += 1
            if probe_error is not None:
                state["probe_errors"].append(probe_error)

    threads = [
        threading.Thread(
            target=worker,
            args=(stream_index,),
            name=f"upload-worker-{stream_index}",
            daemon=False,
        )
        for stream_index in range(max(1, args.parallel_uploads))
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    marker_stop.set()
    if marker_thread is not None:
        marker_thread.join(timeout=0.1)

    if any(thread.is_alive() for thread in threads):
        raise RuntimeError("upload worker remained alive after join")

    with lock:
        aggregate = dict(state)
        aggregate["interval_bytes"] = dict(state["interval_bytes"])
        aggregate["probe_errors"] = [
            f"stream {stream_index}: {message}"
            for stream_index, message in sorted(state["probe_errors"])
        ]

    elapsed = time.monotonic() - started
    confirmed_bytes = aggregate["bytes"]
    local_accepted_bytes = aggregate["local_accepted_bytes"]
    expected_streams = max(1, args.parallel_uploads)
    ack_accounting_valid = (
        not aggregate["probe_errors"] and aggregate["streams"] == expected_streams
    )
    delivery_exact = (
        confirmed_bytes > 0
        and aggregate["streams"] == expected_streams
        and aggregate["complete_streams"] == expected_streams
        and aggregate["failures"] == 0
        and ack_accounting_valid
    )
    status = "ok" if delivery_exact else "loss" if confirmed_bytes > 0 else "fail"
    goodput = confirmed_bytes * 8 / elapsed / 1_000_000 if elapsed > 0 else 0.0
    local_accepted_goodput = (
        local_accepted_bytes * 8 / elapsed / 1_000_000 if elapsed > 0 else 0.0
    )
    result = {
        "case": args.label,
        "protocol": args.protocol,
        "status": status,
        "exit_code": 0 if status != "fail" else 1,
        "mode": "duration-upload",
        "load_duration_s": round(load_duration, 6),
        "drain_timeout_s": round(drain_timeout, 6),
        "parallel_uploads": max(1, args.parallel_uploads),
        "time_s": round(elapsed, 6),
        "goodput_mbps": round(goodput, 3),
        "upload_goodput_mbps": round(goodput, 3),
        "bytes": confirmed_bytes,
        "target_confirmed_bytes": confirmed_bytes,
        "local_accepted_bytes": local_accepted_bytes,
        "local_accepted_goodput_mbps": round(local_accepted_goodput, 3),
        "upload_metric_version": 2,
        "upload_accounting_source": "target_sink_ack",
        "upload_accounting_exact": delivery_exact,
        "upload_accounting_lower_bound": confirmed_bytes > 0 and not delivery_exact,
        "upload_probe_errors": aggregate["probe_errors"],
        "upload_ack_accounting_valid": ack_accounting_valid,
        "complete": delivery_exact,
        "streams": aggregate["streams"],
        "streams_with_delivery": aggregate["streams_with_delivery"],
        "complete_streams": aggregate["complete_streams"],
        "failed_streams": aggregate["failures"],
        "first_write_s": round(aggregate["first_write_at"], 6)
        if aggregate["first_write_at"] is not None
        else None,
        "max_write_gap_s": round(aggregate["max_write_gap_s"], 6),
        "first_delivery_s": round(aggregate["first_delivery_at"], 6)
        if aggregate["first_delivery_at"] is not None
        else None,
        "max_delivery_gap_s": round(aggregate["max_delivery_gap_s"], 6),
        "recovery_gap_s": round(aggregate["recovery_gap_s"], 6),
        "local_recovery_gap_s": round(aggregate["local_recovery_gap_s"], 6),
        "failover_after_s": round(aggregate["failover_after_s"], 6),
        "failover_trigger_source": "marker"
        if args.failover_marker_file
        else "timer",
    }
    result.update(
        interval_metric_fields(
            aggregate["interval_bytes"] if ack_accounting_valid else {},
            args.interval_seconds,
        )
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--protocol", default="tcp-upload")
    parser.add_argument("--proxy")
    parser.add_argument(
        "--mptcp",
        action="store_true",
        help="open direct client sockets with IPPROTO_MPTCP",
    )
    parser.add_argument("--target", required=True)
    parser.add_argument("--failover-after", type=float, required=True)
    parser.add_argument("--failover-marker-file")
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--chunk-bytes", type=int, default=64 * 1024)
    parser.add_argument("--load-duration", type=float, default=30.0)
    parser.add_argument("--parallel-uploads", type=int, default=1)
    parser.add_argument(
        "--interval-seconds", type=float, default=DEFAULT_INTERVAL_SECONDS
    )
    parser.add_argument("--drain-timeout", type=float, default=1.0)
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

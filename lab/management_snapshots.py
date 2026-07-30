#!/usr/bin/env python3
"""Sample release management state and per-interface counters in lab containers."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


STOP_POLL_INTERVAL_SECONDS = 0.05

CONTAINER_SNAPSHOT_SCRIPT = r"""
import fcntl
import json
import socket
import struct
import sys
import urllib.request


def ipv4_address(name):
    request = struct.pack("256s", name.encode("ascii", "ignore")[:15])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        response = fcntl.ioctl(sock.fileno(), 0x8915, request)
        return socket.inet_ntoa(response[20:24])
    except OSError:
        return None
    finally:
        sock.close()


interfaces = {}
with open("/proc/net/dev", encoding="ascii") as handle:
    for line in handle:
        if ":" not in line:
            continue
        name, values = line.split(":", 1)
        name = name.strip()
        fields = values.split()
        if name == "lo" or len(fields) < 16:
            continue
        interfaces[name] = {
            "ipv4": ipv4_address(name),
            "rx_bytes": int(fields[0]),
            "rx_packets": int(fields[1]),
            "tx_bytes": int(fields[8]),
            "tx_packets": int(fields[9]),
        }

result = {"interfaces": interfaces}
try:
    request = urllib.request.Request(
        f"http://127.0.0.1:{int(sys.argv[1])}/api/v2/status",
        headers={"Authorization": f"Bearer {sys.argv[2]}"},
    )
    with urllib.request.urlopen(request, timeout=2.0) as response:
        status = json.load(response)
    if not isinstance(status, dict):
        raise ValueError("management status is not a JSON object")
    if status.get("schema") != "mptunnel.management.v5":
        raise ValueError(
            f"unexpected management status schema: {status.get('schema')!r}"
        )
    expected_fields = {
        "role": str,
        "summary": dict,
        "traffic": dict,
        "paths": list,
        "sessions": list,
        "flows": list,
    }
    for field, expected_type in expected_fields.items():
        if not isinstance(status.get(field), expected_type):
            raise ValueError(
                f"management status field {field!r} is not "
                f"{expected_type.__name__}"
            )
    result["management"] = status
except Exception as error:
    result["management_error"] = f"{type(error).__name__}: {error}"

print(json.dumps(result, sort_keys=True))
"""


def run(argv: list[str], timeout: float = 5.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=timeout,
        check=False,
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def compose_container_ids(
    compose_file: str,
    services: list[str],
    should_stop: Callable[[], bool] | None = None,
) -> dict[str, str]:
    ids: dict[str, str] = {}
    for service in services:
        if should_stop is not None and should_stop():
            break
        result = run(["docker", "compose", "-f", compose_file, "ps", "-q", service])
        lines = result.stdout.strip().splitlines()
        if result.returncode == 0 and lines:
            ids[service] = lines[0]
    return ids


def stop_requested(stop_file: Path | None) -> bool:
    return stop_file is not None and stop_file.exists()


def wait_for_stop_or_deadline(
    stop_file: Path | None,
    deadline_monotonic: float,
    poll_interval: float = STOP_POLL_INTERVAL_SECONDS,
) -> bool:
    while not stop_requested(stop_file):
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(max(poll_interval, 0.001), remaining))
    return True


def container_snapshot(container_id: str, port: int, token: str) -> dict[str, object] | None:
    try:
        result = run(
            [
                "docker",
                "exec",
                container_id,
                "python3",
                "-c",
                CONTAINER_SNAPSHOT_SCRIPT,
                str(port),
                token,
            ]
        )
    except subprocess.TimeoutExpired:
        return None
    if result.returncode != 0:
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def sample(args: argparse.Namespace) -> int:
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    stop_file = Path(args.stop_file) if args.stop_file else None
    started_at = time.monotonic()

    def should_stop() -> bool:
        return stop_requested(stop_file)

    ids = compose_container_ids(args.compose_file, args.services, should_stop)
    with output.open("a", encoding="utf-8") as handle:
        while not should_stop():
            sample_started = time.monotonic()
            for service, container_id in ids.items():
                if should_stop():
                    break
                sample_started_monotonic_ns = time.monotonic_ns()
                payload = container_snapshot(container_id, args.port, args.token)
                sample_finished_monotonic_ns = time.monotonic_ns()
                if payload is None:
                    continue
                row = {
                    "case": args.case,
                    "ts": utc_now(),
                    "t_mono_ms": int((time.monotonic() - started_at) * 1000),
                    "sample_started_monotonic_ns": sample_started_monotonic_ns,
                    "sample_finished_monotonic_ns": sample_finished_monotonic_ns,
                    "service": service,
                    "container_id": container_id[:12],
                    **payload,
                }
                print(json.dumps(row, sort_keys=True), file=handle, flush=True)
            deadline = sample_started + max(args.interval, 0.0)
            if wait_for_stop_or_deadline(stop_file, deadline):
                break
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--compose-file", required=True)
    parser.add_argument("--case", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--stop-file")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--port", type=int, default=17600)
    parser.add_argument("--token", required=True)
    parser.add_argument("--services", nargs="+", default=["client", "server"])
    return parser


def main() -> int:
    return sample(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())

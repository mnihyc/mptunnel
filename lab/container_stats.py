#!/usr/bin/env python3
"""Sample per-container CPU, memory, and network counters for Docker lab runs."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


BYTE_UNITS = {
    "b": 1,
    "kb": 1000,
    "mb": 1000**2,
    "gb": 1000**3,
    "tb": 1000**4,
    "kib": 1024,
    "mib": 1024**2,
    "gib": 1024**3,
    "tib": 1024**4,
}

STOP_POLL_INTERVAL_SECONDS = 0.05


def run(argv: list[str], timeout: float = 3.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=timeout,
        check=False,
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_percent(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value.strip().rstrip("%"))
    except ValueError:
        return None


def parse_size(value: str | None) -> int | None:
    if not value:
        return None
    value = value.strip()
    number = ""
    unit = ""
    for char in value:
        if char.isdigit() or char == ".":
            number += char
        elif not char.isspace():
            unit += char
    if not number:
        return None
    multiplier = BYTE_UNITS.get(unit.lower(), 1)
    return int(float(number) * multiplier)


def parse_mem_usage(value: str | None) -> tuple[int | None, int | None]:
    if not value:
        return None, None
    parts = value.split("/")
    usage = parse_size(parts[0])
    limit = parse_size(parts[1]) if len(parts) > 1 else None
    return usage, limit


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
        container_id = result.stdout.strip().splitlines()
        if result.returncode == 0 and container_id:
            ids[service] = container_id[0]
    return ids


def stop_requested(stop_file: Path | None) -> bool:
    return stop_file is not None and stop_file.exists()


def wait_for_stop_or_deadline(
    stop_file: Path | None,
    deadline_monotonic: float,
    poll_interval: float = STOP_POLL_INTERVAL_SECONDS,
) -> bool:
    """Wait until the sampling deadline, returning early when stop is requested."""
    while not stop_requested(stop_file):
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(max(poll_interval, 0.001), remaining))
    return True


def docker_stats(container_ids: list[str]) -> dict[str, dict[str, object]]:
    if not container_ids:
        return {}
    result = run(
        ["docker", "stats", "--no-stream", "--format", "{{json .}}", *container_ids],
        timeout=8.0,
    )
    stats: dict[str, dict[str, object]] = {}
    if result.returncode != 0:
        return stats
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        stat_id = str(row.get("ID", ""))
        if stat_id:
            stats[stat_id] = row
    return stats


def stat_for_container(stats: dict[str, dict[str, object]], container_id: str) -> dict[str, object]:
    for stat_id, row in stats.items():
        if container_id.startswith(stat_id) or stat_id.startswith(container_id[:12]):
            return row
    return {}


def read_netdev(container_id: str) -> dict[str, int] | None:
    result = run(["docker", "exec", container_id, "cat", "/proc/net/dev"])
    if result.returncode != 0:
        return None
    totals = {
        "rx_bytes": 0,
        "rx_packets": 0,
        "tx_bytes": 0,
        "tx_packets": 0,
    }
    for line in result.stdout.splitlines():
        if ":" not in line:
            continue
        iface, values = line.split(":", 1)
        if iface.strip() == "lo":
            continue
        fields = values.split()
        if len(fields) < 16:
            continue
        totals["rx_bytes"] += int(fields[0])
        totals["rx_packets"] += int(fields[1])
        totals["tx_bytes"] += int(fields[8])
        totals["tx_packets"] += int(fields[9])
    return totals


def snapshot_netdev(
    compose_file: str,
    services: list[str],
) -> dict[str, object]:
    ids = compose_container_ids(compose_file, services)
    service_rows = {}
    for service, container_id in sorted(ids.items()):
        net = read_netdev(container_id)
        if net is None:
            continue
        service_rows[service] = {
            "container_id": container_id[:12],
            **net,
        }
    return {"services": service_rows, "ts": utc_now()}


def load_snapshot(path_text: str | None) -> dict[str, object]:
    if not path_text:
        return {}
    path = Path(path_text)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def apply_snapshot_deltas(
    services: dict[str, dict[str, object]],
    before: dict[str, object],
    after: dict[str, object],
) -> None:
    before_services = before.get("services")
    after_services = after.get("services")
    if not isinstance(before_services, dict) or not isinstance(after_services, dict):
        return
    for service, after_row in after_services.items():
        before_row = before_services.get(service)
        if not isinstance(before_row, dict) or not isinstance(after_row, dict):
            continue
        service_summary = services.setdefault(service, {})
        for field in ("rx_bytes", "rx_packets", "tx_bytes", "tx_packets"):
            before_value = int(before_row.get(field, 0) or 0)
            after_value = int(after_row.get(field, 0) or 0)
            service_summary[f"delta_{field}"] = max(after_value - before_value, 0)
        service_summary["netdev_delta_source"] = "case_before_after_snapshot"


def rate_fields(
    current: dict[str, int],
    previous: tuple[float, dict[str, int]] | None,
    now_monotonic: float,
) -> dict[str, float]:
    if previous is None:
        return {
            "rx_mbps": 0.0,
            "tx_mbps": 0.0,
            "rx_pps": 0.0,
            "tx_pps": 0.0,
        }
    previous_time, previous_counters = previous
    elapsed = max(now_monotonic - previous_time, 1e-6)
    rx_bytes = max(current["rx_bytes"] - previous_counters["rx_bytes"], 0)
    tx_bytes = max(current["tx_bytes"] - previous_counters["tx_bytes"], 0)
    rx_packets = max(current["rx_packets"] - previous_counters["rx_packets"], 0)
    tx_packets = max(current["tx_packets"] - previous_counters["tx_packets"], 0)
    return {
        "rx_mbps": rx_bytes * 8.0 / elapsed / 1_000_000.0,
        "tx_mbps": tx_bytes * 8.0 / elapsed / 1_000_000.0,
        "rx_pps": rx_packets / elapsed,
        "tx_pps": tx_packets / elapsed,
    }


def sample(args: argparse.Namespace) -> int:
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    stop_file = Path(args.stop_file) if args.stop_file else None
    previous_net: dict[str, tuple[float, dict[str, int]]] = {}
    started_at = time.monotonic()
    sample_index: dict[str, int] = defaultdict(int)

    def should_stop() -> bool:
        return stop_requested(stop_file)

    with output.open("a", encoding="utf-8") as handle:
        while not stop_requested(stop_file):
            now_monotonic = time.monotonic()
            ids = compose_container_ids(args.compose_file, args.services, should_stop)
            if should_stop():
                break
            stats = docker_stats(list(ids.values()))
            if should_stop():
                break
            for service, container_id in ids.items():
                if should_stop():
                    break
                sample_index[service] += 1
                stat = stat_for_container(stats, container_id)
                mem_usage, mem_limit = parse_mem_usage(str(stat.get("MemUsage", "")))
                net = read_netdev(container_id)
                if net is None:
                    continue
                rates = rate_fields(net, previous_net.get(service), now_monotonic)
                previous_net[service] = (now_monotonic, net)
                row = {
                    "case": args.case,
                    "ts": utc_now(),
                    "t_mono_ms": int((now_monotonic - started_at) * 1000),
                    "sample_index": sample_index[service],
                    "service": service,
                    "container_id": container_id[:12],
                    "cpu_pct": parse_percent(str(stat.get("CPUPerc", ""))),
                    "mem_bytes": mem_usage,
                    "mem_limit_bytes": mem_limit,
                    "mem_pct": parse_percent(str(stat.get("MemPerc", ""))),
                    **net,
                    **rates,
                }
                print(json.dumps(row, sort_keys=True), file=handle, flush=True)
            next_sample_at = time.monotonic() + max(args.interval, 0.0)
            if wait_for_stop_or_deadline(stop_file, next_sample_at):
                break
    return 0


def safe_avg(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def summarize(args: argparse.Namespace) -> int:
    rows = []
    path = Path(args.input)
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    by_service: dict[str, list[dict[str, object]]] = defaultdict(list)
    for row in rows:
        service = str(row.get("service", "unknown"))
        by_service[service].append(row)

    services = {}
    for service, service_rows in sorted(by_service.items()):
        numeric = {
            key: [
                float(row[key])
                for row in service_rows
                if isinstance(row.get(key), (int, float)) and math.isfinite(float(row[key]))
            ]
            for key in (
                "cpu_pct",
                "mem_bytes",
                "mem_pct",
                "rx_mbps",
                "tx_mbps",
                "rx_pps",
                "tx_pps",
            )
        }
        last = service_rows[-1]
        first = service_rows[0]
        services[service] = {
            "samples": len(service_rows),
            "avg_cpu_pct": round(safe_avg(numeric["cpu_pct"]), 3),
            "max_cpu_pct": round(max(numeric["cpu_pct"], default=0.0), 3),
            "max_mem_bytes": int(max(numeric["mem_bytes"], default=0.0)),
            "max_mem_pct": round(max(numeric["mem_pct"], default=0.0), 3),
            "avg_rx_mbps": round(safe_avg(numeric["rx_mbps"]), 3),
            "avg_tx_mbps": round(safe_avg(numeric["tx_mbps"]), 3),
            "max_rx_mbps": round(max(numeric["rx_mbps"], default=0.0), 3),
            "max_tx_mbps": round(max(numeric["tx_mbps"], default=0.0), 3),
            "avg_rx_pps": round(safe_avg(numeric["rx_pps"]), 3),
            "avg_tx_pps": round(safe_avg(numeric["tx_pps"]), 3),
            "max_rx_pps": round(max(numeric["rx_pps"], default=0.0), 3),
            "max_tx_pps": round(max(numeric["tx_pps"], default=0.0), 3),
            "delta_rx_bytes": int(last.get("rx_bytes", 0)) - int(first.get("rx_bytes", 0)),
            "delta_tx_bytes": int(last.get("tx_bytes", 0)) - int(first.get("tx_bytes", 0)),
            "delta_rx_packets": int(last.get("rx_packets", 0)) - int(first.get("rx_packets", 0)),
            "delta_tx_packets": int(last.get("tx_packets", 0)) - int(first.get("tx_packets", 0)),
        }

    apply_snapshot_deltas(
        services,
        load_snapshot(args.netdev_before),
        load_snapshot(args.netdev_after),
    )

    summary = {
        "file": str(path),
        "samples": len(rows),
        "services": services,
    }
    print(json.dumps(summary, separators=(",", ":"), sort_keys=True))
    return 0


def snapshot(args: argparse.Namespace) -> int:
    print(
        json.dumps(
            snapshot_netdev(args.compose_file, args.services),
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command")

    sample_parser = subparsers.add_parser("sample")
    sample_parser.add_argument("--compose-file", required=True)
    sample_parser.add_argument("--case", required=True)
    sample_parser.add_argument("--output", required=True)
    sample_parser.add_argument("--stop-file", required=True)
    sample_parser.add_argument("--interval", type=float, default=1.0)
    sample_parser.add_argument("--services", nargs="+", default=["client", "server", "target"])
    sample_parser.set_defaults(func=sample)

    summarize_parser = subparsers.add_parser("summarize")
    summarize_parser.add_argument("--input", required=True)
    summarize_parser.add_argument("--netdev-before")
    summarize_parser.add_argument("--netdev-after")
    summarize_parser.set_defaults(func=summarize)

    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("--compose-file", required=True)
    snapshot_parser.add_argument("--services", nargs="+", default=["client", "server", "target"])
    snapshot_parser.set_defaults(func=snapshot)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help(file=sys.stderr)
        return 2
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Collect bounded, live kernel evidence for MPTCP baseline cases."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


MPTCP_SOCKET_COMMAND = (
    "ss",
    "-M",
    "-nH",
    "-O",
    "-e",
    "-i",
    "state",
    "connected",
)
MPTCP_SUBFLOW_COMMAND = (
    "ss",
    "-t",
    "-nH",
    "-O",
    "-e",
    "state",
    "connected",
)
TOKEN_PATTERN = re.compile(r"(?:^|\s)token:([^\s]+)")
ADDITIONAL_SUBFLOWS_PATTERN = re.compile(r"(?:^|\s)subflows:(\d+)")
SUBFLOWS_TOTAL_PATTERN = re.compile(r"(?:^|\s)subflows_total:(\d+)")
STOP_POLL_SECONDS = 0.05


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def run_command(argv: tuple[str, ...]) -> tuple[int, str, str]:
    try:
        result = subprocess.run(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=2.0,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return 127, "", str(exc)
    return result.returncode, result.stdout, result.stderr.strip()


def endpoint_host(endpoint: str) -> str | None:
    if endpoint.startswith("["):
        host, separator, _ = endpoint[1:].partition("]")
        return host if separator else None
    host, separator, _ = endpoint.rpartition(":")
    return host if separator and host else None


def parse_subflow_line(line: str) -> dict[str, str | None]:
    fields = line.split()
    local = fields[3] if len(fields) >= 5 else None
    peer = fields[4] if len(fields) >= 5 else None
    token_match = TOKEN_PATTERN.search(line)
    return {
        "local": local,
        "local_address": endpoint_host(local) if local else None,
        "peer": peer,
        "peer_address": endpoint_host(peer) if peer else None,
        "token": token_match.group(1) if token_match else None,
        "raw": line,
    }


def parse_mptcp_socket_line(line: str) -> dict[str, object]:
    fields = line.split()
    token_match = TOKEN_PATTERN.search(line)
    additional_subflows_match = ADDITIONAL_SUBFLOWS_PATTERN.search(line)
    subflows_total_match = SUBFLOWS_TOTAL_PATTERN.search(line)
    return {
        "local": fields[3] if len(fields) >= 5 else None,
        "peer": fields[4] if len(fields) >= 5 else None,
        "token": token_match.group(1) if token_match else None,
        # Linux mptcpi_subflows excludes the initial subflow. Newer kernels
        # separately expose mptcpi_subflows_total, which includes it.
        "additional_subflows": (
            int(additional_subflows_match.group(1))
            if additional_subflows_match
            else None
        ),
        "subflows_total": (
            int(subflows_total_match.group(1)) if subflows_total_match else None
        ),
        "raw": line,
    }


def reported_subflow_counts(meta_socket: dict[str, object]) -> tuple[int | None, int | None]:
    """Return (additional, total) without combining the two kernel counters."""

    additional = meta_socket.get("additional_subflows")
    total = meta_socket.get("subflows_total")
    additional_count = (
        additional
        if isinstance(additional, int) and not isinstance(additional, bool)
        else None
    )
    total_count = total if isinstance(total, int) and not isinstance(total, bool) else None
    raw = meta_socket.get("raw")
    if isinstance(raw, str):
        if additional_count is None:
            match = ADDITIONAL_SUBFLOWS_PATTERN.search(raw)
            additional_count = int(match.group(1)) if match else None
        if total_count is None:
            match = SUBFLOWS_TOTAL_PATTERN.search(raw)
            total_count = int(match.group(1)) if match else None
    return additional_count, total_count


def take_sample(service: str, started: float) -> dict[str, object]:
    meta_exit, meta_stdout, meta_stderr = run_command(MPTCP_SOCKET_COMMAND)
    tcp_exit, tcp_stdout, tcp_stderr = run_command(MPTCP_SUBFLOW_COMMAND)
    meta_sockets = [
        parse_mptcp_socket_line(line)
        for line in meta_stdout.splitlines()
        if line.strip()
    ]
    subflows = [
        parse_subflow_line(line)
        for line in tcp_stdout.splitlines()
        if "tcp-ulp-mptcp" in line
    ]
    return {
        "kind": "sample",
        "schema_version": 1,
        "service": service,
        "timestamp": utc_now(),
        "elapsed_s": round(time.monotonic() - started, 6),
        "mptcp_socket_query_exit_code": meta_exit,
        "mptcp_subflow_query_exit_code": tcp_exit,
        "mptcp_socket_query_error": meta_stderr or None,
        "mptcp_subflow_query_error": tcp_stderr or None,
        "mptcp_socket_count": len(meta_sockets),
        "mptcp_subflow_count": len(subflows),
        "mptcp_sockets": meta_sockets,
        "mptcp_subflows": subflows,
    }


def wait_for_next_sample(stop_file: Path, deadline: float, interval: float) -> bool:
    wake_at = min(time.monotonic() + interval, deadline)
    while not stop_file.exists():
        remaining = wake_at - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(STOP_POLL_SECONDS, remaining))
    return True


def sample(args: argparse.Namespace) -> int:
    output = Path(args.output)
    stop_file = Path(args.stop_file)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    stop_file.unlink(missing_ok=True)
    started = time.monotonic()
    deadline = started + args.max_duration
    samples = 0
    stop_reason = "max_duration"
    with output.open("a", encoding="utf-8") as handle:
        while time.monotonic() < deadline:
            if stop_file.exists():
                stop_reason = "requested"
                break
            print(
                json.dumps(take_sample(args.service, started), sort_keys=True),
                file=handle,
                flush=True,
            )
            samples += 1
            if wait_for_next_sample(stop_file, deadline, args.interval):
                stop_reason = "requested"
                break
        print(
            json.dumps(
                {
                    "kind": "terminal",
                    "schema_version": 1,
                    "service": args.service,
                    "timestamp": utc_now(),
                    "elapsed_s": round(time.monotonic() - started, 6),
                    "sample_count": samples,
                    "stop_reason": stop_reason,
                },
                sort_keys=True,
            ),
            file=handle,
            flush=True,
        )
    return 0


def empty_service_summary() -> dict[str, object]:
    return {
        "sample_count": 0,
        "successful_query_samples": 0,
        "query_error_samples": 0,
        "max_mptcp_socket_count": 0,
        "max_mptcp_subflow_count": 0,
        "max_reported_additional_subflows_per_connection": None,
        "max_reported_total_subflows_per_connection": None,
        "max_subflows_per_token": None,
        "max_distinct_endpoint_pairs_per_token": None,
        "observed_local_addresses": [],
        "observed_peer_addresses": [],
        "multiple_subflows_per_connection_observed": False,
        "multipath_observed": False,
        "sampler_stop_reason": None,
    }


def summarize_rows(rows: list[dict[str, object]], artifact: str) -> dict[str, object]:
    samples_by_service: dict[str, list[dict[str, object]]] = defaultdict(list)
    terminal_by_service: dict[str, dict[str, object]] = {}
    errors_by_service: dict[str, list[str]] = defaultdict(list)
    for row in rows:
        service = row.get("service")
        if not isinstance(service, str) or not service:
            continue
        if row.get("kind") == "sample":
            samples_by_service[service].append(row)
        elif row.get("kind") == "terminal":
            terminal_by_service[service] = row
        elif row.get("kind") == "sampler_error":
            errors_by_service[service].append(str(row.get("error", "unknown error")))

    services: dict[str, dict[str, object]] = {}
    for service in sorted(
        set(samples_by_service) | set(terminal_by_service) | set(errors_by_service)
    ):
        summary = empty_service_summary()
        local_addresses: set[str] = set()
        peer_addresses: set[str] = set()
        max_per_token = 0
        max_pairs_per_token = 0
        max_reported_additional_subflows = 0
        max_reported_total_subflows = 0
        query_errors = 0
        successful_queries = 0
        for row in samples_by_service.get(service, []):
            summary["sample_count"] = int(summary["sample_count"]) + 1
            meta_exit = row.get("mptcp_socket_query_exit_code")
            subflow_exit = row.get("mptcp_subflow_query_exit_code")
            if meta_exit == 0 and subflow_exit == 0:
                successful_queries += 1
            else:
                query_errors += 1
            summary["max_mptcp_socket_count"] = max(
                int(summary["max_mptcp_socket_count"]),
                int(row.get("mptcp_socket_count", 0) or 0),
            )
            summary["max_mptcp_subflow_count"] = max(
                int(summary["max_mptcp_subflow_count"]),
                int(row.get("mptcp_subflow_count", 0) or 0),
            )
            meta_sockets = row.get("mptcp_sockets")
            if isinstance(meta_sockets, list):
                for meta_socket in meta_sockets:
                    if not isinstance(meta_socket, dict):
                        continue
                    additional, total = reported_subflow_counts(meta_socket)
                    if additional is not None:
                        max_reported_additional_subflows = max(
                            max_reported_additional_subflows, additional
                        )
                    if total is not None:
                        max_reported_total_subflows = max(
                            max_reported_total_subflows, total
                        )
            token_counts: Counter[str] = Counter()
            token_pairs: dict[str, set[tuple[str, str]]] = defaultdict(set)
            subflows = row.get("mptcp_subflows")
            if not isinstance(subflows, list):
                continue
            for subflow in subflows:
                if not isinstance(subflow, dict):
                    continue
                local_address = subflow.get("local_address")
                peer_address = subflow.get("peer_address")
                token = subflow.get("token")
                if isinstance(local_address, str):
                    local_addresses.add(local_address)
                if isinstance(peer_address, str):
                    peer_addresses.add(peer_address)
                if isinstance(token, str) and token:
                    token_counts[token] += 1
                    if isinstance(local_address, str) and isinstance(peer_address, str):
                        token_pairs[token].add((local_address, peer_address))
            if token_counts:
                max_per_token = max(max_per_token, max(token_counts.values()))
            if token_pairs:
                max_pairs_per_token = max(
                    max_pairs_per_token,
                    max(len(pairs) for pairs in token_pairs.values()),
                )
        summary["successful_query_samples"] = successful_queries
        summary["query_error_samples"] = query_errors
        summary["max_subflows_per_token"] = max_per_token or None
        summary["max_reported_additional_subflows_per_connection"] = (
            max_reported_additional_subflows or None
        )
        summary["max_reported_total_subflows_per_connection"] = (
            max_reported_total_subflows or None
        )
        summary["max_distinct_endpoint_pairs_per_token"] = (
            max_pairs_per_token or None
        )
        summary["observed_local_addresses"] = sorted(local_addresses)
        summary["observed_peer_addresses"] = sorted(peer_addresses)
        summary["multiple_subflows_per_connection_observed"] = (
            max_per_token > 1
            or max_reported_additional_subflows >= 1
            or max_reported_total_subflows >= 2
        )
        # One additional subflow already means the MPTCP connection went beyond
        # its initial subflow. New kernels report the inclusive total directly.
        summary["multipath_observed"] = (
            max_pairs_per_token > 1
            or max_reported_additional_subflows >= 1
            or max_reported_total_subflows >= 2
        )
        terminal = terminal_by_service.get(service)
        if terminal is not None:
            summary["sampler_stop_reason"] = terminal.get("stop_reason")
        summary["collection_errors"] = errors_by_service.get(service, [])
        services[service] = summary

    sample_count = sum(int(row["sample_count"]) for row in services.values())
    collection_ok = bool(services) and all(
        int(row["successful_query_samples"]) > 0
        and row["sampler_stop_reason"] in {"requested", "max_duration"}
        and not row["collection_errors"]
        for row in services.values()
    )
    multipath_observed = any(bool(row["multipath_observed"]) for row in services.values())
    aggregation_evidence = (
        "observed"
        if multipath_observed
        else "not_observed"
        if collection_ok
        else "unavailable"
    )
    return {
        "schema_version": 1,
        "source": "ss_mptcp_and_tcp_ulp_snapshots",
        "artifact": artifact,
        "sample_count": sample_count,
        "collection_ok": collection_ok,
        "multipath_observed": multipath_observed,
        "aggregation_evidence": aggregation_evidence,
        "counter_semantics": {
            "subflows": "additional_subflows_excluding_initial",
            "subflows_total": "total_subflows_including_initial",
        },
        "services": services,
        "limitation": (
            "ss -M proves connection subflow membership, not bytes carried per subflow; "
            "subflows excludes the initial subflow while subflows_total includes it"
        ),
    }


def summarize(args: argparse.Namespace) -> int:
    path = Path(args.input)
    rows: list[dict[str, object]] = []
    malformed_rows = 0
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                malformed_rows += 1
                continue
            if isinstance(row, dict):
                rows.append(row)
            else:
                malformed_rows += 1
    result = summarize_rows(rows, args.artifact or args.input)
    if malformed_rows:
        result["malformed_rows"] = malformed_rows
        result["collection_ok"] = False
    print(json.dumps(result, sort_keys=True))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    sampler = subparsers.add_parser("sample")
    sampler.add_argument("--service", required=True)
    sampler.add_argument("--output", required=True)
    sampler.add_argument("--stop-file", required=True)
    sampler.add_argument("--interval", type=float, default=1.0)
    sampler.add_argument("--max-duration", type=float, required=True)
    summarizer = subparsers.add_parser("summarize")
    summarizer.add_argument("--input", required=True)
    summarizer.add_argument("--artifact")
    args = parser.parse_args()
    if args.command == "sample":
        if args.interval <= 0 or args.max_duration <= 0:
            parser.error("sample interval and maximum duration must be positive")
        return sample(args)
    return summarize(args)


if __name__ == "__main__":
    raise SystemExit(main())

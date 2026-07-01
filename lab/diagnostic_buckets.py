#!/usr/bin/env python3
"""Classify focused mptunnel lab failures from diagnostic logs.

The classifier is intentionally lab-side only. It does not define production
policy; it turns existing diagnostic events into a decision-closing summary so
the next algorithm change starts from a concrete failure owner.
"""

from __future__ import annotations

import argparse
import collections
import json
import math
import re
from pathlib import Path
from typing import Any


DIAG_PREFIX = "mptunnel_lab_diag "
PERF_PREFIX = "mptunnel_lab_perf "

BUCKETS = (
    "carrier_teardown",
    "sender_starvation",
    "harmful_admission",
    "product_repair_ordering_debt",
    "control_starvation",
    "lab_noise",
)

TEARDOWN_PATTERNS = (
    "TCP path session closed",
    "session closed",
    "endpoint closed",
    "early eof",
    "connection closed",
    "failed to start",
    "handshake",
    "RemoteClosed",
    "PathHeartbeatTimeout",
)

CONTROL_FRAME_KINDS = {
    "open_stream",
    "open_dgram_flow",
    "stream_ack",
    "stream_max_data",
    "stream_fin",
    "stream_reset",
    "stream_detach",
    "datagram_feedback",
}


def analyze_row(
    row: dict[str, Any],
    log_artifacts: dict[str, Any] | None = None,
    telemetry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    log_artifacts = log_artifacts or row.get("log_artifacts") or {}
    telemetry = telemetry or row.get("container_telemetry") or {}
    logs = read_log_artifacts(log_artifacts)
    for field in ("client_log_tail", "server_log_tail", "probe_stderr_tail"):
        if row.get(field):
            logs.append((field, str(row[field])))

    metrics = collect_metrics(row, logs, telemetry)
    scores, reasons = score_buckets(row, metrics)
    dominant = dominant_bucket(row, metrics, scores)
    return {
        "dominant_bucket": dominant,
        "bucket_scores": {bucket: scores.get(bucket, 0) for bucket in BUCKETS},
        "reasons": reasons[:16],
        "metrics": metrics,
    }


def read_log_artifacts(log_artifacts: dict[str, Any]) -> list[tuple[str, str]]:
    logs: list[tuple[str, str]] = []
    for service, item in (
        (log_artifacts.get("services") or {}).items()
        if isinstance(log_artifacts.get("services"), dict)
        else ()
    ):
        path = Path(str(item.get("file", "")))
        if not path.exists() or not path.is_file():
            continue
        try:
            logs.append((service, path.read_text(encoding="utf-8", errors="replace")))
        except OSError:
            continue
    return logs


def collect_metrics(
    row: dict[str, Any],
    logs: list[tuple[str, str]],
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    events: collections.Counter[str] = collections.Counter()
    candidate_reasons: collections.Counter[str] = collections.Counter()
    selected_underlays: collections.Counter[str] = collections.Counter()
    path_queue_waits: dict[str, list[int]] = collections.defaultdict(list)
    dispatch_queue_delays: list[int] = []
    repair_bytes_after: list[int] = []
    stream_ack_released_bytes: list[int] = []
    enqueue_count = 0
    dispatch_count = 0
    server_response_frames = 0
    sender_decisions = 0
    max_receive_hole_bytes = 0
    max_stream_ordering_debt = 0
    max_command_pending_bytes = 0
    max_server_sender_queue_bytes = 0
    reliable_open_timeouts = 0
    reliable_open_successes = 0
    datagram_timeouts = 0
    teardown_hits: collections.Counter[str] = collections.Counter()

    for _, text in logs:
        lower_text = text.lower()
        for pattern in TEARDOWN_PATTERNS:
            if pattern.lower() in lower_text:
                teardown_hits[pattern] += lower_text.count(pattern.lower())
        for line in text.splitlines():
            parsed = parse_diag_line(line)
            if not parsed:
                continue
            event = parsed.get("event")
            if not event:
                continue
            events[event] += 1
            if event == "server_sender_enqueue":
                enqueue_count += 1
                max_server_sender_queue_bytes = max(
                    max_server_sender_queue_bytes, int_field(parsed, "queue_bytes")
                )
            elif event == "server_sender_dispatch":
                dispatch_count += 1
                dispatch_queue_delays.append(int_field(parsed, "queue_delay_ms"))
            elif event == "server_response_stream_data_frame":
                server_response_frames += 1
            elif (
                event == "sender_service_decision"
                and parsed.get("role") == "server"
                and parsed.get("frame_kind") == "stream_data"
                and parsed.get("decision_kind") == "primary"
            ):
                sender_decisions += 1
            elif event == "receive_hole":
                max_receive_hole_bytes = max(
                    max_receive_hole_bytes, int_field(parsed, "reorder_bytes")
                )
            elif event == "stream_ack_received":
                repair_bytes_after.append(int_field(parsed, "repair_bytes_after"))
                stream_ack_released_bytes.append(int_field(parsed, "released_bytes"))
            elif event == "server_bulk_output_candidate":
                reason = parsed.get("reason")
                if reason:
                    candidate_reasons[reason] += 1
                max_stream_ordering_debt = max(
                    max_stream_ordering_debt, int_field(parsed, "stream_ordering_debt")
                )
                max_command_pending_bytes = max(
                    max_command_pending_bytes, int_field(parsed, "command_pending_bytes")
                )
            elif event == "server_bulk_output_selected":
                selected_underlays[parsed.get("path_underlay", "unknown")] += 1
            elif event == "path_command_queue_send":
                kind = parsed.get("frame_kind") or parsed.get("command_kind") or "unknown"
                path_queue_waits[kind].append(int_field(parsed, "wait_ms"))
            elif event == "reliable_stream_open_timeout":
                reliable_open_timeouts += 1
            elif event == "reliable_stream_open_success":
                reliable_open_successes += 1
            elif event in {"udp_datagram_response_timeout", "udp_datagram_retransmit_after_timeout"}:
                datagram_timeouts += 1

    control_waits = [
        value
        for kind, values in path_queue_waits.items()
        if kind in CONTROL_FRAME_KINDS
        for value in values
    ]
    data_waits = [
        value
        for kind, values in path_queue_waits.items()
        if kind == "stream_data"
        for value in values
    ]
    result_bytes = int(row.get("bytes") or row.get("bulk_bytes") or 0)
    receive_hole_significant = (
        max_receive_hole_bytes > max(1024 * 1024, result_bytes // 100)
        if result_bytes > 0
        else max_receive_hole_bytes > 1024 * 1024
    )
    repair_debt_has_hole_evidence = (
        events.get("receive_hole", 0) > 0 and max_receive_hole_bytes > 256 * 1024
    )
    return {
        "event_counts": dict(events.most_common(32)),
        "server_sender_enqueue_count": enqueue_count,
        "server_sender_dispatch_count": dispatch_count,
        "server_response_stream_data_frames": server_response_frames,
        "server_sender_service_stream_data_decisions": sender_decisions,
        "server_sender_conformance_delta": server_response_frames - sender_decisions,
        "server_sender_dispatch_p95_ms": percentile(dispatch_queue_delays, 0.95),
        "server_sender_dispatch_max_ms": max_or_none(dispatch_queue_delays),
        "server_sender_queue_max_bytes": max_server_sender_queue_bytes,
        "path_queue_control_p95_ms": percentile(control_waits, 0.95),
        "path_queue_control_max_ms": max_or_none(control_waits),
        "path_queue_data_p95_ms": percentile(data_waits, 0.95),
        "path_queue_data_max_ms": max_or_none(data_waits),
        "path_queue_waits_by_kind": {
            kind: {
                "count": len(values),
                "p95_ms": percentile(values, 0.95),
                "max_ms": max_or_none(values),
            }
            for kind, values in sorted(path_queue_waits.items())
        },
        "receive_hole_events": events.get("receive_hole", 0),
        "receive_hole_max_bytes": max_receive_hole_bytes,
        "receive_hole_max_ratio": ratio(max_receive_hole_bytes, result_bytes),
        "receive_hole_significant": receive_hole_significant,
        "stream_ordering_debt_max_bytes": max_stream_ordering_debt,
        "command_pending_max_bytes": max_command_pending_bytes,
        "repair_bytes_after_max": max_or_none(repair_bytes_after),
        "repair_debt_has_hole_evidence": repair_debt_has_hole_evidence,
        "stream_ack_released_bytes_total": sum(stream_ack_released_bytes),
        "candidate_reasons": dict(candidate_reasons.most_common(16)),
        "selected_underlays": dict(selected_underlays),
        "reliable_stream_open_timeouts": reliable_open_timeouts,
        "reliable_stream_open_successes": reliable_open_successes,
        "udp_datagram_timeouts": datagram_timeouts,
        "teardown_hits": dict(teardown_hits.most_common(16)),
        "container_telemetry_available": bool(telemetry),
        "server_avg_tx_mbps": service_metric(telemetry, "server", "avg_tx_mbps"),
        "client_avg_rx_mbps": service_metric(telemetry, "client", "avg_rx_mbps"),
        "result_goodput_mbps": row.get("goodput_mbps") or row.get("bulk_goodput_mbps"),
        "result_status": row.get("status"),
    }


def score_buckets(
    row: dict[str, Any], metrics: dict[str, Any]
) -> tuple[collections.Counter[str], list[str]]:
    scores: collections.Counter[str] = collections.Counter()
    reasons: list[str] = []

    def add(bucket: str, amount: int, reason: str) -> None:
        if amount <= 0:
            return
        scores[bucket] += amount
        reasons.append(f"{bucket}: {reason}")

    if not metrics["event_counts"] and not metrics["teardown_hits"]:
        add("lab_noise", 4, "no diagnostic events or runtime teardown text were captured")

    teardown_count = sum(metrics["teardown_hits"].values())
    add("carrier_teardown", min(teardown_count, 8), f"{teardown_count} teardown-like log hits")

    enqueue_count = metrics["server_sender_enqueue_count"]
    dispatch_count = metrics["server_sender_dispatch_count"]
    if enqueue_count and dispatch_count == 0:
        add("sender_starvation", 8, "server enqueued response bytes but dispatched none")
    elif enqueue_count > dispatch_count:
        missing = enqueue_count - dispatch_count
        add(
            "sender_starvation",
            min(5, max(1, missing * 5 // max(enqueue_count, 1))),
            f"server enqueue/dispatch gap {enqueue_count}/{dispatch_count}",
        )
    if metrics["server_sender_dispatch_max_ms"] and metrics["server_sender_dispatch_max_ms"] > 250:
        add(
            "sender_starvation",
            3,
            f"server sender dispatch max delay {metrics['server_sender_dispatch_max_ms']} ms",
        )
    if metrics["server_sender_conformance_delta"]:
        add(
            "sender_starvation",
            5,
            f"server response STREAM_DATA/decision delta {metrics['server_sender_conformance_delta']}",
        )

    if metrics["receive_hole_events"] and (
        metrics["receive_hole_significant"] or metrics["receive_hole_events"] > 100
    ):
        add(
            "product_repair_ordering_debt",
            min(5, 1 + int(math.log10(metrics["receive_hole_events"] + 1))),
            f"{metrics['receive_hole_events']} receive-hole events",
        )
    if metrics["receive_hole_max_ratio"] and metrics["receive_hole_max_ratio"] > 0.05:
        add(
            "harmful_admission",
            5,
            f"receive-hole max is {metrics['receive_hole_max_ratio']:.3f} of delivered bytes",
        )
    if metrics["receive_hole_max_bytes"] > 4 * 1024 * 1024:
        add(
            "harmful_admission",
            3,
            f"receive-hole max {metrics['receive_hole_max_bytes']} bytes",
        )
    if metrics["stream_ordering_debt_max_bytes"] > 0 and (
        metrics["receive_hole_significant"] or metrics["receive_hole_events"] > 100
    ):
        add(
            "harmful_admission",
            2,
            f"stream ordering debt reached {metrics['stream_ordering_debt_max_bytes']} bytes",
        )
    if (
        len(metrics["selected_underlays"]) > 1
        and metrics["receive_hole_events"]
        and (metrics["receive_hole_significant"] or metrics["receive_hole_events"] > 100)
    ):
        add(
            "harmful_admission",
            2,
            f"mixed underlay selection with receive holes: {metrics['selected_underlays']}",
        )
    if (
        metrics["repair_bytes_after_max"]
        and metrics["repair_bytes_after_max"] > 0
        and metrics["repair_debt_has_hole_evidence"]
    ):
        add(
            "product_repair_ordering_debt",
            3,
            f"repair bytes remained after ACK: max {metrics['repair_bytes_after_max']}",
        )

    control_failures = 0
    for field in ("small_fail", "interactive_fail"):
        try:
            control_failures += int(row.get(field) or 0)
        except (TypeError, ValueError):
            pass
    if row.get("udp_error") or metrics["udp_datagram_timeouts"]:
        control_failures += 1
    if metrics["reliable_stream_open_timeouts"]:
        control_failures += metrics["reliable_stream_open_timeouts"]
    if control_failures:
        add("control_starvation", min(8, control_failures), f"{control_failures} control/open symptoms")
    if metrics["path_queue_control_max_ms"] and metrics["path_queue_control_max_ms"] > 100:
        add(
            "control_starvation",
            4,
            f"control path-queue max wait {metrics['path_queue_control_max_ms']} ms",
        )

    if row.get("status") not in {"ok", "loss"} and sum(scores.values()) == 0:
        add("lab_noise", 2, "row failed without enough diagnostic attribution")

    return scores, reasons


def dominant_bucket(
    row: dict[str, Any], metrics: dict[str, Any], scores: collections.Counter[str]
) -> str:
    if row.get("status") == "ok" and not scores:
        return "none"
    if not scores:
        return "lab_noise"
    return max(BUCKETS, key=lambda bucket: (scores.get(bucket, 0), -BUCKETS.index(bucket)))


def parse_diag_line(line: str) -> dict[str, str] | None:
    if DIAG_PREFIX not in line:
        return None
    payload = line.split(DIAG_PREFIX, 1)[1]
    return {match.group("key"): match.group("value") for match in KEY_VALUE_RE.finditer(payload)}


KEY_VALUE_RE = re.compile(r"(?P<key>[A-Za-z0-9_]+)=(?P<value>[^ \n\r\t]+)")


def int_field(fields: dict[str, str], name: str) -> int:
    value = fields.get(name)
    if value is None:
        return 0
    try:
        return int(value)
    except ValueError:
        return 0


def percentile(values: list[int], rank: float) -> int | None:
    if not values:
        return None
    ordered = sorted(values)
    index = round((len(ordered) - 1) * rank)
    return ordered[index]


def max_or_none(values: list[int]) -> int | None:
    return max(values) if values else None


def ratio(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def service_metric(telemetry: dict[str, Any], service: str, name: str) -> Any:
    return ((telemetry.get("services") or {}).get(service) or {}).get(name)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--row-json", required=True)
    parser.add_argument("--log-artifacts-json", default="{}")
    parser.add_argument("--telemetry-json", default="{}")
    args = parser.parse_args()
    row = json.loads(args.row_json)
    log_artifacts = json.loads(args.log_artifacts_json)
    telemetry = json.loads(args.telemetry_json)
    print(
        json.dumps(
            analyze_row(row, log_artifacts, telemetry),
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

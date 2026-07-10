"""Shared lab result enrichment helpers."""

from __future__ import annotations

import math
from typing import Any


def _flag_enabled(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes"}


def enrich_instrumentation(
    row: dict[str, Any],
    lab_diag_value: Any,
    lab_perf_value: Any,
    lab_diag_events_value: Any,
) -> None:
    """Add consistent instrumentation provenance to an mptunnel result row."""

    lab_diag = _flag_enabled(lab_diag_value)
    lab_perf = _flag_enabled(lab_perf_value)
    instrumented_run = lab_diag or lab_perf
    row["lab_diagnostics_enabled"] = lab_diag
    row["lab_perf_enabled"] = lab_perf
    if lab_diag:
        events = sorted(
            {
                event.strip()
                for event in str(lab_diag_events_value or "").split(",")
                if event.strip()
            }
        )
        row["lab_diagnostic_events"] = ["*"] if not events or "*" in events else events
    else:
        row.pop("lab_diagnostic_events", None)
    row["performance_comparable"] = not instrumented_run
    if instrumented_run:
        row["performance_comparable_reason"] = (
            "diagnostic/perf instrumentation is for causal analysis only; "
            "use non-instrumented release rows for throughput comparisons"
        )
    else:
        row.pop("performance_comparable_reason", None)


def enrich_instrumentation_for_scope(
    row: dict[str, Any],
    mptunnel_row_value: Any,
    lab_diag_value: Any,
    lab_perf_value: Any,
    lab_diag_events_value: Any,
) -> tuple[bool, bool]:
    """Enrich product rows while keeping direct/external controls unlabelled."""

    if not _flag_enabled(mptunnel_row_value):
        for field in (
            "lab_diagnostics_enabled",
            "lab_perf_enabled",
            "lab_diagnostic_events",
            "performance_comparable",
            "performance_comparable_reason",
        ):
            row.pop(field, None)
        return False, False
    enrich_instrumentation(row, lab_diag_value, lab_perf_value, lab_diag_events_value)
    return row["lab_diagnostics_enabled"], row["lab_perf_enabled"]


def _number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def _int_number(value: Any) -> int | None:
    number = _number(value)
    if number is None or number < 0:
        return None
    return int(number)


def application_payload_bytes(row: dict[str, Any]) -> tuple[int | None, str | None]:
    """Return delivered/requested application bytes represented by a lab row.

    The value is intentionally the user-visible payload measured by the probe,
    not mptunnel product-frame bytes. Mixed workload rows should provide an
    explicit all-lane payload sum so overhead estimates do not subtract only the
    bulk transfer while counting latency and datagram traffic in tunnel bytes.
    """

    for field in ("mixed_app_payload_bytes", "bytes", "bulk_bytes"):
        value = _int_number(row.get(field))
        if value is not None and value > 0:
            return value, field

    if row.get("protocol") == "udp":
        attempted = _int_number(row.get("count"))
        received = _int_number(row.get("received"))
        payload_bytes = _int_number(row.get("payload_bytes"))
        if attempted is not None and received is not None and payload_bytes is not None:
            total_payloads = attempted + received
            if total_payloads > 0:
                return total_payloads * payload_bytes, "udp_count_plus_received*payload_bytes"
        if received is not None and payload_bytes is not None and received > 0:
            return received * payload_bytes, "received*payload_bytes"

    return None, None


def client_tunnel_traffic_bytes(telemetry: dict[str, Any]) -> int | None:
    services = telemetry.get("services")
    if not isinstance(services, dict):
        return None
    client = services.get("client")
    if not isinstance(client, dict):
        return None
    rx = _int_number(client.get("delta_rx_bytes"))
    tx = _int_number(client.get("delta_tx_bytes"))
    if rx is None or tx is None:
        return None
    total = rx + tx
    return total if total > 0 else None


def enrich_traffic_overhead(row: dict[str, Any], telemetry: dict[str, Any]) -> None:
    """Add approximate traffic overhead fields to a lab result row in place."""

    app_bytes, app_source = application_payload_bytes(row)
    tunnel_bytes = client_tunnel_traffic_bytes(telemetry)
    if app_bytes is None or tunnel_bytes is None or app_bytes <= 0:
        return

    overhead_bytes = max(tunnel_bytes - app_bytes, 0)
    overhead_ratio = overhead_bytes / app_bytes
    row["app_payload_bytes"] = app_bytes
    row["app_payload_source"] = app_source
    row["tunnel_traffic_bytes_approx"] = tunnel_bytes
    row["traffic_overhead_bytes_approx"] = overhead_bytes
    row["traffic_overhead_ratio_approx"] = round(overhead_ratio, 6)
    row["traffic_overhead_pct_approx"] = round(overhead_ratio * 100.0, 3)
    row["traffic_overhead_source"] = "client_container_non_loopback_netdev_delta_rx_tx"
    row["traffic_overhead_note"] = (
        "Approximate: uses client container non-loopback rx+tx deltas and "
        "probe-visible payload bytes; includes tunnel control, retransmission, "
        "path proof, duplicate/repair traffic, TCP/QUIC/IP headers, and sampling skew."
    )

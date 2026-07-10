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


def service_non_loopback_traffic_bytes(
    telemetry: dict[str, Any], service: str
) -> int | None:
    services = telemetry.get("services")
    if not isinstance(services, dict):
        return None
    service_row = services.get(service)
    if not isinstance(service_row, dict):
        return None
    rx = _int_number(service_row.get("delta_rx_bytes"))
    tx = _int_number(service_row.get("delta_tx_bytes"))
    if rx is None or tx is None:
        return None
    total = rx + tx
    return total if total > 0 else None


def client_edge_traffic_bytes(telemetry: dict[str, Any]) -> int | None:
    return service_non_loopback_traffic_bytes(telemetry, "client")


def enrich_traffic_overhead(row: dict[str, Any], telemetry: dict[str, Any]) -> None:
    """Add case-boundary edge-traffic diagnostics to a lab result row in place."""

    app_bytes, app_source = application_payload_bytes(row)
    client_edge_bytes = client_edge_traffic_bytes(telemetry)
    if app_bytes is None or client_edge_bytes is None or app_bytes <= 0:
        return

    client_probe_excess_bytes = client_edge_bytes - app_bytes
    client_probe_excess_ratio = client_probe_excess_bytes / app_bytes
    overhead_bytes = max(client_probe_excess_bytes, 0)
    overhead_ratio = overhead_bytes / app_bytes
    row["traffic_metric_version"] = 3
    row["app_payload_bytes"] = app_bytes
    row["app_payload_source"] = app_source
    row["traffic_accounting_ratio_reference"] = "probe_payload_bytes"
    row["client_edge_traffic_bytes_approx"] = client_edge_bytes
    row["client_vs_probe_payload_excess_bytes_approx"] = client_probe_excess_bytes
    row["client_vs_probe_payload_excess_ratio_approx"] = round(
        client_probe_excess_ratio, 6
    )
    row["client_vs_probe_payload_excess_pct_approx"] = round(
        client_probe_excess_ratio * 100.0, 3
    )
    row["traffic_accounting_source"] = (
        "client_container_non_loopback_netdev_case_boundary_delta"
    )
    row["traffic_accounting_note"] = (
        "Signed aggregate case-boundary differences. Sequential snapshots, "
        "opposite-direction in-flight bytes, endpoint headers/control, and "
        "unrelated interface traffic can affect them; they are diagnostics, "
        "not transport-expansion estimates."
    )

    # Compatibility fields predate metric version 3 and also appear on direct rows.
    row["tunnel_traffic_bytes_approx"] = client_edge_bytes
    row["traffic_overhead_bytes_approx"] = overhead_bytes
    row["traffic_overhead_ratio_approx"] = round(overhead_ratio, 6)
    row["traffic_overhead_pct_approx"] = round(overhead_ratio * 100.0, 3)
    row["traffic_overhead_source"] = "client_container_non_loopback_netdev_delta_rx_tx"
    row["traffic_overhead_note"] = (
        "Legacy client/probe delivery-window gap: uses client container "
        "non-loopback rx+tx deltas minus probe-visible payload bytes. It mixes "
        "transport expansion with bidirectional in-flight bytes, endpoint "
        "headers/control, unrelated traffic, and sequential snapshot skew, so "
        "it is not independently a transport-overhead measurement."
    )
    row["traffic_expansion_estimate_available"] = False
    row["traffic_expansion_exact_available"] = False
    row["traffic_expansion_unavailable_reasons"] = [
        "aggregate_bidirectional_counters_do_not_separate_directional_inflight_bytes",
        "case_boundary_endpoint_snapshots_are_sequential_not_atomic",
        "aggregate_endpoint_counters_do_not_isolate_transport_wire_traffic",
        "receiver_counters_can_exclude_packets_dropped_before_observation",
    ]
    row["traffic_expansion_unavailable_note"] = (
        "Expansion requires direction-split, per-interface sender accounting "
        "over finite transfers whose endpoint delivery windows are drained."
    )

    target_edge_bytes = service_non_loopback_traffic_bytes(telemetry, "target")
    if target_edge_bytes is None:
        return

    row["traffic_accounting_source"] = (
        "client_and_target_container_non_loopback_netdev_case_boundary_deltas"
    )
    target_probe_excess_bytes = target_edge_bytes - app_bytes
    target_probe_excess_ratio = target_probe_excess_bytes / app_bytes
    endpoint_balance_bytes = client_edge_bytes - target_edge_bytes
    endpoint_balance_ratio = endpoint_balance_bytes / app_bytes
    identity_residual_bytes = client_probe_excess_bytes - (
        target_probe_excess_bytes + endpoint_balance_bytes
    )
    row["target_edge_traffic_bytes_approx"] = target_edge_bytes
    row["target_vs_probe_payload_excess_bytes_approx"] = target_probe_excess_bytes
    row["target_vs_probe_payload_excess_ratio_approx"] = round(
        target_probe_excess_ratio, 6
    )
    row["target_vs_probe_payload_excess_pct_approx"] = round(
        target_probe_excess_ratio * 100.0, 3
    )
    row["client_target_endpoint_balance_bytes_approx"] = endpoint_balance_bytes
    row["client_target_endpoint_balance_ratio_approx"] = round(
        endpoint_balance_ratio, 6
    )
    row["client_target_endpoint_balance_pct_approx"] = round(
        endpoint_balance_ratio * 100.0, 3
    )
    row["traffic_accounting_identity_residual_bytes_approx"] = identity_residual_bytes

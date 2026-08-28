"""Shared lab result enrichment helpers."""

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


RESULT_SCHEMA_VERSION = 2
RUN_MANIFEST_SCHEMA_VERSION = 3
MPTUNNEL_PROTOCOL_VERSION = 8
MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE = {
    "standard": "tcp-tls13-no-alpn+quic-h3-post-data-rfc9297",
    "shared-secret": (
        "tcp-noise-nnpsk0-25519-aesgcm-sha256"
        "+quic-private-initial-h3-post-data-rfc9297"
    ),
}
MPTUNNEL_CARRIER_PRESENTATION = MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE[
    "shared-secret"
]
MPTUNNEL_CARRIER_PRESENTATIONS = frozenset(
    MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE.values()
)

_SAFE_RUN_OVERRIDE = re.compile(
    r"(?:MPTUNNEL_MAX_(?:FRAME_BYTES|PAYLOAD_BYTES|ACK_RANGES|PATHS|STREAMS|"
    r"QUIC_CONCURRENT_BIDI_STREAMS|STREAM_WINDOW_BYTES|REPAIR_BYTES|"
    r"REORDER_BYTES|REINJECTION_CACHE_CHUNKS|REORDER_BUFFER_CHUNKS|"
    r"RETAINED_RECEIVE_RANGES|DATAGRAM_QUEUE_BYTES|PATH_FLIGHT_BYTES|"
    r"RELIABLE_RELAY_CHUNK_BYTES)|"
    r"MPTUNNEL_TCP_PATH_HEARTBEAT_(?:INTERVAL|TIMEOUT)_MS|"
    r"MPTUNNEL_QUIC_PATH_(?:KEEP_ALIVE_INTERVAL|IDLE_TIMEOUT)_MS|"
    r"PATH_PROBE_(?:INTERVAL|TIMEOUT)_MS|"
    r"MPTUNNEL_LAB_OBJECT_MIB|"
    r"MPTUNNEL_LAB_(?:(?:LOWLAT|BALANCED|MILDLOSS|FAT|POOR)_"
    r"(?:RATE|DELAY|JITTER|LOSS)|IDEAL_LOSS|NETEM_LIMIT_PACKETS|MATRIX_(?:GOOD|POOR)_"
    r"(?:RATE|DELAY|JITTER|LOSS)|BLACKHOLE_LOSS|SPIKE_"
    r"(?:FAT|LOWLAT|BALANCED|POOR)_(?:RATE|DELAY|JITTER|LOSS)|"
    r"NETEM_MODE|INTERNET_(?:SEED|INCLUDE_OUTAGES|SCHEDULE_FILE|SCHEDULE_SHA256|GENERATOR_SHA256)|"
    r"HYSTERIA_BALANCED_(?:CLIENT|SERVER)_RATE|"
    r"TCP_CARRIER_QOS_COHORT|TCP_PER_FLOW_QOS_RATE|"
    r"TCP_SHARED_BOTTLENECK_RATE|USE_PATH_HINTS))"
)


def _flag_enabled(value: Any) -> bool:
    return str(value).lower() in {"1", "true", "yes"}


def mptunnel_carrier_presentation(profile: str) -> str:
    """Return the exact wire presentation selected by one lab profile."""

    try:
        return MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE[profile]
    except KeyError as exc:
        raise ValueError(f"unsupported MPTUNNEL transport profile: {profile}") from exc


def _require_sha256(value: Any, field: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError(f"{field} must be a lowercase SHA-256 digest")
    return value


def load_host_reproducibility(
    path: str | Path, expected_sha256: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Load one immutable host snapshot and derive its result-row identity."""

    _require_sha256(expected_sha256, "host snapshot SHA-256")
    try:
        from host_snapshot import load_snapshot
    except ModuleNotFoundError:
        from lab.host_snapshot import load_snapshot

    try:
        snapshot = load_snapshot(path, expected_sha256)
    except (OSError, ValueError, RuntimeError) as exc:
        raise ValueError(str(exc)) from exc
    source = snapshot["source"]
    toolchain = snapshot["toolchain"]
    validity = snapshot["validity"]
    fields = {
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "run_manifest_schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "host_snapshot_schema_version": snapshot["schema_version"],
        "host_validity_rules_version": validity["rules_version"],
        "host_snapshot_sha256": expected_sha256,
        "host_valid": validity["valid"],
        "source_snapshot_sha256": source["snapshot_sha256"],
        "cargo_lock_sha256": source["cargo_lock_sha256"],
        "rustc_version": toolchain["rustc"]["version"],
        "rustc_executable_sha256": toolchain["rustc"]["executable_sha256"],
        "cargo_version": toolchain["cargo"]["version"],
        "cargo_executable_sha256": toolchain["cargo"]["executable_sha256"],
    }
    return snapshot, fields


def load_baseline_lock(
    path: str | Path, expected_sha256: str | None = None
) -> dict[str, Any]:
    payload = Path(path).read_bytes()
    if (
        expected_sha256 is not None
        and hashlib.sha256(payload).hexdigest() != expected_sha256
    ):
        raise ValueError("baseline lock SHA-256 does not match the frozen invocation")
    lock = json.loads(payload)
    if not isinstance(lock, dict) or lock.get("schema_version") != 1:
        raise ValueError("baseline lock schema_version must be 1")
    tools = lock.get("tools")
    if not isinstance(tools, dict) or set(tools) != {"hysteria2", "xray"}:
        raise ValueError("baseline lock must contain hysteria2 and xray")
    for name, tool in tools.items():
        if not isinstance(tool, dict):
            raise ValueError(f"baseline tool {name} must be an object")
        if not isinstance(tool.get("release"), str) or not tool["release"]:
            raise ValueError(f"baseline tool {name} must have a release")
        source = tool.get("source")
        release_path = f"/releases/tag/{tool['release']}"
        if (
            not isinstance(source, str)
            or not source.startswith("https://github.com/")
            or not source.endswith(release_path)
        ):
            raise ValueError(f"baseline tool {name} must have a GitHub source")
        repository_url = source[: -len(release_path)]
        assets = tool.get("assets")
        if not isinstance(assets, dict) or set(assets) != {"amd64", "arm64"}:
            raise ValueError(f"baseline tool {name} must lock amd64 and arm64")
        for architecture, asset in assets.items():
            if not isinstance(asset, dict):
                raise ValueError(
                    f"baseline asset {name}/{architecture} must be an object"
                )
            asset_name = asset.get("name")
            if (
                not isinstance(asset_name, str)
                or re.fullmatch(r"[A-Za-z0-9._+-]+", asset_name) is None
            ):
                raise ValueError(
                    f"baseline asset {name}/{architecture} must have a name"
                )
            url = asset.get("url")
            download_url = (
                f"{repository_url}/releases/download/{tool['release']}/{asset_name}"
            )
            if not isinstance(url, str) or url != download_url:
                raise ValueError(
                    f"baseline asset {name}/{architecture} must have a URL"
                )
            digest = asset.get("sha256")
            if (
                not isinstance(digest, str)
                or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            ):
                raise ValueError(
                    f"baseline asset {name}/{architecture} must have a lowercase SHA-256"
                )
    return lock


def enrich_baseline_identity(row: dict[str, Any], identity_value: Any) -> None:
    """Attach the verified external executables observed for one result row."""

    if identity_value in (None, ""):
        return
    identity = (
        json.loads(identity_value)
        if isinstance(identity_value, str)
        else identity_value
    )
    if not isinstance(identity, dict) or set(identity) != {
        "tool",
        "lock_sha256",
        "client",
        "server",
    }:
        raise ValueError(
            "baseline identity must contain tool, lock_sha256, client, and server"
        )
    if identity["tool"] not in {"xray", "hysteria2"}:
        raise ValueError("baseline identity has an unsupported tool")
    if re.fullmatch(r"[0-9a-f]{64}", identity["lock_sha256"]) is None:
        raise ValueError("baseline identity lock_sha256 must be a SHA-256 digest")
    required = {
        "tool",
        "release",
        "architecture",
        "asset_name",
        "asset_sha256",
        "executable_name",
        "executable_sha256",
        "executable_provenance",
        "version_output",
        "verified",
    }
    for role in ("client", "server"):
        endpoint = identity[role]
        if not isinstance(endpoint, dict) or not required.issubset(endpoint):
            raise ValueError(f"baseline {role} identity is incomplete")
        if endpoint["tool"] != identity["tool"] or endpoint["verified"] is not True:
            raise ValueError(f"baseline {role} identity is not verified")
        for field in ("asset_sha256", "executable_sha256"):
            if re.fullmatch(r"[0-9a-f]{64}", endpoint[field]) is None:
                raise ValueError(f"baseline {role} {field} must be a SHA-256 digest")
        if identity["tool"] == "xray":
            member_digest = endpoint.get("archive_member_sha256")
            if member_digest != endpoint["executable_sha256"]:
                raise ValueError(f"baseline {role} xray archive member does not match")
        elif endpoint["asset_sha256"] != endpoint["executable_sha256"]:
            raise ValueError(f"baseline {role} hysteria asset does not match")
    if identity["client"]["release"] != identity["server"]["release"]:
        raise ValueError("baseline endpoint releases do not match")
    row["baseline_identity"] = identity


def enrich_reproducibility(row: dict[str, Any], metadata_value: Any) -> None:
    """Attach the product source and wire-contract identity used by a lab run."""

    metadata = (
        json.loads(metadata_value) if isinstance(metadata_value, str) else metadata_value
    )
    if not isinstance(metadata, dict):
        raise ValueError("reproducibility metadata must be an object")

    source_commit = metadata.get("source_commit")
    build_profile = metadata.get("mptunnel_build_profile")
    protocol_version = metadata.get("mptunnel_protocol_version")
    carrier_presentation = metadata.get("mptunnel_carrier_presentation")
    transport_profile = metadata.get("mptunnel_transport_profile")
    source_tree_dirty = metadata.get("source_tree_dirty")
    build_features = metadata.get("mptunnel_build_features")
    if metadata.get("result_schema_version") != RESULT_SCHEMA_VERSION:
        raise ValueError(
            f"result_schema_version must be {RESULT_SCHEMA_VERSION}"
        )
    if metadata.get("run_manifest_schema_version") != RUN_MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            f"run_manifest_schema_version must be {RUN_MANIFEST_SCHEMA_VERSION}"
        )
    if metadata.get("host_snapshot_schema_version") != 1:
        raise ValueError("host_snapshot_schema_version must be 1")
    if metadata.get("host_validity_rules_version") != 1:
        raise ValueError("host_validity_rules_version must be 1")
    if not isinstance(metadata.get("host_valid"), bool):
        raise ValueError("host_valid must be a boolean")
    for field in (
        "host_snapshot_sha256",
        "source_snapshot_sha256",
        "cargo_lock_sha256",
        "rustc_executable_sha256",
        "cargo_executable_sha256",
    ):
        _require_sha256(metadata.get(field), field)
    for field in ("rustc_version", "cargo_version"):
        if not isinstance(metadata.get(field), str) or not metadata[field]:
            raise ValueError(f"{field} must be a non-empty string")
    if not isinstance(source_commit, str) or not source_commit:
        raise ValueError("source_commit must be a non-empty string")
    if not isinstance(build_profile, str) or not build_profile:
        raise ValueError("mptunnel_build_profile must be a non-empty string")
    if protocol_version != MPTUNNEL_PROTOCOL_VERSION:
        raise ValueError(
            f"mptunnel_protocol_version must be {MPTUNNEL_PROTOCOL_VERSION}"
        )
    if carrier_presentation not in MPTUNNEL_CARRIER_PRESENTATIONS:
        raise ValueError(
            "mptunnel_carrier_presentation is not a supported v6 TCP/QUIC "
            "wire presentation"
        )
    if transport_profile is not None:
        if not isinstance(transport_profile, str):
            raise ValueError("mptunnel_transport_profile must be a string")
        if carrier_presentation != mptunnel_carrier_presentation(transport_profile):
            raise ValueError(
                "mptunnel_transport_profile does not match "
                "mptunnel_carrier_presentation"
            )
    if not isinstance(source_tree_dirty, bool):
        raise ValueError("source_tree_dirty must be a boolean")
    if not isinstance(build_features, list) or not all(
        isinstance(feature, str) and feature for feature in build_features
    ):
        raise ValueError("mptunnel_build_features must be a string array")

    row["source_commit"] = source_commit
    row["source_tree_dirty"] = source_tree_dirty
    row["mptunnel_build_profile"] = build_profile
    row["mptunnel_build_features"] = sorted(set(build_features))
    row["mptunnel_protocol_version"] = protocol_version
    row["mptunnel_carrier_presentation"] = carrier_presentation
    for field in (
        "result_schema_version",
        "run_manifest_schema_version",
        "host_snapshot_schema_version",
        "host_validity_rules_version",
        "host_snapshot_sha256",
        "host_valid",
        "source_snapshot_sha256",
        "cargo_lock_sha256",
        "rustc_version",
        "rustc_executable_sha256",
        "cargo_version",
        "cargo_executable_sha256",
    ):
        row[field] = metadata[field]
    if not metadata["host_valid"]:
        row["performance_comparable"] = False
        row["performance_comparable_reason"] = (
            "the versioned host validity rules rejected this run; inspect "
            "host-snapshot.json for the retained reasons"
        )

    runtime_fields = (
        "mptunnel_client_runtime",
        "mptunnel_client_runtime_version",
        "mptunnel_client_target",
        "mptunnel_client_sha256",
        "mptunnel_server_target",
        "mptunnel_server_sha256",
    )
    present_runtime_fields = [field for field in runtime_fields if field in metadata]
    if present_runtime_fields and len(present_runtime_fields) != len(runtime_fields):
        raise ValueError("client/server runtime identity must be complete when present")
    if present_runtime_fields:
        for field in runtime_fields:
            value = metadata[field]
            if not isinstance(value, str) or not value:
                raise ValueError(f"{field} must be a non-empty string")
        for field in ("mptunnel_client_sha256", "mptunnel_server_sha256"):
            if re.fullmatch(r"[0-9a-f]{64}", metadata[field]) is None:
                raise ValueError(f"{field} must be a lowercase SHA-256 digest")
        for field in runtime_fields:
            row[field] = metadata[field]


def write_run_manifest(
    path: str | Path, environment: Mapping[str, str]
) -> dict[str, Any]:
    """Persist safe run inputs that are shared by every row in one invocation."""

    env = environment
    overrides = {
        key: value
        for key, value in env.items()
        if _SAFE_RUN_OVERRIDE.fullmatch(key)
        and "SECRET" not in key
        and "TOKEN" not in key
    }
    lock_sha256 = env["BASELINE_LOCK_SHA256"]
    baseline_lock = load_baseline_lock(env["BASELINE_LOCK_FILE"], lock_sha256)
    host_snapshot, host_fields = load_host_reproducibility(
        env["HOST_SNAPSHOT_FILE"], env["HOST_SNAPSHOT_SHA256"]
    )
    product = json.loads(env["RESULT_REPRODUCIBILITY"])
    validated_product: dict[str, Any] = {}
    enrich_reproducibility(validated_product, product)
    for field, value in host_fields.items():
        if validated_product.get(field) != value:
            raise ValueError(
                f"product {field} does not match the frozen host snapshot"
            )
    source = host_snapshot["source"]
    if validated_product["source_commit"] != source["commit"]:
        raise ValueError("product source_commit does not match the host snapshot")
    if validated_product["source_tree_dirty"] != source["tree_dirty"]:
        raise ValueError("product source_tree_dirty does not match the host snapshot")

    manifest = {
        "schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "kind": "mptunnel.lab.run-manifest",
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "result_file": env["RESULT_FILE"],
        "case_filter": env["CASE_FILTER_VALUE"],
        "product": product,
        "workload": {
            "object_mib": int(env["OBJECT_MIB"]),
            "load_duration_seconds": float(env["LOAD_DURATION_SECONDS"]),
            "upload_completion_timeout_seconds": float(
                env["UPLOAD_COMPLETION_TIMEOUT_SECONDS"]
            ),
            "bulk_connections": int(env["BULK_CONNECTIONS"]),
            "failover_after_seconds": float(env["FAILOVER_AFTER_SECONDS"]),
            "failover_profile": env["FAILOVER_PROFILE"],
            "failover_tx_trigger_bytes": int(env["FAILOVER_TX_TRIGGER_BYTES"]),
        },
        "execution": {
            "isolate_cases": env["ISOLATE_CASES_VALUE"] == "1",
            "isolate_containers_per_case": env["ISOLATE_CONTAINERS_VALUE"] == "1",
            "client_settle_seconds": float(env["CLIENT_SETTLE_SECONDS"]),
            "client_start_timeout_seconds": int(env["CLIENT_START_TIMEOUT_SECONDS"]),
            "lab_diagnostics": env["LAB_DIAGNOSTICS_VALUE"],
            "lab_perf": env["LAB_PERF_VALUE"],
            "container_stats": env["CONTAINER_STATS_VALUE"],
            "management_snapshots": env["MANAGEMENT_SNAPSHOTS_VALUE"],
            "use_path_hints": env["USE_PATH_HINTS_VALUE"] == "1",
            "require_competitor_baselines": (
                env["REQUIRE_COMPETITOR_BASELINES_VALUE"] == "1"
            ),
        },
        "containers": {
            role: {"image_id": env[f"{role.upper()}_IMAGE_ID"]}
            for role in ("client", "server", "target")
        },
        "host_snapshot": host_snapshot,
        "runtime": {
            "docker_version": env["DOCKER_VERSION"],
            "compose_version": env["COMPOSE_VERSION"],
        },
        "safe_environment_overrides": dict(sorted(overrides.items())),
        "compose_config": "compose-config.yaml",
        "baseline_lock": {
            "file": "lab/baseline-lock.json",
            "sha256": lock_sha256,
            **baseline_lock,
        },
    }
    Path(path).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


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


def is_proven_upload_measurement(row: dict[str, Any]) -> bool:
    """Return whether an upload row uses receiver-confirmed accounting."""

    version = _number(row.get("upload_metric_version"))
    return (
        version is not None
        and version >= 2
        and row.get("upload_accounting_source")
        in {"target_sink_ack", "target_sink_observer"}
    )


def is_exact_upload_measurement(row: dict[str, Any]) -> bool:
    """Return whether an upload row is exact and performance-comparable."""

    if not is_proven_upload_measurement(row):
        return False
    if row.get("upload_observer_error"):
        return False
    observer_freeze_exit_code = row.get("upload_observer_freeze_exit_code")
    if observer_freeze_exit_code not in (None, 0):
        return False
    version = _number(row.get("upload_metric_version"))
    source = row.get("upload_accounting_source")
    exact_source = (source == "target_sink_ack" and version == 2) or (
        source == "target_sink_observer" and version is not None and version >= 4
    )
    if not exact_source:
        return False
    if source == "target_sink_observer" and (
        row.get("upload_ack_accounting_valid") is not True
        or row.get("upload_probe_errors") != []
    ):
        return False
    if source == "target_sink_observer":
        probe_elapsed = _number(row.get("probe_elapsed_s"))
        observer_elapsed = _number(row.get("observer_elapsed_s"))
        primary_elapsed = _number(row.get("time_s"))
        if (
            row.get("target_observer_snapshot_version") != 2
            or row.get("target_observer_quiesced") is not True
            or row.get("target_observer_finalized") is not True
            or probe_elapsed is None
            or probe_elapsed <= 0
            or observer_elapsed is None
            or observer_elapsed < probe_elapsed
            or primary_elapsed != observer_elapsed
        ):
            return False
    if (
        row.get("upload_accounting_exact") is not True
        or row.get("upload_accounting_lower_bound") is not False
        or row.get("complete") is not True
        or row.get("status") != "ok"
    ):
        return False
    delivered_bytes = _int_number(row.get("bytes"))
    if delivered_bytes is None or delivered_bytes <= 0:
        return False
    probe_errors = row.get("upload_probe_errors")
    if probe_errors is not None and (
        not isinstance(probe_errors, list) or len(probe_errors) > 0
    ):
        return False
    if (
        "upload_ack_accounting_valid" in row
        and row.get("upload_ack_accounting_valid") is not True
    ):
        return False
    failed_streams = _int_number(row.get("failed_streams"))
    return failed_streams in (None, 0)


def _positive_duration(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} is invalid")
    try:
        duration = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} is invalid") from exc
    if not math.isfinite(duration) or duration <= 0:
        raise ValueError(f"{field} is invalid")
    return duration


def _strict_nonnegative_integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} is invalid")
    return value


def enrich_upload_target_observer(
    row: dict[str, Any],
    snapshot: str | dict[str, Any],
    observer_elapsed_s: Any,
) -> None:
    """Make an atomic target-sink snapshot authoritative for an upload row."""

    if isinstance(snapshot, str):
        if not snapshot.strip():
            raise ValueError("target sink observer snapshot is empty")
        parsed = json.loads(snapshot)
    else:
        parsed = snapshot
    if not isinstance(parsed, dict):
        raise ValueError("target sink observer snapshot has an unsupported version")
    snapshot_version = parsed.get("version")
    if isinstance(snapshot_version, bool) or snapshot_version not in (1, 2):
        raise ValueError("target sink observer snapshot has an unsupported version")
    if snapshot_version == 2 and (
        parsed.get("quiesced") is not True or parsed.get("finalized") is not True
    ):
        raise ValueError("target sink observer snapshot is not finalized and quiesced")
    connections = parsed.get("connections")
    if not isinstance(connections, dict):
        raise ValueError("target sink observer connections are missing")

    expected_streams = _strict_nonnegative_integer(
        row.get("parallel_uploads"), "upload parallel stream count"
    )
    if expected_streams <= 0:
        raise ValueError("upload row has no valid parallel stream count")
    if len(connections) > expected_streams:
        raise ValueError("target sink observer contains unexpected connections")
    probe_elapsed_s = _positive_duration(row.get("time_s"), "upload probe elapsed")
    observer_elapsed = _positive_duration(observer_elapsed_s, "target observer elapsed")
    if observer_elapsed < probe_elapsed_s:
        raise ValueError("target observer elapsed is shorter than probe elapsed")

    observed_bytes = 0
    final_connections = 0
    connections_with_delivery = 0
    normalized_ids = set()
    connection_summaries = []
    target_max_receive_gap_ns = None
    for raw_connection_id, raw_connection in connections.items():
        try:
            connection_id = int(raw_connection_id)
        except (TypeError, ValueError) as exc:
            raise ValueError("target sink observer connection ID is invalid") from exc
        if connection_id < 0 or connection_id in normalized_ids:
            raise ValueError("target sink observer connection ID is invalid")
        normalized_ids.add(connection_id)
        if not isinstance(raw_connection, dict):
            raise ValueError("target sink observer connection is invalid")
        connection_bytes = _strict_nonnegative_integer(
            raw_connection.get("bytes"), "target sink observer byte count"
        )
        if not isinstance(raw_connection.get("final"), bool):
            raise ValueError("target sink observer final marker is invalid")
        connection_updated_at = None
        if snapshot_version == 2 or "updated_wall_time_ns" in raw_connection:
            connection_updated_at = _strict_nonnegative_integer(
                raw_connection.get("updated_wall_time_ns"),
                "target sink observer connection timestamp",
            )
        observed_bytes += connection_bytes
        final_connections += int(raw_connection["final"])
        connections_with_delivery += int(connection_bytes > 0)
        connection_summary = {
            "connection_id": connection_id,
            "bytes": connection_bytes,
            "final": raw_connection["final"],
        }
        if connection_updated_at is not None:
            connection_summary["updated_wall_time_ns"] = connection_updated_at
        if "max_receive_gap_ns" in raw_connection:
            max_receive_gap_ns = _strict_nonnegative_integer(
                raw_connection.get("max_receive_gap_ns"),
                "target sink observer maximum receive gap",
            )
            max_receive_gap_start_bytes = _strict_nonnegative_integer(
                raw_connection.get("max_receive_gap_start_bytes"),
                "target sink observer maximum receive gap start",
            )
            max_receive_gap_end_bytes = _strict_nonnegative_integer(
                raw_connection.get("max_receive_gap_end_bytes"),
                "target sink observer maximum receive gap end",
            )
            if max_receive_gap_start_bytes > max_receive_gap_end_bytes:
                raise ValueError("target sink observer maximum receive gap is invalid")
            connection_summary.update(
                {
                    "max_receive_gap_s": round(max_receive_gap_ns / 1_000_000_000, 6),
                    "max_receive_gap_start_bytes": max_receive_gap_start_bytes,
                    "max_receive_gap_end_bytes": max_receive_gap_end_bytes,
                }
            )
            target_max_receive_gap_ns = max(
                target_max_receive_gap_ns or 0, max_receive_gap_ns
            )
        connection_summaries.append(connection_summary)

    local_accepted_bytes = _strict_nonnegative_integer(
        row.get("local_accepted_bytes"), "upload local accepted-byte diagnostic"
    )
    if "target_confirmed_bytes" in row:
        prior_confirmed_bytes = _strict_nonnegative_integer(
            row.get("target_confirmed_bytes"), "upload target-confirmed byte count"
        )
    else:
        prior_confirmed_bytes = 0
    if observed_bytes < prior_confirmed_bytes:
        raise ValueError("target observer is behind its in-band acknowledgement")
    if observed_bytes > local_accepted_bytes:
        raise ValueError("target observer exceeds locally accepted bytes")

    probe_errors = row.get("upload_probe_errors")
    probe_errors_valid = isinstance(probe_errors, list)
    ack_accounting_valid = row.get("upload_ack_accounting_valid")
    legacy_ack_assumed_valid = snapshot_version == 1 and (
        "upload_probe_errors" not in row and "upload_ack_accounting_valid" not in row
    )
    ack_accounting_usable = legacy_ack_assumed_valid or (
        ack_accounting_valid is True and probe_errors_valid and len(probe_errors) == 0
    )
    target_exact = (
        observed_bytes > 0
        and len(connections) == expected_streams
        and final_connections == expected_streams
        and observed_bytes == local_accepted_bytes
    )
    exact = snapshot_version == 2 and target_exact and ack_accounting_usable
    goodput = observed_bytes * 8 / observer_elapsed / 1_000_000
    status = "ok" if exact else "loss" if observed_bytes > 0 else "fail"

    accounting_error_slots = 0
    if probe_errors_valid:
        accounting_error_slots = min(len(probe_errors), expected_streams)
    elif snapshot_version == 2:
        accounting_error_slots = 1
    if "upload_ack_accounting_valid" in row and ack_accounting_valid is not True:
        accounting_error_slots = max(accounting_error_slots, 1)
    complete_streams = min(
        final_connections,
        expected_streams - min(accounting_error_slots, expected_streams),
    )

    updates = {
        "in_band_target_confirmed_bytes": prior_confirmed_bytes,
        "target_confirmed_bytes": observed_bytes,
        "target_observed_bytes": observed_bytes,
        "bytes": observed_bytes,
        "probe_elapsed_s": round(probe_elapsed_s, 6),
        "observer_elapsed_s": round(observer_elapsed, 6),
        "time_s": round(observer_elapsed, 6),
        "goodput_mbps": round(goodput, 3),
        "upload_goodput_mbps": round(goodput, 3),
        "upload_metric_version": 4 if snapshot_version == 2 else 3,
        "upload_accounting_source": "target_sink_observer",
        "upload_accounting_exact": exact,
        "upload_accounting_lower_bound": observed_bytes > 0 and not exact,
        "upload_interval_accounting_source": (
            "target_sink_ack" if ack_accounting_usable else None
        ),
        "target_observer_snapshot_version": snapshot_version,
        "target_observer_quiesced": (
            parsed.get("quiesced") if snapshot_version == 2 else None
        ),
        "target_observer_finalized": (
            parsed.get("finalized") if snapshot_version == 2 else None
        ),
        "target_observer_connections": len(connections),
        "target_observer_final_connections": final_connections,
        "target_observer_unexpected_connections": 0,
        "target_observer_connection_summaries": sorted(
            connection_summaries, key=lambda connection: connection["connection_id"]
        ),
        "streams": expected_streams,
        "streams_with_delivery": min(connections_with_delivery, expected_streams),
        "complete_streams": complete_streams,
        "failed_streams": expected_streams - complete_streams,
        "complete": exact,
        "status": status,
        "exit_code": 0 if status != "fail" else 1,
    }
    snapshot_updated_at = None
    if snapshot_version == 2 or "updated_wall_time_ns" in parsed:
        snapshot_updated_at = _strict_nonnegative_integer(
            parsed.get("updated_wall_time_ns"), "target sink observer timestamp"
        )
    if snapshot_updated_at is not None:
        updates["target_observer_updated_wall_time_ns"] = snapshot_updated_at
    if target_max_receive_gap_ns is not None:
        updates["target_observer_max_receive_gap_s"] = round(
            target_max_receive_gap_ns / 1_000_000_000, 6
        )
    if "merged_max_receive_gap_ns" in parsed:
        merged_gap_ns = _strict_nonnegative_integer(
            parsed.get("merged_max_receive_gap_ns"),
            "target sink observer merged maximum receive gap",
        )
        merged_fields = {
            "target_observer_merged_max_receive_gap_start_connection_id": (
                "merged_max_receive_gap_start_connection_id"
            ),
            "target_observer_merged_max_receive_gap_start_bytes": (
                "merged_max_receive_gap_start_bytes"
            ),
            "target_observer_merged_max_receive_gap_end_connection_id": (
                "merged_max_receive_gap_end_connection_id"
            ),
            "target_observer_merged_max_receive_gap_end_bytes": (
                "merged_max_receive_gap_end_bytes"
            ),
        }
        updates["target_observer_merged_max_receive_gap_s"] = round(
            merged_gap_ns / 1_000_000_000, 6
        )
        for output_field, snapshot_field in merged_fields.items():
            updates[output_field] = _strict_nonnegative_integer(
                parsed.get(snapshot_field),
                f"target sink observer {snapshot_field}",
            )
    row.update(updates)


def application_payload_bytes(row: dict[str, Any]) -> tuple[int | None, str | None]:
    """Return delivered/requested application bytes represented by a lab row.

    The value is intentionally the user-visible payload measured by the probe,
    not mptunnel product-frame bytes. Mixed workload rows should provide an
    explicit all-lane payload sum so overhead estimates do not subtract only the
    bulk transfer while counting latency and datagram traffic in tunnel bytes.
    """

    if str(row.get("protocol", "")).endswith("-upload"):
        if not is_proven_upload_measurement(row):
            return None, None
        # Upload metric v2 makes `bytes` receiver-confirmed. Sender-local
        # acceptance remains useful diagnostics but is not delivered payload.
        value = _int_number(row.get("bytes"))
        if value is not None and value > 0:
            return value, "bytes"
        return None, None

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
                return (
                    total_payloads * payload_bytes,
                    "udp_count_plus_received*payload_bytes",
                )
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

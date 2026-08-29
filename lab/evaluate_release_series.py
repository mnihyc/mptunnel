#!/usr/bin/env python3
"""Evaluate the fixed v0.4.4 two-repetition performance release gate."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
import tempfile
from pathlib import Path

from derive_performance_series import (
    CASE_SERIES,
    DerivationError,
    _validate_dynamic_loss_trace,
    _validate_record,
    load_condition,
    load_repetitions,
    load_result_file,
    parse_repetition_spec,
)
from result_enrichment import MPTUNNEL_CARRIER_PRESENTATION, MPTUNNEL_PROTOCOL_VERSION

RELEASE = "v0.4.4"
SCHEMA_VERSION = 1
RAW_CASE = "baseline_raw_tcp_bulk_interactive_balanced"
REPETITIONS = 2
INTERVAL_SECONDS = 0.2
RAW_INTERVALS = 200
TRIM_INTERVALS = 3
TRIMMED_INTERVALS = 194
WINDOWS = 38
MIN_RATE_RATIO = 0.90
MAX_LATENCY_RATIO = 1.10
RECURRENCE_EPOCHS = (("loss_1_percent", 0, 5), ("loss_2_percent", 2, 6))
RUNTIME_IDENTITY_FIELDS = (
    "mptunnel_client_runtime",
    "mptunnel_client_runtime_version",
    "mptunnel_client_target",
    "mptunnel_client_sha256",
    "mptunnel_server_target",
    "mptunnel_server_sha256",
)
RUNTIME_SHA_FIELDS = {"mptunnel_client_sha256", "mptunnel_server_sha256"}
CONTAINER_ROLES = ("client", "server", "target")


class EvaluationError(ValueError):
    """Input evidence cannot support the frozen release decision."""


def _require(condition, message):
    if not condition:
        raise EvaluationError(message)


def _number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _json_number(value):
    return round(float(value), 9)


def _runtime_identity(record, prefix):
    identity = tuple(record.get(field) for field in RUNTIME_IDENTITY_FIELDS)
    _require(
        all(
            isinstance(value, str)
            and bool(value)
            and (
                field not in RUNTIME_SHA_FIELDS
                or re.fullmatch(r"[0-9a-f]{64}", value) is not None
            )
            for field, value in zip(RUNTIME_IDENTITY_FIELDS, identity)
        ),
        f"{prefix} has no complete client/server runtime identity",
    )
    return identity


def _validate_release_wire_identity(record, prefix):
    _require(
        record.get("mptunnel_build_profile") == "release"
        and record.get("mptunnel_build_features") == []
        and record.get("mptunnel_protocol_version") == MPTUNNEL_PROTOCOL_VERSION
        and record.get("mptunnel_carrier_presentation")
        == MPTUNNEL_CARRIER_PRESENTATION,
        f"{prefix} does not use the frozen release wire/build identity",
    )


def _result_directory_name(path):
    return Path(path).resolve().parent.name


def _release_manifest_image_identity(path):
    manifest_path = Path(path).parent / "run-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EvaluationError(f"cannot read {manifest_path}: {exc}") from exc
    containers = manifest.get("containers")
    _require(
        isinstance(containers, dict),
        f"{manifest_path} has no container image identity",
    )
    workload = manifest.get("workload")
    execution = manifest.get("execution")
    overrides = manifest.get("safe_environment_overrides")
    product = manifest.get("product")
    _require(
        isinstance(product, dict)
        and product.get("mptunnel_build_profile") == "release"
        and product.get("mptunnel_build_features") == []
        and product.get("mptunnel_protocol_version") == MPTUNNEL_PROTOCOL_VERSION
        and product.get("mptunnel_transport_profile") == "shared-secret"
        and product.get("mptunnel_carrier_presentation")
        == MPTUNNEL_CARRIER_PRESENTATION,
        f"{manifest_path} does not use the frozen release wire/build identity",
    )
    _require(
        isinstance(workload, dict)
        and workload.get("object_mib") == 4096
        and workload.get("bulk_connections") == 1
        and isinstance(execution, dict)
        and execution.get("build_product") is False
        and execution.get("build_lab_images") is False
        and execution.get("lab_diagnostics") == "0"
        and execution.get("lab_perf") == "0"
        and execution.get("management_snapshots") == "0"
        and execution.get("container_stats") == "1"
        and execution.get("use_path_hints") is False
        and execution.get("require_competitor_baselines") is True
        and isinstance(overrides, dict)
        and overrides.get("MPTUNNEL_LAB_NETEM_MODE") == "apply"
        and overrides.get("MPTUNNEL_LAB_INTERNET_SEED")
        == "mptunnel-random-internet-v1"
        and overrides.get("MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES") == "0",
        f"{manifest_path} does not use the frozen v0.4.4 release profile",
    )
    identity = tuple(
        containers.get(role, {}).get("image_id")
        if isinstance(containers.get(role), dict)
        else None
        for role in CONTAINER_ROLES
    )
    _require(
        all(
            isinstance(image_id, str)
            and re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is not None
            for image_id in identity
        ),
        f"{manifest_path} has an incomplete or invalid container image-ID triple",
    )
    return identity


def _probe_p95(record, prefix):
    latencies = [
        attempt.get("latency_ms")
        for attempt in record["interactive_attempt_series"]
        if attempt.get("outcome") == "success"
    ]
    _require(
        latencies and all(_number(value) and value >= 0 for value in latencies),
        f"{prefix} has no valid successful interactive latency",
    )
    expected = sorted(latencies)[round((len(latencies) - 1) * 0.95)]
    observed = record.get("interactive_p95_ms")
    _require(
        _number(observed) and math.isclose(observed, expected, rel_tol=0, abs_tol=1e-9),
        f"{prefix} interactive_p95_ms disagrees with its attempt series",
    )
    return float(observed)


def _exact_trajectory(record, prefix):
    _require(
        math.isclose(record.get("test_duration_s", -1), 40.0, rel_tol=0, abs_tol=1e-9)
        and math.isclose(
            record.get("bulk_load_duration_s", -1), 40.0, rel_tol=0, abs_tol=1e-9
        ),
        f"{prefix} does not use the fixed 40-second workload",
    )
    interval = record.get("bulk_interval_seconds")
    raw = record.get("bulk_interval_goodput_raw_mbps")
    trimmed = record.get("bulk_interval_goodput_mbps")
    trim = record.get("bulk_interval_trim_discard_each_end")
    _require(
        _number(interval)
        and math.isclose(interval, INTERVAL_SECONDS, rel_tol=0, abs_tol=1e-12)
        and trim == TRIM_INTERVALS
        and isinstance(raw, list)
        and len(raw) == RAW_INTERVALS
        and all(_number(value) and value >= 0 for value in raw)
        and isinstance(trimmed, list)
        and len(trimmed) == TRIMMED_INTERVALS
        and trimmed == raw[TRIM_INTERVALS:-TRIM_INTERVALS],
        f"{prefix} does not have the exact 200-bin receiver-goodput grid",
    )

    # This is the existing symmetric-full-window derivation specialized to the
    # frozen 40 s / 0.2 s grid: discard two partial bins at each trimmed edge,
    # then form 38 aligned windows [1,2) through [38,39).
    values = []
    for window_start_s in range(1, 39):
        trimmed_start = int(window_start_s / INTERVAL_SECONDS) - TRIM_INTERVALS
        samples = trimmed[trimmed_start : trimmed_start + 5]
        _require(len(samples) == 5, f"{prefix} has an incomplete one-second window")
        values.append(statistics.fmean(samples))
    _require(len(values) == WINDOWS, f"{prefix} does not have 38 aligned windows")
    return values


def _validate_exact_record(record, prefix):
    _require(record.get("status") == "ok", f"{prefix} application status is not ok")
    _require(record.get("bulk_status") == "ok", f"{prefix} bulk status is not ok")
    _require(record.get("bulk_error") in (None, ""), f"{prefix} has a bulk error")
    _require(record.get("interactive_fail") == 0, f"{prefix} has interactive failures")
    _require(
        record.get("interactive_error") in (None, ""),
        f"{prefix} has an interactive error/reset",
    )
    _require(
        isinstance(record.get("bulk_bytes"), int)
        and not isinstance(record["bulk_bytes"], bool)
        and record["bulk_bytes"] > 0,
        f"{prefix} has no exact receiver-owned bulk byte count",
    )
    _require(record.get("host_valid") is True, f"{prefix} host evidence is invalid")
    _require(
        record.get("source_tree_dirty") is False,
        f"{prefix} source snapshot is dirty",
    )
    _require(
        isinstance(record.get("source_commit"), str) and record["source_commit"],
        f"{prefix} has no source commit",
    )
    _require(
        isinstance(record.get("host_snapshot_sha256"), str)
        and re.fullmatch(r"[0-9a-f]{64}", record["host_snapshot_sha256"]) is not None,
        f"{prefix} has no exact host snapshot identity",
    )
    echo = record.get("interactive_attempt_series")
    _require(
        isinstance(echo, list)
        and len(echo) >= 3
        and all(
            isinstance(attempt, dict) and attempt.get("outcome") == "success"
            for attempt in echo
        ),
        f"{prefix} has a missing or failed interactive attempt",
    )
    _require(
        record.get("interactive_count") == len(echo)
        and record.get("interactive_ok") == len(echo),
        f"{prefix} interactive accounting is incomplete",
    )
    previous_end = -math.inf
    for index, attempt in enumerate(echo):
        start = attempt.get("start_offset_s")
        end = attempt.get("end_offset_s")
        latency = attempt.get("latency_ms")
        _require(
            attempt.get("index") == index
            and _number(start)
            and start >= 0
            and _number(end)
            and end >= start
            and start >= previous_end
            and _number(latency)
            and latency >= 0
            and math.isclose(latency, (end - start) * 1000, rel_tol=1e-9, abs_tol=1e-6),
            f"{prefix} interactive attempt {index} is invalid",
        )
        previous_end = end
    trajectory = _exact_trajectory(record, prefix)
    p95 = _probe_p95(record, prefix)
    return trajectory, p95


def _validate_raw_record(record, condition, path):
    prefix = f"{path}:{RAW_CASE}"
    _require(record.get("protocol") == "raw-tcp", f"{prefix} is not raw TCP")
    _require(
        record.get("workload_mode") == "bulk-interactive",
        f"{prefix} does not use bulk-interactive workload",
    )
    _validate_dynamic_loss_trace(
        record, condition["bulk_interactive_dynamic_loss"], RAW_CASE, path
    )
    trajectory, p95 = _validate_exact_record(record, prefix)

    duration = record.get("test_duration_s")
    echo = record.get("interactive_attempt_series")
    interval_s = record.get("interactive_interval_ms", 0) / 1000.0
    _require(
        _number(record.get("bulk_time_s"))
        and record["bulk_time_s"] >= duration * 0.99
        and _number(record.get("interactive_time_s"))
        and record["interactive_time_s"] >= duration * 0.99
        and _number(interval_s)
        and interval_s > 0
        and echo[0].get("start_offset_s", math.inf) <= interval_s
        and echo[-1].get("end_offset_s", -math.inf) >= duration - interval_s,
        f"{prefix} does not persist across the fixed workload",
    )
    return trajectory, p95


def _epoch_median(trajectory, epoch):
    # Index zero is [1,2). Four stable windows follow each at-most-250 ms
    # transition: [epoch*5+1, epoch*5+5).
    first = epoch * 5
    values = trajectory[first : first + 4]
    _require(len(values) == 4, f"epoch {epoch} has no four-window stable interval")
    return statistics.median(values)


def _minimum_comparison(identifier, actual, reference, factor):
    _require(
        _number(actual) and _number(reference) and reference > 0,
        f"{identifier} requires finite values and a positive reference",
    )
    required = reference * factor
    return {
        "id": identifier,
        "actual": _json_number(actual),
        "reference": _json_number(reference),
        "factor": factor,
        "required_minimum": _json_number(required),
        "ratio": _json_number(actual / reference),
        "pass": actual >= required,
    }


def _maximum_comparison(identifier, actual, reference, factor):
    _require(
        _number(actual) and _number(reference) and reference > 0,
        f"{identifier} requires finite values and a positive reference",
    )
    allowed = reference * factor
    return {
        "id": identifier,
        "actual": _json_number(actual),
        "reference": _json_number(reference),
        "factor": factor,
        "allowed_maximum": _json_number(allowed),
        "ratio": _json_number(actual / reference),
        "pass": actual <= allowed,
    }


def _evaluate_repetition(
    repetition, raw_path, common_source_commit, common_runtime_identity
):
    repetition_id = repetition["id"]
    raw_records = load_result_file(raw_path)
    _require(
        set(raw_records) == {RAW_CASE},
        f"{raw_path} must contain exactly the separate {RAW_CASE} row",
    )
    raw = raw_records[RAW_CASE]
    raw_condition = load_condition(raw_path)
    product_condition = dict(repetition["condition"])
    product_probe = product_condition.pop("probe")
    _require(
        raw_condition == product_condition,
        f"{repetition_id} raw and product manifest conditions differ",
    )
    raw_trajectory, raw_p95 = _validate_raw_record(raw, raw_condition, raw_path)
    _require(
        record_workload_signature(raw)
        == record_workload_signature_from_probe(product_probe),
        f"{repetition_id} raw and product probe workloads differ",
    )
    _require(
        raw.get("source_commit") == common_source_commit,
        f"{repetition_id} raw control source commit differs from the candidate",
    )
    _validate_release_wire_identity(raw, f"{raw_path}:{RAW_CASE}")
    _require(
        _runtime_identity(raw, f"{raw_path}:{RAW_CASE}") == common_runtime_identity,
        f"{repetition_id} raw control runtime identity differs from the product cohort",
    )

    subjects = {}
    metrics = {}
    for series_id, label, case in CASE_SERIES:
        record = repetition["records"][case]
        source = repetition["record_sources"][case]
        _validate_record(record, series_id, case, source)
        trajectory, p95 = _validate_exact_record(record, f"{source}:{case}")
        _require(
            record.get("source_commit") == common_source_commit,
            f"{repetition_id} {series_id} source commit differs from the candidate",
        )
        epoch_medians = {
            str(epoch): _epoch_median(trajectory, epoch) for epoch in (0, 2, 5, 6)
        }
        metrics[series_id] = {
            "rate": statistics.median(trajectory),
            "p95": p95,
            "epochs": epoch_medians,
        }
        subjects[series_id] = {
            "label": label,
            "receiver_bytes": record["bulk_bytes"],
            "interactive_attempts": len(record["interactive_attempt_series"]),
            "trajectory_windows": len(trajectory),
            "trajectory_first_window_s": [1, 2],
            "trajectory_last_window_s": [38, 39],
            "goodput_median_mbps": _json_number(metrics[series_id]["rate"]),
            "epoch_goodput_median_mbps": {
                epoch: _json_number(value) for epoch, value in epoch_medians.items()
            },
            "interactive_p95_ms": _json_number(p95),
        }

    raw_epoch_medians = {
        str(epoch): _json_number(_epoch_median(raw_trajectory, epoch))
        for epoch in (0, 2, 5, 6)
    }
    raw_summary = {
        "case": RAW_CASE,
        "source": _result_directory_name(raw_path),
        "receiver_bytes": raw["bulk_bytes"],
        "interactive_attempts": len(raw["interactive_attempt_series"]),
        "trajectory_windows": len(raw_trajectory),
        "trajectory_first_window_s": [1, 2],
        "trajectory_last_window_s": [38, 39],
        "goodput_median_mbps": _json_number(statistics.median(raw_trajectory)),
        "epoch_goodput_median_mbps": raw_epoch_medians,
        "interactive_p95_ms": _json_number(raw_p95),
        "pass": True,
    }

    comparisons = []
    for series_id in ("mpp_tcp", "mpp_quic", "mpp_default"):
        medians = metrics[series_id]["epochs"]
        for loss_id, early_epoch, late_epoch in RECURRENCE_EPOCHS:
            comparisons.append(
                _minimum_comparison(
                    f"{series_id}.{loss_id}.epoch_{late_epoch}_over_{early_epoch}",
                    medians[str(late_epoch)],
                    medians[str(early_epoch)],
                    MIN_RATE_RATIO,
                )
            )

    rates = {key: value["rate"] for key, value in metrics.items()}
    external_rate = max(rates["xray_vmess"], rates["hysteria2"])
    comparisons.extend(
        (
            _minimum_comparison(
                "mpp_quic.over_faster_external_goodput",
                rates["mpp_quic"],
                external_rate,
                MIN_RATE_RATIO,
            ),
            _minimum_comparison(
                "mpp_default.over_faster_external_goodput",
                rates["mpp_default"],
                external_rate,
                MIN_RATE_RATIO,
            ),
            _minimum_comparison(
                "mpp_default.over_faster_mpp_single_goodput",
                rates["mpp_default"],
                max(rates["mpp_tcp"], rates["mpp_quic"]),
                MIN_RATE_RATIO,
            ),
        )
    )

    p95 = {key: value["p95"] for key, value in metrics.items()}
    comparisons.extend(
        (
            _maximum_comparison(
                "mpp_default.over_lower_external_p95",
                p95["mpp_default"],
                min(p95["xray_vmess"], p95["hysteria2"]),
                MAX_LATENCY_RATIO,
            ),
            _maximum_comparison(
                "mpp_quic.over_mpp_tcp_p95",
                p95["mpp_quic"],
                p95["mpp_tcp"],
                MAX_LATENCY_RATIO,
            ),
        )
    )
    failures = [
        comparison["id"] for comparison in comparisons if not comparison["pass"]
    ]
    return {
        "id": repetition_id,
        "status": "pass" if not failures else "fail",
        "product_sources": sorted(
            {
                _result_directory_name(path)
                for path in repetition["record_sources"].values()
            }
        ),
        "raw_control": raw_summary,
        "subjects": subjects,
        "comparisons": comparisons,
        "failed_criteria": failures,
    }


def record_workload_signature(record):
    return {
        field: record.get(field)
        for field in (
            "test_duration_s",
            "bulk_load_duration_s",
            "bulk_interval_seconds",
            "bulk_interval_trim_discard_each_end",
            "interactive_interval_ms",
            "interactive_timeout_ms",
            "interactive_payload_bytes",
        )
    }


def record_workload_signature_from_probe(probe):
    return {field: probe.get(field) for field in record_workload_signature({})}


def parse_product_repetition(value):
    paths = parse_repetition_spec(value)
    _require(
        len(paths) == 1,
        "each --product-repetition must name exactly one results.jsonl path",
    )
    return paths


def evaluate(product_specs, raw_paths, candidate_commit):
    _require(
        isinstance(candidate_commit, str)
        and re.fullmatch(r"[0-9a-f]{40}", candidate_commit) is not None,
        "--candidate-commit must be an exact 40-character lowercase Git commit",
    )
    _require(
        len(product_specs) == REPETITIONS and len(raw_paths) == REPETITIONS,
        "exactly two --product-repetition and two paired --raw-control inputs are required",
    )
    product_paths = [parse_product_repetition(spec) for spec in product_specs]
    repetitions = load_repetitions(product_paths)
    _require(
        len(repetitions) == REPETITIONS, "exactly two product repetitions are required"
    )

    product_directories = {
        path.resolve().parent for paths in product_paths for path in paths
    }
    raw_directories = [Path(path).resolve().parent for path in raw_paths]
    product_directory_names = {directory.name for directory in product_directories}
    raw_directory_names = [directory.name for directory in raw_directories]
    _require(
        len(set(raw_directories)) == REPETITIONS
        and len(set(raw_directory_names)) == REPETITIONS
        and not product_directories.intersection(raw_directories)
        and not product_directory_names.intersection(raw_directory_names),
        "product repetitions and paired raw controls require distinct result "
        "directories and names",
    )
    container_image_identities = {
        _release_manifest_image_identity(path)
        for paths in product_paths
        for path in paths
    }
    container_image_identities.update(
        _release_manifest_image_identity(Path(path)) for path in raw_paths
    )
    _require(
        len(container_image_identities) == 1,
        "all product and raw runs must identify one container image-ID triple",
    )
    container_image_identity = next(iter(container_image_identities))

    commits = {
        record.get("source_commit")
        for repetition in repetitions
        for record in repetition["records"].values()
    }
    _require(
        len(commits) == 1 and next(iter(commits)),
        "all product rows must identify one candidate source commit",
    )
    source_commit = next(iter(commits))
    _require(
        source_commit == candidate_commit,
        "product cohort source commit does not match --candidate-commit",
    )
    product_records = [
        (record, repetition["record_sources"][case], case)
        for repetition in repetitions
        for case, record in repetition["records"].items()
    ]
    for record, path, case in product_records:
        _validate_release_wire_identity(record, f"{path}:{case}")
    runtime_identities = {
        _runtime_identity(record, f"{path}:{case}")
        for record, path, case in product_records
    }
    _require(
        len(runtime_identities) == 1,
        "all product rows must identify one client/server runtime binary pair",
    )
    runtime_identity = next(iter(runtime_identities))
    _require(
        runtime_identity[0:2] == ("native", "native")
        and runtime_identity[2] == runtime_identity[4]
        and runtime_identity[3] == runtime_identity[5],
        "the release cohort must use one symmetric native client/server runtime",
    )
    results = []
    for repetition, raw_path in zip(repetitions, raw_paths):
        try:
            result = _evaluate_repetition(
                repetition,
                Path(raw_path),
                source_commit,
                runtime_identity,
            )
        except (DerivationError, EvaluationError, OSError, ValueError) as exc:
            result = {
                "id": repetition["id"],
                "status": "invalid",
                "product_sources": sorted(
                    {
                        _result_directory_name(path)
                        for path in repetition["record_sources"].values()
                    }
                ),
                "raw_control_source": _result_directory_name(raw_path),
                "errors": [str(exc)],
            }
        results.append(result)
    invalid = any(result["status"] == "invalid" for result in results)
    if not invalid:
        _require(
            len(
                {
                    result["raw_control"]["interactive_attempts"]
                    for result in results
                }
            )
            == 1,
            "paired raw controls must have equal interactive attempt counts",
        )
        for series_id, _label, _case in CASE_SERIES:
            _require(
                len(
                    {
                        result["subjects"][series_id]["interactive_attempts"]
                        for result in results
                    }
                )
                == 1,
                f"paired {series_id} runs must have equal interactive attempt counts",
            )
    passed = all(result["status"] == "pass" for result in results)
    return {
        "schema_version": SCHEMA_VERSION,
        "release": RELEASE,
        "status": "pass" if passed else "invalid" if invalid else "fail",
        "candidate_commit": candidate_commit,
        "source_commit": source_commit,
        "runtime_identity": dict(zip(RUNTIME_IDENTITY_FIELDS, runtime_identity)),
        "container_image_identity": dict(
            zip(CONTAINER_ROLES, container_image_identity)
        ),
        "criteria": {
            "repetitions": REPETITIONS,
            "raw_controls": REPETITIONS,
            "receiver_goodput_grid": {
                "interval_s": INTERVAL_SECONDS,
                "raw_intervals": RAW_INTERVALS,
                "trim_each_edge": TRIM_INTERVALS,
                "aligned_one_second_windows": WINDOWS,
                "window_range_s": [1, 39],
            },
            "stable_epoch_windows": "[epoch*5+1, epoch*5+5)",
            "recurrence_pairs": [[5, 0], [6, 2]],
            "minimum_goodput_ratio": MIN_RATE_RATIO,
            "maximum_latency_ratio": MAX_LATENCY_RATIO,
            "throughput_reference": "maximum median",
            "latency_reference": "minimum per-run probe p95",
            "receiver_authority": "probe bulk_bytes; rounded rate bins are trajectory evidence",
            "reset_scope": "probe-observed application errors and failed attempts",
        },
        "repetitions": results,
        "failed_repetitions": [
            result["id"] for result in results if result["status"] != "pass"
        ],
    }


def _write_json(payload, output_path):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=output.parent,
        prefix=f".{output.name}.",
        suffix=".tmp",
        delete=False,
    )
    temporary = Path(handle.name)
    try:
        with handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--product-repetition",
        action="append",
        default=[],
        help="one exact-five results.jsonl for one repetition",
    )
    parser.add_argument(
        "--raw-control",
        action="append",
        type=Path,
        default=[],
        help="separate raw-TCP results file paired by option order",
    )
    parser.add_argument("--candidate-commit")
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        verdict = evaluate(
            args.product_repetition, args.raw_control, args.candidate_commit
        )
        status = (
            0
            if verdict["status"] == "pass"
            else 2 if verdict["status"] == "invalid" else 1
        )
    except (DerivationError, EvaluationError, OSError, ValueError) as exc:
        verdict = {
            "schema_version": SCHEMA_VERSION,
            "release": RELEASE,
            "status": "invalid",
            "candidate_commit": args.candidate_commit,
            "errors": [str(exc)],
            "repetitions": [],
        }
        status = 2
    try:
        _write_json(verdict, args.output)
    except OSError as exc:
        print(
            f"release-series evaluator could not write {args.output}: {exc}",
            file=sys.stderr,
        )
        return 2
    if status:
        print(
            f"release-series evaluation {verdict['status']}: {args.output}",
            file=sys.stderr,
        )
    return status


if __name__ == "__main__":
    raise SystemExit(main())

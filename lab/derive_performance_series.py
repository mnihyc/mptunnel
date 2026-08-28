#!/usr/bin/env python3
"""Derive a compact five-product time-series dataset from accepted lab runs."""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
import tempfile
from collections import Counter
from pathlib import Path

CASE_SERIES = (
    (
        "mpp_tcp",
        "MPP TCP",
        "mptunnel_tcp_bulk_interactive_balanced",
    ),
    (
        "mpp_quic",
        "MPP QUIC",
        "mptunnel_quic_bulk_interactive_balanced",
    ),
    (
        "mpp_default",
        "MPP TCP+QUIC (default)",
        "mptunnel_tcp_quic_bulk_interactive_balanced",
    ),
    (
        "xray_vmess",
        "Xray VMess/TCP",
        "baseline_vmess_tcp_bulk_interactive_balanced",
    ),
    (
        "hysteria2",
        "Hysteria2 Brutal",
        "baseline_hysteria2_udp_bulk_interactive_balanced",
    ),
)
REQUIRED_CASES = {case for _, _, case in CASE_SERIES}

GOODPUT_WINDOW_SECONDS = 1.0


class DerivationError(ValueError):
    """Raw result evidence cannot support the requested derived figure."""


def _require(condition, message):
    if not condition:
        raise DerivationError(message)


def _finite_non_negative(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _rounded(value):
    return round(float(value), 3)


def _summary(values, repetitions, outcomes=None):
    if not values:
        summary = {
            "low": None,
            "median": None,
            "high": None,
            "available": 0,
            "repetitions": repetitions,
        }
    else:
        summary = {
            "low": _rounded(min(values)),
            "median": _rounded(statistics.median(values)),
            "high": _rounded(max(values)),
            "available": len(values),
            "repetitions": repetitions,
        }
    if outcomes is not None:
        summary["outcomes"] = dict(sorted(outcomes.items()))
    return summary


def load_result_file(path):
    records = {}
    try:
        with Path(path).open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                record = json.loads(line)
                case = record.get("case")
                _require(
                    isinstance(case, str) and case,
                    f"{path}:{line_number} has no case",
                )
                _require(case not in records, f"{path} repeats case {case}")
                records[case] = record
    except (OSError, json.JSONDecodeError) as exc:
        raise DerivationError(f"cannot read {path}: {exc}") from exc
    return records


def load_condition(path):
    manifest_path = Path(path).parent / "run-manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DerivationError(f"cannot read {manifest_path}: {exc}") from exc
    overrides = manifest.get("safe_environment_overrides")
    workload = manifest.get("workload")
    execution = manifest.get("execution")
    _require(
        isinstance(overrides, dict)
        and isinstance(workload, dict)
        and isinstance(execution, dict),
        f"{manifest_path} omits condition metadata",
    )
    _require(
        execution.get("isolate_cases") is True
        and execution.get("isolate_containers_per_case") is True,
        f"{manifest_path} did not isolate cases and containers",
    )
    return {
        "netem_mode": overrides.get("MPTUNNEL_LAB_NETEM_MODE"),
        "internet_seed": overrides.get("MPTUNNEL_LAB_INTERNET_SEED"),
        "include_outages": overrides.get("MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES"),
        "mpp_path_hints": overrides.get("MPTUNNEL_LAB_USE_PATH_HINTS"),
        "hysteria_client_rate": overrides.get(
            "MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE"
        ),
        "hysteria_server_rate": overrides.get(
            "MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE"
        ),
        "load_duration_s": workload.get("load_duration_seconds"),
        "bulk_connections": workload.get("bulk_connections"),
        "object_mib": workload.get("object_mib"),
        "case_isolation": execution.get("isolate_cases"),
        "container_isolation": execution.get("isolate_containers_per_case"),
    }


def parse_repetition_spec(value):
    items = [item.strip() for item in value.split(",")]
    _require(items and all(items), "empty --repetition path")
    return [Path(item) for item in items]


def record_probe_condition(record):
    return {
        field: record.get(field)
        for field in (
            "mode",
            "target",
            "tcp_echo_target",
            "test_duration_s",
            "bulk_load_duration_s",
            "bulk_interval_seconds",
            "bulk_interval_trim_discard_each_end",
            "interactive_interval_ms",
            "interactive_timeout_ms",
            "interactive_payload_bytes",
        )
    }


def load_repetitions(repetition_paths):
    _require(len(repetition_paths) >= 2, "at least two valid repetitions are required")
    seen_directories = set()
    seen_directory_names = set()
    repetitions = []
    for index, paths in enumerate(repetition_paths, 1):
        repetition_id = f"repetition-{index}"
        _require(paths, f"{repetition_id} has no result files")
        records = {}
        record_sources = {}
        conditions = []
        result_directory_names = []
        for path in paths:
            path = Path(path)
            result_directory = path.resolve().parent
            _require(
                result_directory not in seen_directories,
                f"duplicate result directory {result_directory}",
            )
            directory_name = result_directory.name
            _require(
                directory_name not in seen_directory_names,
                f"duplicate result directory name {directory_name}",
            )
            seen_directories.add(result_directory)
            seen_directory_names.add(directory_name)
            result_directory_names.append(directory_name)
            conditions.append(load_condition(path))
            for case, record in load_result_file(path).items():
                _require(
                    case in REQUIRED_CASES,
                    f"{repetition_id} contains unexpected case {case}",
                )
                _require(
                    case not in records,
                    f"{repetition_id} repeats case {case} across result files",
                )
                records[case] = record
                record_sources[case] = path
        _require(
            all(condition == conditions[0] for condition in conditions[1:]),
            f"lab conditions differ within {repetition_id}",
        )
        missing = REQUIRED_CASES - records.keys()
        _require(
            not missing,
            f"{repetition_id} omits required cases: {', '.join(sorted(missing))}",
        )
        probe_conditions = [
            record_probe_condition(records[case]) for case in sorted(records)
        ]
        _require(
            all(condition == probe_conditions[0] for condition in probe_conditions[1:]),
            f"probe conditions differ within {repetition_id}",
        )
        condition = dict(conditions[0])
        _require(
            _finite_non_negative(condition.get("load_duration_s"))
            and condition["load_duration_s"] > 0
            and math.isclose(
                probe_conditions[0].get("test_duration_s", -1),
                condition["load_duration_s"],
                rel_tol=0,
                abs_tol=1e-9,
            )
            and math.isclose(
                probe_conditions[0].get("bulk_load_duration_s", -1),
                condition["load_duration_s"],
                rel_tol=0,
                abs_tol=1e-9,
            ),
            f"manifest and probe durations differ within {repetition_id}",
        )
        condition["probe"] = probe_conditions[0]
        repetitions.append(
            {
                "id": repetition_id,
                "records": records,
                "record_sources": record_sources,
                "condition": condition,
                "source": {
                    "id": repetition_id,
                    "result_dirs": result_directory_names,
                },
            }
        )
    _require(
        all(
            repetition["condition"] == repetitions[0]["condition"]
            for repetition in repetitions[1:]
        ),
        "lab conditions differ across logical repetitions",
    )
    return repetitions


def _validate_record(record, series_id, case, path):
    prefix = f"{path}:{case}"
    _require(
        record.get("status") in ("ok", "loss"),
        f"{prefix} bulk-interactive status is neither ok nor accounted loss",
    )
    _require(record.get("bulk_status") == "ok", f"{prefix} bulk status is not ok")
    _require(record.get("host_valid") is True, f"{prefix} host evidence is invalid")
    _require(
        record.get("source_tree_dirty") is False,
        f"{prefix} source snapshot is dirty",
    )
    _require(
        record.get("workload_mode") == "bulk-interactive",
        f"{prefix} did not run the matched bulk-interactive workload",
    )
    if series_id.startswith("mpp_"):
        _require(
            record.get("performance_comparable") is True,
            f"{prefix} MPP row is not performance-comparable",
        )
    else:
        expected_tool = "xray" if series_id == "xray_vmess" else "hysteria2"
        identity = record.get("baseline_identity")
        _require(
            isinstance(identity, dict)
            and identity.get("tool") == expected_tool
            and isinstance(identity.get("lock_sha256"), str)
            and identity["lock_sha256"],
            f"{prefix} has no locked {expected_tool} identity",
        )
        for endpoint in ("client", "server"):
            endpoint_identity = identity.get(endpoint)
            _require(
                isinstance(endpoint_identity, dict)
                and endpoint_identity.get("tool") == expected_tool
                and endpoint_identity.get("verified") is True,
                f"{prefix} {endpoint} {expected_tool} identity is not verified",
            )

    duration = record.get("test_duration_s")
    bulk_time = record.get("bulk_time_s")
    _require(
        _finite_non_negative(duration)
        and duration > 0
        and _finite_non_negative(bulk_time)
        and bulk_time >= duration * 0.99,
        f"{prefix} did not cover the declared duration",
    )
    _require(
        isinstance(record.get("bulk_bytes"), int)
        and not isinstance(record["bulk_bytes"], bool)
        and record["bulk_bytes"] > 0,
        f"{prefix} delivered no bulk bytes",
    )

    samples = record.get("bulk_interval_goodput_mbps")
    _require(
        isinstance(samples, list)
        and len(samples) >= 15
        and all(_finite_non_negative(value) for value in samples),
        f"{prefix} has no usable goodput series",
    )
    interval = record.get("bulk_interval_seconds")
    _require(
        _finite_non_negative(interval) and interval > 0,
        f"{prefix} has an invalid goodput interval",
    )
    intervals_per_window = round(GOODPUT_WINDOW_SECONDS / interval)
    _require(
        intervals_per_window >= 1
        and math.isclose(
            intervals_per_window * interval,
            GOODPUT_WINDOW_SECONDS,
            rel_tol=0,
            abs_tol=1e-9,
        ),
        f"{prefix} interval cannot form exact one-second windows",
    )
    raw_samples = record.get("bulk_interval_goodput_raw_mbps")
    _require(
        isinstance(raw_samples, list)
        and all(_finite_non_negative(value) for value in raw_samples),
        f"{prefix} has no usable raw goodput grid",
    )
    trim = record.get("bulk_interval_trim_discard_each_end")
    _require(
        isinstance(trim, int) and not isinstance(trim, bool) and trim >= 0,
        f"{prefix} goodput trim is invalid",
    )
    _require(
        len(raw_samples) > 2 * trim
        and samples == raw_samples[trim : len(raw_samples) - trim],
        f"{prefix} trimmed goodput series disagrees with its raw grid",
    )
    _require(
        len(raw_samples) * interval >= duration * 0.99,
        f"{prefix} raw goodput grid does not cover the declared duration",
    )

    echo = record.get("interactive_attempt_series")
    _require(isinstance(echo, list) and len(echo) >= 3, f"{prefix} has no echo series")
    interactive_ok = record.get("interactive_ok")
    interactive_fail = record.get("interactive_fail")
    _require(
        record.get("interactive_count") == len(echo)
        and isinstance(interactive_ok, int)
        and not isinstance(interactive_ok, bool)
        and interactive_ok >= 0
        and isinstance(interactive_fail, int)
        and not isinstance(interactive_fail, bool)
        and interactive_fail >= 0
        and interactive_ok + interactive_fail == len(echo),
        f"{prefix} echo attempts are not completely accounted",
    )
    previous_index = -1
    previous_time = -math.inf
    previous_end = -math.inf
    for attempt in echo:
        _require(isinstance(attempt, dict), f"{prefix} echo attempt is not an object")
        index = attempt.get("index")
        time_s = attempt.get("start_offset_s")
        end_s = attempt.get("end_offset_s")
        outcome = attempt.get("outcome")
        latency = attempt.get("latency_ms")
        _require(
            isinstance(index, int)
            and not isinstance(index, bool)
            and index == previous_index + 1,
            f"{prefix} echo indexes are not contiguous from zero",
        )
        _require(
            _finite_non_negative(time_s)
            and time_s > previous_time
            and _finite_non_negative(end_s)
            and end_s >= time_s
            and time_s >= previous_end,
            f"{prefix} echo times are invalid or non-monotonic",
        )
        _require(
            isinstance(outcome, str) and outcome, f"{prefix} echo outcome is invalid"
        )
        if outcome == "success":
            _require(
                _finite_non_negative(latency)
                and math.isclose(
                    latency,
                    (end_s - time_s) * 1000.0,
                    rel_tol=1e-9,
                    abs_tol=1e-6,
                ),
                f"{prefix} successful echo has no finite latency",
            )
        else:
            _require(
                latency is None,
                f"{prefix} failed echo latency must remain null",
            )
        previous_index = index
        previous_time = time_s
        previous_end = end_s
    successful_attempts = sum(attempt["outcome"] == "success" for attempt in echo)
    _require(
        successful_attempts == interactive_ok
        and len(echo) - successful_attempts == interactive_fail,
        f"{prefix} echo outcome counts disagree with the attempt series",
    )
    expected_status = "ok" if interactive_fail == 0 else "loss"
    _require(
        record.get("status") == expected_status,
        f"{prefix} status disagrees with accounted echo outcomes",
    )
    echo_interval_ms = record.get("interactive_interval_ms")
    _require(
        isinstance(echo_interval_ms, int)
        and not isinstance(echo_interval_ms, bool)
        and echo_interval_ms > 0,
        f"{prefix} echo interval is invalid",
    )
    echo_interval_s = echo_interval_ms / 1000.0
    interactive_time_s = record.get("interactive_time_s")
    _require(
        _finite_non_negative(interactive_time_s)
        and interactive_time_s >= duration * 0.99
        and echo[0]["start_offset_s"] <= echo_interval_s
        and echo[-1]["end_offset_s"] >= duration - echo_interval_s,
        f"{prefix} echo series does not persist across the declared duration",
    )
    if series_id.startswith("mpp_"):
        _require(
            record.get("status") == "ok" and interactive_fail == 0,
            f"{prefix} MPP echo continuity gate failed",
        )


def _goodput_series(records, repetitions):
    interval = records[0]["bulk_interval_seconds"]
    trim = records[0].get("bulk_interval_trim_discard_each_end")
    _require(
        isinstance(trim, int) and not isinstance(trim, bool) and trim >= 0,
        "bulk_interval_trim_discard_each_end is invalid",
    )
    length = len(records[0]["bulk_interval_goodput_mbps"])
    _require(
        all(
            record["bulk_interval_seconds"] == interval
            and record.get("bulk_interval_trim_discard_each_end") == trim
            and len(record["bulk_interval_goodput_mbps"]) == length
            for record in records
        ),
        "goodput sampling grids differ across repetitions",
    )
    per_window = round(GOODPUT_WINDOW_SECONDS / interval)
    partial = length % per_window
    leading_discard = partial // 2
    trailing_discard = partial - leading_discard
    usable_end = length - trailing_discard
    _require(
        usable_end - leading_discard >= per_window * 3,
        "goodput series cannot form a persistent full-window trajectory",
    )
    output = []
    for start in range(leading_discard, usable_end, per_window):
        values = [
            statistics.fmean(
                record["bulk_interval_goodput_mbps"][start : start + per_window]
            )
            for record in records
        ]
        center_s = (trim + start + per_window / 2) * interval
        sample = {"time_s": round(center_s, 6)}
        sample.update(_summary(values, repetitions))
        output.append(sample)
    return output


def _latency_series(records, repetitions):
    indexed = [
        {attempt["index"]: attempt for attempt in record["interactive_attempt_series"]}
        for record in records
    ]
    indexes = sorted({index for attempts in indexed for index in attempts})
    output = []
    for index in indexes:
        times = []
        latencies = []
        outcomes = Counter()
        for attempts in indexed:
            attempt = attempts.get(index)
            if attempt is None:
                outcomes["not_recorded"] += 1
                continue
            times.append(attempt["start_offset_s"])
            outcomes[attempt["outcome"]] += 1
            if attempt["latency_ms"] is not None:
                latencies.append(attempt["latency_ms"])
        _require(times, f"echo index {index} has no recorded time")
        sample = {"time_s": round(statistics.median(times), 6)}
        sample.update(_summary(latencies, repetitions, outcomes))
        output.append(sample)
    return output


def _implementation(series_id, records):
    if series_id.startswith("mpp_"):
        protocols = {record.get("mptunnel_protocol_version") for record in records}
        profiles = {record.get("mptunnel_build_profile") for record in records}
        _require(
            len(protocols) == 1 and next(iter(protocols)) is not None,
            f"{series_id} protocol versions differ across repetitions",
        )
        _require(
            profiles == {"release"},
            f"{series_id} was not consistently built in release mode",
        )
        carrier = {
            "mpp_tcp": "MPP/TCP",
            "mpp_quic": "MPP/QUIC",
            "mpp_default": "MPP/TCP+QUIC (default)",
        }[series_id]
        return {
            "tool": "mptunnel",
            "carrier": carrier,
            "protocol_version": next(iter(protocols)),
            "build_profile": "release",
        }

    identities = [record["baseline_identity"] for record in records]
    tools = {identity["tool"] for identity in identities}
    locks = {identity["lock_sha256"] for identity in identities}
    releases = {
        identity[endpoint].get("release")
        for identity in identities
        for endpoint in ("client", "server")
    }
    _require(len(tools) == 1, f"{series_id} baseline tools differ across repetitions")
    _require(
        len(locks) == 1,
        f"{series_id} baseline locks differ across repetitions",
    )
    _require(
        len(releases) == 1 and next(iter(releases)),
        f"{series_id} baseline releases differ across endpoints or repetitions",
    )
    return {
        "tool": next(iter(tools)),
        "release": next(iter(releases)),
        "carrier": "VMess/TCP" if series_id == "xray_vmess" else "QUIC/Brutal",
    }


def derive_dataset(repetition_paths, title, subtitle, condition_note, cohort_id):
    logical_repetitions = load_repetitions(repetition_paths)
    repetitions = len(logical_repetitions)
    commits = set()
    series = []
    for series_id, label, case in CASE_SERIES:
        records = []
        for repetition in logical_repetitions:
            record = repetition["records"][case]
            path = repetition["record_sources"][case]
            _validate_record(record, series_id, case, path)
            commits.add(record.get("source_commit"))
            records.append(record)
        series.append(
            {
                "id": series_id,
                "label": label,
                "valid_repetitions": repetitions,
                "implementation": _implementation(series_id, records),
                "goodput": _goodput_series(records, repetitions),
                "latency": _latency_series(records, repetitions),
            }
        )
    _require(
        len(commits) == 1 and next(iter(commits)),
        "source_commit must be present and identical across the cohort",
    )
    return {
        "schema_version": 1,
        "figure": {
            "title": title,
            "subtitle": subtitle,
            "condition_note": condition_note,
            "variability_note": (
                "Line: pointwise median; band: min–max across available "
                f"observations in {repetitions} valid repetitions."
            ),
        },
        "provenance": {
            "cohort_id": cohort_id,
            "source_commit": next(iter(commits)),
            "source_runs": [repetition["source"] for repetition in logical_repetitions],
            "valid_repetitions": repetitions,
            "aggregation": "pointwise_median_min_max",
            "goodput_source": "bulk_interval_goodput_mbps",
            "goodput_window_s": GOODPUT_WINDOW_SECONDS,
            "goodput_window_alignment": "symmetric_full_windows",
            "latency_source": "interactive_attempt_series",
            "latency_alignment": "attempt_index",
            "condition": logical_repetitions[0]["condition"],
        },
        "series": series,
    }


def write_json(dataset, output_path):
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
            json.dump(dataset, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    inputs = parser.add_mutually_exclusive_group(required=True)
    inputs.add_argument(
        "--result",
        action="append",
        type=Path,
        help="one complete results.jsonl per logical repetition",
    )
    inputs.add_argument(
        "--repetition",
        action="append",
        help="comma-separated results.jsonl files forming one logical repetition",
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cohort-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--subtitle", required=True)
    parser.add_argument("--condition-note", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        repetition_paths = (
            [[path] for path in args.result]
            if args.result is not None
            else [parse_repetition_spec(value) for value in args.repetition]
        )
        dataset = derive_dataset(
            repetition_paths,
            args.title,
            args.subtitle,
            args.condition_note,
            args.cohort_id,
        )
        write_json(dataset, args.output)
    except DerivationError as exc:
        print(f"performance-series derivation error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

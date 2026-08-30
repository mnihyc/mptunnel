#!/usr/bin/env python3
"""Derive the separate two-run raw-TCP context trajectory for v0.4.7."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

from derive_performance_series import (
    DerivationError,
    _goodput_series,
    _latency_series,
    load_condition,
    load_result_file,
    record_probe_condition,
    write_json,
)
from evaluate_release_series import (
    RAW_CASE,
    RELEASE,
    REPETITIONS,
    RUNTIME_IDENTITY_FIELDS,
    EvaluationError,
    _runtime_identity,
    _validate_raw_record,
)

DATASET_KIND = "raw_tcp_direct_control"
COMPARISON_ROLE = "context-only"
ROUTE_SCOPE = "direct-client-to-target"


class RawControlError(ValueError):
    """Raw control evidence cannot support the separate publication figure."""


def _require(condition, message):
    if not condition:
        raise RawControlError(message)


def _load_passing_verdict(path, candidate_commit, raw_paths):
    try:
        payload = Path(path).read_bytes()
        verdict = json.loads(payload)
    except (OSError, json.JSONDecodeError) as exc:
        raise RawControlError(f"cannot read release verdict {path}: {exc}") from exc

    _require(isinstance(verdict, dict), "release verdict must be an object")
    _require(
        verdict.get("schema_version") == 1
        and verdict.get("release") == RELEASE
        and verdict.get("status") == "pass",
        f"release verdict must be a passing {RELEASE} evaluation",
    )
    _require(
        verdict.get("candidate_commit") == candidate_commit
        and verdict.get("source_commit") == candidate_commit,
        "release verdict candidate/source commit differs from --candidate-commit",
    )
    criteria = verdict.get("criteria")
    _require(
        isinstance(criteria, dict)
        and criteria.get("repetitions") == REPETITIONS
        and criteria.get("raw_controls") == REPETITIONS,
        "release verdict does not cover exactly two paired raw controls",
    )
    _require(
        verdict.get("failed_repetitions") == [],
        "release verdict retains a failed repetition",
    )
    repetitions = verdict.get("repetitions")
    _require(
        isinstance(repetitions, list)
        and [item.get("id") if isinstance(item, dict) else None for item in repetitions]
        == ["repetition-1", "repetition-2"]
        and all(item.get("status") == "pass" for item in repetitions),
        "release verdict must contain exactly two passing paired repetitions",
    )

    runtime = verdict.get("runtime_identity")
    _require(
        isinstance(runtime, dict) and set(runtime) == set(RUNTIME_IDENTITY_FIELDS),
        "release verdict has no exact client/server runtime identity",
    )
    runtime_identity = _runtime_identity(runtime, "release verdict")

    raw_directory_names = {path.resolve().parent.name for path in raw_paths}
    product_directory_names = []
    for repetition, raw_path in zip(repetitions, raw_paths):
        raw_summary = repetition.get("raw_control")
        _require(
            isinstance(raw_summary, dict)
            and raw_summary.get("case") == RAW_CASE
            and raw_summary.get("pass") is True
            and raw_summary.get("trajectory_windows") == 38,
            f"{repetition['id']} has no accepted raw-control trajectory",
        )
        _require(
            raw_summary.get("source") == raw_path.resolve().parent.name,
            f"{repetition['id']} is not paired with the supplied raw control",
        )
        product_sources = repetition.get("product_sources")
        _require(
            isinstance(product_sources, list) and product_sources,
            f"{repetition['id']} has no product source provenance",
        )
        _require(
            all(
                isinstance(source, str) and source and Path(source).name == source
                for source in product_sources
            ),
            f"{repetition['id']} product source provenance is not sanitized",
        )
        product_directory_names.extend(product_sources)
    _require(
        len(set(product_directory_names)) == len(product_directory_names),
        "release verdict repeats a product result-directory name",
    )
    _require(
        not raw_directory_names.intersection(set(product_directory_names)),
        "raw controls must remain in route-distinct result directories",
    )
    return verdict, hashlib.sha256(payload).hexdigest(), runtime_identity


def derive_dataset(
    raw_paths,
    verdict_path,
    candidate_commit,
    title,
    subtitle,
    condition_note,
    cohort_id,
):
    _require(
        isinstance(candidate_commit, str)
        and re.fullmatch(r"[0-9a-f]{40}", candidate_commit) is not None,
        "--candidate-commit must be an exact 40-character lowercase Git commit",
    )
    paths = [Path(path) for path in raw_paths]
    _require(len(paths) == REPETITIONS, "exactly two --raw-control inputs are required")
    directories = [path.resolve().parent for path in paths]
    directory_names = [directory.name for directory in directories]
    _require(
        len(set(directories)) == REPETITIONS
        and len(set(directory_names)) == REPETITIONS,
        "raw controls require two distinct result directories and names",
    )

    verdict, verdict_sha256, runtime_identity = _load_passing_verdict(
        verdict_path, candidate_commit, paths
    )
    records = []
    conditions = []
    probes = []
    host_snapshots = []
    for path in paths:
        rows = load_result_file(path)
        _require(
            set(rows) == {RAW_CASE},
            f"{path} must contain exactly the separate {RAW_CASE} row",
        )
        record = rows[RAW_CASE]
        condition = load_condition(path)
        _validate_raw_record(record, condition, path)
        _require(
            record.get("source_commit") == candidate_commit,
            f"{path}:{RAW_CASE} source commit differs from --candidate-commit",
        )
        _require(
            record.get("mptunnel_build_features") == [],
            f"{path}:{RAW_CASE} must record the unmodified release feature set",
        )
        _require(
            _runtime_identity(record, f"{path}:{RAW_CASE}") == runtime_identity,
            f"{path}:{RAW_CASE} runtime identity differs from the passing verdict",
        )
        records.append(record)
        conditions.append(condition)
        probes.append(record_probe_condition(record))
        host_snapshots.append(record["host_snapshot_sha256"])

    _require(
        conditions[0] == conditions[1],
        "raw-control dynamic conditions differ across repetitions",
    )
    _require(
        probes[0] == probes[1],
        "raw-control probe workloads differ across repetitions",
    )
    condition = dict(conditions[0])
    condition["probe"] = probes[0]
    goodput = _goodput_series(records, REPETITIONS)
    latency = _latency_series(records, REPETITIONS)
    _require(
        len(goodput) == 38
        and goodput[0]["time_s"] == 1.5
        and goodput[-1]["time_s"] == 38.5,
        "raw-control goodput is not the exact 38-window pointwise trajectory",
    )
    _require(
        len(latency) >= 3
        and all(sample["available"] == REPETITIONS for sample in latency),
        "raw-control latency is not a complete pointwise attempt trajectory",
    )

    return {
        "schema_version": 1,
        "dataset_kind": DATASET_KIND,
        "figure": {
            "title": title,
            "subtitle": subtitle,
            "condition_note": condition_note,
            "variability_note": (
                "Line: pointwise median; band: min–max across the two accepted "
                "direct-control repetitions."
            ),
        },
        "provenance": {
            "cohort_id": cohort_id,
            "candidate_commit": candidate_commit,
            "source_commit": candidate_commit,
            "paired_candidate_runtime_identity": dict(
                zip(RUNTIME_IDENTITY_FIELDS, runtime_identity)
            ),
            "host_snapshot_sha256": host_snapshots,
            "release_verdict": {
                "file": Path(verdict_path).name,
                "sha256": verdict_sha256,
                "status": verdict["status"],
                "release": verdict["release"],
                "candidate_commit": verdict["candidate_commit"],
            },
            "comparison_scope": {
                "role": COMPARISON_ROLE,
                "route": ROUTE_SCOPE,
                "included_in_exact_five": False,
                "included_in_product_comparisons": False,
            },
            "source_runs": [
                {"id": f"repetition-{index}", "result_dirs": [directory_name]}
                for index, directory_name in enumerate(directory_names, 1)
            ],
            "valid_repetitions": REPETITIONS,
            "aggregation": "pointwise_median_min_max",
            "goodput_source": "bulk_interval_goodput_mbps",
            "goodput_window_s": 1.0,
            "goodput_window_alignment": "symmetric_full_windows",
            "latency_source": "interactive_attempt_series",
            "latency_alignment": "attempt_index",
            "condition": condition,
        },
        "series": [
            {
                "id": "raw_tcp",
                "label": "Raw TCP (direct control)",
                "valid_repetitions": REPETITIONS,
                "implementation": {
                    "tool": "raw-tcp",
                    "carrier": "Direct TCP",
                    "route_scope": ROUTE_SCOPE,
                },
                "goodput": goodput,
                "latency": latency,
            }
        ],
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-control", action="append", type=Path, default=[])
    parser.add_argument("--release-verdict", required=True, type=Path)
    parser.add_argument("--candidate-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cohort-id", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--subtitle", required=True)
    parser.add_argument("--condition-note", required=True)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        dataset = derive_dataset(
            args.raw_control,
            args.release_verdict,
            args.candidate_commit,
            args.title,
            args.subtitle,
            args.condition_note,
            args.cohort_id,
        )
        write_json(dataset, args.output)
    except (DerivationError, EvaluationError, RawControlError) as exc:
        print(f"raw-control derivation error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

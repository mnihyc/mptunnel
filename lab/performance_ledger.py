#!/usr/bin/env python3
"""Versioned, append-only performance champion ledger.

The existing lab runner writes result JSONL rows with reproducible MPTunnel
binary identities. This module deliberately does not change that runner. A
separate evidence builder can arrange adjacent runs into the registration and
comparison documents accepted here.

After metric direction is normalized, at least seven adjacent matched pairs
feed a preregistered two-sided 95% paired-bootstrap classification against both
the immediate accepted parent and the active historical champion. Confidence
intervals wholly below zero prove regression, intervals wholly above zero
prove improvement, and overlap is inconclusive. A project-owner-approved
latency/stability tradeoff can advance the accepted head after proven
regression, but it never replaces the historical champion.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import os
import re
import statistics
import sys
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from result_enrichment import (
        HOST_VALIDITY_RULES_VERSION,
        MPTUNNEL_CARRIER_PRESENTATION,
        MPTUNNEL_CARRIER_PRESENTATIONS,
        MPTUNNEL_PROTOCOL_VERSION,
        RESULT_SCHEMA_VERSION,
        RUN_MANIFEST_SCHEMA_VERSION,
        load_baseline_lock,
    )
except ModuleNotFoundError:
    from lab.result_enrichment import (
        HOST_VALIDITY_RULES_VERSION,
        MPTUNNEL_CARRIER_PRESENTATION,
        MPTUNNEL_CARRIER_PRESENTATIONS,
        MPTUNNEL_PROTOCOL_VERSION,
        RESULT_SCHEMA_VERSION,
        RUN_MANIFEST_SCHEMA_VERSION,
        load_baseline_lock,
    )


LEDGER_SCHEMA_VERSION = 2
EVIDENCE_SCHEMA_VERSION = 2
DECISION_SCHEMA_VERSION = 2
LEDGER_KIND = "mptunnel.performance-ledger"
REGISTRATION_KIND = "mptunnel.performance-champion-registration"
COMPARISON_KIND = "mptunnel.performance-comparison"
TRADEOFF_KIND = "mptunnel.performance-tradeoff-approval"
DECISION_KIND = "mptunnel.performance-decision"

PASS = "PASS"
FAIL = "FAIL"
INCONCLUSIVE = "INCONCLUSIVE"
APPROVED_TRADEOFF = "APPROVED_LATENCY_STABILITY_TRADEOFF"

HIGHER_IS_BETTER = "higher"
LOWER_IS_BETTER = "lower"
PROVEN_REGRESSION = "PROVEN_REGRESSION"
PROVEN_IMPROVEMENT = "PROVEN_IMPROVEMENT"
DELTA_INCONCLUSIVE = "INCONCLUSIVE"
MINIMUM_PAIR_COUNT = 7
BOOTSTRAP_METHOD = "deterministic-paired-bootstrap-percentile-v2"
BOOTSTRAP_CONFIDENCE_LEVEL = 0.95
MINIMUM_BOOTSTRAP_ITERATIONS = 10_000
TEST_STATISTIC = "paired_median_normalized_delta"

_ID_RE = re.compile(r"[a-z0-9][a-z0-9._/-]{0,127}")
_COMMIT_RE = re.compile(r"[0-9a-f]{40}|[0-9a-f]{64}")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")

_SUBJECT_FIELDS = {
    "result_schema_version",
    "run_manifest_schema_version",
    "host_snapshot_schema_version",
    "host_validity_rules_version",
    "host_snapshot_sha256",
    "host_valid",
    "source_commit",
    "source_tree_dirty",
    "source_snapshot_sha256",
    "mptunnel_build_profile",
    "mptunnel_build_features",
    "mptunnel_protocol_version",
    "mptunnel_carrier_presentation",
    "mptunnel_client_runtime",
    "mptunnel_client_runtime_version",
    "mptunnel_client_target",
    "mptunnel_client_sha256",
    "mptunnel_server_target",
    "mptunnel_server_sha256",
    "cargo_lock_sha256",
    "rustc_version",
    "rustc_executable_sha256",
    "cargo_version",
    "cargo_executable_sha256",
}

_INFERENCE_FIELDS = {
    "method",
    "preregistered",
    "confidence_level",
    "bootstrap_iterations",
    "bootstrap_seed",
    "alternative",
    "test_statistic",
}

_INFERENCE_RESULT_FIELDS = {
    "method",
    "confidence_level",
    "bootstrap_iterations",
    "bootstrap_seed",
    "test_statistic",
    "test_statistic_value",
    "two_sided_confidence_interval_95",
    "classification",
}

_DECISION_RECORD_FIELDS = {
    "sequence",
    "decision_id",
    "status",
    "subject",
    "subject_sha256",
    "evidence_sha256",
    "pair_count",
    "candidate_median",
    "parent_subject_sha256",
    "parent_paired_median_normalized_delta",
    "parent_inference",
    "champion_subject_sha256",
    "champion_paired_median_normalized_delta",
    "champion_inference",
    "promoted_to_champion",
    "tradeoff_record_sha256",
    "observed_normalized_regression",
    "previous_record_sha256",
    "record_sha256",
}

_CHAMPION_RECORD_FIELDS = {
    "sequence",
    "accepted_sequence",
    "decision_id",
    "subject",
    "subject_sha256",
    "evidence_sha256",
    "median_value",
    "observation_count",
    "previous_record_sha256",
    "record_sha256",
}


class LedgerError(ValueError):
    """Raised when ledger or evidence data violates the frozen contract."""


def _reject_json_constant(value: str) -> None:
    raise LedgerError(f"non-finite JSON number is not allowed: {value}")


def _load_json(path: str | Path) -> dict[str, Any]:
    try:
        with Path(path).open("r", encoding="utf-8") as handle:
            value = json.load(handle, parse_constant=_reject_json_constant)
    except (OSError, json.JSONDecodeError) as exc:
        raise LedgerError(f"cannot load JSON from {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LedgerError(f"{path} must contain one JSON object")
    return value


def _canonical_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise LedgerError(f"value is not canonical JSON: {exc}") from exc


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_write_json(path: str | Path, value: Mapping[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    mode = destination.stat().st_mode & 0o777 if destination.exists() else 0o644
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            dir=destination.parent,
            encoding="utf-8",
            prefix=f".{destination.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.chmod(temporary_name, mode)
        os.replace(temporary_name, destination)
        temporary_name = None
    finally:
        if temporary_name is not None:
            try:
                Path(temporary_name).unlink()
            except FileNotFoundError:
                pass


def _require_exact_keys(value: Any, expected: set[str], context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise LedgerError(f"{context} must be an object")
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unknown = sorted(actual - expected)
        details = []
        if missing:
            details.append(f"missing {missing}")
        if unknown:
            details.append(f"unknown {unknown}")
        raise LedgerError(f"{context} fields are invalid: {', '.join(details)}")
    return value


def _require_identifier(value: Any, context: str) -> str:
    if not isinstance(value, str) or _ID_RE.fullmatch(value) is None:
        raise LedgerError(
            f"{context} must match {_ID_RE.pattern!r} and use lowercase stable text"
        )
    if "::" in value:
        raise LedgerError(f"{context} cannot contain '::'")
    return value


def _require_nonempty_string(value: Any, context: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LedgerError(f"{context} must be a non-empty string")
    return value


def _require_sha256(value: Any, context: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise LedgerError(f"{context} must be a lowercase SHA-256 digest")
    return value


def _require_positive_integer(value: Any, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise LedgerError(f"{context} must be a positive integer")
    return value


def _require_number(value: Any, context: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise LedgerError(f"{context} must be a finite number")
    result = float(value)
    if not math.isfinite(result):
        raise LedgerError(f"{context} must be a finite number")
    return result


def _optional_number(value: Any, context: str) -> float | None:
    if value is None:
        return None
    return _require_number(value, context)


def _require_json_object(value: Any, context: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not value:
        raise LedgerError(f"{context} must be a non-empty JSON object")
    if not all(isinstance(key, str) and key for key in value):
        raise LedgerError(f"{context} keys must be non-empty strings")
    _canonical_bytes(value)
    return value


def subject_identity_from_result(row: Mapping[str, Any]) -> dict[str, Any]:
    """Extract and validate the exact MPTunnel identity emitted by the lab.

    Extra result metrics are intentionally ignored. All current runtime
    identity fields are mandatory for champion evidence.
    """

    missing = sorted(_SUBJECT_FIELDS - set(row))
    if missing:
        raise LedgerError(f"result row is missing subject identity fields: {missing}")
    identity = {field: copy.deepcopy(row[field]) for field in _SUBJECT_FIELDS}
    return validate_subject_identity(identity)


def validate_subject_identity(value: Any) -> dict[str, Any]:
    subject = _require_exact_keys(value, _SUBJECT_FIELDS, "subject identity")
    version_fields = {
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "run_manifest_schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "host_snapshot_schema_version": 1,
        "host_validity_rules_version": HOST_VALIDITY_RULES_VERSION,
    }
    for field, expected in version_fields.items():
        if subject[field] != expected:
            raise LedgerError(f"subject {field} must be {expected}")
    if subject["host_valid"] is not True:
        raise LedgerError("subject host_valid must be true")
    _require_sha256(subject["host_snapshot_sha256"], "subject host_snapshot_sha256")
    commit = subject["source_commit"]
    if not isinstance(commit, str) or _COMMIT_RE.fullmatch(commit) is None:
        raise LedgerError("subject source_commit must be a lowercase Git commit digest")
    if not isinstance(subject["source_tree_dirty"], bool):
        raise LedgerError("subject source_tree_dirty must be boolean")
    _require_sha256(subject["source_snapshot_sha256"], "subject source_snapshot_sha256")
    _require_nonempty_string(
        subject["mptunnel_build_profile"], "subject mptunnel_build_profile"
    )
    features = subject["mptunnel_build_features"]
    if (
        not isinstance(features, list)
        or not all(isinstance(feature, str) and feature for feature in features)
        or features != sorted(set(features))
    ):
        raise LedgerError(
            "subject mptunnel_build_features must be a sorted unique string array"
        )
    if subject["mptunnel_protocol_version"] != MPTUNNEL_PROTOCOL_VERSION:
        raise LedgerError(
            f"subject mptunnel_protocol_version must be "
            f"{MPTUNNEL_PROTOCOL_VERSION}"
        )
    if subject["mptunnel_carrier_presentation"] not in MPTUNNEL_CARRIER_PRESENTATIONS:
        raise LedgerError(
            "subject mptunnel_carrier_presentation is not a supported v6 "
            "TCP/QUIC wire presentation"
        )
    for field in (
        "mptunnel_client_runtime",
        "mptunnel_client_runtime_version",
        "mptunnel_client_target",
        "mptunnel_server_target",
    ):
        _require_nonempty_string(subject[field], f"subject {field}")
    for field in (
        "mptunnel_client_sha256",
        "mptunnel_server_sha256",
        "cargo_lock_sha256",
        "rustc_executable_sha256",
        "cargo_executable_sha256",
    ):
        _require_sha256(subject[field], f"subject {field}")
    for field in ("rustc_version", "cargo_version"):
        _require_nonempty_string(subject[field], f"subject {field}")
    return copy.deepcopy(subject)


def subject_sha256(subject: Mapping[str, Any]) -> str:
    return _digest(validate_subject_identity(subject))


def validate_cell(value: Any) -> dict[str, Any]:
    cell = _require_exact_keys(value, {"id", "dimensions"}, "cell")
    _require_identifier(cell["id"], "cell id")
    _require_json_object(cell["dimensions"], "cell dimensions")
    return copy.deepcopy(cell)


def validate_metric(value: Any) -> dict[str, Any]:
    metric = _require_exact_keys(value, {"id", "unit", "direction"}, "metric")
    _require_identifier(metric["id"], "metric id")
    _require_nonempty_string(metric["unit"], "metric unit")
    if metric["direction"] not in {HIGHER_IS_BETTER, LOWER_IS_BETTER}:
        raise LedgerError("metric direction must be 'higher' or 'lower'")
    return copy.deepcopy(metric)


def validate_inference(value: Any) -> dict[str, Any]:
    inference = _require_exact_keys(value, _INFERENCE_FIELDS, "inference contract")
    if inference["method"] != BOOTSTRAP_METHOD:
        raise LedgerError(f"inference method must be {BOOTSTRAP_METHOD!r}")
    if inference["preregistered"] is not True:
        raise LedgerError("inference contract must be preregistered")
    if inference["confidence_level"] != BOOTSTRAP_CONFIDENCE_LEVEL:
        raise LedgerError(
            f"inference confidence_level must be {BOOTSTRAP_CONFIDENCE_LEVEL}"
        )
    iterations = _require_positive_integer(
        inference["bootstrap_iterations"], "inference bootstrap_iterations"
    )
    if iterations < MINIMUM_BOOTSTRAP_ITERATIONS:
        raise LedgerError(
            f"inference bootstrap_iterations must be at least "
            f"{MINIMUM_BOOTSTRAP_ITERATIONS}"
        )
    seed = inference["bootstrap_seed"]
    if (
        isinstance(seed, bool)
        or not isinstance(seed, int)
        or seed < 0
        or seed > (1 << 64) - 1
    ):
        raise LedgerError("inference bootstrap_seed must be an unsigned 64-bit integer")
    if inference["alternative"] != "two_sided":
        raise LedgerError("inference alternative must be 'two_sided'")
    if inference["test_statistic"] != TEST_STATISTIC:
        raise LedgerError(f"inference test_statistic must be {TEST_STATISTIC!r}")
    return copy.deepcopy(inference)


def cell_key(cell: Mapping[str, Any], metric: Mapping[str, Any]) -> str:
    checked_cell = validate_cell(cell)
    checked_metric = validate_metric(metric)
    return f"{checked_cell['id']}::{checked_metric['id']}"


def _load_baseline_snapshot(path: str | Path) -> dict[str, Any]:
    baseline_path = Path(path)
    try:
        payload = baseline_path.read_bytes()
    except OSError as exc:
        raise LedgerError(f"cannot read baseline lock {path}: {exc}") from exc
    try:
        document = load_baseline_lock(baseline_path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise LedgerError(f"invalid baseline lock {path}: {exc}") from exc
    return {
        "sha256": _sha256_bytes(payload),
        "document": document,
    }


def new_ledger(baseline_lock_path: str | Path) -> dict[str, Any]:
    ledger = {
        "schema_version": LEDGER_SCHEMA_VERSION,
        "kind": LEDGER_KIND,
        "baseline_lock": _load_baseline_snapshot(baseline_lock_path),
        "cells": [],
    }
    validate_ledger(ledger, baseline_lock_path)
    return ledger


def _normalize_registration(value: Any) -> dict[str, Any]:
    registration = _require_exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "registration_id",
            "cell",
            "metric",
            "inference",
            "baseline_lock_sha256",
            "subject",
            "required_pair_count",
            "declared_repeat_count",
            "repetitions",
        },
        "champion registration",
    )
    if registration["schema_version"] != EVIDENCE_SCHEMA_VERSION:
        raise LedgerError(
            f"registration schema_version must be {EVIDENCE_SCHEMA_VERSION}"
        )
    if registration["kind"] != REGISTRATION_KIND:
        raise LedgerError(f"registration kind must be {REGISTRATION_KIND!r}")
    _require_identifier(registration["registration_id"], "registration id")
    registration["cell"] = validate_cell(registration["cell"])
    registration["metric"] = validate_metric(registration["metric"])
    registration["inference"] = validate_inference(registration["inference"])
    _require_sha256(
        registration["baseline_lock_sha256"], "registration baseline_lock_sha256"
    )
    registration["subject"] = validate_subject_identity(registration["subject"])
    required = _require_positive_integer(
        registration["required_pair_count"], "registration required_pair_count"
    )
    if required < MINIMUM_PAIR_COUNT:
        raise LedgerError(
            f"registration required_pair_count must be at least {MINIMUM_PAIR_COUNT}"
        )
    declared = _require_positive_integer(
        registration["declared_repeat_count"], "registration declared_repeat_count"
    )
    repetitions = registration["repetitions"]
    if not isinstance(repetitions, list):
        raise LedgerError("registration repetitions must be an array")
    normalized_repetitions = []
    identifiers: set[str] = set()
    for index, repetition in enumerate(repetitions):
        checked = _require_exact_keys(
            repetition, {"repeat_id", "value"}, f"registration repetition {index}"
        )
        repeat_id = _require_identifier(
            checked["repeat_id"], f"registration repetition {index} repeat_id"
        )
        if repeat_id in identifiers:
            raise LedgerError(f"duplicate registration repeat_id {repeat_id!r}")
        identifiers.add(repeat_id)
        normalized_repetitions.append(
            {
                "repeat_id": repeat_id,
                "value": _require_number(
                    checked["value"], f"registration repetition {repeat_id} value"
                ),
            }
        )
    if declared != len(normalized_repetitions):
        raise LedgerError(
            "registration declared_repeat_count does not match repetitions"
        )
    if len(normalized_repetitions) < required:
        raise LedgerError(
            f"registration has {len(normalized_repetitions)} repetitions; "
            f"{required} are required"
        )
    registration["repetitions"] = sorted(
        normalized_repetitions, key=lambda item: item["repeat_id"]
    )
    return copy.deepcopy(registration)


def _normalize_comparison(value: Any) -> dict[str, Any]:
    comparison = _require_exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "comparison_id",
            "cell",
            "metric",
            "inference",
            "baseline_lock_sha256",
            "candidate",
            "parent",
            "champion",
            "required_pair_count",
            "declared_pair_count",
            "pairs",
        },
        "performance comparison",
    )
    if comparison["schema_version"] != EVIDENCE_SCHEMA_VERSION:
        raise LedgerError(
            f"comparison schema_version must be {EVIDENCE_SCHEMA_VERSION}"
        )
    if comparison["kind"] != COMPARISON_KIND:
        raise LedgerError(f"comparison kind must be {COMPARISON_KIND!r}")
    _require_identifier(comparison["comparison_id"], "comparison id")
    comparison["cell"] = validate_cell(comparison["cell"])
    comparison["metric"] = validate_metric(comparison["metric"])
    comparison["inference"] = validate_inference(comparison["inference"])
    _require_sha256(
        comparison["baseline_lock_sha256"], "comparison baseline_lock_sha256"
    )
    for role in ("candidate", "parent", "champion"):
        comparison[role] = validate_subject_identity(comparison[role])
    required = _require_positive_integer(
        comparison["required_pair_count"], "comparison required_pair_count"
    )
    if required < MINIMUM_PAIR_COUNT:
        raise LedgerError(
            f"comparison required_pair_count must be at least {MINIMUM_PAIR_COUNT}"
        )
    declared = _require_positive_integer(
        comparison["declared_pair_count"], "comparison declared_pair_count"
    )
    pairs = comparison["pairs"]
    if not isinstance(pairs, list):
        raise LedgerError("comparison pairs must be an array")
    normalized_pairs = []
    identifiers: set[str] = set()
    for index, pair in enumerate(pairs):
        checked = _require_exact_keys(
            pair,
            {"pair_id", "candidate", "parent", "champion"},
            f"comparison pair {index}",
        )
        pair_id = _require_identifier(
            checked["pair_id"], f"comparison pair {index} pair_id"
        )
        if pair_id in identifiers:
            raise LedgerError(f"duplicate comparison pair_id {pair_id!r}")
        identifiers.add(pair_id)
        normalized_pairs.append(
            {
                "pair_id": pair_id,
                "candidate": _require_number(
                    checked["candidate"], f"comparison pair {pair_id} candidate"
                ),
                "parent": _require_number(
                    checked["parent"], f"comparison pair {pair_id} parent"
                ),
                "champion": _require_number(
                    checked["champion"], f"comparison pair {pair_id} champion"
                ),
            }
        )
    if declared != len(normalized_pairs):
        raise LedgerError("comparison declared_pair_count does not match pairs")
    if len(normalized_pairs) < required:
        raise LedgerError(
            f"comparison has {len(normalized_pairs)} pairs; {required} are required"
        )
    comparison["pairs"] = sorted(normalized_pairs, key=lambda item: item["pair_id"])
    return copy.deepcopy(comparison)


def _normalize_tradeoff(value: Any) -> dict[str, Any]:
    tradeoff = _require_exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "record_id",
            "comparison_id",
            "cell",
            "metric",
            "candidate",
            "preregistered",
            "approved",
            "approved_by",
            "theoretical_necessity",
            "benefit",
            "pareto_evidence_sha256",
            "ablation_evidence_sha256",
        },
        "tradeoff approval",
    )
    if tradeoff["schema_version"] != EVIDENCE_SCHEMA_VERSION:
        raise LedgerError(f"tradeoff schema_version must be {EVIDENCE_SCHEMA_VERSION}")
    if tradeoff["kind"] != TRADEOFF_KIND:
        raise LedgerError(f"tradeoff kind must be {TRADEOFF_KIND!r}")
    _require_identifier(tradeoff["record_id"], "tradeoff record id")
    _require_identifier(tradeoff["comparison_id"], "tradeoff comparison id")
    tradeoff["cell"] = validate_cell(tradeoff["cell"])
    tradeoff["metric"] = validate_metric(tradeoff["metric"])
    tradeoff["candidate"] = validate_subject_identity(tradeoff["candidate"])
    if tradeoff["preregistered"] is not True:
        raise LedgerError("tradeoff approval must be preregistered")
    if tradeoff["approved"] is not True:
        raise LedgerError("tradeoff approval requires explicit approved=true")
    _require_nonempty_string(tradeoff["approved_by"], "tradeoff approved_by")
    _require_nonempty_string(
        tradeoff["theoretical_necessity"], "tradeoff theoretical_necessity"
    )
    benefit = _require_exact_keys(
        tradeoff["benefit"],
        {"kind", "metric_id", "unit", "required_gain", "observed_gain"},
        "tradeoff benefit",
    )
    if benefit["kind"] not in {"latency", "stability"}:
        raise LedgerError("tradeoff benefit kind must be latency or stability")
    _require_identifier(benefit["metric_id"], "tradeoff benefit metric_id")
    _require_nonempty_string(benefit["unit"], "tradeoff benefit unit")
    required_gain = _require_number(
        benefit["required_gain"], "tradeoff benefit required_gain"
    )
    observed_gain = _require_number(
        benefit["observed_gain"], "tradeoff benefit observed_gain"
    )
    if required_gain <= 0 or observed_gain < required_gain:
        raise LedgerError(
            "tradeoff observed benefit must meet a positive required gain"
        )
    _require_sha256(
        tradeoff["pareto_evidence_sha256"], "tradeoff pareto_evidence_sha256"
    )
    _require_sha256(
        tradeoff["ablation_evidence_sha256"], "tradeoff ablation_evidence_sha256"
    )
    return copy.deepcopy(tradeoff)


def _record_hash(record: Mapping[str, Any]) -> str:
    body = dict(record)
    body.pop("record_sha256", None)
    return _digest(body)


def _percentile(sorted_values: Sequence[float], probability: float) -> float:
    if not sorted_values:
        raise LedgerError("cannot calculate a percentile without samples")
    position = (len(sorted_values) - 1) * probability
    lower_index = int(math.floor(position))
    upper_index = int(math.ceil(position))
    if lower_index == upper_index:
        value = sorted_values[lower_index]
    else:
        fraction = position - lower_index
        value = (
            sorted_values[lower_index] * (1.0 - fraction)
            + sorted_values[upper_index] * fraction
        )
    return 0.0 if value == 0 else float(value)


def _splitmix64_next(state: int) -> tuple[int, int]:
    mask = (1 << 64) - 1
    state = (state + 0x9E3779B97F4A7C15) & mask
    value = state
    value = ((value ^ (value >> 30)) * 0xBF58476D1CE4E5B9) & mask
    value = ((value ^ (value >> 27)) * 0x94D049BB133111EB) & mask
    value ^= value >> 31
    return state, value & mask


def _validate_inference_result(
    value: Any,
    contract: Mapping[str, Any],
    context: str,
) -> dict[str, Any]:
    result = _require_exact_keys(value, _INFERENCE_RESULT_FIELDS, context)
    checked_contract = validate_inference(contract)
    for name in (
        "method",
        "confidence_level",
        "bootstrap_iterations",
        "bootstrap_seed",
        "test_statistic",
    ):
        if result[name] != checked_contract[name]:
            raise LedgerError(f"{context} {name} does not match the cell contract")
    statistic = _require_number(
        result["test_statistic_value"], f"{context} test_statistic_value"
    )
    interval = result["two_sided_confidence_interval_95"]
    if not isinstance(interval, list) or len(interval) != 2:
        raise LedgerError(f"{context} confidence interval must contain two values")
    lower = _require_number(interval[0], f"{context} confidence interval lower")
    upper = _require_number(interval[1], f"{context} confidence interval upper")
    if lower > upper:
        raise LedgerError(f"{context} confidence interval is reversed")
    expected_classification = _classify_interval(lower, upper)
    if result["classification"] != expected_classification:
        raise LedgerError(f"{context} classification is inconsistent")
    if statistic != result["test_statistic_value"]:
        raise LedgerError(f"{context} test statistic is not canonical")
    return copy.deepcopy(result)


def _classify_interval(lower: float, upper: float) -> str:
    if upper < 0:
        return PROVEN_REGRESSION
    if lower > 0:
        return PROVEN_IMPROVEMENT
    return DELTA_INCONCLUSIVE


def _inference_is_exact_equivalence(inference: Mapping[str, Any]) -> bool:
    return (
        inference["test_statistic_value"] == 0
        and inference["two_sided_confidence_interval_95"] == [0.0, 0.0]
    )


def _paired_bootstrap_inference(
    deltas: Sequence[float], contract: Mapping[str, Any]
) -> dict[str, Any]:
    checked_contract = validate_inference(contract)
    if not deltas:
        raise LedgerError("paired bootstrap requires at least one delta")
    observed = float(statistics.median(deltas))
    state = checked_contract["bootstrap_seed"]
    bootstrap_medians = []
    for _ in range(checked_contract["bootstrap_iterations"]):
        sample = []
        for _ in deltas:
            state, random_value = _splitmix64_next(state)
            sample.append(deltas[random_value % len(deltas)])
        sample_median = float(statistics.median(sample))
        bootstrap_medians.append(0.0 if sample_median == 0 else sample_median)
    bootstrap_medians.sort()
    lower_two_sided = _percentile(bootstrap_medians, 0.025)
    upper_two_sided = _percentile(bootstrap_medians, 0.975)
    result = {
        "method": checked_contract["method"],
        "confidence_level": checked_contract["confidence_level"],
        "bootstrap_iterations": checked_contract["bootstrap_iterations"],
        "bootstrap_seed": checked_contract["bootstrap_seed"],
        "test_statistic": checked_contract["test_statistic"],
        "test_statistic_value": 0.0 if observed == 0 else observed,
        "two_sided_confidence_interval_95": [
            lower_two_sided,
            upper_two_sided,
        ],
        "classification": _classify_interval(
            lower_two_sided,
            upper_two_sided,
        ),
    }
    return _validate_inference_result(result, checked_contract, "bootstrap result")


def _new_decision_record(
    *,
    sequence: int,
    decision_id: str,
    status: str,
    subject: Mapping[str, Any],
    evidence_sha256: str,
    pair_count: int,
    candidate_median: float,
    parent_subject_sha256: str | None,
    parent_delta: float | None,
    parent_inference: Mapping[str, Any] | None,
    champion_subject_sha256: str | None,
    champion_delta: float | None,
    champion_inference: Mapping[str, Any] | None,
    promoted: bool,
    tradeoff_record_sha256: str | None,
    observed_normalized_regression: float | None,
    previous_record_sha256: str | None,
) -> dict[str, Any]:
    record = {
        "sequence": sequence,
        "decision_id": decision_id,
        "status": status,
        "subject": copy.deepcopy(subject),
        "subject_sha256": subject_sha256(subject),
        "evidence_sha256": evidence_sha256,
        "pair_count": pair_count,
        "candidate_median": candidate_median,
        "parent_subject_sha256": parent_subject_sha256,
        "parent_paired_median_normalized_delta": parent_delta,
        "parent_inference": copy.deepcopy(parent_inference),
        "champion_subject_sha256": champion_subject_sha256,
        "champion_paired_median_normalized_delta": champion_delta,
        "champion_inference": copy.deepcopy(champion_inference),
        "promoted_to_champion": promoted,
        "tradeoff_record_sha256": tradeoff_record_sha256,
        "observed_normalized_regression": observed_normalized_regression,
        "previous_record_sha256": previous_record_sha256,
    }
    record["record_sha256"] = _record_hash(record)
    return record


def _new_champion_record(
    *,
    sequence: int,
    accepted_sequence: int,
    decision_id: str,
    subject: Mapping[str, Any],
    evidence_sha256: str,
    median_value: float,
    observation_count: int,
    previous_record_sha256: str | None,
) -> dict[str, Any]:
    record = {
        "sequence": sequence,
        "accepted_sequence": accepted_sequence,
        "decision_id": decision_id,
        "subject": copy.deepcopy(subject),
        "subject_sha256": subject_sha256(subject),
        "evidence_sha256": evidence_sha256,
        "median_value": median_value,
        "observation_count": observation_count,
        "previous_record_sha256": previous_record_sha256,
    }
    record["record_sha256"] = _record_hash(record)
    return record


def _validate_decision_record(
    record: Any,
    *,
    index: int,
    required_pair_count: int,
    inference_contract: Mapping[str, Any],
    previous_hash: str | None,
) -> dict[str, Any]:
    checked = _require_exact_keys(
        record, _DECISION_RECORD_FIELDS, f"accepted history record {index + 1}"
    )
    sequence = _require_positive_integer(
        checked["sequence"], f"accepted history record {index + 1} sequence"
    )
    if sequence != index + 1:
        raise LedgerError("accepted history sequence is not contiguous")
    _require_identifier(
        checked["decision_id"], f"accepted history record {sequence} decision_id"
    )
    if checked["status"] not in {PASS, APPROVED_TRADEOFF}:
        raise LedgerError("accepted history can contain only accepted statuses")
    validate_subject_identity(checked["subject"])
    expected_subject_digest = subject_sha256(checked["subject"])
    if checked["subject_sha256"] != expected_subject_digest:
        raise LedgerError("accepted history subject_sha256 does not match subject")
    _require_sha256(
        checked["evidence_sha256"],
        f"accepted history record {sequence} evidence_sha256",
    )
    pair_count = _require_positive_integer(
        checked["pair_count"], f"accepted history record {sequence} pair_count"
    )
    if pair_count < required_pair_count:
        raise LedgerError("accepted history record has too few pairs")
    _require_number(
        checked["candidate_median"],
        f"accepted history record {sequence} candidate_median",
    )
    if not isinstance(checked["promoted_to_champion"], bool):
        raise LedgerError("accepted history promoted_to_champion must be boolean")
    for name in ("parent_subject_sha256", "champion_subject_sha256"):
        if checked[name] is not None:
            _require_sha256(checked[name], f"accepted history record {sequence} {name}")
    for name in (
        "parent_paired_median_normalized_delta",
        "champion_paired_median_normalized_delta",
    ):
        _optional_number(checked[name], f"accepted history record {sequence} {name}")
    if sequence == 1:
        if any(
            checked[name] is not None
            for name in (
                "parent_subject_sha256",
                "parent_paired_median_normalized_delta",
                "parent_inference",
                "champion_subject_sha256",
                "champion_paired_median_normalized_delta",
                "champion_inference",
                "tradeoff_record_sha256",
                "observed_normalized_regression",
                "previous_record_sha256",
            )
        ):
            raise LedgerError("initial accepted history record has prior references")
        if checked["status"] != PASS or checked["promoted_to_champion"] is not True:
            raise LedgerError("initial accepted history record must be a champion PASS")
    else:
        for name in (
            "parent_subject_sha256",
            "parent_paired_median_normalized_delta",
            "parent_inference",
            "champion_subject_sha256",
            "champion_paired_median_normalized_delta",
            "champion_inference",
        ):
            if checked[name] is None:
                raise LedgerError(
                    f"accepted history record {sequence} is missing {name}"
                )
        if checked["previous_record_sha256"] != previous_hash:
            raise LedgerError("accepted history hash chain is broken")
        parent_delta = checked["parent_paired_median_normalized_delta"]
        champion_delta = checked["champion_paired_median_normalized_delta"]
        assert parent_delta is not None and champion_delta is not None
        parent_inference = _validate_inference_result(
            checked["parent_inference"],
            inference_contract,
            f"accepted history record {sequence} parent inference",
        )
        champion_inference = _validate_inference_result(
            checked["champion_inference"],
            inference_contract,
            f"accepted history record {sequence} champion inference",
        )
        if parent_inference["test_statistic_value"] != parent_delta:
            raise LedgerError(
                "accepted history parent statistic does not match paired median"
            )
        if champion_inference["test_statistic_value"] != champion_delta:
            raise LedgerError(
                "accepted history champion statistic does not match paired median"
            )
        classifications = {
            parent_inference["classification"],
            champion_inference["classification"],
        }
        proven_regression = PROVEN_REGRESSION in classifications
        proven_improvement = PROVEN_IMPROVEMENT in classifications
        exact_equivalence = _inference_is_exact_equivalence(
            parent_inference
        ) and _inference_is_exact_equivalence(champion_inference)
        if checked["status"] == APPROVED_TRADEOFF:
            _require_sha256(
                checked["tradeoff_record_sha256"],
                f"accepted history record {sequence} tradeoff_record_sha256",
            )
            if checked["promoted_to_champion"]:
                raise LedgerError("an approved tradeoff cannot become champion")
            if not proven_regression:
                raise LedgerError(
                    "an approved tradeoff must contain proven repeated regression"
                )
            observed_regression = _require_number(
                checked["observed_normalized_regression"],
                f"accepted history record {sequence} observed_normalized_regression",
            )
            expected_regression = max(0.0, -parent_delta, -champion_delta)
            if observed_regression != expected_regression:
                raise LedgerError(
                    "approved tradeoff observed regression does not match evidence"
                )
        elif checked["tradeoff_record_sha256"] is not None:
            raise LedgerError("ordinary PASS cannot carry a tradeoff record")
        else:
            if checked["observed_normalized_regression"] is not None:
                raise LedgerError("ordinary PASS cannot carry an observed tradeoff cost")
            if proven_regression:
                raise LedgerError("ordinary PASS contains proven repeated regression")
            if not proven_improvement and not exact_equivalence:
                raise LedgerError(
                    "ordinary PASS must prove an improvement or exact equivalence"
                )
            expected_promotion = (
                champion_inference["classification"] == PROVEN_IMPROVEMENT
            )
            if checked["promoted_to_champion"] != expected_promotion:
                raise LedgerError(
                    "ordinary PASS champion promotion does not match repeated evidence"
                )
    _require_sha256(
        checked["record_sha256"],
        f"accepted history record {sequence} record_sha256",
    )
    if checked["record_sha256"] != _record_hash(checked):
        raise LedgerError("accepted history record hash does not match its content")
    return checked


def _validate_champion_record(
    record: Any,
    *,
    index: int,
    required_pair_count: int,
    previous_hash: str | None,
    accepted_history: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    checked = _require_exact_keys(
        record, _CHAMPION_RECORD_FIELDS, f"champion history record {index + 1}"
    )
    sequence = _require_positive_integer(
        checked["sequence"], f"champion history record {index + 1} sequence"
    )
    if sequence != index + 1:
        raise LedgerError("champion history sequence is not contiguous")
    accepted_sequence = _require_positive_integer(
        checked["accepted_sequence"],
        f"champion history record {sequence} accepted_sequence",
    )
    if accepted_sequence > len(accepted_history):
        raise LedgerError("champion record references a missing accepted decision")
    accepted = accepted_history[accepted_sequence - 1]
    _require_identifier(
        checked["decision_id"], f"champion history record {sequence} decision_id"
    )
    validate_subject_identity(checked["subject"])
    expected_subject_digest = subject_sha256(checked["subject"])
    if checked["subject_sha256"] != expected_subject_digest:
        raise LedgerError("champion history subject_sha256 does not match subject")
    _require_sha256(
        checked["evidence_sha256"],
        f"champion history record {sequence} evidence_sha256",
    )
    _require_number(
        checked["median_value"], f"champion history record {sequence} median_value"
    )
    observations = _require_positive_integer(
        checked["observation_count"],
        f"champion history record {sequence} observation_count",
    )
    if observations < required_pair_count:
        raise LedgerError("champion history record has too few observations")
    if sequence == 1:
        if checked["previous_record_sha256"] is not None or accepted_sequence != 1:
            raise LedgerError("initial champion history link is invalid")
    elif checked["previous_record_sha256"] != previous_hash:
        raise LedgerError("champion history hash chain is broken")
    if (
        accepted["decision_id"] != checked["decision_id"]
        or accepted["subject"] != checked["subject"]
        or accepted["subject_sha256"] != checked["subject_sha256"]
        or accepted["evidence_sha256"] != checked["evidence_sha256"]
        or accepted["candidate_median"] != checked["median_value"]
        or accepted["pair_count"] != checked["observation_count"]
        or accepted["status"] != PASS
        or accepted["promoted_to_champion"] is not True
    ):
        raise LedgerError("champion record does not match its accepted decision")
    _require_sha256(
        checked["record_sha256"],
        f"champion history record {sequence} record_sha256",
    )
    if checked["record_sha256"] != _record_hash(checked):
        raise LedgerError("champion history record hash does not match its content")
    return checked


def validate_ledger(
    value: Any, baseline_lock_path: str | Path | None = None
) -> dict[str, Any]:
    ledger = _require_exact_keys(
        value,
        {"schema_version", "kind", "baseline_lock", "cells"},
        "performance ledger",
    )
    if ledger["schema_version"] != LEDGER_SCHEMA_VERSION:
        raise LedgerError(f"ledger schema_version must be {LEDGER_SCHEMA_VERSION}")
    if ledger["kind"] != LEDGER_KIND:
        raise LedgerError(f"ledger kind must be {LEDGER_KIND!r}")
    snapshot = _require_exact_keys(
        ledger["baseline_lock"], {"sha256", "document"}, "ledger baseline_lock"
    )
    _require_sha256(snapshot["sha256"], "ledger baseline_lock sha256")
    document = snapshot["document"]
    if not isinstance(document, dict):
        raise LedgerError("ledger baseline_lock document must be an object")
    if document.get("schema_version") != 1:
        raise LedgerError("ledger baseline_lock document schema_version must be 1")
    if baseline_lock_path is not None:
        current = _load_baseline_snapshot(baseline_lock_path)
        if snapshot != current:
            raise LedgerError(
                "ledger baseline lock identity does not exactly match the supplied lock"
            )
    cells = ledger["cells"]
    if not isinstance(cells, list):
        raise LedgerError("ledger cells must be an array")
    keys: list[str] = []
    global_decision_ids: set[str] = set()
    for cell_index, entry in enumerate(cells):
        checked = _require_exact_keys(
            entry,
            {
                "key",
                "cell",
                "metric",
                "inference",
                "required_pair_count",
                "accepted_history",
                "champion_history",
            },
            f"ledger cell {cell_index}",
        )
        checked_cell = validate_cell(checked["cell"])
        checked_metric = validate_metric(checked["metric"])
        checked_inference = validate_inference(checked["inference"])
        expected_key = cell_key(checked_cell, checked_metric)
        if checked["key"] != expected_key:
            raise LedgerError(f"ledger cell key must be {expected_key!r}")
        keys.append(expected_key)
        required = _require_positive_integer(
            checked["required_pair_count"],
            f"ledger cell {expected_key} required_pair_count",
        )
        if required < MINIMUM_PAIR_COUNT:
            raise LedgerError(
                f"ledger cell {expected_key} requires fewer than "
                f"{MINIMUM_PAIR_COUNT} pairs"
            )
        accepted = checked["accepted_history"]
        champions = checked["champion_history"]
        if not isinstance(accepted, list) or not accepted:
            raise LedgerError(f"ledger cell {expected_key} has no accepted history")
        if not isinstance(champions, list) or not champions:
            raise LedgerError(f"ledger cell {expected_key} has no champion history")
        previous_hash = None
        validated_accepted: list[dict[str, Any]] = []
        for history_index, record in enumerate(accepted):
            validated = _validate_decision_record(
                record,
                index=history_index,
                required_pair_count=required,
                inference_contract=checked_inference,
                previous_hash=previous_hash,
            )
            if validated["decision_id"] in global_decision_ids:
                raise LedgerError(
                    f"duplicate ledger decision id {validated['decision_id']!r}"
                )
            global_decision_ids.add(validated["decision_id"])
            if history_index > 0:
                expected_parent = validated_accepted[-1]["subject_sha256"]
                if validated["parent_subject_sha256"] != expected_parent:
                    raise LedgerError(
                        "accepted history parent is not the immediate accepted head"
                    )
            validated_accepted.append(validated)
            previous_hash = validated["record_sha256"]
        previous_hash = None
        last_accepted_sequence = 0
        for history_index, record in enumerate(champions):
            validated = _validate_champion_record(
                record,
                index=history_index,
                required_pair_count=required,
                previous_hash=previous_hash,
                accepted_history=validated_accepted,
            )
            if validated["accepted_sequence"] <= last_accepted_sequence:
                raise LedgerError(
                    "champion accepted_sequence must increase monotonically"
                )
            last_accepted_sequence = validated["accepted_sequence"]
            previous_hash = validated["record_sha256"]
        active_champion = champions[-1]["subject_sha256"]
        champion_cursor = champions[0]["subject_sha256"]
        champion_by_accepted_sequence = {
            champion["accepted_sequence"]: champion["subject_sha256"]
            for champion in champions
        }
        for accepted_record in validated_accepted[1:]:
            if accepted_record["champion_subject_sha256"] != champion_cursor:
                raise LedgerError(
                    "accepted history champion reference is not the active champion"
                )
            promoted_digest = champion_by_accepted_sequence.get(
                accepted_record["sequence"]
            )
            if accepted_record["promoted_to_champion"]:
                if promoted_digest != accepted_record["subject_sha256"]:
                    raise LedgerError(
                        "promoted accepted decision is missing its champion record"
                    )
                champion_cursor = promoted_digest
            elif promoted_digest is not None:
                raise LedgerError(
                    "unpromoted accepted decision unexpectedly has a champion record"
                )
        if champion_cursor != active_champion:
            raise LedgerError("active champion history is inconsistent")
    if keys != sorted(keys) or len(keys) != len(set(keys)):
        raise LedgerError("ledger cells must have unique, sorted keys")
    return ledger


def _find_cell(ledger: Mapping[str, Any], key: str) -> dict[str, Any]:
    for entry in ledger["cells"]:
        if entry["key"] == key:
            return entry
    raise LedgerError(f"ledger does not contain cell {key!r}")


def register_champion(
    ledger: dict[str, Any],
    registration_value: Mapping[str, Any],
    baseline_lock_path: str | Path,
) -> dict[str, Any]:
    validate_ledger(ledger, baseline_lock_path)
    registration = _normalize_registration(copy.deepcopy(registration_value))
    baseline_digest = ledger["baseline_lock"]["sha256"]
    if registration["baseline_lock_sha256"] != baseline_digest:
        raise LedgerError("registration baseline lock identity does not match ledger")
    key = cell_key(registration["cell"], registration["metric"])
    if any(entry["key"] == key for entry in ledger["cells"]):
        raise LedgerError(f"cell {key!r} is already registered")
    if any(
        registration["registration_id"] == accepted["decision_id"]
        for entry in ledger["cells"]
        for accepted in entry["accepted_history"]
    ):
        raise LedgerError(
            f"decision id {registration['registration_id']!r} already exists"
        )
    values = [repetition["value"] for repetition in registration["repetitions"]]
    median_value = float(statistics.median(values))
    evidence_sha256 = _digest(registration)
    accepted = _new_decision_record(
        sequence=1,
        decision_id=registration["registration_id"],
        status=PASS,
        subject=registration["subject"],
        evidence_sha256=evidence_sha256,
        pair_count=len(values),
        candidate_median=median_value,
        parent_subject_sha256=None,
        parent_delta=None,
        parent_inference=None,
        champion_subject_sha256=None,
        champion_delta=None,
        champion_inference=None,
        promoted=True,
        tradeoff_record_sha256=None,
        observed_normalized_regression=None,
        previous_record_sha256=None,
    )
    champion = _new_champion_record(
        sequence=1,
        accepted_sequence=1,
        decision_id=registration["registration_id"],
        subject=registration["subject"],
        evidence_sha256=evidence_sha256,
        median_value=median_value,
        observation_count=len(values),
        previous_record_sha256=None,
    )
    ledger["cells"].append(
        {
            "key": key,
            "cell": registration["cell"],
            "metric": registration["metric"],
            "inference": registration["inference"],
            "required_pair_count": registration["required_pair_count"],
            "accepted_history": [accepted],
            "champion_history": [champion],
        }
    )
    ledger["cells"].sort(key=lambda entry: entry["key"])
    validate_ledger(ledger, baseline_lock_path)
    return {
        "schema_version": DECISION_SCHEMA_VERSION,
        "kind": DECISION_KIND,
        "decision_id": registration["registration_id"],
        "cell_key": key,
        "status": PASS,
        "direction": registration["metric"]["direction"],
        "pair_count": len(values),
        "candidate_subject_sha256": subject_sha256(registration["subject"]),
        "candidate_median": median_value,
        "immediate_parent": None,
        "historical_champion": None,
        "promoted_to_champion": True,
        "evidence_sha256": evidence_sha256,
        "tradeoff_record_sha256": None,
        "observed_normalized_regression": None,
        "committed": True,
    }


def _normalized_delta(candidate: float, reference: float, direction: str) -> float:
    delta = (
        candidate - reference
        if direction == HIGHER_IS_BETTER
        else reference - candidate
    )
    return 0.0 if delta == 0 else delta


def compare_candidate(
    ledger: dict[str, Any],
    comparison_value: Mapping[str, Any],
    baseline_lock_path: str | Path,
    *,
    tradeoff_value: Mapping[str, Any] | None = None,
    commit: bool = False,
) -> dict[str, Any]:
    validate_ledger(ledger, baseline_lock_path)
    comparison = _normalize_comparison(copy.deepcopy(comparison_value))
    if comparison["baseline_lock_sha256"] != ledger["baseline_lock"]["sha256"]:
        raise LedgerError("comparison baseline lock identity does not match ledger")
    key = cell_key(comparison["cell"], comparison["metric"])
    entry = _find_cell(ledger, key)
    if comparison["cell"] != entry["cell"]:
        raise LedgerError("comparison cell identity does not exactly match ledger")
    if comparison["metric"] != entry["metric"]:
        raise LedgerError("comparison metric identity does not exactly match ledger")
    if comparison["inference"] != entry["inference"]:
        raise LedgerError("comparison inference contract does not exactly match ledger")
    if comparison["required_pair_count"] != entry["required_pair_count"]:
        raise LedgerError(
            "comparison required_pair_count does not exactly match the cell contract"
        )
    accepted_head = entry["accepted_history"][-1]
    champion_head = entry["champion_history"][-1]
    if comparison["parent"] != accepted_head["subject"]:
        raise LedgerError(
            "comparison parent identity is not the immediate accepted parent"
        )
    if comparison["champion"] != champion_head["subject"]:
        raise LedgerError(
            "comparison champion identity is not the active historical champion"
        )
    if any(
        comparison["comparison_id"] == accepted["decision_id"]
        for ledger_entry in ledger["cells"]
        for accepted in ledger_entry["accepted_history"]
    ):
        raise LedgerError(f"decision id {comparison['comparison_id']!r} already exists")

    direction = comparison["metric"]["direction"]
    candidate_values = [pair["candidate"] for pair in comparison["pairs"]]
    parent_deltas = [
        _normalized_delta(pair["candidate"], pair["parent"], direction)
        for pair in comparison["pairs"]
    ]
    champion_deltas = [
        _normalized_delta(pair["candidate"], pair["champion"], direction)
        for pair in comparison["pairs"]
    ]
    candidate_median = float(statistics.median(candidate_values))
    parent_delta = float(statistics.median(parent_deltas))
    champion_delta = float(statistics.median(champion_deltas))
    parent_inference = _paired_bootstrap_inference(
        parent_deltas, comparison["inference"]
    )
    champion_inference = _paired_bootstrap_inference(
        champion_deltas, comparison["inference"]
    )
    classifications = {
        parent_inference["classification"],
        champion_inference["classification"],
    }
    proven_regression = PROVEN_REGRESSION in classifications
    proven_improvement = PROVEN_IMPROVEMENT in classifications
    exact_equivalence = _inference_is_exact_equivalence(
        parent_inference
    ) and _inference_is_exact_equivalence(champion_inference)
    ordinary_pass = (
        not proven_regression and (proven_improvement or exact_equivalence)
    )
    normalized_tradeoff = None
    tradeoff_sha256 = None
    observed_normalized_regression = None
    if tradeoff_value is not None:
        normalized_tradeoff = _normalize_tradeoff(copy.deepcopy(tradeoff_value))
        if not proven_regression:
            raise LedgerError(
                "a latency/stability tradeoff requires proven repeated regression"
            )
        if normalized_tradeoff["comparison_id"] != comparison["comparison_id"]:
            raise LedgerError("tradeoff comparison identity does not match evidence")
        if normalized_tradeoff["cell"] != comparison["cell"]:
            raise LedgerError("tradeoff cell identity does not match comparison")
        if normalized_tradeoff["metric"] != comparison["metric"]:
            raise LedgerError("tradeoff metric identity does not match comparison")
        if normalized_tradeoff["candidate"] != comparison["candidate"]:
            raise LedgerError("tradeoff candidate identity does not match comparison")
        observed_normalized_regression = max(
            0.0, -parent_delta, -champion_delta
        )
        tradeoff_sha256 = _digest(normalized_tradeoff)

    if proven_regression:
        status = (
            APPROVED_TRADEOFF if normalized_tradeoff is not None else FAIL
        )
    elif ordinary_pass:
        status = PASS
    else:
        status = INCONCLUSIVE
    promoted = (
        status == PASS
        and champion_inference["classification"] == PROVEN_IMPROVEMENT
    )
    evidence_sha256 = _digest(comparison)

    result = {
        "schema_version": DECISION_SCHEMA_VERSION,
        "kind": DECISION_KIND,
        "decision_id": comparison["comparison_id"],
        "cell_key": key,
        "status": status,
        "direction": direction,
        "pair_count": len(comparison["pairs"]),
        "candidate_subject_sha256": subject_sha256(comparison["candidate"]),
        "candidate_median": candidate_median,
        "immediate_parent": {
            "subject_sha256": accepted_head["subject_sha256"],
            "paired_median_normalized_delta": parent_delta,
            "inference": parent_inference,
        },
        "historical_champion": {
            "subject_sha256": champion_head["subject_sha256"],
            "paired_median_normalized_delta": champion_delta,
            "inference": champion_inference,
        },
        "promoted_to_champion": promoted,
        "evidence_sha256": evidence_sha256,
        "tradeoff_record_sha256": tradeoff_sha256,
        "observed_normalized_regression": observed_normalized_regression,
        "committed": False,
    }

    if commit and status in {PASS, APPROVED_TRADEOFF}:
        accepted_sequence = len(entry["accepted_history"]) + 1
        decision = _new_decision_record(
            sequence=accepted_sequence,
            decision_id=comparison["comparison_id"],
            status=status,
            subject=comparison["candidate"],
            evidence_sha256=evidence_sha256,
            pair_count=len(comparison["pairs"]),
            candidate_median=candidate_median,
            parent_subject_sha256=accepted_head["subject_sha256"],
            parent_delta=parent_delta,
            parent_inference=parent_inference,
            champion_subject_sha256=champion_head["subject_sha256"],
            champion_delta=champion_delta,
            champion_inference=champion_inference,
            promoted=promoted,
            tradeoff_record_sha256=tradeoff_sha256,
            observed_normalized_regression=observed_normalized_regression,
            previous_record_sha256=accepted_head["record_sha256"],
        )
        entry["accepted_history"].append(decision)
        if promoted:
            entry["champion_history"].append(
                _new_champion_record(
                    sequence=len(entry["champion_history"]) + 1,
                    accepted_sequence=accepted_sequence,
                    decision_id=comparison["comparison_id"],
                    subject=comparison["candidate"],
                    evidence_sha256=evidence_sha256,
                    median_value=candidate_median,
                    observation_count=len(comparison["pairs"]),
                    previous_record_sha256=champion_head["record_sha256"],
                )
            )
        validate_ledger(ledger, baseline_lock_path)
        result["committed"] = True
    return result


def _json_stdout(value: Mapping[str, Any]) -> None:
    sys.stdout.write(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )


def _default_baseline_lock() -> str:
    return str(Path(__file__).with_name("baseline-lock.json"))


def _add_common_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--ledger", required=True, help="performance ledger JSON path")
    parser.add_argument(
        "--baseline-lock",
        default=_default_baseline_lock(),
        help="frozen competitor baseline lock (default: lab/baseline-lock.json)",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser(
        "init", help="create an empty ledger bound to the current baseline lock"
    )
    _add_common_paths(init_parser)

    register_parser = subparsers.add_parser(
        "register", help="register the initial immutable champion for one cell"
    )
    _add_common_paths(register_parser)
    register_parser.add_argument("--evidence", required=True)

    compare_parser = subparsers.add_parser(
        "compare", help="compare candidate pairs with parent and champion"
    )
    _add_common_paths(compare_parser)
    compare_parser.add_argument("--evidence", required=True)
    compare_parser.add_argument(
        "--tradeoff",
        help="explicit project-owner latency/stability tradeoff approval JSON",
    )
    compare_parser.add_argument(
        "--commit",
        action="store_true",
        help="append a PASS or approved tradeoff decision",
    )

    validate_parser = subparsers.add_parser(
        "validate", help="validate schema, identities, and immutable hash chains"
    )
    _add_common_paths(validate_parser)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "init":
            ledger_path = Path(args.ledger)
            if ledger_path.exists():
                raise LedgerError(
                    f"refusing to overwrite existing ledger {ledger_path}"
                )
            ledger = new_ledger(args.baseline_lock)
            _atomic_write_json(ledger_path, ledger)
            _json_stdout(
                {
                    "schema_version": DECISION_SCHEMA_VERSION,
                    "kind": "mptunnel.performance-ledger-initialized",
                    "ledger": str(ledger_path),
                    "baseline_lock_sha256": ledger["baseline_lock"]["sha256"],
                }
            )
            return 0

        ledger = _load_json(args.ledger)
        validate_ledger(ledger, args.baseline_lock)
        if args.command == "validate":
            _json_stdout(
                {
                    "schema_version": DECISION_SCHEMA_VERSION,
                    "kind": "mptunnel.performance-ledger-validation",
                    "status": PASS,
                    "cell_count": len(ledger["cells"]),
                    "baseline_lock_sha256": ledger["baseline_lock"]["sha256"],
                }
            )
            return 0
        if args.command == "register":
            decision = register_champion(
                ledger, _load_json(args.evidence), args.baseline_lock
            )
            _atomic_write_json(args.ledger, ledger)
            _json_stdout(decision)
            return 0
        if args.command == "compare":
            tradeoff = _load_json(args.tradeoff) if args.tradeoff else None
            decision = compare_candidate(
                ledger,
                _load_json(args.evidence),
                args.baseline_lock,
                tradeoff_value=tradeoff,
                commit=args.commit,
            )
            if decision["committed"]:
                _atomic_write_json(args.ledger, ledger)
            _json_stdout(decision)
            return (
                0
                if decision["status"] in {PASS, APPROVED_TRADEOFF}
                else 1
            )
        raise LedgerError(f"unsupported command {args.command!r}")
    except LedgerError as exc:
        sys.stderr.write(f"performance ledger error: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

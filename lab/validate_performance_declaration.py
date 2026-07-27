#!/usr/bin/env python3
"""Validate F0 performance-impact declarations and acceptance evidence."""

from __future__ import annotations

import argparse
import json
import re
import sys
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping


REGISTRY_PATH = Path(__file__).with_name("performance-impact-registry.json")
SHA256_RE = re.compile(r"[0-9a-f]{64}")
CHANGE_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{2,127}")
BASE_COMPLETENESS = {
    "randomized_adjacent_runs",
    "raw_artifacts_complete",
    "identities_complete",
    "matched_inputs",
    "summary_regenerable",
    "invalid_runs_retained",
}


class ContractError(ValueError):
    """A registry, declaration, or acceptance contract is invalid."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ContractError(message)


def _is_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _string_list(
    value: Any, field: str, *, allow_empty: bool = False
) -> list[str]:
    _require(isinstance(value, list), f"{field} must be an array")
    _require(
        allow_empty or bool(value),
        f"{field} must not be empty",
    )
    _require(
        all(isinstance(item, str) and item for item in value),
        f"{field} must contain non-empty strings",
    )
    _require(len(value) == len(set(value)), f"{field} must not contain duplicates")
    return value


def _object(value: Any, field: str) -> dict[str, Any]:
    _require(isinstance(value, dict), f"{field} must be an object")
    return value


def load_json(path: str | Path) -> dict[str, Any]:
    source = Path(path)
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ContractError(f"cannot load {source}: {error}") from error
    return _object(value, str(source))


def normalize_path(raw: str) -> str:
    _require(isinstance(raw, str) and bool(raw.strip()), "changed path is empty")
    value = raw.strip()
    while value.startswith("./"):
        value = value[2:]
    _require("\\" not in value, f"changed path must use '/' separators: {raw!r}")
    path = PurePosixPath(value)
    _require(not path.is_absolute(), f"changed path must be repository-relative: {raw!r}")
    _require(
        bool(path.parts) and all(part not in {"", ".", ".."} for part in path.parts),
        f"changed path is not normalized: {raw!r}",
    )
    return path.as_posix()


def normalize_paths(values: Iterable[str], field: str) -> list[str]:
    paths = [normalize_path(value) for value in values if value.strip()]
    _require(len(paths) == len(set(paths)), f"{field} contains duplicate paths")
    return paths


def read_changed_paths(path: str) -> list[str]:
    try:
        text = sys.stdin.read() if path == "-" else Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise ContractError(f"cannot load changed paths from {path}: {error}") from error
    return normalize_paths(text.splitlines(), "authoritative changed paths")


def _cell_metric_ids(
    registry: Mapping[str, Any], cell_id: str
) -> set[str]:
    cell = registry["cells"][cell_id]
    return {
        metric
        for set_id in cell["metric_sets"]
        for metric in registry["metric_sets"][set_id]
    }


def validate_registry(registry: Mapping[str, Any]) -> None:
    _require(registry.get("schema_version") == 2, "registry schema_version must be 2")
    _require(
        isinstance(registry.get("registry_revision"), str)
        and bool(registry["registry_revision"]),
        "registry_revision must be a non-empty string",
    )

    lanes = _object(registry.get("lanes"), "registry lanes")
    _require("quick" in lanes, "registry must define the quick lane")
    ranks: set[int] = set()
    for lane_id, lane_value in lanes.items():
        lane = _object(lane_value, f"lane {lane_id}")
        rank = lane.get("rank")
        _require(_is_integer(rank) and rank >= 0, f"lane {lane_id} rank is invalid")
        _require(rank not in ranks, f"lane rank {rank} is duplicated")
        ranks.add(rank)
        _require(
            isinstance(lane.get("acceptance_capable"), bool),
            f"lane {lane_id} acceptance_capable must be boolean",
        )
        _require(
            lane.get("role")
            in {"triage_only", "repeated_affected_cell_evidence",
                "complete_packaged_competitive_evidence"},
            f"lane {lane_id} has an unsupported role",
        )
    _require(
        lanes["quick"]["acceptance_capable"] is False
        and lanes["quick"]["role"] == "triage_only",
        "quick must be a triage-only, non-acceptance lane",
    )
    _require(
        any(lane["acceptance_capable"] for lane in lanes.values()),
        "registry must define an acceptance-capable lane",
    )

    metrics = _object(registry.get("metrics"), "registry metrics")
    _require(bool(metrics), "registry metrics must not be empty")
    for metric_id, metric_value in metrics.items():
        metric = _object(metric_value, f"metric {metric_id}")
        _require(
            isinstance(metric.get("unit"), str) and bool(metric["unit"]),
            f"metric {metric_id} must define a unit",
        )
        _require(
            metric.get("better") in {"higher", "lower"},
            f"metric {metric_id} must define higher or lower as better",
        )

    metric_sets = _object(registry.get("metric_sets"), "registry metric_sets")
    used_metrics: set[str] = set()
    for set_id, value in metric_sets.items():
        members = _string_list(value, f"metric set {set_id}")
        unknown = set(members) - set(metrics)
        _require(not unknown, f"metric set {set_id} has unknown metrics: {sorted(unknown)}")
        used_metrics.update(members)
    _require(
        used_metrics == set(metrics),
        f"metrics missing from metric sets: {sorted(set(metrics) - used_metrics)}",
    )

    cells = _object(registry.get("cells"), "registry cells")
    _require(bool(cells), "registry cells must not be empty")
    for cell_id, cell_value in cells.items():
        cell = _object(cell_value, f"cell {cell_id}")
        _string_list(cell.get("profiles"), f"cell {cell_id} profiles")
        sets = _string_list(cell.get("metric_sets"), f"cell {cell_id} metric_sets")
        unknown_sets = set(sets) - set(metric_sets)
        _require(
            not unknown_sets,
            f"cell {cell_id} has unknown metric sets: {sorted(unknown_sets)}",
        )
        lane = cell.get("minimum_lane")
        _require(lane in lanes, f"cell {cell_id} has unknown minimum lane {lane!r}")
        for field in ("minimum_valid_pairs", "minimum_triggered_events"):
            value = cell.get(field)
            _require(
                _is_integer(value) and value >= 0,
                f"cell {cell_id} {field} must be a non-negative integer",
            )
        _require(
            cell["minimum_valid_pairs"] > 0
            or cell["minimum_triggered_events"] > 0,
            f"cell {cell_id} has no repeated-evidence minimum",
        )
        _string_list(
            cell.get("required_references"),
            f"cell {cell_id} required_references",
        )

    groups = _object(registry.get("cell_groups"), "registry cell_groups")
    for group_id, value in groups.items():
        members = _string_list(value, f"cell group {group_id}")
        unknown = set(members) - set(cells)
        _require(not unknown, f"cell group {group_id} has unknown cells: {sorted(unknown)}")

    scopes = _object(registry.get("impact_scopes"), "registry impact_scopes")
    _require("full-matrix" in scopes, "registry must define full-matrix")
    for scope_id, scope_value in scopes.items():
        scope = _object(scope_value, f"impact scope {scope_id}")
        scope_groups = _string_list(
            scope.get("cell_groups"), f"impact scope {scope_id} cell_groups"
        )
        unknown_groups = set(scope_groups) - set(groups)
        _require(
            not unknown_groups,
            f"impact scope {scope_id} has unknown groups: {sorted(unknown_groups)}",
        )
        scope_sets = _string_list(
            scope.get("metric_sets"), f"impact scope {scope_id} metric_sets"
        )
        _require(
            scope_sets == ["*"] or "*" not in scope_sets,
            f"impact scope {scope_id} must use '*' alone",
        )
        unknown_sets = set(scope_sets) - set(metric_sets) - {"*"}
        _require(
            not unknown_sets,
            f"impact scope {scope_id} has unknown metric sets: {sorted(unknown_sets)}",
        )
        if "requires_dual_run" in scope:
            _require(
                isinstance(scope["requires_dual_run"], bool),
                f"impact scope {scope_id} requires_dual_run must be boolean",
            )

    ignored = _string_list(
        registry.get("ignored_path_patterns"),
        "registry ignored_path_patterns",
        allow_empty=True,
    )
    _require(all(pattern.strip() == pattern for pattern in ignored), "path patterns have whitespace")
    rules = registry.get("path_rules")
    _require(isinstance(rules, list) and bool(rules), "registry path_rules must be an array")
    rule_ids: set[str] = set()
    for index, rule_value in enumerate(rules):
        rule = _object(rule_value, f"path rule {index}")
        rule_id = rule.get("id")
        _require(
            isinstance(rule_id, str) and rule_id and rule_id not in rule_ids,
            f"path rule {index} has an invalid or duplicate id",
        )
        rule_ids.add(rule_id)
        _string_list(rule.get("patterns"), f"path rule {rule_id} patterns")
        required = _string_list(
            rule.get("required_scopes"), f"path rule {rule_id} required_scopes"
        )
        unknown = set(required) - set(scopes)
        _require(not unknown, f"path rule {rule_id} has unknown scopes: {sorted(unknown)}")

    # This also proves every scope expands to at least one applicable metric.
    for scope_id in scopes:
        coverage = scope_requirements(registry, [scope_id])
        _require(bool(coverage), f"impact scope {scope_id} expands to no coverage")


def classify_paths(
    registry: Mapping[str, Any], changed_paths: Iterable[str]
) -> dict[str, Any]:
    required_scopes: set[str] = set()
    matches: dict[str, dict[str, Any]] = {}
    ignored: list[str] = []
    unclassified: list[str] = []
    for path in changed_paths:
        if any(
            fnmatchcase(path, pattern)
            for pattern in registry["ignored_path_patterns"]
        ):
            ignored.append(path)
            continue
        matched_rule = next(
            (
                rule
                for rule in registry["path_rules"]
                if any(fnmatchcase(path, pattern) for pattern in rule["patterns"])
            ),
            None,
        )
        if matched_rule is None:
            unclassified.append(path)
            continue
        scopes = list(matched_rule["required_scopes"])
        required_scopes.update(scopes)
        matches[path] = {"rule": matched_rule["id"], "required_scopes": scopes}
    return {
        "required_scopes": sorted(required_scopes),
        "matches": dict(sorted(matches.items())),
        "ignored": sorted(ignored),
        "unclassified": sorted(unclassified),
    }


def scope_requirements(
    registry: Mapping[str, Any], scope_ids: Iterable[str]
) -> dict[str, set[str]]:
    coverage: dict[str, set[str]] = {}
    for scope_id in scope_ids:
        scope = registry["impact_scopes"][scope_id]
        cell_ids = {
            cell_id
            for group_id in scope["cell_groups"]
            for cell_id in registry["cell_groups"][group_id]
        }
        for cell_id in cell_ids:
            cell = registry["cells"][cell_id]
            selected_sets = (
                cell["metric_sets"]
                if scope["metric_sets"] == ["*"]
                else [
                    set_id
                    for set_id in cell["metric_sets"]
                    if set_id in scope["metric_sets"]
                ]
            )
            selected_metrics = {
                metric
                for set_id in selected_sets
                for metric in registry["metric_sets"][set_id]
            }
            if selected_metrics:
                coverage.setdefault(cell_id, set()).update(selected_metrics)
    return coverage


def required_payload(
    registry: Mapping[str, Any], changed_paths: Iterable[str]
) -> dict[str, Any]:
    classification = classify_paths(registry, changed_paths)
    coverage = scope_requirements(registry, classification["required_scopes"])
    return {
        "schema_version": 2,
        "registry_revision": registry["registry_revision"],
        "declaration_required": bool(classification["required_scopes"]),
        "required_scopes": classification["required_scopes"],
        "path_classification": {
            key: classification[key] for key in ("matches", "ignored", "unclassified")
        },
        "affected": [
            {
                "cell_id": cell_id,
                "profiles": registry["cells"][cell_id]["profiles"],
                "metrics": sorted(metrics),
                "minimum_lane": registry["cells"][cell_id]["minimum_lane"],
                "minimum_valid_pairs": registry["cells"][cell_id][
                    "minimum_valid_pairs"
                ],
                "minimum_triggered_events": registry["cells"][cell_id][
                    "minimum_triggered_events"
                ],
                "required_references": registry["cells"][cell_id][
                    "required_references"
                ],
            }
            for cell_id, metrics in sorted(coverage.items())
        ],
    }


def _validate_quick_triage(value: Any) -> dict[str, Any]:
    quick = _object(value, "quick_triage")
    _require(
        quick.get("status") in {"not_run", "clear", "signal"},
        "quick_triage status must be not_run, clear, or signal",
    )
    digests = _string_list(
        quick.get("artifact_sha256"),
        "quick_triage artifact_sha256",
        allow_empty=True,
    )
    _require(
        all(SHA256_RE.fullmatch(digest) for digest in digests),
        "quick_triage artifact_sha256 contains an invalid digest",
    )
    return quick


def validate_declaration(
    registry: Mapping[str, Any],
    declaration: Mapping[str, Any],
    *,
    external_changed_paths: list[str] | None = None,
    phase: str = "declaration",
) -> dict[str, Any]:
    _require(declaration.get("schema_version") == 2, "declaration schema_version must be 2")
    _require(
        declaration.get("registry_revision") == registry["registry_revision"],
        "declaration registry_revision does not match the registry",
    )
    change_id = declaration.get("change_id")
    _require(
        isinstance(change_id, str) and CHANGE_ID_RE.fullmatch(change_id) is not None,
        "change_id must be a stable 3-128 character identifier",
    )
    rationale = declaration.get("rationale")
    _require(
        isinstance(rationale, str) and bool(rationale.strip()),
        "declaration rationale must be non-empty",
    )
    changed = normalize_paths(
        _string_list(declaration.get("changed_paths"), "changed_paths"),
        "changed_paths",
    )
    if external_changed_paths is not None:
        _require(
            set(changed) == set(external_changed_paths),
            "declaration changed_paths do not exactly match the authoritative changed paths",
        )

    classification = classify_paths(registry, changed)
    required_scopes = set(classification["required_scopes"])
    status = declaration.get("impact_status")
    _require(
        status in {"affected", "not_affected"},
        "impact_status must be affected or not_affected",
    )
    uncertainty = declaration.get("uncertainty")
    _require(
        uncertainty in {"bounded", "uncertain"},
        "uncertainty must be bounded or uncertain",
    )
    scopes = _string_list(
        declaration.get("impact_scopes"),
        "impact_scopes",
        allow_empty=True,
    )
    unknown_scopes = set(scopes) - set(registry["impact_scopes"])
    _require(not unknown_scopes, f"unknown impact scopes: {sorted(unknown_scopes)}")

    affected_value = declaration.get("affected")
    _require(isinstance(affected_value, list), "affected must be an array")
    evidence_value = declaration.get("acceptance_evidence")
    _require(isinstance(evidence_value, list), "acceptance_evidence must be an array")
    quick = _validate_quick_triage(declaration.get("quick_triage"))

    if required_scopes:
        _require(
            status == "affected",
            "a performance-sensitive changed path cannot be declared not_affected",
        )
    if status == "not_affected":
        _require(not scopes, "not_affected declaration cannot name impact scopes")
        _require(not affected_value, "not_affected declaration cannot name affected cells")
        _require(
            not evidence_value,
            "not_affected declaration cannot contain acceptance evidence",
        )
        _require(
            uncertainty == "bounded",
            "not_affected declaration cannot have uncertain impact",
        )
        return {
            "change_id": change_id,
            "impact_status": status,
            "classification": classification,
            "coverage": {},
        }

    _require(bool(scopes), "affected declaration must name impact scopes")
    missing_scopes = required_scopes - set(scopes)
    _require(
        not missing_scopes,
        f"declaration omits path-required impact scopes: {sorted(missing_scopes)}",
    )
    if uncertainty == "uncertain":
        _require(
            "full-matrix" in scopes,
            "uncertain impact requires the full-matrix scope",
        )

    required = scope_requirements(registry, scopes)
    declared: dict[str, set[str]] = {}
    for index, entry_value in enumerate(affected_value):
        entry = _object(entry_value, f"affected entry {index}")
        cell_id = entry.get("cell_id")
        _require(cell_id in registry["cells"], f"affected entry has unknown cell {cell_id!r}")
        _require(cell_id not in declared, f"affected cell {cell_id} is duplicated")
        metric_ids = _string_list(
            entry.get("metrics"), f"affected cell {cell_id} metrics"
        )
        applicable = _cell_metric_ids(registry, cell_id)
        unknown_metrics = set(metric_ids) - applicable
        _require(
            not unknown_metrics,
            f"affected cell {cell_id} has inapplicable metrics: {sorted(unknown_metrics)}",
        )
        declared[cell_id] = set(metric_ids)

    _require(bool(declared), "affected declaration must name cells and metrics")
    missing_cells = set(required) - set(declared)
    _require(not missing_cells, f"declaration omits required cells: {sorted(missing_cells)}")
    for cell_id, metrics in required.items():
        missing_metrics = metrics - declared[cell_id]
        _require(
            not missing_metrics,
            f"declaration omits required metrics for {cell_id}: {sorted(missing_metrics)}",
        )

    result = {
        "change_id": change_id,
        "impact_status": status,
        "classification": classification,
        "coverage": declared,
        "scopes": scopes,
        "quick_triage": quick,
    }
    if phase == "acceptance":
        validate_acceptance(registry, declaration, result)
    return result


def _nonnegative_integer(value: Any, field: str) -> int:
    _require(
        _is_integer(value) and value >= 0,
        f"{field} must be a non-negative integer",
    )
    return value


def validate_acceptance(
    registry: Mapping[str, Any],
    declaration: Mapping[str, Any],
    validated: Mapping[str, Any],
) -> None:
    evidence_list = declaration["acceptance_evidence"]
    _require(bool(evidence_list), "acceptance requires complete repeated evidence")
    expected = {
        (cell_id, metric)
        for cell_id, metrics in validated["coverage"].items()
        for metric in metrics
    }
    covered: set[tuple[str, str]] = set()
    dual_run_required = any(
        registry["impact_scopes"][scope_id].get("requires_dual_run") is True
        for scope_id in validated["scopes"]
    )

    for index, value in enumerate(evidence_list):
        evidence = _object(value, f"acceptance evidence {index}")
        cell_id = evidence.get("cell_id")
        _require(
            cell_id in validated["coverage"],
            f"acceptance evidence names undeclared cell {cell_id!r}",
        )
        metric_ids = _string_list(
            evidence.get("metrics"), f"acceptance evidence {index} metrics"
        )
        for metric in metric_ids:
            pair = (cell_id, metric)
            _require(pair in expected, f"acceptance evidence covers undeclared pair {pair}")
            _require(pair not in covered, f"acceptance evidence duplicates pair {pair}")
            covered.add(pair)

        lane_id = evidence.get("lane")
        _require(lane_id in registry["lanes"], f"unknown evidence lane {lane_id!r}")
        lane = registry["lanes"][lane_id]
        cell = registry["cells"][cell_id]
        _require(
            lane["acceptance_capable"] is True,
            f"{lane_id} is triage only and cannot authorize acceptance",
        )
        minimum_lane = registry["lanes"][cell["minimum_lane"]]
        _require(
            lane["rank"] >= minimum_lane["rank"],
            f"{cell_id} requires {cell['minimum_lane']} evidence, not {lane_id}",
        )

        profiles = _string_list(
            evidence.get("covered_profiles"),
            f"acceptance evidence {index} covered_profiles",
        )
        _require(
            set(profiles) == set(cell["profiles"]),
            f"acceptance evidence for {cell_id} does not cover every registered profile",
        )
        valid_pairs = _nonnegative_integer(
            evidence.get("valid_pairs"),
            f"acceptance evidence {index} valid_pairs",
        )
        triggered_events = _nonnegative_integer(
            evidence.get("triggered_events"),
            f"acceptance evidence {index} triggered_events",
        )
        _require(
            valid_pairs >= cell["minimum_valid_pairs"],
            f"{cell_id} requires at least {cell['minimum_valid_pairs']} valid pairs",
        )
        _require(
            triggered_events >= cell["minimum_triggered_events"],
            f"{cell_id} requires at least {cell['minimum_triggered_events']} triggered events",
        )

        references = set(
            _string_list(
                evidence.get("comparison_references"),
                f"acceptance evidence {index} comparison_references",
            )
        )
        missing_references = set(cell["required_references"]) - references
        _require(
            not missing_references,
            f"acceptance evidence for {cell_id} omits references: {sorted(missing_references)}",
        )

        artifacts = evidence.get("artifacts")
        _require(
            isinstance(artifacts, list) and bool(artifacts),
            f"acceptance evidence {index} artifacts must be non-empty",
        )
        artifact_kinds: set[str] = set()
        artifact_digests: set[str] = set()
        for artifact_index, artifact_value in enumerate(artifacts):
            artifact = _object(
                artifact_value,
                f"acceptance evidence {index} artifact {artifact_index}",
            )
            kind = artifact.get("kind")
            digest = artifact.get("sha256")
            _require(
                kind in {"raw", "summary", "identity"},
                f"acceptance evidence artifact has unsupported kind {kind!r}",
            )
            _require(
                isinstance(digest, str) and SHA256_RE.fullmatch(digest) is not None,
                "acceptance evidence artifact has an invalid SHA-256",
            )
            _require(
                digest not in artifact_digests,
                "acceptance evidence repeats an artifact digest",
            )
            artifact_kinds.add(kind)
            artifact_digests.add(digest)
        _require(
            {"raw", "summary"}.issubset(artifact_kinds),
            "acceptance evidence must retain raw and generated-summary artifacts",
        )

        completeness = _object(
            evidence.get("completeness"),
            f"acceptance evidence {index} completeness",
        )
        for field in BASE_COMPLETENESS:
            _require(
                completeness.get(field) is True,
                f"acceptance evidence completeness field {field} must be true",
            )
        _require(
            isinstance(completeness.get("definition_dual_run"), bool),
            "acceptance evidence definition_dual_run must be boolean",
        )
        if dual_run_required:
            _require(
                completeness["definition_dual_run"] is True,
                "measurement-contract changes require dual-run evidence",
            )

        statistical = _object(
            evidence.get("statistical_test"),
            f"acceptance evidence {index} statistical_test",
        )
        _require(
            statistical.get("method") == "paired_bootstrap_two_sided_95",
            "acceptance evidence must use paired_bootstrap_two_sided_95",
        )
        _require(
            statistical.get("hypothesis") == "directional_zero_classification",
            "acceptance evidence must use directional_zero_classification",
        )
        _require(
            statistical.get("preregistered") is True,
            "acceptance statistical test must be preregistered",
        )

        result = evidence.get("result")
        _require(
            result in {"pass", "approved_latency_stability_tradeoff"},
            "acceptance result must pass or carry an approved tradeoff",
        )
        if result == "approved_latency_stability_tradeoff":
            record = evidence.get("tradeoff_record")
            _require(
                isinstance(record, str) and bool(record.strip()),
                "approved tradeoff evidence must name its tradeoff record",
            )

    missing = expected - covered
    _require(
        not missing,
        f"acceptance evidence is incomplete for {len(missing)} cell/metric pairs",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=REGISTRY_PATH)
    parser.add_argument("--declaration", type=Path)
    parser.add_argument("--changed-paths-file")
    parser.add_argument(
        "--phase",
        choices=("declaration", "acceptance"),
        default="declaration",
    )
    parser.add_argument("--print-required", action="store_true")
    parser.add_argument("--check-registry", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        registry = load_json(args.registry)
        validate_registry(registry)
        if args.check_registry and not (
            args.declaration or args.changed_paths_file or args.print_required
        ):
            print(
                f"valid performance registry {registry['registry_revision']}: "
                f"{len(registry['cells'])} cells, {len(registry['metrics'])} metrics"
            )
            return 0

        external_paths = (
            read_changed_paths(args.changed_paths_file)
            if args.changed_paths_file is not None
            else None
        )
        if args.print_required:
            _require(
                external_paths is not None,
                "--print-required requires --changed-paths-file",
            )
            print(json.dumps(required_payload(registry, external_paths), indent=2, sort_keys=True))
            return 0

        if args.declaration is None:
            _require(
                external_paths is not None,
                "--declaration or --changed-paths-file is required",
            )
            required = required_payload(registry, external_paths)
            if required["declaration_required"]:
                raise ContractError(
                    "performance declaration required for scopes: "
                    + ", ".join(required["required_scopes"])
                )
            print("no performance declaration required for the supplied paths")
            return 0

        declaration = load_json(args.declaration)
        validated = validate_declaration(
            registry,
            declaration,
            external_changed_paths=external_paths,
            phase=args.phase,
        )
        print(
            f"valid {args.phase} contract for {validated['change_id']} "
            f"({validated['impact_status']}, {len(validated['coverage'])} cells)"
        )
        return 0
    except ContractError as error:
        print(f"performance declaration invalid: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

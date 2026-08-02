#!/usr/bin/env python3
"""Generate and summarize reproducible link-flapping schedules."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path
from typing import Iterable, Iterator


GENERATOR_ID = "sha256-counter-v1"
TRANSITION_MODEL = "baseline-then-selected-condition"
MODE_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")
SUPPORTED_MODES = {
    "apply",
    "apply-lowlat",
    "apply-balanced",
    "apply-mildloss",
    "apply-fat",
    "apply-poor",
    "ideal-lowlat",
    "ideal-balanced",
    "ideal-mildloss",
    "ideal-fat",
    "ideal-poor",
    "ideal-all-lowlat",
    "ideal-all-balanced",
    "ideal-all-fat",
    "ideal-all-poor",
    "blackhole-lowlat",
    "blackhole-balanced",
    "blackhole-fat",
    "blackhole-poor",
    "spike-lowlat",
    "spike-balanced",
    "spike-fat",
    "spike-poor",
    "unconstrained",
    "unconstrained-all",
    "clear",
}
TRACE_INTEGER_FIELDS = (
    "index",
    "planned_offset_seconds",
    "hold_seconds",
    "event_start_offset_ms",
    "client_apply_start_offset_ms",
    "client_apply_end_offset_ms",
    "client_command_exit_code",
    "server_apply_start_offset_ms",
    "server_apply_end_offset_ms",
    "server_command_exit_code",
)


def normalize_modes(raw_modes: str) -> list[str]:
    modes = [mode.strip() for mode in raw_modes.split(",")]
    if not modes or any(not mode for mode in modes):
        raise ValueError("flapping mode list must contain non-empty comma-separated names")
    invalid = [
        mode
        for mode in modes
        if not MODE_PATTERN.fullmatch(mode)
        or (mode not in SUPPORTED_MODES and not re.fullmatch(r"matrix-b[01]{3}", mode))
    ]
    if invalid:
        raise ValueError(f"invalid flapping mode name: {invalid[0]}")
    return modes


def normalize_bounds(min_seconds: int, max_seconds: int) -> tuple[int, int]:
    if min_seconds < 0 or max_seconds < 0:
        raise ValueError("flapping hold bounds must be non-negative")
    normalized_min = max(1, min_seconds)
    return normalized_min, max(normalized_min, max_seconds)


def deterministic_draw(seed: str, index: int, purpose: str) -> int:
    payload = f"{GENERATOR_ID}\0{seed}\0{index}\0{purpose}".encode("utf-8")
    return int.from_bytes(hashlib.sha256(payload).digest()[:8], "big")


def generate_schedule(
    seed: str,
    modes: list[str],
    min_seconds: int,
    max_seconds: int,
    count: int,
) -> Iterator[dict[str, int | str]]:
    if not seed:
        raise ValueError("flapping seed must not be empty")
    if count < 1:
        raise ValueError("flapping schedule count must be positive")
    min_seconds, max_seconds = normalize_bounds(min_seconds, max_seconds)
    planned_offset_seconds = 0
    for index in range(count):
        event = choose_event(seed, modes, min_seconds, max_seconds, index)
        event["planned_offset_seconds"] = planned_offset_seconds
        yield event
        planned_offset_seconds += int(event["hold_seconds"])


def choose_event(
    seed: str,
    modes: list[str],
    min_seconds: int,
    max_seconds: int,
    index: int,
) -> dict[str, int | str]:
    if not seed:
        raise ValueError("flapping seed must not be empty")
    if index < 0:
        raise ValueError("flapping event index must be non-negative")
    min_seconds, max_seconds = normalize_bounds(min_seconds, max_seconds)
    hold_width = max_seconds - min_seconds + 1
    return {
        "index": index,
        "mode": modes[deterministic_draw(seed, index, "mode") % len(modes)],
        "hold_seconds": min_seconds
        + deterministic_draw(seed, index, "hold") % hold_width,
    }


def schedule_digest(events: Iterable[dict[str, object]]) -> str:
    digest = hashlib.sha256()
    for event in events:
        digest.update(
            (
                f"{int(event['index'])}\t{event['mode']}\t"
                f"{int(event['hold_seconds'])}\t"
                f"{int(event['planned_offset_seconds'])}\n"
            ).encode("utf-8")
        )
    return digest.hexdigest()


def trace_digest(events: Iterable[dict[str, object]]) -> str:
    digest = hashlib.sha256()
    for event in events:
        digest.update(
            json.dumps(event, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def read_trace(path: Path) -> tuple[list[dict[str, object]], bool, str | None]:
    events: list[dict[str, object]] = []
    if not path.exists():
        return events, False, "trace artifact does not exist"
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            return events, False, f"invalid trace row {line_number}: {exc.msg}"
        if not isinstance(event, dict):
            return events, False, f"invalid trace row {line_number}: expected object"
        if not isinstance(event.get("mode"), str):
            return events, False, f"invalid trace row {line_number}: missing mode"
        try:
            normalize_modes(str(event["mode"]))
            for field in TRACE_INTEGER_FIELDS:
                value = event[field]
                if isinstance(value, bool) or int(value) != value:
                    raise ValueError(field)
        except (KeyError, TypeError, ValueError) as exc:
            return events, False, f"invalid trace row {line_number}: {exc}"
        timing = [
            int(event[field])
            for field in (
                "event_start_offset_ms",
                "client_apply_start_offset_ms",
                "client_apply_end_offset_ms",
                "server_apply_start_offset_ms",
                "server_apply_end_offset_ms",
            )
        ]
        if any(value < 0 for value in timing) or timing != sorted(timing):
            return events, False, f"invalid trace row {line_number}: non-monotonic timing"
        if events and int(event["index"]) != int(events[-1]["index"]) + 1:
            return events, False, f"invalid trace row {line_number}: non-contiguous index"
        events.append(event)
    return events, True, None


def build_metadata(
    *,
    seed: str,
    seed_source: str,
    raw_modes: str,
    min_seconds: int,
    max_seconds: int,
    trace_path: Path,
    probe_started_unix_seconds: str | None = None,
    schedule_origin_unix_ms: str | None = None,
    schedule_origin_monotonic_ms: str | None = None,
    stop_requested_offset_ms: int | None = None,
    worker_exit_code: int | None = None,
    restore_exit_code: int | None = None,
) -> dict[str, object]:
    modes = normalize_modes(raw_modes)
    min_seconds, max_seconds = normalize_bounds(min_seconds, max_seconds)
    applied, trace_parse_complete, trace_error = read_trace(trace_path)
    expected = list(
        generate_schedule(seed, modes, min_seconds, max_seconds, max(1, len(applied)))
    )
    applied_matches_plan = bool(applied) and all(
        int(event.get("index", -1)) == int(planned["index"])
        and event.get("mode") == planned["mode"]
        and event.get("hold_seconds") == planned["hold_seconds"]
        and event.get("planned_offset_seconds")
        == planned["planned_offset_seconds"]
        for event, planned in zip(applied, expected)
    ) and len(applied) <= len(expected)
    dwell_timing_valid = all(
        int(following["event_start_offset_ms"])
        - int(current["server_apply_end_offset_ms"])
        >= int(current["hold_seconds"]) * 1000 - 100
        for current, following in zip(applied, applied[1:])
    )
    profile = {
        "generator": GENERATOR_ID,
        "transition_model": TRANSITION_MODEL,
        "configured_modes": modes,
        "min_hold_seconds": min_seconds,
        "max_hold_seconds": max_seconds,
    }
    schedule_profile_sha256 = hashlib.sha256(
        json.dumps(profile, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()
    schedule_id_sha256 = hashlib.sha256(
        json.dumps(
            {
                "generator": GENERATOR_ID,
                "schedule_profile_sha256": schedule_profile_sha256,
                "seed": seed,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()
    metadata: dict[str, object] = {
        "schema_version": 1,
        "generator": GENERATOR_ID,
        "transition_model": TRANSITION_MODEL,
        "schedule_profile_sha256": schedule_profile_sha256,
        "schedule_id_sha256": schedule_id_sha256,
        "seed": seed,
        "seed_source": seed_source,
        "configured_modes": modes,
        "min_hold_seconds": min_seconds,
        "max_hold_seconds": max_seconds,
        "applied_event_count": len(applied),
        "applied_schedule_sha256": schedule_digest(applied),
        "applied_trace_sha256": trace_digest(applied),
        "applied_schedule_matches_plan": applied_matches_plan,
        "completed_dwell_count": max(0, len(applied) - 1),
        "completed_dwell_timing_valid": dwell_timing_valid,
        "command_failure_count": sum(
            int(event.get("client_command_exit_code", 1) != 0)
            + int(event.get("server_command_exit_code", 1) != 0)
            for event in applied
        ),
        "trace_artifact": str(trace_path),
    }
    metadata["trace_parse_complete"] = trace_parse_complete
    if trace_error is not None:
        metadata["trace_error"] = trace_error
    if probe_started_unix_seconds is not None:
        metadata["probe_started_unix_seconds"] = probe_started_unix_seconds
    if schedule_origin_unix_ms is not None:
        metadata["schedule_origin_unix_ms"] = schedule_origin_unix_ms
    if schedule_origin_monotonic_ms is not None:
        metadata["schedule_origin_monotonic_ms"] = schedule_origin_monotonic_ms
    if applied:
        metadata["first_event_start_offset_ms"] = applied[0]["event_start_offset_ms"]
    if stop_requested_offset_ms is not None:
        metadata["stop_requested_offset_ms"] = stop_requested_offset_ms
    if worker_exit_code is not None:
        metadata["worker_exit_code"] = worker_exit_code
    if restore_exit_code is not None:
        metadata["restore_exit_code"] = restore_exit_code
    metadata["trace_complete"] = (
        trace_parse_complete
        and bool(applied)
        and applied_matches_plan
        and dwell_timing_valid
        and metadata["command_failure_count"] == 0
        and worker_exit_code == 0
        and restore_exit_code == 0
    )
    return metadata


def attach_metadata_to_result(
    row: dict[str, object], metadata: dict[str, object]
) -> None:
    if not metadata:
        return
    row["flapping"] = metadata
    if not metadata.get("trace_complete", False):
        row["probe_status_before_flapping_validation"] = row.get("status")
        row["status"] = "fail"
        row["flapping_failure_reason"] = "flapping trace is incomplete or invalid"
        row.setdefault("failure_reason", row["flapping_failure_reason"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate")
    generate.add_argument("--seed", required=True)
    generate.add_argument("--modes", required=True)
    generate.add_argument("--min-seconds", required=True, type=int)
    generate.add_argument("--max-seconds", required=True, type=int)
    generate.add_argument("--count", type=int, default=4096)

    choose = subparsers.add_parser("choose")
    choose.add_argument("--seed", required=True)
    choose.add_argument("--modes", required=True)
    choose.add_argument("--min-seconds", required=True, type=int)
    choose.add_argument("--max-seconds", required=True, type=int)
    choose.add_argument("--index", required=True, type=int)

    metadata = subparsers.add_parser("metadata")
    metadata.add_argument("--seed", required=True)
    metadata.add_argument("--seed-source", required=True)
    metadata.add_argument("--modes", required=True)
    metadata.add_argument("--min-seconds", required=True, type=int)
    metadata.add_argument("--max-seconds", required=True, type=int)
    metadata.add_argument("--trace", required=True, type=Path)
    metadata.add_argument("--probe-started-unix-seconds")
    metadata.add_argument("--schedule-origin-unix-ms")
    metadata.add_argument("--schedule-origin-monotonic-ms")
    metadata.add_argument("--stop-requested-offset-ms", type=int)
    metadata.add_argument("--worker-exit-code", type=int)
    metadata.add_argument("--restore-exit-code", type=int)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "generate":
            modes = normalize_modes(args.modes)
            for event in generate_schedule(
                args.seed,
                modes,
                args.min_seconds,
                args.max_seconds,
                args.count,
            ):
                print(
                    f"{event['index']}\t{event['mode']}\t{event['hold_seconds']}\t"
                    f"{event['planned_offset_seconds']}"
                )
            return 0
        if args.command == "choose":
            event = choose_event(
                args.seed,
                normalize_modes(args.modes),
                args.min_seconds,
                args.max_seconds,
                args.index,
            )
            print(f"{event['index']}\t{event['mode']}\t{event['hold_seconds']}")
            return 0
        metadata = build_metadata(
            seed=args.seed,
            seed_source=args.seed_source,
            raw_modes=args.modes,
            min_seconds=args.min_seconds,
            max_seconds=args.max_seconds,
            trace_path=args.trace,
            probe_started_unix_seconds=args.probe_started_unix_seconds,
            schedule_origin_unix_ms=args.schedule_origin_unix_ms,
            schedule_origin_monotonic_ms=args.schedule_origin_monotonic_ms,
            stop_requested_offset_ms=args.stop_requested_offset_ms,
            worker_exit_code=args.worker_exit_code,
            restore_exit_code=args.restore_exit_code,
        )
        print(json.dumps(metadata, separators=(",", ":"), sort_keys=True))
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise SystemExit(str(exc)) from exc


if __name__ == "__main__":
    raise SystemExit(main())

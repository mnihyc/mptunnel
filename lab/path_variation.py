#!/usr/bin/env python3
"""Generate and validate reproducible 20-link condition schedules."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


GENERATOR_ID = "sha256-condition-permutation-v1"
PATHS = (
    ("tcp", "172.31.10"),
    ("tcp", "172.31.15"),
    ("tcp", "172.31.16"),
    ("tcp", "172.31.20"),
    ("tcp", "172.31.30"),
    ("tcp", "172.31.41"),
    ("tcp", "172.31.42"),
    ("tcp", "172.31.43"),
    ("tcp", "172.31.44"),
    ("tcp", "172.31.45"),
    ("udp", "172.31.51"),
    ("udp", "172.31.52"),
    ("udp", "172.31.53"),
    ("udp", "172.31.54"),
    ("udp", "172.31.55"),
    ("udp", "172.31.56"),
    ("udp", "172.31.57"),
    ("udp", "172.31.58"),
    ("udp", "172.31.59"),
    ("udp", "172.31.60"),
)
BASE_RATES_MBPS = (30, 35, 40, 45, 50, 60, 70, 80, 90, 100)
RATE_BANDS = {
    "access": BASE_RATES_MBPS,
    "gigabit": tuple(rate * 10 for rate in BASE_RATES_MBPS),
    "multi-gigabit": tuple(rate * 100 for rate in BASE_RATES_MBPS),
}
QUALITY_PROFILES = (
    # delay_ms, jitter_ms, loss_percent
    (10, 0, 0.0),
    (20, 1, 0.0),
    (30, 2, 0.01),
    (40, 3, 0.05),
    (60, 5, 0.1),
    (80, 8, 0.2),
    (100, 10, 0.5),
    (140, 15, 1.0),
    (250, 60, 5.0),
    (420, 120, 10.0),
)


def _ranking(
    seed: str,
    epoch: int,
    direction: str,
    transport: str,
    purpose: str,
    indices: list[int],
) -> list[int]:
    if not seed:
        raise ValueError("variation seed must not be empty")
    if epoch < 0:
        raise ValueError("variation epoch must be non-negative")
    if direction not in {"client", "server"}:
        raise ValueError("variation direction must be client or server")

    def digest(index: int) -> bytes:
        payload = (
            f"{GENERATOR_ID}\0{seed}\0{epoch}\0{direction}\0{transport}\0{purpose}\0{index}"
        ).encode("utf-8")
        return hashlib.sha256(payload).digest()

    return sorted(indices, key=lambda index: (digest(index), index))


def profiles(
    seed: str, epoch: int, direction: str, rate_band: str
) -> list[dict[str, object]]:
    try:
        band_rates = RATE_BANDS[rate_band]
    except KeyError as exc:
        raise ValueError(f"unknown rate band {rate_band!r}") from exc
    rates = [0] * len(PATHS)
    qualities: list[tuple[int, int, float] | None] = [None] * len(PATHS)
    for transport in ("tcp", "udp"):
        indices = [
            index
            for index, (path_transport, _) in enumerate(PATHS)
            if path_transport == transport
        ]
        for rate, index in zip(
            band_rates,
            _ranking(seed, epoch, direction, transport, "rate", indices),
            strict=True,
        ):
            rates[index] = rate
        for quality, index in zip(
            QUALITY_PROFILES,
            _ranking(seed, epoch, direction, transport, "quality", indices),
            strict=True,
        ):
            qualities[index] = quality
    return [
        {
            "path_index": index + 1,
            "transport": transport,
            "subnet_prefix": prefix,
            "rate_mbps": rates[index],
            "delay_ms": qualities[index][0],
            "jitter_ms": qualities[index][1],
            "loss_percent": qualities[index][2],
        }
        for index, (transport, prefix) in enumerate(PATHS)
    ]


def _profile_error(value: object, rate_band: str) -> str | None:
    if not isinstance(value, list) or len(value) != len(PATHS):
        return "each direction must contain exactly 20 profiles"
    expected_prefixes = {prefix for _, prefix in PATHS}
    prefixes = {
        row.get("subnet_prefix") for row in value if isinstance(row, dict)
    }
    if prefixes != expected_prefixes:
        return "profile subnet inventory does not match the 20-link topology"
    transports = [row.get("transport") for row in value if isinstance(row, dict)]
    if transports.count("tcp") != 10 or transports.count("udp") != 10:
        return "profile must contain exactly 10 TCP and 10 UDP links"
    expected_rates = sorted(RATE_BANDS[rate_band])
    expected_qualities = sorted(QUALITY_PROFILES)
    for transport in ("tcp", "udp"):
        observed_rates = sorted(
            row.get("rate_mbps")
            for row in value
            if isinstance(row, dict) and row.get("transport") == transport
        )
        observed_qualities = sorted(
            (
                row.get("delay_ms"),
                row.get("jitter_ms"),
                row.get("loss_percent"),
            )
            for row in value
            if isinstance(row, dict) and row.get("transport") == transport
        )
        if observed_rates != expected_rates:
            return f"{transport} profiles do not match the {rate_band} rate band"
        if observed_qualities != expected_qualities:
            return f"{transport} profiles do not match the declared quality set"
    return None


def trace_metadata(
    path: Path, seed: str, rate_band: str, expected_epochs: int | None = None
) -> dict[str, object]:
    band_rates = RATE_BANDS[rate_band]
    rows: list[dict[str, object]] = []
    error: str | None = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        lines = []
        error = str(exc)
    for line_number, line in enumerate(lines, 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            error = f"invalid trace row {line_number}: {exc.msg}"
            break
        if not isinstance(row, dict):
            error = f"invalid trace row {line_number}: expected object"
            break
        rows.append(row)

    expected_epoch = 0
    previous_event_start = -1
    highest_rate_paths: dict[str, dict[str, list[str]]] = {
        direction: {transport: [] for transport in ("tcp", "udp")}
        for direction in ("client", "server")
    }
    if error is None:
        for row in rows:
            try:
                epoch = int(row["epoch"])
                if epoch != expected_epoch:
                    raise ValueError("non-contiguous epoch")
                expected_epoch += 1
                if bool(row["preconditioned"]) != (epoch == 0):
                    raise ValueError("only the initial epoch may be preconditioned")
                if int(row["client_exit_code"]) != 0 or int(row["server_exit_code"]) != 0:
                    raise ValueError("condition application failed")
                event_start = int(row["event_start_offset_ms"])
                client_start = int(row["client_apply_start_offset_ms"])
                client_end = int(row["client_apply_end_offset_ms"])
                server_start = int(row["server_apply_start_offset_ms"])
                server_end = int(row["server_apply_end_offset_ms"])
                if (
                    event_start < previous_event_start
                    or client_start < event_start
                    or client_end < client_start
                    or server_start < event_start
                    or server_end < server_start
                ):
                    raise ValueError("non-monotonic application timing")
                previous_event_start = event_start
                for direction in ("client", "server"):
                    direction_profiles = row[f"{direction}_profiles"]
                    profile_error = _profile_error(direction_profiles, rate_band)
                    if profile_error:
                        raise ValueError(profile_error)
                    expected = profiles(seed, epoch, direction, rate_band)
                    if direction_profiles != expected:
                        raise ValueError(
                            f"{direction} profiles do not match the seeded schedule"
                        )
                    for transport in ("tcp", "udp"):
                        highest_rate = max(
                            (
                                item
                                for item in direction_profiles
                                if item["transport"] == transport
                            ),
                            key=lambda item: item["rate_mbps"],
                        )
                        highest_rate_paths[direction][transport].append(
                            str(highest_rate["subnet_prefix"])
                        )
            except (KeyError, TypeError, ValueError) as exc:
                error = f"invalid epoch {expected_epoch - 1}: {exc}"
                break

    digest = hashlib.sha256()
    for row in rows:
        digest.update(
            json.dumps(row, separators=(",", ":"), sort_keys=True).encode("utf-8")
        )
        digest.update(b"\n")
    highest_rate_path_change_count = {
        direction: {
            transport: sum(
                left != right for left, right in zip(paths, paths[1:])
            )
            for transport, paths in by_transport.items()
        }
        for direction, by_transport in highest_rate_paths.items()
    }
    complete = (
        error is None
        and bool(rows)
        and (expected_epochs is None or len(rows) == expected_epochs)
        and all(
            count > 0
            for by_transport in highest_rate_path_change_count.values()
            for count in by_transport.values()
        )
    )
    return {
        "schema_version": 2,
        "generator": GENERATOR_ID,
        "seed": seed,
        "rate_band": rate_band,
        "epoch_count": len(rows),
        "expected_epoch_count": expected_epochs,
        "tcp_link_count": 10,
        "udp_link_count": 10,
        "aggregate_rate_mbps_per_direction": 2 * sum(band_rates),
        "minimum_link_rate_mbps": min(band_rates),
        "maximum_link_rate_mbps": max(band_rates),
        "highest_rate_path_by_epoch": highest_rate_paths,
        "highest_rate_path_change_count": highest_rate_path_change_count,
        "trace_sha256": digest.hexdigest(),
        "trace_artifact": str(path),
        "trace_complete": complete,
        **({"trace_error": error} if error else {}),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    render = subparsers.add_parser("profiles")
    render.add_argument("--seed", required=True)
    render.add_argument("--epoch", required=True, type=int)
    render.add_argument("--direction", required=True, choices=("client", "server"))
    render.add_argument("--rate-band", required=True, choices=tuple(RATE_BANDS))
    render.add_argument("--format", choices=("json", "tsv"), default="json")
    metadata = subparsers.add_parser("metadata")
    metadata.add_argument("--seed", required=True)
    metadata.add_argument("--rate-band", required=True, choices=tuple(RATE_BANDS))
    metadata.add_argument("--trace", required=True, type=Path)
    metadata.add_argument("--expected-epochs", type=int)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "profiles":
        generated = profiles(args.seed, args.epoch, args.direction, args.rate_band)
        if args.format == "json":
            print(json.dumps(generated, separators=(",", ":"), sort_keys=True))
        else:
            for row in generated:
                print(
                    f"{row['subnet_prefix']}\t{row['rate_mbps']}mbit\t"
                    f"{row['delay_ms']}ms\t{row['jitter_ms']}ms\t"
                    f"{row['loss_percent']}%"
                )
        return 0
    metadata = trace_metadata(
        args.trace, args.seed, args.rate_band, args.expected_epochs
    )
    print(json.dumps(metadata, separators=(",", ":"), sort_keys=True))
    return 0 if metadata["trace_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

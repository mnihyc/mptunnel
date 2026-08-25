#!/usr/bin/env python3
"""Generate canonical, replayable random Internet-condition schedules.

The generator deliberately does not use :mod:`random`: every draw is addressed by
``(generator, model, seed, epoch, direction, path, purpose)`` and comes from
SHA-256.  Consequently a schedule is stable across Python processes, hash seeds,
machines, and protocol implementations.  A comparison between MPTUNNEL, Xray,
Hysteria2, and MPTCP is valid only when every subject records the same
``schedule_sha256`` and applies the same rows at the same epochs.

Canonical schedule JSON has this version-1 shape::

    {
      "schema_version": 1,
      "generator": "sha256-stratified-internet-v1",
      "model": "representative-five-strata-v1",
      "application_scope": "protocol-neutral-network-conditions",
      "seed": "...",
      "topology": "five-path",
      "epoch_count": 2,
      "directions": ["client", "server"],
      "path_inventory": ["172.31.10", ...],
      "include_outages": false,
      "tsv_columns": ["subnet_prefix", ...],
      "rows": [ROW, ...],
      "schedule_sha256": "..."
    }

Each ROW contains ``epoch``, ``direction``, ``path_index``, ``subnet_prefix``,
``stratum``, the traffic-control fields ``rate``, ``delay``, ``jitter``,
``delay_correlation``, ``loss``, ``loss_correlation``, ``reorder``,
``reorder_correlation``, ``duplicate``, ``corrupt``, an independent non-zero
uint32 ``netem_seed``, and boolean ``outage``.  Rate, time, and probability
values are already legal ``tc netem`` tokens (``kbit``, ``ms``, and ``%``).
Correlation is zero whenever its associated random component is zero; jitter
never exceeds delay; reorder is only emitted with positive delay.  An outage is
represented by exactly ``loss=100%`` and zero loss correlation.

Five strata (fiber, fixed wireless, mobile, congested, and satellite) are
assigned as a seeded permutation across the five paths in every epoch and
direction.  Enabling outages chooses exactly one seeded path-direction row per
epoch, modeling a local path outage without blackholing the entire topology.

``schedule_sha256`` is SHA-256 over canonical UTF-8 JSON of the complete object
with only that field omitted.  Canonical JSON uses sorted keys, no insignificant
whitespace, ASCII escapes, integer/boolean scalars, and no floating-point values.
Validation checks both the digest and exact regeneration from the declared seed.

The shell renderer is intentionally headerless by default.  Its columns are::

    subnet_prefix  rate  delay  jitter  delay_correlation  loss
    loss_correlation  reorder  reorder_correlation  duplicate  corrupt
    netem_seed  outage

Run ``render --header`` when a human-readable header is desired.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path
from typing import Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
GENERATOR_ID = "sha256-stratified-internet-v1"
MODEL_ID = "representative-five-strata-v1"
APPLICATION_SCOPE = "protocol-neutral-network-conditions"
DIRECTIONS = ("client", "server")
MAX_EPOCHS = 100_000
UINT32_MAX = (1 << 32) - 1

TOPOLOGIES: dict[str, tuple[str, ...]] = {
    "five-path": (
        "172.31.10",
        "172.31.15",
        "172.31.16",
        "172.31.20",
        "172.31.30",
    ),
}

# Bounds use exact integer units: kbit/s, milliseconds, and probability parts
# per million.  One probability ppm renders as 0.0001%, while 1_000_000 ppm is
# 100%.  Ranges represent access technologies and stressed-but-observed Internet
# behavior rather than arbitrary fuzzing values.
STRATA: tuple[dict[str, object], ...] = (
    {
        "name": "fiber",
        "rate_kbit": (80_000, 1_000_000),
        "delay_ms": (2, 25),
        "jitter_ms": (0, 4),
        "delay_correlation_ppm": (0, 150_000),
        "loss_ppm": (0, 500),
        "loss_correlation_ppm": (0, 200_000),
        "reorder_ppm": (0, 100),
        "reorder_correlation_ppm": (0, 150_000),
        "duplicate_ppm": (0, 50),
        "corrupt_ppm": (0, 10),
    },
    {
        "name": "fixed-wireless",
        "rate_kbit": (10_000, 300_000),
        "delay_ms": (8, 60),
        "jitter_ms": (1, 20),
        "delay_correlation_ppm": (50_000, 350_000),
        "loss_ppm": (100, 8_000),
        "loss_correlation_ppm": (50_000, 450_000),
        "reorder_ppm": (10, 3_000),
        "reorder_correlation_ppm": (20_000, 400_000),
        "duplicate_ppm": (5, 1_000),
        "corrupt_ppm": (1, 100),
    },
    {
        "name": "mobile",
        "rate_kbit": (1_500, 150_000),
        "delay_ms": (20, 150),
        "jitter_ms": (4, 60),
        "delay_correlation_ppm": (200_000, 750_000),
        "loss_ppm": (500, 30_000),
        "loss_correlation_ppm": (250_000, 800_000),
        "reorder_ppm": (50, 20_000),
        "reorder_correlation_ppm": (150_000, 750_000),
        "duplicate_ppm": (10, 5_000),
        "corrupt_ppm": (1, 500),
    },
    {
        "name": "congested",
        "rate_kbit": (256, 25_000),
        "delay_ms": (70, 450),
        "jitter_ms": (15, 200),
        "delay_correlation_ppm": (400_000, 900_000),
        "loss_ppm": (5_000, 100_000),
        "loss_correlation_ppm": (450_000, 950_000),
        "reorder_ppm": (1_000, 50_000),
        "reorder_correlation_ppm": (350_000, 900_000),
        "duplicate_ppm": (200, 20_000),
        "corrupt_ppm": (50, 2_000),
    },
    {
        "name": "satellite",
        "rate_kbit": (2_000, 100_000),
        "delay_ms": (250, 700),
        "jitter_ms": (5, 100),
        "delay_correlation_ppm": (100_000, 600_000),
        "loss_ppm": (500, 50_000),
        "loss_correlation_ppm": (150_000, 700_000),
        "reorder_ppm": (10, 10_000),
        "reorder_correlation_ppm": (100_000, 600_000),
        "duplicate_ppm": (10, 5_000),
        "corrupt_ppm": (1, 500),
    },
)
STRATA_BY_NAME = {str(item["name"]): item for item in STRATA}

TSV_COLUMNS = (
    "subnet_prefix",
    "rate",
    "delay",
    "jitter",
    "delay_correlation",
    "loss",
    "loss_correlation",
    "reorder",
    "reorder_correlation",
    "duplicate",
    "corrupt",
    "netem_seed",
    "outage",
)
ROW_FIELDS = {
    "epoch",
    "direction",
    "path_index",
    "subnet_prefix",
    "stratum",
    *TSV_COLUMNS[1:],
}
ROOT_FIELDS = {
    "schema_version",
    "generator",
    "model",
    "application_scope",
    "seed",
    "topology",
    "epoch_count",
    "directions",
    "path_inventory",
    "include_outages",
    "tsv_columns",
    "rows",
    "schedule_sha256",
}

RATE_TOKEN = re.compile(r"^[1-9][0-9]*kbit$")
TIME_TOKEN = re.compile(r"^(?:0|[1-9][0-9]*)ms$")
PERCENT_TOKEN = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,4})?%$")


def canonical_json(value: object) -> str:
    """Return the single canonical JSON representation used for identities."""

    return json.dumps(
        value,
        ensure_ascii=True,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _require_integer(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _validate_generation_inputs(seed: str, epoch: int, direction: str, topology: str) -> None:
    if not isinstance(seed, str) or not seed:
        raise ValueError("condition seed must not be empty")
    if "\x00" in seed:
        raise ValueError("condition seed must not contain NUL")
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch < 0:
        raise ValueError("condition epoch must be a non-negative integer")
    if epoch >= MAX_EPOCHS:
        raise ValueError(f"condition epoch must be less than {MAX_EPOCHS}")
    if direction not in DIRECTIONS:
        raise ValueError(f"condition direction must be one of {', '.join(DIRECTIONS)}")
    if topology not in TOPOLOGIES:
        raise ValueError(f"unknown condition topology {topology!r}")


def deterministic_draw(seed: str, *coordinates: object) -> int:
    """Return an addressable 256-bit deterministic draw."""

    if not isinstance(seed, str) or not seed:
        raise ValueError("condition seed must not be empty")
    payload = [GENERATOR_ID, MODEL_ID, seed, *coordinates]
    return int.from_bytes(
        hashlib.sha256(canonical_json(payload).encode("utf-8")).digest(), "big"
    )


def _draw_between(
    seed: str,
    low: int,
    high: int,
    *coordinates: object,
) -> int:
    if low > high:
        raise ValueError("invalid deterministic draw bounds")
    return low + deterministic_draw(seed, *coordinates) % (high - low + 1)


def _bounds(stratum: Mapping[str, object], field: str) -> tuple[int, int]:
    value = stratum[field]
    if (
        not isinstance(value, tuple)
        or len(value) != 2
        or any(isinstance(item, bool) or not isinstance(item, int) for item in value)
    ):
        raise RuntimeError(f"invalid model bounds for {field}")
    return value


def _percent_token(ppm: int) -> str:
    if not 0 <= ppm <= 1_000_000:
        raise ValueError("probability ppm must be between zero and one million")
    whole, fractional = divmod(ppm, 10_000)
    if fractional == 0:
        return f"{whole}%"
    return f"{whole}.{fractional:04d}".rstrip("0") + "%"


def _percent_ppm(token: object, field: str) -> int:
    if not isinstance(token, str) or PERCENT_TOKEN.fullmatch(token) is None:
        raise ValueError(f"{field} is not a legal tc percentage token")
    number = token[:-1]
    whole_text, separator, fractional_text = number.partition(".")
    fractional_text = (fractional_text + "0000")[:4] if separator else "0000"
    ppm = int(whole_text) * 10_000 + int(fractional_text)
    if ppm > 1_000_000 or _percent_token(ppm) != token:
        raise ValueError(f"{field} is not a canonical tc percentage token")
    return ppm


def _rate_kbit(token: object) -> int:
    if not isinstance(token, str) or RATE_TOKEN.fullmatch(token) is None:
        raise ValueError("rate is not a legal positive tc kbit token")
    return int(token[:-4])


def _time_ms(token: object, field: str) -> int:
    if not isinstance(token, str) or TIME_TOKEN.fullmatch(token) is None:
        raise ValueError(f"{field} is not a legal tc millisecond token")
    return int(token[:-2])


def _ranked_path_indices(
    seed: str, epoch: int, direction: str, topology: str
) -> list[int]:
    prefixes = TOPOLOGIES[topology]
    return sorted(
        range(len(prefixes)),
        key=lambda index: (
            deterministic_draw(
                seed,
                topology,
                epoch,
                direction,
                prefixes[index],
                "stratum-ranking",
            ),
            index,
        ),
    )


def _stratum_by_path(
    seed: str, epoch: int, direction: str, topology: str
) -> dict[int, Mapping[str, object]]:
    prefixes = TOPOLOGIES[topology]
    if len(prefixes) != len(STRATA):
        raise ValueError(
            f"topology {topology!r} must have exactly {len(STRATA)} paths for this model"
        )
    return {
        path_index: STRATA[rank]
        for rank, path_index in enumerate(
            _ranked_path_indices(seed, epoch, direction, topology)
        )
    }


def _outage_coordinate(seed: str, epoch: int, topology: str) -> tuple[str, int]:
    prefixes = TOPOLOGIES[topology]
    return min(
        (
            (direction, path_index)
            for direction in DIRECTIONS
            for path_index in range(len(prefixes))
        ),
        key=lambda coordinate: (
            deterministic_draw(
                seed,
                topology,
                epoch,
                coordinate[0],
                prefixes[coordinate[1]],
                "outage-ranking",
            ),
            DIRECTIONS.index(coordinate[0]),
            coordinate[1],
        ),
    )


def _netem_seed(
    seed: str,
    epoch: int,
    direction: str,
    path_offset: int,
    topology: str,
) -> int:
    prefixes = TOPOLOGIES[topology]
    ordinal = (
        epoch * len(DIRECTIONS) * len(prefixes)
        + DIRECTIONS.index(direction) * len(prefixes)
        + path_offset
    )

    def permute(value: int) -> int:
        # Six-round keyed Feistel gives a deterministic permutation of uint32.
        # It avoids both hash truncation collisions and the visible relationship
        # between adjacent seeds that a simpler affine permutation would have.
        left, right = value >> 16, value & 0xFFFF
        for round_index in range(6):
            round_value = deterministic_draw(
                seed,
                topology,
                "packet-rng-feistel",
                round_index,
                right,
            ) & 0xFFFF
            left, right = right, left ^ round_value
        return (left << 16) | right

    # Cycle-walk the permutation over the non-zero uint32 domain.  Since the
    # supported coordinate ordinals are distinct and below uint32, the results
    # are also distinct, while zero can never leak to tc.
    candidate = ordinal + 1
    while True:
        candidate = permute(candidate)
        if candidate != 0:
            return candidate


def render_rows(
    seed: str,
    epoch: int,
    direction: str,
    topology: str = "five-path",
    include_outages: bool = False,
) -> list[dict[str, object]]:
    """Render every path row for one seed-addressable epoch and direction."""

    _validate_generation_inputs(seed, epoch, direction, topology)
    if not isinstance(include_outages, bool):
        raise ValueError("include_outages must be boolean")
    prefixes = TOPOLOGIES[topology]
    strata = _stratum_by_path(seed, epoch, direction, topology)
    outage_coordinate = (
        _outage_coordinate(seed, epoch, topology) if include_outages else None
    )
    rows: list[dict[str, object]] = []
    for path_offset, prefix in enumerate(prefixes):
        stratum = strata[path_offset]
        coordinate = (topology, epoch, direction, prefix)

        def draw(field: str) -> int:
            low, high = _bounds(stratum, field)
            return _draw_between(seed, low, high, *coordinate, field)

        rate_kbit = draw("rate_kbit")
        delay_ms = draw("delay_ms")
        jitter_low, jitter_high = _bounds(stratum, "jitter_ms")
        jitter_ms = _draw_between(
            seed,
            jitter_low,
            min(jitter_high, delay_ms),
            *coordinate,
            "jitter_ms",
        )
        delay_correlation_ppm = (
            draw("delay_correlation_ppm") if jitter_ms else 0
        )
        loss_ppm = draw("loss_ppm")
        loss_correlation_ppm = draw("loss_correlation_ppm") if loss_ppm else 0
        reorder_ppm = draw("reorder_ppm")
        reorder_correlation_ppm = (
            draw("reorder_correlation_ppm") if reorder_ppm else 0
        )
        duplicate_ppm = draw("duplicate_ppm")
        corrupt_ppm = draw("corrupt_ppm")
        outage = outage_coordinate == (direction, path_offset)
        if outage:
            loss_ppm = 1_000_000
            loss_correlation_ppm = 0
        netem_seed = _netem_seed(
            seed, epoch, direction, path_offset, topology
        )
        rows.append(
            {
                "epoch": epoch,
                "direction": direction,
                "path_index": path_offset + 1,
                "subnet_prefix": prefix,
                "stratum": str(stratum["name"]),
                "rate": f"{rate_kbit}kbit",
                "delay": f"{delay_ms}ms",
                "jitter": f"{jitter_ms}ms",
                "delay_correlation": _percent_token(delay_correlation_ppm),
                "loss": _percent_token(loss_ppm),
                "loss_correlation": _percent_token(loss_correlation_ppm),
                "reorder": _percent_token(reorder_ppm),
                "reorder_correlation": _percent_token(
                    reorder_correlation_ppm
                ),
                "duplicate": _percent_token(duplicate_ppm),
                "corrupt": _percent_token(corrupt_ppm),
                "netem_seed": netem_seed,
                "outage": outage,
            }
        )
    return rows


def _schedule_payload(schedule: Mapping[str, object]) -> dict[str, object]:
    return {
        key: value for key, value in schedule.items() if key != "schedule_sha256"
    }


def schedule_sha256(schedule: Mapping[str, object]) -> str:
    """Calculate a schedule's semantic identity, excluding its identity field."""

    return _canonical_sha256(_schedule_payload(schedule))


def build_schedule(
    seed: str,
    epoch_count: int,
    topology: str = "five-path",
    include_outages: bool = False,
) -> dict[str, object]:
    """Build a complete canonical schedule document."""

    if isinstance(epoch_count, bool) or not isinstance(epoch_count, int):
        raise ValueError("epoch count must be an integer")
    if not 1 <= epoch_count <= MAX_EPOCHS:
        raise ValueError(f"epoch count must be between 1 and {MAX_EPOCHS}")
    _validate_generation_inputs(seed, 0, DIRECTIONS[0], topology)
    if not isinstance(include_outages, bool):
        raise ValueError("include_outages must be boolean")
    rows = [
        row
        for epoch in range(epoch_count)
        for direction in DIRECTIONS
        for row in render_rows(
            seed, epoch, direction, topology, include_outages
        )
    ]
    schedule: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "generator": GENERATOR_ID,
        "model": MODEL_ID,
        "application_scope": APPLICATION_SCOPE,
        "seed": seed,
        "topology": topology,
        "epoch_count": epoch_count,
        "directions": list(DIRECTIONS),
        "path_inventory": list(TOPOLOGIES[topology]),
        "include_outages": include_outages,
        "tsv_columns": list(TSV_COLUMNS),
        "rows": rows,
    }
    schedule["schedule_sha256"] = schedule_sha256(schedule)
    return schedule


def _validate_row(
    row: object,
    expected: Mapping[str, object],
    context: str,
) -> None:
    if not isinstance(row, dict):
        raise ValueError(f"{context} must be an object")
    if set(row) != ROW_FIELDS:
        raise ValueError(f"{context} fields do not match schema")
    if row != expected:
        raise ValueError(f"{context} does not match seeded replay")
    epoch = _require_integer(row["epoch"], f"{context} epoch")
    path_index = _require_integer(row["path_index"], f"{context} path_index")
    if epoch < 0 or path_index < 1:
        raise ValueError(f"{context} has an invalid coordinate")
    if row["direction"] not in DIRECTIONS:
        raise ValueError(f"{context} has an invalid direction")
    stratum_name = row["stratum"]
    if not isinstance(stratum_name, str) or stratum_name not in STRATA_BY_NAME:
        raise ValueError(f"{context} has an invalid stratum")
    if not isinstance(row["outage"], bool):
        raise ValueError(f"{context} outage must be boolean")
    seed = _require_integer(row["netem_seed"], f"{context} netem_seed")
    if not 1 <= seed <= UINT32_MAX:
        raise ValueError(f"{context} netem_seed is outside uint32")
    rate = _rate_kbit(row["rate"])
    delay = _time_ms(row["delay"], f"{context} delay")
    jitter = _time_ms(row["jitter"], f"{context} jitter")
    if jitter > delay:
        raise ValueError(f"{context} jitter exceeds delay")
    delay_correlation = _percent_ppm(
        row["delay_correlation"], f"{context} delay_correlation"
    )
    loss = _percent_ppm(row["loss"], f"{context} loss")
    loss_correlation = _percent_ppm(
        row["loss_correlation"], f"{context} loss_correlation"
    )
    reorder = _percent_ppm(row["reorder"], f"{context} reorder")
    reorder_correlation = _percent_ppm(
        row["reorder_correlation"], f"{context} reorder_correlation"
    )
    duplicate = _percent_ppm(row["duplicate"], f"{context} duplicate")
    corrupt = _percent_ppm(row["corrupt"], f"{context} corrupt")
    if jitter == 0 and delay_correlation != 0:
        raise ValueError(f"{context} correlates zero jitter")
    if loss == 0 and loss_correlation != 0:
        raise ValueError(f"{context} correlates zero loss")
    if reorder == 0 and reorder_correlation != 0:
        raise ValueError(f"{context} correlates zero reordering")
    if reorder > 0 and delay == 0:
        raise ValueError(f"{context} reorders without positive delay")
    if bool(row["outage"]) != (loss == 1_000_000):
        raise ValueError(f"{context} outage does not match 100% loss")
    if row["outage"] and loss_correlation != 0:
        raise ValueError(f"{context} outage loss must be uncorrelated")
    bounds = STRATA_BY_NAME[stratum_name]
    numeric = {
        "rate_kbit": rate,
        "delay_ms": delay,
        "jitter_ms": jitter,
        "delay_correlation_ppm": delay_correlation,
        "loss_ppm": loss,
        "loss_correlation_ppm": loss_correlation,
        "reorder_ppm": reorder,
        "reorder_correlation_ppm": reorder_correlation,
        "duplicate_ppm": duplicate,
        "corrupt_ppm": corrupt,
    }
    for field, value in numeric.items():
        if row["outage"] and field in {"loss_ppm", "loss_correlation_ppm"}:
            continue
        low, high = _bounds(bounds, field)
        if not low <= value <= high:
            raise ValueError(f"{context} {field} is outside its stratum")


def validate_schedule(schedule: object) -> dict[str, object]:
    """Strictly validate identity, schema, bounds, and exact seeded replay."""

    if not isinstance(schedule, dict):
        raise ValueError("schedule must be a JSON object")
    if set(schedule) != ROOT_FIELDS:
        raise ValueError("schedule fields do not match schema")
    if schedule["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported schedule schema version")
    if schedule["generator"] != GENERATOR_ID or schedule["model"] != MODEL_ID:
        raise ValueError("unsupported schedule generator or model")
    if schedule["application_scope"] != APPLICATION_SCOPE:
        raise ValueError("schedule application scope is not protocol-neutral")
    identity = schedule["schedule_sha256"]
    if (
        not isinstance(identity, str)
        or re.fullmatch(r"[0-9a-f]{64}", identity) is None
        or identity != schedule_sha256(schedule)
    ):
        raise ValueError("schedule_sha256 does not match canonical payload")
    seed = schedule["seed"]
    topology = schedule["topology"]
    epoch_count = _require_integer(schedule["epoch_count"], "epoch_count")
    include_outages = schedule["include_outages"]
    if not isinstance(seed, str) or not seed:
        raise ValueError("schedule seed must not be empty")
    if not isinstance(topology, str) or topology not in TOPOLOGIES:
        raise ValueError("schedule topology is unsupported")
    if not isinstance(include_outages, bool):
        raise ValueError("schedule include_outages must be boolean")
    if schedule["directions"] != list(DIRECTIONS):
        raise ValueError("schedule direction inventory does not match schema")
    if schedule["path_inventory"] != list(TOPOLOGIES[topology]):
        raise ValueError("schedule path inventory does not match topology")
    if schedule["tsv_columns"] != list(TSV_COLUMNS):
        raise ValueError("schedule TSV columns do not match schema")
    if not 1 <= epoch_count <= MAX_EPOCHS:
        raise ValueError("schedule epoch count is outside supported bounds")
    rows = schedule["rows"]
    if not isinstance(rows, list):
        raise ValueError("schedule rows must be a list")
    expected = build_schedule(seed, epoch_count, topology, include_outages)
    expected_rows = expected["rows"]
    if len(rows) != len(expected_rows):
        raise ValueError("schedule row count does not match topology")
    for index, (row, expected_row) in enumerate(zip(rows, expected_rows, strict=True)):
        _validate_row(row, expected_row, f"schedule row {index}")
    if schedule != expected:
        raise ValueError("schedule does not exactly match seeded replay")
    return schedule


def schedule_metadata(schedule: object) -> dict[str, object]:
    """Return bounded provenance and coverage metadata for a valid schedule."""

    checked = validate_schedule(schedule)
    rows = checked["rows"]
    assert isinstance(rows, list)
    rates = [_rate_kbit(row["rate"]) for row in rows]
    delays = [_time_ms(row["delay"], "delay") for row in rows]
    loss_values = [_percent_ppm(row["loss"], "loss") for row in rows]
    seeds = [int(row["netem_seed"]) for row in rows]
    stratum_counts = Counter(str(row["stratum"]) for row in rows)
    direction_counts = Counter(str(row["direction"]) for row in rows)
    return {
        "schema_version": SCHEMA_VERSION,
        "generator": GENERATOR_ID,
        "model": MODEL_ID,
        "application_scope": APPLICATION_SCOPE,
        "schedule_sha256": checked["schedule_sha256"],
        "seed": checked["seed"],
        "topology": checked["topology"],
        "epoch_count": checked["epoch_count"],
        "path_count": len(checked["path_inventory"]),
        "direction_count": len(DIRECTIONS),
        "row_count": len(rows),
        "include_outages": checked["include_outages"],
        "outage_count": sum(bool(row["outage"]) for row in rows),
        "stratum_counts": dict(sorted(stratum_counts.items())),
        "direction_row_counts": dict(sorted(direction_counts.items())),
        "minimum_rate_kbit": min(rates),
        "maximum_rate_kbit": max(rates),
        "minimum_delay_ms": min(delays),
        "maximum_delay_ms": max(delays),
        "maximum_loss_ppm": max(loss_values),
        "netem_seed_count": len(seeds),
        "unique_netem_seed_count": len(set(seeds)),
        "canonical_payload_bytes": len(
            canonical_json(_schedule_payload(checked)).encode("utf-8")
        ),
    }


def load_schedule(path: Path) -> dict[str, object]:
    """Load and strictly validate a canonical schedule artifact."""

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"cannot read schedule artifact: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid schedule JSON: {exc.msg}") from exc
    return validate_schedule(value)


def rows_for(
    schedule: object, epoch: int, direction: str
) -> list[dict[str, object]]:
    """Select an exact epoch/direction replay from a valid artifact."""

    checked = validate_schedule(schedule)
    if isinstance(epoch, bool) or not isinstance(epoch, int) or not (
        0 <= epoch < int(checked["epoch_count"])
    ):
        raise ValueError("replay epoch is outside the schedule")
    if direction not in DIRECTIONS:
        raise ValueError("replay direction is unsupported")
    return [
        row
        for row in checked["rows"]
        if row["epoch"] == epoch and row["direction"] == direction
    ]


def _render_tsv(rows: Iterable[Mapping[str, object]], header: bool = False) -> str:
    lines = ["\t".join(TSV_COLUMNS)] if header else []
    for row in rows:
        lines.append(
            "\t".join(
                "1"
                if column == "outage" and row[column] is True
                else "0"
                if column == "outage"
                else str(row[column])
                for column in TSV_COLUMNS
            )
        )
    return "\n".join(lines)


def _print_rows(
    rows: Sequence[Mapping[str, object]], output_format: str, header: bool
) -> None:
    if output_format == "json":
        print(canonical_json(list(rows)))
    elif output_format == "tsv":
        print(_render_tsv(rows, header))
    else:  # Defensive guard for direct callers; argparse constrains the CLI.
        raise ValueError(f"unsupported row format {output_format!r}")


def _add_row_output_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=("json", "tsv"), default="json")
    parser.add_argument(
        "--header", action="store_true", help="include the TSV header row"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    render = subparsers.add_parser("render", help="render one epoch/direction")
    render.add_argument("--seed", required=True)
    render.add_argument("--epoch", required=True, type=int)
    render.add_argument("--direction", required=True, choices=DIRECTIONS)
    render.add_argument("--topology", choices=tuple(TOPOLOGIES), default="five-path")
    render.add_argument("--include-outages", action="store_true")
    _add_row_output_arguments(render)

    generate = subparsers.add_parser("generate", help="emit a canonical schedule")
    generate.add_argument("--seed", required=True)
    generate.add_argument("--epochs", required=True, type=int)
    generate.add_argument(
        "--topology", choices=tuple(TOPOLOGIES), default="five-path"
    )
    generate.add_argument("--include-outages", action="store_true")

    for command in ("validate", "metadata"):
        artifact = subparsers.add_parser(command)
        artifact.add_argument("--schedule", required=True, type=Path)

    replay = subparsers.add_parser("replay", help="render rows from an artifact")
    replay.add_argument("--schedule", required=True, type=Path)
    replay.add_argument(
        "--expect-sha256",
        help="require the artifact to have this exact canonical schedule identity",
    )
    replay.add_argument("--epoch", required=True, type=int)
    replay.add_argument("--direction", required=True, choices=DIRECTIONS)
    _add_row_output_arguments(replay)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "render":
            _print_rows(
                render_rows(
                    args.seed,
                    args.epoch,
                    args.direction,
                    args.topology,
                    args.include_outages,
                ),
                args.format,
                args.header,
            )
            return 0
        if args.command == "generate":
            print(
                canonical_json(
                    build_schedule(
                        args.seed,
                        args.epochs,
                        args.topology,
                        args.include_outages,
                    )
                )
            )
            return 0
        schedule = load_schedule(args.schedule)
        if args.command == "replay":
            if (
                args.expect_sha256 is not None
                and schedule["schedule_sha256"] != args.expect_sha256
            ):
                raise ValueError("schedule identity does not match --expect-sha256")
            _print_rows(
                rows_for(schedule, args.epoch, args.direction),
                args.format,
                args.header,
            )
            return 0
        metadata = schedule_metadata(schedule)
        if args.command == "validate":
            metadata = {
                "schedule_sha256": metadata["schedule_sha256"],
                "valid": True,
            }
        print(canonical_json(metadata))
        return 0
    except (TypeError, ValueError) as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

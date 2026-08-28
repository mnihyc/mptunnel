#!/usr/bin/env python3
"""Render checked-in performance time series as a dependency-free SVG figure."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)

WIDTH = 1120
HEIGHT = 780
PLOT_LEFT = 92
PLOT_RIGHT = 1084
PLOT_WIDTH = PLOT_RIGHT - PLOT_LEFT
GOODPUT_TOP = 174
LATENCY_TOP = 470
PANEL_HEIGHT = 208
GOODPUT_BOTTOM = GOODPUT_TOP + PANEL_HEIGHT
LATENCY_BOTTOM = LATENCY_TOP + PANEL_HEIGHT

EXPECTED_STYLE = {
    "mpp_tcp": ("#0072B2", ""),
    "mpp_quic": ("#009E73", "8 3"),
    "mpp_default": ("#D55E00", ""),
    "xray_vmess": ("#5B6472", "3 3"),
    "hysteria2": ("#CC79A7", "10 3 2 3"),
}


class DatasetError(ValueError):
    """The derived-series JSON does not meet the publishing contract."""


def _svg(parent, tag, attributes=None, text=None):
    element = ET.SubElement(parent, f"{{{SVG_NS}}}{tag}", attributes or {})
    if text is not None:
        element.text = str(text)
    return element


def _require(condition, message):
    if not condition:
        raise DatasetError(message)


def _is_number(value):
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
    )


def _validate_metric_samples(series_id, metric, samples, expected_repetitions):
    _require(isinstance(samples, list), f"{series_id}.{metric} must be an array")
    _require(
        len(samples) >= 3,
        f"{series_id}.{metric} must contain a real ordered series, not scalar points",
    )
    previous_time = -math.inf
    for index, sample in enumerate(samples):
        prefix = f"{series_id}.{metric}[{index}]"
        _require(isinstance(sample, dict), f"{prefix} must be an object")
        time_s = sample.get("time_s")
        _require(_is_number(time_s) and time_s >= 0, f"{prefix}.time_s is invalid")
        _require(time_s > previous_time, f"{series_id}.{metric} times must increase")
        previous_time = time_s

        repetitions = sample.get("repetitions")
        available = sample.get("available")
        _require(
            isinstance(repetitions, int)
            and not isinstance(repetitions, bool)
            and repetitions == expected_repetitions,
            f"{prefix}.repetitions must match provenance",
        )
        _require(
            isinstance(available, int)
            and not isinstance(available, bool)
            and 0 <= available <= repetitions,
            f"{prefix}.available must be between zero and repetitions",
        )

        values = [sample.get(name) for name in ("low", "median", "high")]
        if available == 0:
            _require(
                values == [None, None, None],
                f"{prefix} unavailable values must remain null, never zero",
            )
        else:
            _require(
                all(_is_number(value) and value >= 0 for value in values),
                f"{prefix} available bounds must be finite non-negative numbers",
            )
            _require(
                values[0] <= values[1] <= values[2],
                f"{prefix} must satisfy low <= median <= high",
            )

        outcomes = sample.get("outcomes")
        if metric == "latency":
            _require(isinstance(outcomes, dict), f"{prefix}.outcomes must be an object")
            _require(
                all(
                    isinstance(key, str)
                    and isinstance(count, int)
                    and not isinstance(count, bool)
                    and count >= 0
                    for key, count in outcomes.items()
                ),
                f"{prefix}.outcomes counts are invalid",
            )
            _require(
                sum(outcomes.values()) == repetitions,
                f"{prefix}.outcomes must account for every repetition",
            )
            _require(
                outcomes.get("success", 0) == available,
                f"{prefix}.outcomes success count must match availability",
            )
        else:
            _require(
                outcomes is None,
                f"{prefix}.outcomes is only valid for latency samples",
            )


def validate_dataset(dataset):
    _require(isinstance(dataset, dict), "dataset must be an object")
    _require(dataset.get("schema_version") == 1, "unsupported schema_version")
    figure = dataset.get("figure")
    _require(isinstance(figure, dict), "figure metadata is required")
    for field in ("title", "subtitle", "variability_note", "condition_note"):
        _require(
            isinstance(figure.get(field), str) and figure[field].strip(),
            f"figure.{field} is required",
        )

    provenance = dataset.get("provenance")
    _require(isinstance(provenance, dict), "provenance is required")
    _require(
        provenance.get("aggregation") == "pointwise_median_min_max",
        "provenance must declare pointwise_median_min_max aggregation",
    )
    _require(
        provenance.get("goodput_window_alignment") == "symmetric_full_windows",
        "provenance must declare symmetric full-window goodput alignment",
    )
    run_count = provenance.get("valid_repetitions")
    _require(
        isinstance(run_count, int)
        and not isinstance(run_count, bool)
        and run_count >= 2,
        "provenance.valid_repetitions must be at least two",
    )
    _require(
        isinstance(provenance.get("source_runs"), list)
        and len(provenance["source_runs"]) == run_count,
        "provenance.source_runs must identify every valid repetition",
    )
    repetition_ids = set()
    result_directories = set()
    for index, source_run in enumerate(provenance["source_runs"]):
        prefix = f"provenance.source_runs[{index}]"
        _require(isinstance(source_run, dict), f"{prefix} must be an object")
        repetition_id = source_run.get("id")
        _require(
            isinstance(repetition_id, str)
            and repetition_id
            and repetition_id not in repetition_ids,
            f"{prefix}.id must be a unique non-empty string",
        )
        repetition_ids.add(repetition_id)
        directories = source_run.get("result_dirs")
        _require(
            isinstance(directories, list) and directories,
            f"{prefix}.result_dirs must be a non-empty array",
        )
        for directory in directories:
            _require(
                isinstance(directory, str)
                and directory
                and Path(directory).name == directory
                and directory not in result_directories,
                f"{prefix}.result_dirs must contain unique directory names",
            )
            result_directories.add(directory)
    condition = provenance.get("condition")
    _require(isinstance(condition, dict), "provenance.condition is required")
    for field in (
        "netem_mode",
        "internet_seed",
        "include_outages",
        "mpp_path_hints",
        "hysteria_client_rate",
        "hysteria_server_rate",
    ):
        _require(
            isinstance(condition.get(field), str) and condition[field],
            f"provenance.condition.{field} is required",
        )
    _require(
        _is_number(condition.get("load_duration_s"))
        and condition["load_duration_s"] > 0
        and isinstance(condition.get("bulk_connections"), int)
        and not isinstance(condition["bulk_connections"], bool)
        and condition["bulk_connections"] > 0
        and isinstance(condition.get("object_mib"), int)
        and not isinstance(condition["object_mib"], bool)
        and condition["object_mib"] > 0,
        "provenance.condition workload is invalid",
    )
    _require(
        condition.get("case_isolation") is True
        and condition.get("container_isolation") is True,
        "provenance.condition must preserve case and container isolation",
    )
    probe = condition.get("probe")
    _require(isinstance(probe, dict), "provenance.condition.probe is required")
    _require(
        probe.get("mode") == "socks5"
        and isinstance(probe.get("target"), str)
        and probe["target"]
        and isinstance(probe.get("tcp_echo_target"), str)
        and probe["tcp_echo_target"]
        and _is_number(probe.get("bulk_interval_seconds"))
        and probe["bulk_interval_seconds"] > 0
        and isinstance(probe.get("bulk_interval_trim_discard_each_end"), int)
        and not isinstance(probe["bulk_interval_trim_discard_each_end"], bool)
        and probe["bulk_interval_trim_discard_each_end"] >= 0
        and isinstance(probe.get("interactive_interval_ms"), int)
        and not isinstance(probe["interactive_interval_ms"], bool)
        and probe["interactive_interval_ms"] > 0
        and isinstance(probe.get("interactive_timeout_ms"), int)
        and not isinstance(probe["interactive_timeout_ms"], bool)
        and probe["interactive_timeout_ms"] > 0
        and isinstance(probe.get("interactive_payload_bytes"), int)
        and not isinstance(probe["interactive_payload_bytes"], bool)
        and probe["interactive_payload_bytes"] > 0
        and _is_number(probe.get("test_duration_s"))
        and math.isclose(
            probe["test_duration_s"],
            condition["load_duration_s"],
            rel_tol=0,
            abs_tol=1e-9,
        )
        and _is_number(probe.get("bulk_load_duration_s"))
        and math.isclose(
            probe["bulk_load_duration_s"],
            condition["load_duration_s"],
            rel_tol=0,
            abs_tol=1e-9,
        ),
        "provenance.condition.probe workload is invalid",
    )

    series = dataset.get("series")
    expected_series_ids = list(EXPECTED_STYLE)
    _require(
        isinstance(series, list)
        and [entry.get("id") if isinstance(entry, dict) else None for entry in series]
        == expected_series_ids,
        "series must contain the exact five ordered implementation trajectories",
    )
    seen = set()
    for entry in series:
        _require(isinstance(entry, dict), "each series entry must be an object")
        series_id = entry.get("id")
        _require(
            isinstance(series_id, str) and series_id,
            "each series must have an id",
        )
        _require(series_id not in seen, f"duplicate series id {series_id}")
        _require(series_id in EXPECTED_STYLE, f"no accessible style for {series_id}")
        seen.add(series_id)
        _require(
            isinstance(entry.get("label"), str) and entry["label"].strip(),
            f"{series_id}.label is required",
        )
        implementation = entry.get("implementation")
        expected_tool = (
            "mptunnel"
            if series_id.startswith("mpp_")
            else "xray" if series_id == "xray_vmess" else "hysteria2"
        )
        _require(
            isinstance(implementation, dict)
            and implementation.get("tool") == expected_tool
            and isinstance(implementation.get("carrier"), str)
            and implementation["carrier"],
            f"{series_id}.implementation does not identify its tool and carrier",
        )
        if series_id.startswith("mpp_"):
            _require(
                isinstance(implementation.get("protocol_version"), int)
                and implementation["protocol_version"] > 0
                and implementation.get("build_profile") == "release",
                f"{series_id}.implementation does not identify the release protocol",
            )
        else:
            _require(
                isinstance(implementation.get("release"), str)
                and implementation["release"],
                f"{series_id}.implementation does not identify the baseline release",
            )
        repetitions = entry.get("valid_repetitions")
        _require(
            isinstance(repetitions, int)
            and not isinstance(repetitions, bool)
            and repetitions == run_count,
            f"{series_id}.valid_repetitions must match provenance",
        )
        _validate_metric_samples(series_id, "goodput", entry.get("goodput"), run_count)
        _validate_metric_samples(series_id, "latency", entry.get("latency"), run_count)
    return dataset


def load_dataset(path):
    try:
        with Path(path).open(encoding="utf-8") as handle:
            dataset = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetError(f"cannot load {path}: {exc}") from exc
    return validate_dataset(dataset)


def _nice_ceiling(value):
    if value <= 0:
        return 1.0
    rough_step = value / 4.0
    magnitude = 10 ** math.floor(math.log10(rough_step))
    normalized = rough_step / magnitude
    if normalized <= 1:
        step = magnitude
    elif normalized <= 2:
        step = 2 * magnitude
    elif normalized <= 5:
        step = 5 * magnitude
    else:
        step = 10 * magnitude
    return math.ceil(value / step) * step


def _format_tick(value):
    if abs(value) >= 1000:
        return f"{value / 1000:g}k"
    if float(value).is_integer():
        return str(int(value))
    return f"{value:.1f}".rstrip("0").rstrip(".")


def _format_coordinate(value):
    return f"{value:.2f}".rstrip("0").rstrip(".")


def _metric_display_extent(dataset, metric):
    """Keep isolated buffering spikes from flattening the useful trajectory.

    The raw low/median/high values remain in the dataset and hover text. The
    display domain follows the 97.5th percentile of measured medians; points
    above it receive an explicit overflow marker instead of silently changing
    the scale for every implementation.
    """
    values = [
        sample["median"]
        for entry in dataset["series"]
        for sample in entry[metric]
        if sample["median"] is not None
    ]
    _require(values, f"{metric} has no available values")
    values.sort()
    rank = max(1, math.ceil(len(values) * 0.975))
    return values[rank - 1]


def _time_extent(dataset):
    return max(
        sample["time_s"]
        for entry in dataset["series"]
        for metric in ("goodput", "latency")
        for sample in entry[metric]
    )


def _segments(samples):
    segments = []
    current = []
    for sample in samples:
        if sample["median"] is None:
            if current:
                segments.append(current)
                current = []
        else:
            current.append(sample)
    if current:
        segments.append(current)
    return segments


def _path_for_line(samples, x_position, y_position):
    return " ".join(
        ("M" if index == 0 else "L")
        + _format_coordinate(x_position(sample["time_s"]))
        + ","
        + _format_coordinate(y_position(sample["median"]))
        for index, sample in enumerate(samples)
    )


def _path_for_band(samples, x_position, y_position):
    upper = [
        (x_position(sample["time_s"]), y_position(sample["high"])) for sample in samples
    ]
    lower = [
        (x_position(sample["time_s"]), y_position(sample["low"]))
        for sample in reversed(samples)
    ]
    points = upper + lower
    return (
        " ".join(
            ("M" if index == 0 else "L")
            + _format_coordinate(x)
            + ","
            + _format_coordinate(y)
            for index, (x, y) in enumerate(points)
        )
        + " Z"
    )


def _draw_axes(root, top, bottom, y_max, unit, panel_label, panel_title, x_max, show_x):
    plot_group = _svg(root, "g", {"class": "panel"})
    _svg(
        plot_group,
        "text",
        {"x": str(PLOT_LEFT), "y": str(top - 20), "class": "panel-title"},
        f"{panel_label}  {panel_title}",
    )
    for index in range(5):
        fraction = index / 4
        y = bottom - fraction * PANEL_HEIGHT
        value = fraction * y_max
        _svg(
            plot_group,
            "line",
            {
                "x1": str(PLOT_LEFT),
                "x2": str(PLOT_RIGHT),
                "y1": _format_coordinate(y),
                "y2": _format_coordinate(y),
                "class": "grid",
            },
        )
        _svg(
            plot_group,
            "text",
            {
                "x": str(PLOT_LEFT - 12),
                "y": _format_coordinate(y + 4),
                "class": "tick y-tick",
            },
            _format_tick(value),
        )
    _svg(
        plot_group,
        "text",
        {
            "x": "22",
            "y": _format_coordinate((top + bottom) / 2),
            "class": "axis-label",
            "transform": f"rotate(-90 22 {_format_coordinate((top + bottom) / 2)})",
        },
        unit,
    )
    _svg(
        plot_group,
        "line",
        {
            "x1": str(PLOT_LEFT),
            "x2": str(PLOT_LEFT),
            "y1": str(top),
            "y2": str(bottom),
            "class": "axis",
        },
    )
    _svg(
        plot_group,
        "line",
        {
            "x1": str(PLOT_LEFT),
            "x2": str(PLOT_RIGHT),
            "y1": str(bottom),
            "y2": str(bottom),
            "class": "axis",
        },
    )
    if show_x:
        for index in range(6):
            fraction = index / 5
            x = PLOT_LEFT + fraction * PLOT_WIDTH
            value = fraction * x_max
            _svg(
                plot_group,
                "line",
                {
                    "x1": _format_coordinate(x),
                    "x2": _format_coordinate(x),
                    "y1": str(bottom),
                    "y2": str(bottom + 5),
                    "class": "axis",
                },
            )
            _svg(
                plot_group,
                "text",
                {
                    "x": _format_coordinate(x),
                    "y": str(bottom + 22),
                    "class": "tick x-tick",
                },
                _format_tick(value),
            )
        _svg(
            plot_group,
            "text",
            {
                "x": _format_coordinate((PLOT_LEFT + PLOT_RIGHT) / 2),
                "y": str(bottom + 48),
                "class": "axis-label x-axis-label",
            },
            "Elapsed time (s)",
        )


def _draw_metric(root, dataset, metric, top, bottom, y_max, x_max, clip_id):
    def x_position(value):
        return PLOT_LEFT + value / x_max * PLOT_WIDTH

    def y_position(value):
        return bottom - value / y_max * PANEL_HEIGHT

    group = _svg(root, "g", {"clip-path": f"url(#{clip_id})"})
    for series_index, entry in enumerate(dataset["series"]):
        color, dash = EXPECTED_STYLE[entry["id"]]
        for segment in _segments(entry[metric]):
            if len(segment) >= 2:
                _svg(
                    group,
                    "path",
                    {
                        "d": _path_for_band(segment, x_position, y_position),
                        "fill": color,
                        "class": "variability-band",
                    },
                )
        for segment in _segments(entry[metric]):
            if len(segment) < 2:
                continue
            attributes = {
                "d": _path_for_line(segment, x_position, y_position),
                "stroke": color,
                "class": "series-line",
                "data-series": entry["id"],
                "data-metric": metric,
            }
            if dash:
                attributes["stroke-dasharray"] = dash
            _svg(group, "path", attributes)

        for sample in entry[metric]:
            if sample["median"] is not None:
                marker_y = y_position(min(sample["median"], y_max))
                hit = _svg(
                    group,
                    "circle",
                    {
                        "cx": _format_coordinate(x_position(sample["time_s"])),
                        "cy": _format_coordinate(marker_y),
                        "r": "6",
                        "class": "hit-target",
                    },
                )
                _svg(
                    hit,
                    "title",
                    text=(
                        f"{entry['label']}: {sample['median']:g} "
                        f"({'Mbps' if metric == 'goodput' else 'ms'}) at "
                        f"{sample['time_s']:g} s; range {sample['low']:g}–"
                        f"{sample['high']:g}; {sample['available']}/"
                        f"{sample['repetitions']} repetitions available"
                    ),
                )
                if sample["median"] > y_max:
                    x = x_position(sample["time_s"])
                    overflow = _svg(
                        group,
                        "path",
                        {
                            "d": (
                                f"M{_format_coordinate(x)},{top + 2} "
                                f"L{_format_coordinate(x + 4)},{top + 8} "
                                f"L{_format_coordinate(x - 4)},{top + 8} Z"
                            ),
                            "fill": color,
                            "class": "overflow-marker",
                        },
                    )
                    _svg(
                        overflow,
                        "title",
                        text=(
                            f"{entry['label']}: {sample['median']:g} "
                            f"({'Mbps' if metric == 'goodput' else 'ms'}) at "
                            f"{sample['time_s']:g} s exceeds the {y_max:g} "
                            "display domain"
                        ),
                    )

            if metric == "latency" and sample["available"] < sample["repetitions"]:
                x = x_position(sample["time_s"])
                marker_y = bottom - 7 - series_index * 7
                if sample["available"] == 0:
                    marker = _svg(
                        group,
                        "path",
                        {
                            "d": (
                                f"M{_format_coordinate(x - 3)},{marker_y - 3} "
                                f"L{_format_coordinate(x + 3)},{marker_y + 3} "
                                f"M{_format_coordinate(x + 3)},{marker_y - 3} "
                                f"L{_format_coordinate(x - 3)},{marker_y + 3}"
                            ),
                            "stroke": color,
                            "class": "availability-marker unavailable",
                        },
                    )
                else:
                    marker = _svg(
                        group,
                        "path",
                        {
                            "d": (
                                f"M{_format_coordinate(x)},{marker_y - 4} "
                                f"L{_format_coordinate(x + 4)},{marker_y + 3} "
                                f"L{_format_coordinate(x - 4)},{marker_y + 3} Z"
                            ),
                            "fill": "none",
                            "stroke": color,
                            "class": "availability-marker partial",
                        },
                    )
                outcomes = sample["outcomes"]
                outcome_text = ", ".join(
                    f"{name}={count}" for name, count in sorted(outcomes.items())
                )
                _svg(
                    marker,
                    "title",
                    text=(
                        f"{entry['label']}: echo availability "
                        f"{sample['available']}/{sample['repetitions']} at "
                        f"{sample['time_s']:g} s"
                        + (f" ({outcome_text})" if outcome_text else "")
                    ),
                )


def _draw_legend(root, dataset):
    group = _svg(root, "g", {"class": "legend", "aria-label": "Series legend"})
    cell_width = PLOT_WIDTH / len(dataset["series"])
    for index, entry in enumerate(dataset["series"]):
        color, dash = EXPECTED_STYLE[entry["id"]]
        x = PLOT_LEFT + index * cell_width
        line_attributes = {
            "x1": _format_coordinate(x),
            "x2": _format_coordinate(x + 28),
            "y1": "134",
            "y2": "134",
            "stroke": color,
            "class": "legend-line",
        }
        if dash:
            line_attributes["stroke-dasharray"] = dash
        _svg(group, "line", line_attributes)
        _svg(
            group,
            "text",
            {"x": _format_coordinate(x + 35), "y": "138", "class": "legend-label"},
            entry["label"],
        )


def render_svg(dataset):
    validate_dataset(dataset)
    x_max = _nice_ceiling(_time_extent(dataset))
    goodput_max = _nice_ceiling(_metric_display_extent(dataset, "goodput") * 1.04)
    latency_max = _nice_ceiling(_metric_display_extent(dataset, "latency") * 1.04)

    root = ET.Element(
        f"{{{SVG_NS}}}svg",
        {
            "viewBox": f"0 0 {WIDTH} {HEIGHT}",
            "width": str(WIDTH),
            "height": str(HEIGHT),
            "role": "img",
            "aria-labelledby": "figure-title figure-description",
        },
    )
    _svg(root, "title", {"id": "figure-title"}, dataset["figure"]["title"])
    _svg(
        root,
        "desc",
        {"id": "figure-description"},
        dataset["figure"]["subtitle"]
        + " "
        + dataset["figure"]["variability_note"]
        + " "
        + "Unavailable echo attempts are gaps, never zero-latency samples.",
    )
    _svg(
        root,
        "style",
        text="""
            svg { background: #ffffff; color: #172033; font-family: Inter, ui-sans-serif,
                  system-ui, -apple-system, BlinkMacSystemFont, \"Segoe UI\", sans-serif; }
            text { fill: #172033; }
            .figure-title { font-size: 25px; font-weight: 680; letter-spacing: -0.25px; }
            .subtitle { fill: #596579; font-size: 13px; }
            .condition { fill: #344054; font-size: 12px; font-weight: 560; }
            .panel-title { font-size: 14px; font-weight: 660; }
            .grid { stroke: #E4E8EF; stroke-width: 1; }
            .axis { stroke: #8B96A8; stroke-width: 1; }
            .tick { fill: #596579; font-size: 11px; }
            .y-tick { text-anchor: end; }
            .x-tick { text-anchor: middle; }
            .axis-label { fill: #344054; font-size: 12px; font-weight: 560; text-anchor: middle; }
            .series-line { fill: none; stroke-width: 2.35; stroke-linejoin: round;
                           stroke-linecap: round; }
            [data-series=\"mpp_default\"] { stroke-width: 3; }
            .variability-band { opacity: 0.075; stroke: none; }
            .legend-line { stroke-width: 3; stroke-linecap: round; }
            .legend-label { fill: #344054; font-size: 11.5px; font-weight: 560; }
            .hit-target { fill: transparent; stroke: none; pointer-events: all; }
            .availability-marker { stroke-width: 1.6; stroke-linecap: round;
                                   stroke-linejoin: round; }
            .overflow-marker { stroke: #ffffff; stroke-width: 0.8; }
            .footer { fill: #667085; font-size: 10.5px; }
            .footer-strong { fill: #344054; font-weight: 650; }
        """,
    )
    definitions = _svg(root, "defs")
    for clip_id, top in (("goodput-clip", GOODPUT_TOP), ("latency-clip", LATENCY_TOP)):
        clip = _svg(definitions, "clipPath", {"id": clip_id})
        _svg(
            clip,
            "rect",
            {
                "x": str(PLOT_LEFT - 6),
                "y": str(top - 6),
                "width": str(PLOT_WIDTH + 12),
                "height": str(PANEL_HEIGHT + 12),
            },
        )

    _svg(
        root,
        "text",
        {"x": str(PLOT_LEFT), "y": "43", "class": "figure-title"},
        dataset["figure"]["title"],
    )
    _svg(
        root,
        "text",
        {"x": str(PLOT_LEFT), "y": "69", "class": "subtitle"},
        dataset["figure"]["subtitle"],
    )
    _svg(
        root,
        "text",
        {"x": str(PLOT_LEFT), "y": "91", "class": "condition"},
        dataset["figure"]["condition_note"],
    )
    _draw_legend(root, dataset)
    _draw_axes(
        root,
        GOODPUT_TOP,
        GOODPUT_BOTTOM,
        goodput_max,
        "Receiver goodput (Mbps)",
        "a",
        "Receiver goodput over time",
        x_max,
        False,
    )
    _draw_axes(
        root,
        LATENCY_TOP,
        LATENCY_BOTTOM,
        latency_max,
        "Echo RTT (ms)",
        "b",
        "Persistent application-echo RTT over time",
        x_max,
        True,
    )
    _draw_metric(
        root,
        dataset,
        "goodput",
        GOODPUT_TOP,
        GOODPUT_BOTTOM,
        goodput_max,
        x_max,
        "goodput-clip",
    )
    _draw_metric(
        root,
        dataset,
        "latency",
        LATENCY_TOP,
        LATENCY_BOTTOM,
        latency_max,
        x_max,
        "latency-clip",
    )
    _svg(
        root,
        "text",
        {"x": str(PLOT_LEFT), "y": "752", "class": "footer footer-strong"},
        dataset["figure"]["variability_note"],
    )
    _svg(
        root,
        "text",
        {"x": str(PLOT_LEFT), "y": "769", "class": "footer"},
        "Echo gaps and × denote zero available attempts; △ denotes partial availability; ▲ marks a median above the display domain. Hover reports exact values.",
    )
    ET.indent(root, space="  ")
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        + ET.tostring(root, encoding="unicode", short_empty_elements=True)
        + "\n"
    )


def write_svg(dataset, output_path):
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    svg = render_svg(dataset)
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
            handle.write(svg)
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Render a validated performance-series JSON file as SVG."
    )
    parser.add_argument("input", type=Path, help="derived performance-series JSON")
    parser.add_argument("output", type=Path, help="destination SVG")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        write_svg(load_dataset(args.input), args.output)
    except DatasetError as exc:
        print(f"performance-series error: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

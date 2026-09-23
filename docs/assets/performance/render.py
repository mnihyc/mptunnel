#!/usr/bin/env python3
"""Render public performance figures from checked-in observation data."""

from __future__ import annotations

import argparse
import html
import json
import math
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch


FIGURE_DIR = Path(__file__).resolve().parent
DEFAULT_DATA = FIGURE_DIR / "current-observations.json"
DEFAULT_SVG = FIGURE_DIR / "shared-link-tradeoffs.svg"
DEFAULT_PNG = FIGURE_DIR / "shared-link-tradeoffs.png"
DEFAULT_NARROW_SVG = FIGURE_DIR / "shared-link-tradeoffs-narrow.svg"
DEFAULT_INDEPENDENT_DATA = FIGURE_DIR / "independent-paths-series.json"
DEFAULT_INDEPENDENT_SVG = FIGURE_DIR / "independent-paths.svg"
DEFAULT_INDEPENDENT_PNG = FIGURE_DIR / "independent-paths.png"
DEFAULT_SHARED_DATA = FIGURE_DIR / "shared-link-series.json"
DEFAULT_SHARED_TIMELINE_SVG = FIGURE_DIR / "shared-link-timeline.svg"
DEFAULT_SHARED_TIMELINE_PNG = FIGURE_DIR / "shared-link-timeline.png"

TITLE = "Download goodput and responsiveness"
EXPECTED_SYSTEMS = {"mixed", "h2", "quic", "xray", "raw"}
SYSTEM_LABELS = {
    "mixed": "MPTUNNEL TCP+QUIC",
    "h2": "Hysteria2",
    "quic": "MPTUNNEL QUIC",
    "xray": "Xray VMess/TCP",
    "raw": "Direct TCP",
}
SYSTEM_CHART_LABELS = {
    "mixed": "MPTUNNEL TCP+QUIC\n4 carriers: 3 TCP + 1 QUIC",
    "h2": "Hysteria2",
    "quic": "MPTUNNEL QUIC\n1 QUIC carrier",
    "xray": "Xray VMess/TCP",
    "raw": "Direct TCP",
}
SYSTEM_COLORS = {
    "mixed": "#176B9A",
    "h2": "#D58B24",
    "quic": "#238C83",
    "xray": "#795A9B",
    "raw": "#77838E",
}


def _fail(message: str) -> None:
    raise ValueError(f"Invalid competitive observations: {message}")


def _number(value: Any, description: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value < 0
    ):
        _fail(f"{description} must be a finite, non-negative number")
    return float(value)


def _count(value: Any, description: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _fail(f"{description} must be a non-negative integer")
    return value


def validate_observations(document: Any) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Check the precise single-run comparison and the fields used in the figure."""
    if not isinstance(document, dict):
        _fail("top-level value must be an object")
    comparison = document.get("competitive")
    if not isinstance(comparison, dict):
        _fail("competitive must be an object")
    conditions = comparison.get("conditions")
    if not isinstance(conditions, dict):
        _fail("competitive.conditions must be an object")

    if conditions.get("shared_bottleneck_mbps") != {"down": 500, "up": 100}:
        _fail("expected the shared 500 Mbps down / 100 Mbps up bottleneck")
    delay = conditions.get("delay_ms")
    if not isinstance(delay, dict) or delay.get("down") != 70 or delay.get("up") != 30:
        _fail("expected 70 ms down plus 30 ms up directional delay")
    if conditions.get("duration_s") != 40:
        _fail("expected 40-second observations")
    if conditions.get("nominal_jitter_clear_s") != 8:
        _fail("expected initial jitter to clear around 8 seconds")
    if conditions.get("configured_random_loss") != "none":
        _fail("expected no configured random loss")
    if conditions.get("bulk") != "time-limited HTTP body read":
        _fail("expected a time-limited HTTP body read")
    echo_condition = conditions.get("echo")
    if (
        not isinstance(echo_condition, dict)
        or echo_condition.get("payload_bytes") != 64
        or echo_condition.get("sequential_period_ms") != 500
        or echo_condition.get("timeout_ms") != 3000
    ):
        _fail("expected the recorded sequential 500 ms, 64-byte echo schedule")

    rows = comparison.get("rows")
    if not isinstance(rows, list) or len(rows) != len(EXPECTED_SYSTEMS):
        _fail("competitive.rows must contain exactly five single observations")
    by_system: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            _fail(f"rows[{index}] must be an object")
        system = row.get("system")
        if system not in EXPECTED_SYSTEMS or system in by_system:
            _fail("rows must contain each expected system exactly once")
        goodput = _number(row.get("goodput_mbps"), f"{system}.goodput_mbps")
        reply_p95 = _number(row.get("echo_p95_ms"), f"{system}.echo_p95_ms")
        failures = _count(row.get("echo_failures"), f"{system}.echo_failures")
        offered = _count(row.get("echo_offered"), f"{system}.echo_offered")
        successful = _count(row.get("echo_successful"), f"{system}.echo_successful")
        if offered == 0 or successful == 0:
            _fail(f"{system} must have offered and successful echo requests")
        if failures != 0 or successful != offered:
            _fail(f"{system} must retain zero failures and all offered replies")
        row["goodput_mbps"] = goodput
        row["echo_p95_ms"] = reply_p95
        row["echo_offered"] = offered
        row["echo_successful"] = successful
        row["echo_failures"] = failures
        by_system[system] = row

    if set(by_system) != EXPECTED_SYSTEMS:
        _fail("competitive rows do not cover the expected five systems")

    build = document.get("measured_candidate")
    if not isinstance(build, dict):
        _fail("measured_candidate build metadata is required")
    version = build.get("version")
    revision = build.get("source_revision")
    if not isinstance(version, str) or not version.lower().startswith("mptunnel "):
        _fail("measured_candidate.version must identify MPTUNNEL")
    if (
        not isinstance(revision, str)
        or len(revision) < 7
        or re.fullmatch(r"[0-9a-f]{7,}", revision) is None
    ):
        _fail("measured_candidate.source_revision must be a hexadecimal revision")
    if "0.6.0" not in version:
        _fail("expected the measured MPTUNNEL 0.6.0 development build")
    competitive_settings = conditions.get("other_settings")
    if not isinstance(competitive_settings, dict):
        _fail("competitive controller and protocol settings are required")
    h2_setting = competitive_settings.get("Hysteria2")
    vmess_setting = competitive_settings.get("Xray")
    if not isinstance(h2_setting, str) or "Brutal" not in h2_setting or "500/100" not in h2_setting:
        _fail("expected Hysteria2 Brutal configured at 500/100 Mbps")
    if not isinstance(vmess_setting, str) or "VMess/TCP" not in vmess_setting:
        _fail("expected the Xray VMess/TCP configuration")

    comparison["_rows_by_system"] = by_system
    comparison["_measured_version"] = version
    comparison["_measured_revision"] = revision
    comparison["_h2_setting"] = h2_setting
    comparison["_vmess_setting"] = vmess_setting
    return conditions, rows


def _accessible_description(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: row["goodput_mbps"], reverse=True)
    measurements = "; ".join(
        f"{SYSTEM_LABELS[row['system']]}: "
        f"{row['goodput_mbps']:.1f} Mbps download throughput and "
        f"{row['echo_p95_ms']:.1f} ms loaded reply p95"
        for row in ordered
    )
    return (
        "Two aligned horizontal bar charts compare download throughput, where higher is better, "
        "and loaded reply 95th-percentile latency, where lower is better. They show five "
        "single observations, each summarizing a separate 40-second product run against the same "
        "physical 500 Mbps down, 100 Mbps up link limit; the products did not compete "
        "simultaneously. The full-run download averages include startup, and the configured "
        "jitter was cleared after about 8–9 seconds, so these are run summaries rather than "
        "steady-state capacity rankings. Within each run, that product's bulk download and "
        "small requests share the link. MPTUNNEL TCP+QUIC used 3 TCP carriers plus 1 QUIC carrier; "
        "MPTUNNEL QUIC used 1 QUIC carrier. The dashed line marks the configured 500 Mbps "
        "download capacity. "
        + measurements
        + "."
    )


def _add_accessible_svg_header(path: Path, title: str, description: str) -> None:
    content = path.read_text(encoding="utf-8")
    root_match = re.search(r"<svg\b[^>]*>", content)
    if root_match is None:
        raise ValueError(f"Matplotlib did not produce an SVG root element at {path}")
    root = root_match.group(0)
    root = root[:-1] + ' role="img" aria-labelledby="figure-title figure-description">'
    accessible_nodes = (
        "\n<title id=\"figure-title\">"
        + html.escape(title, quote=False)
        + "</title>\n<desc id=\"figure-description\">"
        + html.escape(description, quote=False)
        + "</desc>"
    )
    content = content[: root_match.start()] + root + accessible_nodes + content[root_match.end() :]
    content = "\n".join(line.rstrip() for line in content.splitlines()) + "\n"
    path.write_text(content, encoding="utf-8")


def render_narrow_tradeoffs(rows: list[dict[str, Any]], svg_path: Path) -> None:
    """Render a stacked SVG variant that stays legible in narrow page columns."""
    ordered = sorted(rows, key=lambda row: row["goodput_mbps"], reverse=True)
    labels = [SYSTEM_CHART_LABELS[row["system"]] for row in ordered]
    colors = [SYSTEM_COLORS[row["system"]] for row in ordered]
    y = list(range(len(ordered)))
    throughput = [row["goodput_mbps"] for row in ordered]
    reply_p95 = [row["echo_p95_ms"] for row in ordered]
    matplotlib.rcParams["svg.hashsalt"] = "mptunnel-public-shared-link-tradeoffs-narrow-v1"

    fig = plt.figure(figsize=(6.8, 8.4), facecolor="#FFFFFF")
    throughput_ax, latency_ax = fig.subplots(
        2,
        1,
        sharey=True,
        gridspec_kw={"left": 0.38, "right": 0.965, "bottom": 0.10, "top": 0.700, "hspace": 0.48},
    )
    fig.text(
        0.055,
        0.968,
        TITLE,
        ha="left",
        va="top",
        fontsize=19.5,
        fontweight="semibold",
        color="#192B3A",
    )
    fig.text(
        0.055,
        0.911,
        "Separate product runs · shared bottleneck: 500 down / 100 up Mbps",
        ha="left",
        va="top",
        fontsize=11.7,
        color="#435563",
    )
    fig.text(
        0.055,
        0.881,
        "40 s whole-run averages include startup · jitter clears by ~8–9 s",
        ha="left",
        va="top",
        fontsize=11.7,
        color="#435563",
    )
    for axis, values, heading, xmax, xticks in (
        (
            throughput_ax,
            throughput,
            "Download throughput\nMbps · higher is better",
            560,
            [0, 100, 200, 300, 400, 500],
        ),
        (
            latency_ax,
            reply_p95,
            "Loaded reply p95\nms · lower is better",
            940,
            [0, 200, 400, 600, 800],
        ),
    ):
        axis.set_facecolor("#FFFFFF")
        axis.barh(y, values, height=0.58, color=colors, edgecolor="none", zorder=3)
        axis.set_xlim(0, xmax)
        axis.set_xticks(xticks)
        axis.set_yticks(y, labels)
        axis.set_title(heading, loc="left", pad=10, fontsize=13.0, fontweight="semibold", color="#243847")
        axis.tick_params(axis="x", labelsize=12.1, colors="#52616D", length=0, pad=7)
        axis.tick_params(axis="y", labelsize=12.5, colors="#263746", length=0, pad=10)
        axis.xaxis.grid(True, color="#E5E9ED", linewidth=0.8, zorder=0)
        axis.set_axisbelow(True)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_visible(False)
        axis.spines["bottom"].set_color("#AAB4BC")
        axis.spines["bottom"].set_linewidth(0.8)
        for value, row_y in zip(values, y):
            axis.text(
                value + xmax * 0.018,
                row_y,
                f"{value:.1f}",
                ha="left",
                va="center",
                fontsize=12.3,
                color="#243847",
                clip_on=False,
            )

    throughput_ax.axvline(0, color="#586875", linewidth=1.0, zorder=2)
    throughput_ax.axvline(500, color="#586875", linewidth=1.4, linestyle=(0, (4, 3)), zorder=2)
    throughput_ax.invert_yaxis()
    latency_ax.axvline(0, color="#586875", linewidth=1.0, zorder=2)
    latency_ax.tick_params(axis="y", labelleft=True)
    capacity_handle = Line2D(
        [0],
        [0],
        color="#586875",
        linewidth=1.4,
        linestyle=(0, (4, 3)),
        label="Configured download capacity: 500 Mbps",
    )
    fig.legend(
        handles=[capacity_handle],
        loc="upper right",
        bbox_to_anchor=(0.965, 0.832),
        frameon=False,
        handlelength=2.1,
        handletextpad=0.55,
        borderaxespad=0,
        fontsize=11.4,
        labelcolor="#435563",
    )

    description = _accessible_description(ordered) + " This narrow version stacks the two panels vertically."
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        svg_path,
        format="svg",
        facecolor="#FFFFFF",
        metadata={
            "Title": TITLE,
            "Description": description,
            "Creator": "docs/assets/performance/render.py",
            "Date": None,
        },
    )
    plt.close(fig)
    _add_accessible_svg_header(svg_path, TITLE, description)


def load_independent_observation(data_path: Path) -> dict[str, Any]:
    """Load and validate the current-build download timeline and echo series."""
    document = json.loads(data_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or document.get("schema") != "mptunnel.independent-paths-series.v1":
        raise ValueError("Independent-path data must use mptunnel.independent-paths-series.v1")
    conditions = document.get("conditions")
    if not isinstance(conditions, dict) or conditions.get("duration_s") != 40:
        raise ValueError("Independent-path data must describe the 40-second run")
    topology = conditions.get("topology", "")
    if not isinstance(topology, str) or "QUIC" not in topology or "TCP" not in topology or "200" not in topology:
        raise ValueError("Independent-path topology must retain the configured QUIC and TCP links")
    qos = conditions.get("qos")
    outage = conditions.get("udp_outage")
    if (
        not isinstance(qos, dict)
        or qos.get("link") != 46
        or qos.get("rate_mbps") != 10
        or qos.get("nominal_interval_s") != [15, 25]
    ):
        raise ValueError("Independent-path QoS must retain the nominal 10 Mbps, 15–25 second interval")
    if (
        not isinstance(outage, dict)
        or outage.get("protocol") != "UDP"
        or outage.get("applies_to_both_links") is not True
        or outage.get("nominal_interval_s") != [30, 33]
    ):
        raise ValueError("Independent-path UDP outage conditions are missing")

    series = document.get("series")
    if not isinstance(series, list):
        raise ValueError("Independent-path series must be an array")
    matches = [
        row
        for row in series
        if isinstance(row, dict)
        and row.get("case") == "fm-13-independent-aggregate-down-candidate"
        and row.get("direction") == "down"
        and row.get("variant") == "candidate"
    ]
    if len(matches) != 1:
        raise ValueError("Expected one current-build independent-link download observation")
    row = matches[0]
    build = row.get("build")
    if not isinstance(build, dict):
        raise ValueError("Independent download build metadata is required")
    version = build.get("version")
    revision = build.get("source_revision")
    if not isinstance(version, str) or "0.6.0" not in version:
        raise ValueError("The independent-path download must identify MPTUNNEL 0.6.0")
    if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
        raise ValueError("The independent-path source revision must be a full hexadecimal hash")

    goodput = row.get("goodput")
    if (
        not isinstance(goodput, dict)
        or goodput.get("unit") != "Mbit/s"
        or goodput.get("interval_seconds") != 1
        or "probe" not in goodput.get("origin", "").lower()
    ):
        raise ValueError("Download bins must remain one-second probe-origin Mbit/s observations")
    bins = goodput.get("raw_bins")
    if not isinstance(bins, list) or len(bins) != 40:
        raise ValueError("The figure requires all forty raw one-second download bins")
    bin_values = [_number(value, f"download raw_bins[{index}]") for index, value in enumerate(bins)]

    echo_summary = row.get("echo_summary")
    attempts = row.get("echo_attempts")
    if not isinstance(echo_summary, dict) or not isinstance(attempts, list):
        raise ValueError("Concurrent echo attempts and summary are required")
    attempt_count = _count(echo_summary.get("attempts"), "echo_summary.attempts")
    successes = _count(echo_summary.get("successes"), "echo_summary.successes")
    failures = _count(echo_summary.get("failures"), "echo_summary.failures")
    if len(attempts) != attempt_count or attempt_count != 80 or successes != 80 or failures != 0:
        raise ValueError("The current download run must retain eighty successful echo attempts")
    echo_times: list[float] = []
    echo_latencies: list[float] = []
    for index, attempt in enumerate(attempts):
        if not isinstance(attempt, dict) or attempt.get("outcome") != "success":
            raise ValueError(f"echo_attempts[{index}] is not a successful reply")
        start_s = _number(attempt.get("start_offset_s"), f"echo_attempts[{index}].start_offset_s")
        end_s = _number(attempt.get("end_offset_s"), f"echo_attempts[{index}].end_offset_s")
        latency = _number(attempt.get("latency_ms"), f"echo_attempts[{index}].latency_ms")
        if start_s > end_s or end_s > 40:
            raise ValueError(f"echo_attempts[{index}] falls outside the 40-second probe window")
        echo_times.append(start_s)
        echo_latencies.append(latency)
    echo_p95 = _number(echo_summary.get("p95_success_latency_ms"), "echo_summary.p95_success_latency_ms")

    interventions = row.get("actual_interventions")
    if not isinstance(interventions, dict):
        raise ValueError("Logged intervention transitions are required")
    actual_qos = interventions.get("qos")
    shape_calls = actual_qos.get("shape_calls") if isinstance(actual_qos, dict) else None
    if not isinstance(shape_calls, list):
        raise ValueError("Logged QoS shape-call bounds are required")
    server_calls = [call for call in shape_calls if isinstance(call, dict) and call.get("physical_side") == "server"]
    rate_limited = [call for call in server_calls if call.get("rate_mbps") == 10]
    if not rate_limited:
        raise ValueError("No 10 Mbps server-side QUIC-link command was recorded")
    first_limited = min(rate_limited, key=lambda call: call["command_start_s_from_probe_marker"])
    restores = [
        call
        for call in server_calls
        if call.get("rate_mbps") == 200
        and call.get("command_start_s_from_probe_marker") > first_limited["command_start_s_from_probe_marker"]
    ]
    if not restores:
        raise ValueError("The restore to 200 Mbps must bound the logged QoS command interval")
    qos_start = _number(first_limited.get("command_end_s_from_probe_marker"), "QoS 10 Mbps command end")
    qos_end = _number(min(restores, key=lambda call: call["command_start_s_from_probe_marker"]).get("command_start_s_from_probe_marker"), "QoS restore command start")
    if not 0 <= qos_start < qos_end <= 40:
        raise ValueError("Logged QoS command bounds fall outside the transfer")

    actual_outage = interventions.get("udp_outage")
    transitions = actual_outage.get("transition_points") if isinstance(actual_outage, dict) else None
    if not isinstance(transitions, list):
        raise ValueError("Logged UDP transition points are required")
    enabled = [point for point in transitions if isinstance(point, dict) and point.get("enabled") is True]
    disabled = [point for point in transitions if isinstance(point, dict) and point.get("enabled") is False]
    if len(enabled) != 1 or len(disabled) != 1:
        raise ValueError("Expected one UDP outage enable and disable transition")
    outage_start = _number(enabled[0].get("event_point_s_from_probe_marker"), "UDP outage enable point")
    outage_end = _number(disabled[0].get("event_point_s_from_probe_marker"), "UDP outage disable point")
    if not 0 <= outage_start < outage_end <= 40:
        raise ValueError("UDP transition points fall outside the transfer")

    transfer = row.get("whole_transfer")
    if not isinstance(transfer, dict) or transfer.get("duration_s", 0) < 40:
        raise ValueError("Whole-transfer average and duration are required")
    average = _number(transfer.get("goodput_mbps"), "whole_transfer.goodput_mbps")
    return {
        "row": row,
        "version": version,
        "revision": revision,
        "bins": bin_values,
        "echo_times": echo_times,
        "echo_latencies": echo_latencies,
        "echo_p95": echo_p95,
        "qos_start": qos_start,
        "qos_end": qos_end,
        "outage_start": outage_start,
        "outage_end": outage_end,
        "average": average,
    }


def render_independent_paths(
    data_path: Path = DEFAULT_INDEPENDENT_DATA,
    svg_path: Path = DEFAULT_INDEPENDENT_SVG,
    png_path: Path = DEFAULT_INDEPENDENT_PNG,
) -> None:
    """Plot every raw download bin and the concurrent sequential echo replies."""
    observation = load_independent_observation(data_path)
    version = observation["version"].replace("mptunnel ", "MPTUNNEL ")
    revision = observation["revision"]
    qos_start = observation["qos_start"]
    qos_end = observation["qos_end"]
    outage_start = observation["outage_start"]
    outage_end = observation["outage_end"]
    bins = observation["bins"]
    echo_times = observation["echo_times"]
    echo_latencies = observation["echo_latencies"]
    echo_p95 = observation["echo_p95"]
    average = observation["average"]

    matplotlib.rcParams["svg.hashsalt"] = "mptunnel-independent-paths-v1"
    title = "Aggregation and recovery over two links"
    description = (
        "The upper time series shows all forty raw one-second bins of aggregate application download throughput, "
        "including startup. It combines 3 TCP carriers on one 200 Mbps link with 1 QUIC carrier on another "
        "200 Mbps link. The dashed horizontal line is the configured 200 Mbps capacity of one link; it is a "
        "setting reference, not a separately measured TCP throughput series. The TCP link remains configured "
        "at 200 Mbps while the QUIC link is shaped. The original 0–15 seconds are the baseline period. "
        f"The shaded QUIC command window spans {qos_start:.2f}–{qos_end:.2f} seconds. The shaded UDP block "
        f"runs between recorded transition points at {outage_start:.2f} and {outage_end:.2f} seconds. The lower "
        f"panel plots all eighty successful concurrent echo replies; their measured p95 is {echo_p95:.1f} ms. "
        f"Whole-transfer aggregate download average: {average:.1f} Mbps. Measured build: {version}, source {revision[:8]}."
    )
    fig = plt.figure(figsize=(8.2, 8.2), facecolor="#FFFFFF")
    throughput_ax, echo_ax = fig.subplots(
        2,
        1,
        sharex=True,
        gridspec_kw={"left": 0.14, "right": 0.965, "bottom": 0.16, "top": 0.72, "hspace": 0.34},
    )
    fig.text(
        0.035,
        0.962,
        title,
        ha="left",
        va="top",
        fontsize=19.0,
        fontweight="semibold",
        color="#192B3A",
    )
    fig.text(
        0.035,
        0.910,
        "3 TCP carriers on one 200 Mbps link · 1 QUIC carrier on another",
        ha="left",
        va="top",
        fontsize=12.0,
        color="#435563",
    )
    fig.text(
        0.035,
        0.878,
        "0–15 s is baseline · shaded bands mark QUIC restriction and UDP outage",
        ha="left",
        va="top",
        fontsize=12.0,
        color="#435563",
    )

    qos_color = "#D58B24"
    outage_color = "#C76558"
    for axis in (throughput_ax, echo_ax):
        axis.axvspan(qos_start, qos_end, facecolor=qos_color, alpha=0.14, linewidth=0, zorder=0)
        axis.axvspan(outage_start, outage_end, facecolor=outage_color, alpha=0.14, linewidth=0, zorder=0)
        axis.set_xlim(0, 40)
        axis.set_facecolor("#FFFFFF")
        axis.set_axisbelow(True)
        axis.xaxis.grid(True, color="#E5E9ED", linewidth=0.8, zorder=0)
        axis.yaxis.grid(True, color="#E5E9ED", linewidth=0.8, zorder=0)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_color("#AAB4BC")
        axis.spines["bottom"].set_color("#AAB4BC")
        axis.spines["left"].set_linewidth(0.8)
        axis.spines["bottom"].set_linewidth(0.8)
        axis.tick_params(axis="both", labelsize=11.5, colors="#52616D", length=0, pad=7)

    throughput_ax.stairs(
        bins,
        list(range(len(bins) + 1)),
        color=SYSTEM_COLORS["mixed"],
        linewidth=2.0,
        zorder=3,
    )
    throughput_ax.axhline(
        200,
        color="#586875",
        linewidth=1.35,
        linestyle=(0, (4, 3)),
        zorder=2,
    )
    throughput_ax.set_ylim(0, 700)
    throughput_ax.set_yticks([0, 200, 400, 600])
    throughput_ax.set_ylabel("Mbps", fontsize=11.5, color="#435563", labelpad=9)
    throughput_ax.set_title(
        "Download speed · application delivery",
        loc="left",
        pad=10,
        fontsize=12.4,
        fontweight="semibold",
        color="#243847",
    )

    echo_ax.scatter(
        echo_times,
        echo_latencies,
        s=17,
        color=SYSTEM_COLORS["xray"],
        edgecolors="#FFFFFF",
        linewidths=0.4,
        zorder=3,
    )
    echo_ax.axhline(echo_p95, color=SYSTEM_COLORS["xray"], linewidth=1.2, linestyle=(0, (4, 3)), zorder=2)
    echo_ax.set_ylim(0, 400)
    echo_ax.set_yticks([0, 100, 200, 300, 400])
    echo_ax.set_ylabel("Latency (ms)", fontsize=11.5, color="#435563", labelpad=9)
    echo_ax.set_title(
        "Response time · 80/80 echo replies",
        loc="left",
        pad=10,
        fontsize=12.4,
        fontweight="semibold",
        color="#243847",
    )
    echo_ax.text(
        0.987,
        echo_p95 + 9,
        f"p95 {echo_p95:.1f} ms",
        transform=echo_ax.get_yaxis_transform(),
        ha="right",
        va="bottom",
        fontsize=11.1,
        color="#694D88",
        bbox={"facecolor": "#FFFFFF", "edgecolor": "none", "alpha": 0.88, "pad": 1.5},
    )
    echo_ax.set_xticks(list(range(0, 41, 5)))
    echo_ax.set_xlabel("Time from download start (s)", fontsize=11.5, color="#435563", labelpad=9)

    legend_handles = [
        Line2D(
            [0],
            [0],
            color="#586875",
            linewidth=1.35,
            linestyle=(0, (4, 3)),
            label="One link: 200 Mbps",
        ),
        Patch(
            facecolor=qos_color,
            edgecolor="none",
            alpha=0.22,
            label="QUIC at 10 Mbps",
        ),
        Patch(
            facecolor=outage_color,
            edgecolor="none",
            alpha=0.22,
            label="UDP outage",
        ),
    ]
    fig.legend(
        handles=legend_handles,
        loc="upper right",
        bbox_to_anchor=(0.965, 0.827),
        frameon=False,
        ncol=3,
        columnspacing=1.35,
        handlelength=1.9,
        handletextpad=0.55,
        borderaxespad=0,
        fontsize=11.0,
        labelcolor="#435563",
    )
    fig.text(
        0.035,
        0.053,
        f"Linux · {version}-dev ({revision[:8]}) · 40 s download",
        ha="left",
        va="top",
        fontsize=11.2,
        color="#52616D",
    )

    svg_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        png_path,
        format="png",
        dpi=110,
        facecolor="#FFFFFF",
        metadata={"Title": title, "Description": description, "Software": "Matplotlib"},
    )
    fig.savefig(
        svg_path,
        format="svg",
        facecolor="#FFFFFF",
        metadata={
            "Title": title,
            "Description": description,
            "Creator": "docs/assets/performance/render.py",
            "Date": None,
        },
    )
    plt.close(fig)
    _add_accessible_svg_header(svg_path, title, description)
    print(f"Validated current download bins/echoes and event offsets from {data_path}")
    print(f"Rendered {svg_path} and {png_path}")


def render(
    data_path: Path = DEFAULT_DATA,
    svg_path: Path = DEFAULT_SVG,
    png_path: Path = DEFAULT_PNG,
    narrow_svg_path: Path = DEFAULT_NARROW_SVG,
    independent_data_path: Path = DEFAULT_INDEPENDENT_DATA,
    independent_svg_path: Path = DEFAULT_INDEPENDENT_SVG,
    independent_png_path: Path = DEFAULT_INDEPENDENT_PNG,
) -> None:
    document = json.loads(data_path.read_text(encoding="utf-8"))
    conditions, input_rows = validate_observations(document)
    comparison = document["competitive"]
    rows = sorted(input_rows, key=lambda row: row["goodput_mbps"], reverse=True)
    labels = [SYSTEM_CHART_LABELS[row["system"]] for row in rows]
    colors = [SYSTEM_COLORS[row["system"]] for row in rows]
    y = list(range(len(rows)))
    throughput = [row["goodput_mbps"] for row in rows]
    reply_p95 = [row["echo_p95_ms"] for row in rows]
    matplotlib.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 11.3,
            "svg.fonttype": "none",
            "svg.hashsalt": "mptunnel-public-shared-link-tradeoffs-v1",
            "axes.unicode_minus": False,
        }
    )

    fig = plt.figure(figsize=(13.4, 7.4), facecolor="#FFFFFF")
    axes = fig.subplots(
        1,
        2,
        sharey=True,
        gridspec_kw={"left": 0.245, "right": 0.965, "bottom": 0.205, "top": 0.715, "wspace": 0.19},
    )
    throughput_ax, latency_ax = axes
    fig.text(
        0.035,
        0.962,
        TITLE,
        ha="left",
        va="top",
        fontsize=20,
        fontweight="semibold",
        color="#192B3A",
    )
    fig.text(
        0.035,
        0.910,
        "Separate product runs · shared bottleneck: 500 down / 100 up Mbps",
        ha="left",
        va="top",
        fontsize=12.3,
        color="#435563",
    )
    fig.text(
        0.035,
        0.878,
        "40 s whole-run averages include startup · jitter clears by ~8–9 s",
        ha="left",
        va="top",
        fontsize=12.3,
        color="#435563",
    )

    for axis, values, heading, xmax, xticks in (
        (
            throughput_ax,
            throughput,
            "Download throughput\n(Mbps · higher is better)",
            570,
            [0, 100, 200, 300, 400, 500],
        ),
        (
            latency_ax,
            reply_p95,
            "Loaded reply p95\n(ms · lower is better)",
            940,
            [0, 200, 400, 600, 800],
        ),
    ):
        axis.set_facecolor("#FFFFFF")
        axis.barh(y, values, height=0.58, color=colors, edgecolor="none", zorder=3)
        axis.set_xlim(0, xmax)
        axis.set_xticks(xticks)
        axis.set_yticks(y, labels)
        axis.set_title(heading, loc="left", pad=11, fontsize=12.5, fontweight="semibold", color="#243847")
        axis.tick_params(axis="x", labelsize=11.3, colors="#52616D", length=0, pad=8)
        axis.tick_params(axis="y", labelsize=12.0, colors="#263746", length=0, pad=12)
        axis.xaxis.grid(True, color="#E5E9ED", linewidth=0.8, zorder=0)
        axis.set_axisbelow(True)
        axis.spines["top"].set_visible(False)
        axis.spines["right"].set_visible(False)
        axis.spines["left"].set_visible(False)
        axis.spines["bottom"].set_color("#AAB4BC")
        axis.spines["bottom"].set_linewidth(0.8)
        for value, row_y in zip(values, y):
            axis.text(
                value + xmax * 0.017,
                row_y,
                f"{value:.1f}",
                ha="left",
                va="center",
                fontsize=11.3,
                color="#243847",
                clip_on=False,
            )

    throughput_ax.axvline(0, color="#586875", linewidth=1.0, zorder=2)
    throughput_ax.axvline(
        500,
        color="#586875",
        linewidth=1.4,
        linestyle=(0, (4, 3)),
        zorder=2,
    )
    throughput_ax.invert_yaxis()
    latency_ax.axvline(0, color="#586875", linewidth=1.0, zorder=2)
    latency_ax.tick_params(axis="y", labelleft=False)

    capacity_handle = Line2D(
        [0],
        [0],
        color="#586875",
        linewidth=1.4,
        linestyle=(0, (4, 3)),
        label="Configured download capacity: 500 Mbps",
    )
    fig.legend(
        handles=[capacity_handle],
        loc="upper right",
        bbox_to_anchor=(0.965, 0.842),
        frameon=False,
        handlelength=2.5,
        handletextpad=0.7,
        borderaxespad=0,
        fontsize=11.3,
        labelcolor="#435563",
    )

    measured_version = comparison["_measured_version"].replace("mptunnel ", "MPTUNNEL ")
    revision = comparison["_measured_revision"]
    footer_lines = [
        f"Linux · 40 s per system · {measured_version}-dev ({revision[:8]})",
        "Concurrent echo probes all succeeded.",
    ]
    for text, y_position in zip(footer_lines, (0.102, 0.052)):
        fig.text(
            0.035,
            y_position,
            text,
            ha="left",
            va="top",
            fontsize=11.5,
            color="#52616D",
        )

    description = _accessible_description(rows)
    svg_path.parent.mkdir(parents=True, exist_ok=True)
    png_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        png_path,
        format="png",
        dpi=110,
        facecolor="#FFFFFF",
        metadata={"Title": TITLE, "Description": description, "Software": "Matplotlib"},
    )
    fig.savefig(
        svg_path,
        format="svg",
        facecolor="#FFFFFF",
        metadata={
            "Title": TITLE,
            "Description": description,
            "Creator": "docs/assets/performance/render.py",
            "Date": None,
        },
    )
    plt.close(fig)
    _add_accessible_svg_header(svg_path, TITLE, description)
    render_narrow_tradeoffs(rows, narrow_svg_path)
    print(f"Validated {len(rows)} single observations from {data_path}")
    print(f"Rendered {svg_path}, {png_path}, and {narrow_svg_path}")
    render_independent_paths(independent_data_path, independent_svg_path, independent_png_path)


def render_shared_timeline() -> None:
    """Show every delivery bin and echo attempt from the five shared-link runs."""
    data_path = FIGURE_DIR / "shared-link-series.json"
    document = json.loads(data_path.read_text(encoding="utf-8"))
    if document.get("schema") != "mptunnel.shared-link-series.v1":
        raise ValueError("Unexpected shared-link series schema")
    series = {row["system"]: row for row in document["series"]}
    if set(series) != EXPECTED_SYSTEMS or len(document["series"]) != len(series):
        raise ValueError("Shared timeline needs one observation for each system")
    order = ["mixed", "quic", "h2", "xray", "raw"]
    colors = {**SYSTEM_COLORS, "quic": "#238C83"}
    markers = {"mixed": "^", "quic": "o", "h2": "D", "xray": "x", "raw": "+"}
    max_rate = max_latency = 0.0
    for system in order:
        row = series[system]
        bins = row["goodput"]["raw_bins"]
        if row["goodput"]["interval_seconds"] != 1 or len(bins) != 40:
            raise ValueError(f"{system}: expected all forty one-second bins")
        max_rate = max(max_rate, *(_number(v, "goodput bin") for v in bins))
        attempts = row["echo_attempts"]
        summary = row["echo_summary"]
        if len(attempts) != summary["attempt_count"] or summary["failure_count"] != 0:
            raise ValueError(f"{system}: echo outcomes differ from the stated comparison")
        if any(a["outcome"] != "success" for a in attempts):
            raise ValueError(f"{system}: unexpected failed echo")
        max_latency = max(max_latency, *(_number(a["latency_ms"], "echo latency") for a in attempts))

    matplotlib.rcParams["svg.hashsalt"] = "mptunnel-shared-link-timeline-v1"
    title = "Download goodput and echo latency"
    fig, axes = plt.subplots(2, 1, figsize=(10.4, 8.7), sharex=True,
                             gridspec_kw={"height_ratios": [1.1, 1]}, facecolor="white")
    fig.subplots_adjust(left=0.115, right=0.97, top=0.76, bottom=0.12, hspace=0.38)
    fig.text(0.04, 0.96, title, fontsize=19, fontweight="semibold", color="#192B3A")
    fig.text(0.04, 0.921,
             "Separate product runs · shared 500/100 Mbps bottleneck · 40 s includes startup · jitter clears by ~8–9 s",
             fontsize=11.5, color="#435563")
    handles = [Line2D([0], [0], color=colors[s], lw=2, marker=markers[s],
                      markersize=4, label=SYSTEM_CHART_LABELS[s]) for s in order]
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(0.04, 0.898),
               ncol=3, frameon=False, fontsize=10.5, handlelength=1.5, columnspacing=1.6)
    clear_start = min(row["jitter_clear"]["union_s"][0] for row in series.values())
    clear_end = max(row["jitter_clear"]["union_s"][1] for row in series.values())
    for ax in axes:
        ax.set_axisbelow(True)
        ax.grid(axis="y", color="#DCE4EB", linewidth=0.6)
        ax.spines[["top", "right"]].set_visible(False)
        ax.spines[["left", "bottom"]].set_color("#9CACB9")
        ax.tick_params(colors="#435563", labelsize=10.5)
        ax.set_xlim(0, 40)
        ax.set_xticks(range(0, 41, 5))
        ax.axvspan(clear_start, clear_end, color="#D58B24", alpha=0.16, zorder=0)
    for system in order:
        row = series[system]
        axes[0].stairs(row["goodput"]["raw_bins"], list(range(41)),
                       color=colors[system], linewidth=1.5, alpha=0.95)
        attempts = row["echo_attempts"]
        axes[1].scatter([a["start_offset_s"] for a in attempts],
                        [a["latency_ms"] for a in attempts],
                        color=colors[system], marker=markers[system], s=13, linewidths=0.8)
    axes[0].set_title("Download delivery · every second", loc="left", pad=10,
                      fontsize=12.5, fontweight="semibold", color="#192B3A")
    axes[0].set_ylabel("Mbps", fontsize=11.5, color="#435563")
    axes[0].set_ylim(0, math.ceil(max(500, max_rate) / 100) * 100 + 25)
    axes[0].axhline(500, color="#81909C", linewidth=0.9, linestyle=(0, (4, 3)))
    axes[0].text(39.4, 509, "500 Mbps configured capacity", ha="right", fontsize=9.5,
                 color="#52616D", bbox={"facecolor":"white", "edgecolor":"none", "pad":1.4})
    axes[0].annotate("Jitter clears", xy=((clear_start + clear_end) / 2, axes[0].get_ylim()[1] * 0.88),
                     xytext=(12.0, axes[0].get_ylim()[1] * 0.93),
                     fontsize=10.5, color="#87611F", arrowprops={"arrowstyle":"-", "color":"#87611F"})
    axes[1].set_title("Response time · every echo request", loc="left", pad=10,
                      fontsize=12.5, fontweight="semibold", color="#192B3A")
    axes[1].set_ylabel("Milliseconds", fontsize=11.5, color="#435563")
    axes[1].set_ylim(0, math.ceil(max_latency / 100) * 100 + 40)
    axes[1].set_xlabel("Seconds after the transfer starts", fontsize=11.5, color="#435563")
    fig.text(0.04, 0.035, "Linux · one 40 s run per system · MPTUNNEL 0.6.0-dev (1e8abedf)",
             fontsize=10.5, color="#52616D")
    description = (
        "Two time-series panels compare MPTUNNEL TCP+QUIC with 3 TCP carriers and 1 QUIC "
        "carrier, MPTUNNEL QUIC with 1 QUIC carrier, Hysteria2, Xray VMess/TCP and direct TCP. "
        "Each product has its own 40-second run against the same physical 500 Mbps down, "
        "100 Mbps up link limit; the products did not compete simultaneously. Within each run, "
        "the product download and echo probes shared the link. The first panel shows all forty one-second download "
        "delivery bins; the second shows every echo latency at its request start time. "
        "These full-run traces include startup; jitter clears around 8–9 seconds. The shaded band spans "
        "the recorded jitter-clear commands across the five runs. "
        "The dashed horizontal line is configured capacity, 500 Mbps. "
        "All echo requests succeeded. Each system has its own capture, starting at time zero."
    )
    svg_path = FIGURE_DIR / "shared-link-timeline.svg"
    png_path = FIGURE_DIR / "shared-link-timeline.png"
    fig.savefig(svg_path, metadata={"Title": title, "Description": description,
                "Creator": "docs/assets/performance/render.py", "Date": None})
    fig.savefig(png_path, dpi=110, metadata={"Title":title, "Description":description,
                "Software":"Matplotlib"})
    plt.close(fig)
    _add_accessible_svg_header(svg_path, title, description)
    print(f"Rendered {svg_path} and {png_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA, help="curated observations JSON")
    parser.add_argument("--svg", type=Path, default=DEFAULT_SVG, help="accessible SVG output path")
    parser.add_argument("--png", type=Path, default=DEFAULT_PNG, help="PNG output path")
    parser.add_argument("--narrow-svg", type=Path, default=DEFAULT_NARROW_SVG, help="compact mobile SVG output path")
    parser.add_argument("--independent-data", type=Path, default=DEFAULT_INDEPENDENT_DATA, help="independent-path data JSON")
    parser.add_argument("--independent-svg", type=Path, default=DEFAULT_INDEPENDENT_SVG, help="independent-path SVG output")
    parser.add_argument("--independent-png", type=Path, default=DEFAULT_INDEPENDENT_PNG, help="independent-path PNG output")
    arguments = parser.parse_args()
    render(
        arguments.data,
        arguments.svg,
        arguments.png,
        arguments.narrow_svg,
        arguments.independent_data,
        arguments.independent_svg,
        arguments.independent_png,
    )
    render_shared_timeline()


if __name__ == "__main__":
    main()

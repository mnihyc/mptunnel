#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_root="${RESULT_ROOT:-lab/results/perf-diagnostics-${timestamp}}"
mkdir -p "$result_root"

compose_file="${COMPOSE_FILE:-lab/docker-compose.yml}"
result_file="${RESULT_FILE:-${result_root}/results.jsonl}"
summary_file="${PERF_SUMMARY_FILE:-${result_root}/component-summary.json}"
stats_file="${DOCKER_STATS_FILE:-${result_root}/docker-stats.jsonl}"
stats_interval="${MPTUNNEL_PERF_STATS_INTERVAL_SECONDS:-1}"
stop_file="${result_root}/.stop-sampler"

export CASE_FILTER="${CASE_FILTER:-mptunnel_mixed_single_balanced,mptunnel_reliable_mixed_single_balanced,mptunnel_udp_stream_single_balanced,mptunnel_mixed_multipath_all,mptunnel_reliable_mixed_multipath_all,mptunnel_udp_stream_multipath_all}"
export RESULT_FILE="$result_file"
export MPTUNNEL_LAB_DIAGNOSTICS="${MPTUNNEL_LAB_DIAGNOSTICS:-1}"
export MPTUNNEL_LAB_DIAG="${MPTUNNEL_LAB_DIAG:-1}"
export MPTUNNEL_LAB_PERF="${MPTUNNEL_LAB_PERF:-1}"
export MPTUNNEL_LAB_PERF_INTERVAL_MS="${MPTUNNEL_LAB_PERF_INTERVAL_MS:-1000}"
export MPTUNNEL_LAB_LOAD_DURATION_SECONDS="${MPTUNNEL_LAB_LOAD_DURATION_SECONDS:-20}"
export MPTUNNEL_LAB_LOG_TAIL_BYTES="${MPTUNNEL_LAB_LOG_TAIL_BYTES:-24000}"
export MPTUNNEL_LAB_LOG_TAIL_LINES="${MPTUNNEL_LAB_LOG_TAIL_LINES:-360}"

compose() {
  docker compose -f "$compose_file" "$@"
}

sample_docker_stats() {
  while [[ ! -f "$stop_file" ]]; do
    local now
    local -a ids
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    mapfile -t ids < <(compose ps -q client server target 2>/dev/null || true)
    if (( ${#ids[@]} > 0 )); then
      # docker stats is sampled outside the release build and records container-level CPU/RAM.
      docker stats --no-stream --format '{{json .}}' "${ids[@]}" 2>/dev/null \
        | NOW="$now" python3 -c 'import os, sys; [print(f"""{{"ts":"{os.environ["NOW"]}","docker_stats":{line.rstrip()}}}""") for line in sys.stdin if line.strip()]' \
        >> "$stats_file" || true
    fi
    sleep "$stats_interval"
  done
}

summarize_component_perf() {
  python3 - "$result_file" "$summary_file" <<'PY'
import json
import re
import sys
from pathlib import Path

result_file = Path(sys.argv[1])
summary_file = Path(sys.argv[2])
field_re = re.compile(r"(\w+)=([^ ]+)")
component_rows = {}

def parse_perf_line(line):
    if not line.startswith("mptunnel_lab_perf "):
        return None
    fields = dict(field_re.findall(line))
    component = fields.get("component")
    if not component:
        return None
    parsed = {"component": component}
    for key in (
        "interval_count",
        "interval_bytes",
        "interval_total_us",
        "interval_avg_us",
        "interval_max_us",
        "total_count",
        "total_bytes",
        "total_us",
        "total_avg_us",
        "total_max_us",
        "pid",
    ):
        value = fields.get(key)
        if value is not None:
            try:
                parsed[key] = int(value)
            except ValueError:
                parsed[key] = value
    return parsed

if result_file.exists():
    for raw in result_file.read_text().splitlines():
        if not raw.strip():
            continue
        row = json.loads(raw)
        case = row.get("case", "unknown")
        for log_field, side in (("client_log_tail", "client"), ("server_log_tail", "server")):
            for line in row.get(log_field, "").splitlines():
                perf = parse_perf_line(line)
                if not perf:
                    continue
                key = (case, side, perf["component"])
                current = component_rows.get(key)
                if current is None or perf.get("total_count", 0) >= current.get("total_count", 0):
                    component_rows[key] = {"case": case, "side": side, **perf}

side_totals = {}
for row in component_rows.values():
    side_key = (row["case"], row["side"])
    side_totals[side_key] = side_totals.get(side_key, 0) + row.get("total_us", 0)

rows = []
for row in component_rows.values():
    total_us = side_totals.get((row["case"], row["side"]), 0)
    share = (row.get("total_us", 0) / total_us) if total_us else 0.0
    rows.append({**row, "component_time_share": share})

rows.sort(key=lambda item: (item["case"], item["side"], -item.get("total_us", 0)))
summary = {
    "result_file": str(result_file),
    "component_count": len(rows),
    "components": rows,
}
summary_file.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")

for row in rows[:40]:
    print(
        f'{row["case"]} {row["side"]} {row["component"]} '
        f'total_us={row.get("total_us", 0)} total_count={row.get("total_count", 0)} '
        f'total_bytes={row.get("total_bytes", 0)} share={row["component_time_share"]:.3f}'
    )
PY
}

rm -f "$stop_file"
sample_docker_stats &
sampler_pid="$!"
cleanup() {
  touch "$stop_file"
  wait "$sampler_pid" 2>/dev/null || true
}
trap cleanup EXIT

"$script_dir/run-heterogeneous-ablation.sh"
cleanup
trap - EXIT

summarize_component_perf

printf 'results=%s\n' "$result_file"
printf 'component_summary=%s\n' "$summary_file"
printf 'docker_stats=%s\n' "$stats_file"

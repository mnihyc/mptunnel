#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

compose_file="${COMPOSE_FILE:-lab/docker-compose.yml}"
result_dir="${RESULT_DIR:-lab/results}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="${RESULT_FILE:-$result_dir/heterogeneous-$timestamp.jsonl}"
object_mib="${MPTUNNEL_LAB_OBJECT_MIB:-1024}"
large_http_path="${MPTUNNEL_LAB_LARGE_HTTP_PATH:-/large.bin}"
small_http_path="${MPTUNNEL_LAB_SMALL_HTTP_PATH:-/small.bin}"
small_object_kib="${MPTUNNEL_LAB_SMALL_OBJECT_KIB:-32}"
load_duration_seconds="${MPTUNNEL_LAB_LOAD_DURATION_SECONDS:-30}"
bulk_connections="${MPTUNNEL_LAB_BULK_CONNECTIONS:-2}"
proxy_port="${PROXY_PORT:-1080}"
baseline_proxy_port="${BASELINE_PROXY_PORT:-1090}"
server_port="${SERVER_PORT:-7443}"
baseline_vmess_port="${BASELINE_VMESS_PORT:-18443}"
baseline_hysteria2_port="${BASELINE_HYSTERIA2_PORT:-18444}"
baseline_mptcp_port="${BASELINE_MPTCP_PORT:-18081}"
curl_timeout="${CURL_TIMEOUT_SECONDS:-120}"
udp_payload_bytes="${UDP_PAYLOAD_BYTES:-512}"
udp_timeout_ms="${UDP_TIMEOUT_MS:-2500}"
tcp_echo_payload_bytes="${TCP_ECHO_PAYLOAD_BYTES:-64}"
tcp_echo_timeout_ms="${TCP_ECHO_TIMEOUT_MS:-5000}"
tcp_echo_interval_ms="${TCP_ECHO_INTERVAL_MS:-500}"
failover_after="${FAILOVER_AFTER_SECONDS:-2}"
build_product="${BUILD_PRODUCT:-1}"
build_lab_images="${BUILD_LAB_IMAGES:-1}"
lab_diagnostics="${MPTUNNEL_LAB_DIAGNOSTICS:-0}"
lab_perf="${MPTUNNEL_LAB_PERF:-0}"
lab_perf_interval_ms="${MPTUNNEL_LAB_PERF_INTERVAL_MS:-1000}"
lab_log_level="${MPTUNNEL_LAB_LOG:-info}"
case "$lab_perf" in
  1|true|TRUE|yes|YES)
    log_tail_bytes="${MPTUNNEL_LAB_LOG_TAIL_BYTES:-16000}"
    log_tail_lines="${MPTUNNEL_LAB_LOG_TAIL_LINES:-240}"
    ;;
  *)
    log_tail_bytes="${MPTUNNEL_LAB_LOG_TAIL_BYTES:-4000}"
    log_tail_lines="${MPTUNNEL_LAB_LOG_TAIL_LINES:-120}"
    ;;
esac
case_filter="${CASE_FILTER:-}"
client_settle_seconds="${CLIENT_SETTLE_SECONDS:-2}"
isolate_cases="${ISOLATE_CASES:-1}"
isolate_containers="${ISOLATE_CONTAINERS_PER_CASE:-1}"
if [[ -n "${MPTUNNEL_LAB_SECRET:-}" ]]; then
  secret="$MPTUNNEL_LAB_SECRET"
else
  secret="$(python3 -c 'import uuid; print(uuid.uuid4())')"
fi
baseline_uuid="${BASELINE_UUID:-$(SECRET="$secret" python3 -c 'import os, uuid; print(uuid.uuid5(uuid.NAMESPACE_URL, os.environ["SECRET"]))')}"
saturate_protocol="${MPTUNNEL_LAB_SATURATE_PROTOCOL:-udp}"
saturate_udp_packet_bytes="${MPTUNNEL_LAB_SATURATE_UDP_PACKET_BYTES:-1200}"
saturate_tcp_parallel="${MPTUNNEL_LAB_SATURATE_TCP_PARALLEL:-4}"
saturate_lowlat_bandwidth="${MPTUNNEL_LAB_SATURATE_LOWLAT_BANDWIDTH:-40M}"
saturate_balanced_bandwidth="${MPTUNNEL_LAB_SATURATE_BALANCED_BANDWIDTH:-160M}"
saturate_fat_bandwidth="${MPTUNNEL_LAB_SATURATE_FAT_BANDWIDTH:-400M}"
saturate_poor_bandwidth="${MPTUNNEL_LAB_SATURATE_POOR_BANDWIDTH:-12M}"
flap_min_seconds="${MPTUNNEL_LAB_FLAP_MIN_SECONDS:-1}"
flap_max_seconds="${MPTUNNEL_LAB_FLAP_MAX_SECONDS:-4}"
flap_modes="${MPTUNNEL_LAB_FLAP_MODES:-apply-lowlat,apply-balanced,apply-fat,apply-poor,spike-lowlat,spike-balanced,spike-fat,spike-poor,blackhole-lowlat,blackhole-balanced,blackhole-fat,blackhole-poor}"
flapper_pid=""
flapper_stop_file=""

compose() {
  docker compose -f "$compose_file" "$@"
}

exec_in() {
  local service="$1"
  shift
  compose exec -T "$service" bash -lc "$*"
}

exec_netem() {
  local service="$1"
  local mode="$2"
  compose exec -T \
    -e MPTUNNEL_LAB_LOWLAT_RATE="${MPTUNNEL_LAB_LOWLAT_RATE:-30mbit}" \
    -e MPTUNNEL_LAB_LOWLAT_DELAY="${MPTUNNEL_LAB_LOWLAT_DELAY:-20ms}" \
    -e MPTUNNEL_LAB_LOWLAT_JITTER="${MPTUNNEL_LAB_LOWLAT_JITTER:-2ms}" \
    -e MPTUNNEL_LAB_LOWLAT_LOSS="${MPTUNNEL_LAB_LOWLAT_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_BALANCED_RATE="${MPTUNNEL_LAB_BALANCED_RATE:-120mbit}" \
    -e MPTUNNEL_LAB_BALANCED_DELAY="${MPTUNNEL_LAB_BALANCED_DELAY:-80ms}" \
    -e MPTUNNEL_LAB_BALANCED_JITTER="${MPTUNNEL_LAB_BALANCED_JITTER:-10ms}" \
    -e MPTUNNEL_LAB_BALANCED_LOSS="${MPTUNNEL_LAB_BALANCED_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_FAT_RATE="${MPTUNNEL_LAB_FAT_RATE:-300mbit}" \
    -e MPTUNNEL_LAB_FAT_DELAY="${MPTUNNEL_LAB_FAT_DELAY:-180ms}" \
    -e MPTUNNEL_LAB_FAT_JITTER="${MPTUNNEL_LAB_FAT_JITTER:-20ms}" \
    -e MPTUNNEL_LAB_FAT_LOSS="${MPTUNNEL_LAB_FAT_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_POOR_RATE="${MPTUNNEL_LAB_POOR_RATE:-8mbit}" \
    -e MPTUNNEL_LAB_POOR_DELAY="${MPTUNNEL_LAB_POOR_DELAY:-420ms}" \
    -e MPTUNNEL_LAB_POOR_JITTER="${MPTUNNEL_LAB_POOR_JITTER:-120ms}" \
    -e MPTUNNEL_LAB_POOR_LOSS="${MPTUNNEL_LAB_POOR_LOSS:-10.00%}" \
    -e MPTUNNEL_LAB_IDEAL_LOSS="${MPTUNNEL_LAB_IDEAL_LOSS:-0.00%}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_RATE="${MPTUNNEL_LAB_MATRIX_GOOD_RATE:-200mbit}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_RATE="${MPTUNNEL_LAB_MATRIX_POOR_RATE:-25mbit}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_DELAY="${MPTUNNEL_LAB_MATRIX_GOOD_DELAY:-50ms}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_DELAY="${MPTUNNEL_LAB_MATRIX_POOR_DELAY:-250ms}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_JITTER="${MPTUNNEL_LAB_MATRIX_GOOD_JITTER:-5ms}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_JITTER="${MPTUNNEL_LAB_MATRIX_POOR_JITTER:-60ms}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_LOSS="${MPTUNNEL_LAB_MATRIX_GOOD_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_LOSS="${MPTUNNEL_LAB_MATRIX_POOR_LOSS:-15.00%}" \
    -e MPTUNNEL_LAB_BLACKHOLE_LOSS="${MPTUNNEL_LAB_BLACKHOLE_LOSS:-100%}" \
    -e MPTUNNEL_LAB_SPIKE_FAT_RATE="${MPTUNNEL_LAB_SPIKE_FAT_RATE:-20mbit}" \
    -e MPTUNNEL_LAB_SPIKE_FAT_DELAY="${MPTUNNEL_LAB_SPIKE_FAT_DELAY:-900ms}" \
    -e MPTUNNEL_LAB_SPIKE_FAT_JITTER="${MPTUNNEL_LAB_SPIKE_FAT_JITTER:-250ms}" \
    -e MPTUNNEL_LAB_SPIKE_FAT_LOSS="${MPTUNNEL_LAB_SPIKE_FAT_LOSS:-10.00%}" \
    -e MPTUNNEL_LAB_SPIKE_LOWLAT_RATE="${MPTUNNEL_LAB_SPIKE_LOWLAT_RATE:-10mbit}" \
    -e MPTUNNEL_LAB_SPIKE_LOWLAT_DELAY="${MPTUNNEL_LAB_SPIKE_LOWLAT_DELAY:-650ms}" \
    -e MPTUNNEL_LAB_SPIKE_LOWLAT_JITTER="${MPTUNNEL_LAB_SPIKE_LOWLAT_JITTER:-180ms}" \
    -e MPTUNNEL_LAB_SPIKE_LOWLAT_LOSS="${MPTUNNEL_LAB_SPIKE_LOWLAT_LOSS:-10.00%}" \
    -e MPTUNNEL_LAB_SPIKE_BALANCED_RATE="${MPTUNNEL_LAB_SPIKE_BALANCED_RATE:-25mbit}" \
    -e MPTUNNEL_LAB_SPIKE_BALANCED_DELAY="${MPTUNNEL_LAB_SPIKE_BALANCED_DELAY:-500ms}" \
    -e MPTUNNEL_LAB_SPIKE_BALANCED_JITTER="${MPTUNNEL_LAB_SPIKE_BALANCED_JITTER:-140ms}" \
    -e MPTUNNEL_LAB_SPIKE_BALANCED_LOSS="${MPTUNNEL_LAB_SPIKE_BALANCED_LOSS:-15.00%}" \
    -e MPTUNNEL_LAB_SPIKE_POOR_RATE="${MPTUNNEL_LAB_SPIKE_POOR_RATE:-2mbit}" \
    -e MPTUNNEL_LAB_SPIKE_POOR_DELAY="${MPTUNNEL_LAB_SPIKE_POOR_DELAY:-1200ms}" \
    -e MPTUNNEL_LAB_SPIKE_POOR_JITTER="${MPTUNNEL_LAB_SPIKE_POOR_JITTER:-350ms}" \
    -e MPTUNNEL_LAB_SPIKE_POOR_LOSS="${MPTUNNEL_LAB_SPIKE_POOR_LOSS:-25.00%}" \
    "$service" bash -lc "/workspace/lab/configure-netem.sh '$mode'"
}

build_mptunnel_binary() {
  if [[ "$lab_diagnostics" == "1" ]]; then
    cargo build --release --bin mptunnel --features lab-diagnostics
  else
    cargo build --release --bin mptunnel
  fi
}

append_skipped_result() {
  local case_name="$1"
  local protocol="$2"
  local reason="$3"
  CASE_NAME="$case_name" PROTOCOL="$protocol" REASON="$reason" python3 - <<'PY' >> "$result_file"
import json
import os

print(json.dumps({
    "case": os.environ["CASE_NAME"],
    "protocol": os.environ["PROTOCOL"],
    "status": "skipped",
    "reason": os.environ["REASON"],
}, sort_keys=True))
PY
}

append_download_probe_result() {
  local case_name="$1"
  local exit_code="$2"
  local output="$3"
  local probe_stderr="$4"
  local client_log server_log

  client_log="$(exec_in client "for file in /tmp/mptunnel-client-*.log; do [ -f \"\$file\" ] || continue; echo \"== \$(basename \"\$file\") ==\"; tail -n '${log_tail_lines}' \"\$file\"; done | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  server_log="$(exec_in server "tail -n '${log_tail_lines}' /tmp/mptunnel-server.log 2>/dev/null | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"

  ROW="$output" \
  EXIT_CODE="$exit_code" \
  PROBE_STDERR="$probe_stderr" \
  CLIENT_LOG="$client_log" \
  SERVER_LOG="$server_log" \
  LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
  LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
  LOG_TAIL_BYTES="$log_tail_bytes" \
  python3 - "$case_name" <<'PY' >> "$result_file"
import json
import os
import sys

case = sys.argv[1]
raw = os.environ.get("ROW", "")
try:
    row = json.loads(raw) if raw else {}
except json.JSONDecodeError:
    row = {"raw_output": raw}
if not row:
    try:
        exit_code = int(os.environ.get("EXIT_CODE", "124"))
    except ValueError:
        exit_code = 124
    row = {
        "case": case,
        "protocol": "tcp",
        "status": "fail",
        "exit_code": exit_code,
    }
lab_diag = os.environ.get("LAB_DIAG", "").lower() in ("1", "true", "yes")
lab_perf = os.environ.get("LAB_PERF", "").lower() in ("1", "true", "yes")
try:
    log_tail_bytes = int(os.environ.get("LOG_TAIL_BYTES", "4000"))
except ValueError:
    log_tail_bytes = 4000
if row.get("status") != "ok" or lab_diag or lab_perf:
    for env_name, field in (
        ("PROBE_STDERR", "probe_stderr_tail"),
        ("CLIENT_LOG", "client_log_tail"),
        ("SERVER_LOG", "server_log_tail"),
    ):
        value = os.environ.get(env_name, "")
        if value:
            row[field] = value[-log_tail_bytes:]
print(json.dumps(row, sort_keys=True))
PY
}

run_unproxied_download_probe_case() {
  local case_name="$1"
  local protocol="$2"
  local target="$3"
  local out_file="/tmp/mptunnel-probe-${case_name}.out"
  local err_file="/tmp/mptunnel-probe-${case_name}.err"
  set +e
  local output probe_stderr
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout $((curl_timeout + 10))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --protocol '${protocol}' --target '${target}' --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
}

run_tcp_download_probe_case() {
  local case_name="$1"
  local out_file="/tmp/mptunnel-probe-${case_name}.out"
  local err_file="/tmp/mptunnel-probe-${case_name}.err"
  set +e
  local output probe_stderr
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout $((curl_timeout + 10))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
}

stop_process() {
  local service="$1"
  local pid_file="$2"
  exec_in "$service" "if [ -f '$pid_file' ]; then kill \$(cat '$pid_file') >/dev/null 2>&1 || true; rm -f '$pid_file'; fi" \
    >/dev/null 2>&1 || true
}

stop_baselines() {
  for service in client server target; do
    exec_in "$service" "\
      for file in /tmp/mptunnel-baseline-*.pid; do \
        [ -f \"\$file\" ] || continue; \
        kill \$(cat \"\$file\") >/dev/null 2>&1 || true; \
        rm -f \"\$file\"; \
      done" >/dev/null 2>&1 || true
  done
}

stop_client() {
  stop_process client /tmp/mptunnel-client.pid
  sleep "$client_settle_seconds"
}

stop_server() {
  stop_process server /tmp/mptunnel-server.pid
}

stop_saturation() {
  for service in client server; do
    exec_in "$service" "\
      for file in /tmp/mptunnel-iperf-*.pid; do \
        [ -f \"\$file\" ] || continue; \
        kill \$(cat \"\$file\") >/dev/null 2>&1 || true; \
        rm -f \"\$file\"; \
      done" >/dev/null 2>&1 || true
  done
}

stop_random_flapping() {
  if [[ -n "$flapper_stop_file" ]]; then
    touch "$flapper_stop_file" >/dev/null 2>&1 || true
  fi
  if [[ -n "$flapper_pid" ]]; then
    kill "$flapper_pid" >/dev/null 2>&1 || true
    wait "$flapper_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$flapper_stop_file" ]]; then
    rm -f "$flapper_stop_file"
  fi
  flapper_pid=""
  flapper_stop_file=""
  apply_netem apply >/dev/null 2>&1 || true
}

cleanup() {
  stop_random_flapping
  stop_saturation
  stop_baselines
  stop_client
  stop_server
  if [[ "${KEEP_LAB:-0}" != "1" ]]; then
    compose down --remove-orphans >/dev/null 2>&1 || true
  fi
}

apply_netem() {
  local mode="$1"
  exec_netem client "$mode"
  exec_netem server "$mode"
  exec_netem target "$mode"
}

apply_failover_blackhole() {
  exec_netem client blackhole-fat
  exec_netem server blackhole-fat
}

apply_latency_spike_fat() {
  exec_netem client spike-fat
  exec_netem server spike-fat
}

start_saturation_pair() {
  local name="$1"
  local client_ip="$2"
  local server_ip="$3"
  local port="$4"
  local bandwidth="$5"
  local client_server_pid="/tmp/mptunnel-iperf-${name}-client-server.pid"
  local server_server_pid="/tmp/mptunnel-iperf-${name}-server-server.pid"
  local c2s_pid="/tmp/mptunnel-iperf-${name}-c2s.pid"
  local s2c_pid="/tmp/mptunnel-iperf-${name}-s2c.pid"

  stop_saturation
  exec_in client "iperf3 -s -B '${client_ip}' -p '${port}' >/tmp/mptunnel-iperf-${name}-client-server.log 2>&1 & echo \$! >'${client_server_pid}'"
  exec_in server "iperf3 -s -B '${server_ip}' -p '${port}' >/tmp/mptunnel-iperf-${name}-server-server.log 2>&1 & echo \$! >'${server_server_pid}'"
  sleep 0.5

  case "$saturate_protocol" in
    udp)
      exec_in client "while true; do iperf3 -u -c '${server_ip}' -B '${client_ip}' -p '${port}' -b '${bandwidth}' -l '${saturate_udp_packet_bytes}' -t 86400; sleep 0.2; done >/tmp/mptunnel-iperf-${name}-c2s.log 2>&1 & echo \$! >'${c2s_pid}'"
      exec_in server "while true; do iperf3 -u -c '${client_ip}' -B '${server_ip}' -p '${port}' -b '${bandwidth}' -l '${saturate_udp_packet_bytes}' -t 86400; sleep 0.2; done >/tmp/mptunnel-iperf-${name}-s2c.log 2>&1 & echo \$! >'${s2c_pid}'"
      ;;
    tcp)
      exec_in client "while true; do iperf3 -c '${server_ip}' -B '${client_ip}' -p '${port}' -P '${saturate_tcp_parallel}' -t 86400; sleep 0.2; done >/tmp/mptunnel-iperf-${name}-c2s.log 2>&1 & echo \$! >'${c2s_pid}'"
      exec_in server "while true; do iperf3 -c '${client_ip}' -B '${server_ip}' -p '${port}' -P '${saturate_tcp_parallel}' -t 86400; sleep 0.2; done >/tmp/mptunnel-iperf-${name}-s2c.log 2>&1 & echo \$! >'${s2c_pid}'"
      ;;
    *)
      echo "MPTUNNEL_LAB_SATURATE_PROTOCOL must be udp or tcp" >&2
      return 2
      ;;
  esac
  sleep 1
}

start_saturation_path() {
  local path_name="$1"
  case "$path_name" in
    lowlat)
      start_saturation_pair lowlat 172.31.10.10 172.31.10.20 5201 "$saturate_lowlat_bandwidth"
      ;;
    balanced)
      start_saturation_pair balanced 172.31.15.10 172.31.15.20 5204 "$saturate_balanced_bandwidth"
      ;;
    fat)
      start_saturation_pair fat 172.31.20.10 172.31.20.20 5202 "$saturate_fat_bandwidth"
      ;;
    poor)
      start_saturation_pair poor 172.31.30.10 172.31.30.20 5203 "$saturate_poor_bandwidth"
      ;;
    *)
      echo "unknown saturation path: $path_name" >&2
      return 2
      ;;
  esac
}

start_random_flapping() {
  local min_seconds="$flap_min_seconds"
  local max_seconds="$flap_max_seconds"
  if ! [[ "$min_seconds" =~ ^[0-9]+$ && "$max_seconds" =~ ^[0-9]+$ ]]; then
    echo "MPTUNNEL_LAB_FLAP_MIN_SECONDS and MPTUNNEL_LAB_FLAP_MAX_SECONDS must be non-negative integers" >&2
    return 2
  fi
  if (( min_seconds < 1 )); then
    min_seconds=1
  fi
  if (( max_seconds < min_seconds )); then
    max_seconds="$min_seconds"
  fi

  stop_random_flapping
  flapper_stop_file="${result_dir}/flapper-${timestamp}.stop"
  rm -f "$flapper_stop_file"
  (
    IFS=',' read -r -a modes <<< "$flap_modes"
    if (( ${#modes[@]} == 0 )); then
      exit 0
    fi
    while [[ ! -f "$flapper_stop_file" ]]; do
      mode="${modes[$((RANDOM % ${#modes[@]}))]}"
      exec_netem client "$mode" >/dev/null 2>&1 || true
      exec_netem server "$mode" >/dev/null 2>&1 || true
      if (( max_seconds == min_seconds )); then
        sleep_seconds="$min_seconds"
      else
        sleep_seconds="$((min_seconds + RANDOM % (max_seconds - min_seconds + 1)))"
      fi
      sleep "$sleep_seconds"
    done
  ) &
  flapper_pid="$!"
}

should_run_case() {
  local case_name="$1"
  local pattern

  if [[ -z "$case_filter" ]]; then
    return 0
  fi

  IFS=',' read -r -a patterns <<< "$case_filter"
  for pattern in "${patterns[@]}"; do
    # CASE_FILTER supports shell-style globs, for example mptunnel_mixed_*.
    # shellcheck disable=SC2254
    case "$case_name" in
      $pattern) return 0 ;;
    esac
  done
  return 1
}

start_target_services() {
  exec_in target "mkdir -p /tmp/mptunnel-lab && truncate -s '${object_mib}M' /tmp/mptunnel-lab/large.bin"
  exec_in target "dd if=/dev/zero of=/tmp/mptunnel-lab/small.bin bs=1K count='${small_object_kib}' status=none && printf 'mptunnel lab target\\n' >/tmp/mptunnel-lab/index.html"
  exec_in target "if [ -f /tmp/mptunnel-http.pid ]; then kill \$(cat /tmp/mptunnel-http.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-http.pid; fi"
  exec_in target "if [ -f /tmp/mptunnel-udp-echo.pid ]; then kill \$(cat /tmp/mptunnel-udp-echo.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-udp-echo.pid; fi"
  exec_in target "if [ -f /tmp/mptunnel-tcp-echo.pid ]; then kill \$(cat /tmp/mptunnel-tcp-echo.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-tcp-echo.pid; fi"
  exec_in target "python3 -m http.server 8080 --bind 0.0.0.0 --directory /tmp/mptunnel-lab >/tmp/mptunnel-http.log 2>&1 & echo \$! >/tmp/mptunnel-http.pid"
  exec_in target "python3 /workspace/lab/udp_echo.py --bind 0.0.0.0:9090 >/tmp/mptunnel-udp-echo.log 2>&1 & echo \$! >/tmp/mptunnel-udp-echo.pid"
  exec_in target "python3 /workspace/lab/tcp_echo.py --bind 0.0.0.0:10022 >/tmp/mptunnel-tcp-echo.log 2>&1 & echo \$! >/tmp/mptunnel-tcp-echo.pid"
}

start_server() {
  stop_server
  exec_in server "\
    MPTUNNEL_LAB_DIAG='${MPTUNNEL_LAB_DIAG:-0}' \
    MPTUNNEL_LAB_PERF='${lab_perf}' \
    MPTUNNEL_LAB_PERF_INTERVAL_MS='${lab_perf_interval_ms}' \
    MPTUNNEL_LOG='${lab_log_level}' /workspace/target/release/mptunnel \
      --secret '${secret}' \
      server \
      --bind-path tcp://172.31.10.20:${server_port} \
      --bind-path tcp://172.31.15.20:${server_port} \
      --bind-path tcp://172.31.20.20:${server_port} \
      --bind-path tcp://172.31.30.20:${server_port} \
      --bind-path udp://172.31.10.20:${server_port} \
      --bind-path udp://172.31.15.20:${server_port} \
      --bind-path udp://172.31.20.20:${server_port} \
      --bind-path udp://172.31.30.20:${server_port} \
      --outbound direct \
      >/tmp/mptunnel-server.log 2>&1 & echo \$! >/tmp/mptunnel-server.pid"
  sleep 1
  exec_in server "kill -0 \$(cat /tmp/mptunnel-server.pid)"
}

start_client_with_netem() {
  local profile="$1"
  local netem_mode="$2"
  shift 2
  local path_args="$*"
  local probe_args=""
  if [[ -n "${PATH_PROBE_INTERVAL_MS:-}" ]]; then
    probe_args="${probe_args} --path-probe-interval-ms '${PATH_PROBE_INTERVAL_MS}'"
  fi
  if [[ -n "${PATH_PROBE_TIMEOUT_MS:-}" ]]; then
    probe_args="${probe_args} --path-probe-timeout-ms '${PATH_PROBE_TIMEOUT_MS}'"
  fi
  if [[ "$isolate_cases" == "1" ]]; then
    stop_client
    if [[ "$isolate_containers" == "1" ]]; then
      stop_server
      compose down --remove-orphans >/dev/null 2>&1 || true
      compose up -d --remove-orphans >/dev/null
    fi
    apply_netem "$netem_mode"
    start_target_services
    start_server
  else
    stop_client
    apply_netem "$netem_mode"
  fi
  exec_in client "\
    MPTUNNEL_LAB_DIAG='${MPTUNNEL_LAB_DIAG:-0}' \
    MPTUNNEL_LAB_PERF='${lab_perf}' \
    MPTUNNEL_LAB_PERF_INTERVAL_MS='${lab_perf_interval_ms}' \
    MPTUNNEL_LOG='${lab_log_level}' /workspace/target/release/mptunnel \
      --secret '${secret}' \
      client \
      --listen 127.0.0.1:${proxy_port} \
      ${probe_args} \
      ${path_args} \
      >/tmp/mptunnel-client-${profile}.log 2>&1 & echo \$! >/tmp/mptunnel-client.pid"
  sleep 1
  exec_in client "kill -0 \$(cat /tmp/mptunnel-client.pid)"
}

start_client() {
  local profile="$1"
  shift
  start_client_with_netem "$profile" apply "$@"
}

start_tun_client() {
  local profile="$1"
  shift
  local path_args="$*"
  local probe_args=""
  if [[ -n "${PATH_PROBE_INTERVAL_MS:-}" ]]; then
    probe_args="${probe_args} --path-probe-interval-ms '${PATH_PROBE_INTERVAL_MS}'"
  fi
  if [[ -n "${PATH_PROBE_TIMEOUT_MS:-}" ]]; then
    probe_args="${probe_args} --path-probe-timeout-ms '${PATH_PROBE_TIMEOUT_MS}'"
  fi
  if [[ "$isolate_cases" == "1" ]]; then
    stop_client
    if [[ "$isolate_containers" == "1" ]]; then
      stop_server
      compose down --remove-orphans >/dev/null 2>&1 || true
      compose up -d --remove-orphans >/dev/null
    fi
    apply_netem apply
    start_target_services
    start_server
  else
    stop_client
  fi
  exec_in client "\
    MPTUNNEL_LAB_DIAG='${MPTUNNEL_LAB_DIAG:-0}' \
    MPTUNNEL_LAB_PERF='${lab_perf}' \
    MPTUNNEL_LAB_PERF_INTERVAL_MS='${lab_perf_interval_ms}' \
    MPTUNNEL_LOG='${lab_log_level}' /workspace/target/release/mptunnel \
      --secret '${secret}' \
      client \
      --tun \
      --tun-name mptun0 \
      --tun-ipv4 10.88.0.1 \
      --tun-ipv4-prefix 24 \
      ${probe_args} \
      ${path_args} \
      >/tmp/mptunnel-client-${profile}.log 2>&1 & echo \$! >/tmp/mptunnel-client.pid"
  sleep 1
  exec_in client "kill -0 \$(cat /tmp/mptunnel-client.pid)"
  exec_in client "deadline=\$((SECONDS + 10)); while ! ip link show mptun0 >/dev/null 2>&1 && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; ip link show mptun0 >/dev/null"
  exec_in client "ip route replace 172.31.40.30/32 dev mptun0"
}

run_tun_download_case() {
  local case_name="$1"
  shift
  start_tun_client "$case_name" "$@"
  run_unproxied_download_probe_case "$case_name" "tun" "172.31.40.30:8080"
}

run_udp_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  set +e
  local output
  output="$(exec_in client "python3 /workspace/lab/socks5_udp_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:9090 --load-duration '${load_duration_seconds}' --payload-bytes '${udp_payload_bytes}' --timeout-ms '${udp_timeout_ms}'" 2>/dev/null)"
  local exit_code="$?"
  set -e
  if [[ "$exit_code" == "0" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    printf '{"case":"%s","protocol":"udp","status":"fail","exit_code":%s}\n' "$case_name" "$exit_code" >> "$result_file"
  fi
}

prepare_baseline_case() {
  local netem_mode="$1"
  stop_baselines
  stop_client
  if [[ "$isolate_cases" == "1" && "$isolate_containers" == "1" ]]; then
    stop_server
    compose down --remove-orphans >/dev/null 2>&1 || true
    compose up -d --remove-orphans >/dev/null
  else
    stop_server
  fi
  apply_netem "$netem_mode"
  start_target_services
}

run_baseline_download_probe_case() {
  local case_name="$1"
  local protocol="$2"
  local proxy_port_arg="$3"
  local out_file="/tmp/mptunnel-baseline-${case_name}.out"
  local err_file="/tmp/mptunnel-baseline-${case_name}.err"
  local output probe_stderr exit_code
  set +e
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout $((curl_timeout + 10))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port_arg} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  exit_code="$?"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  if [[ -n "$output" ]]; then
    ROW="$output" PROTOCOL="$protocol" python3 - <<'PY' >> "$result_file"
import json
import os

row = json.loads(os.environ["ROW"])
row["protocol"] = os.environ["PROTOCOL"]
print(json.dumps(row, sort_keys=True))
PY
  else
    append_download_probe_result "$case_name" "$exit_code" "" "$probe_stderr"
  fi
}

ensure_baseline_tool() {
  local service="$1"
  local tool="$2"
  exec_in "$service" "bash /workspace/lab/baseline-tools.sh 'ensure-${tool}'"
}

run_vmess_baseline_case() {
  local case_name="$1"
  local server_ip="$2"
  prepare_baseline_case apply
  if ! ensure_baseline_tool server xray || ! ensure_baseline_tool client xray; then
    append_skipped_result "$case_name" "vmess" "xray baseline binary unavailable"
    return 0
  fi
  exec_in server "bash /workspace/lab/baseline-tools.sh write-xray-server '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}'"
  exec_in client "bash /workspace/lab/baseline-tools.sh write-xray-client '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}' 127.0.0.1 '${baseline_proxy_port}'"
  exec_in server "bash /workspace/lab/baseline-tools.sh run-xray-server >/tmp/mptunnel-baseline-vmess-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-server.pid"
  exec_in client "bash /workspace/lab/baseline-tools.sh run-xray-client >/tmp/mptunnel-baseline-vmess-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-client.pid)"; then
    append_skipped_result "$case_name" "vmess" "xray baseline failed to start"
    stop_baselines
    return 0
  fi
  run_baseline_download_probe_case "$case_name" "vmess" "$baseline_proxy_port"
  stop_baselines
}

run_hysteria2_baseline_case() {
  local case_name="$1"
  local server_ip="$2"
  prepare_baseline_case apply
  if ! ensure_baseline_tool server hysteria2 || ! ensure_baseline_tool client hysteria2; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 baseline binary unavailable"
    return 0
  fi
  if ! exec_in server "bash /workspace/lab/baseline-tools.sh write-hysteria-server '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}'"; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 TLS certificate generation unavailable"
    return 0
  fi
  exec_in client "bash /workspace/lab/baseline-tools.sh write-hysteria-client '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}' 127.0.0.1 '${baseline_proxy_port}'"
  exec_in server "bash /workspace/lab/baseline-tools.sh run-hysteria-server >/tmp/mptunnel-baseline-hysteria-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-server.pid"
  exec_in client "bash /workspace/lab/baseline-tools.sh run-hysteria-client >/tmp/mptunnel-baseline-hysteria-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-client.pid)"; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 baseline failed to start"
    stop_baselines
    return 0
  fi
  run_baseline_download_probe_case "$case_name" "hysteria2" "$baseline_proxy_port"
  stop_baselines
}

configure_mptcp_endpoints() {
  local service="$1"
  shift
  exec_in "$service" "ip mptcp limits set subflows 4 add_addr_accepted 4 && { ip mptcp endpoint flush || true; }"
  local id=1
  local addr dev
  for addr in "$@"; do
    dev="$(exec_in "$service" "ip -o addr show | awk -v ip='${addr}' '\$4 ~ \"^\" ip \"/\" {print \$2; exit}'" 2>/dev/null || true)"
    if [[ -n "$dev" ]]; then
      exec_in "$service" "ip mptcp endpoint add '${addr}' dev '${dev}' id '${id}' signal subflow fullmesh 2>/dev/null || ip mptcp endpoint add '${addr}' dev '${dev}' id '${id}' signal 2>/dev/null || true"
    fi
    id=$((id + 1))
  done
  exec_in "$service" "ip mptcp endpoint show | grep -q ."
}

run_mptcp_baseline_case() {
  local case_name="$1"
  prepare_baseline_case apply
  if ! exec_in client "python3 /workspace/lab/mptcp_http.py check" || \
     ! exec_in target "python3 /workspace/lab/mptcp_http.py check"; then
    append_skipped_result "$case_name" "mptcp" "kernel MPTCP sockets unavailable"
    return 0
  fi
  if ! configure_mptcp_endpoints client 172.31.10.10 172.31.15.10 172.31.20.10 172.31.30.10 || \
     ! configure_mptcp_endpoints target 172.31.10.30 172.31.15.30 172.31.20.30 172.31.30.30; then
    append_skipped_result "$case_name" "mptcp" "kernel MPTCP endpoint configuration unavailable"
    return 0
  fi
  exec_in target "python3 /workspace/lab/mptcp_http.py serve --bind 0.0.0.0:${baseline_mptcp_port} --file /tmp/mptunnel-lab/large.bin >/tmp/mptunnel-baseline-mptcp-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-mptcp-server.pid"
  sleep 1
  if ! exec_in target "kill -0 \$(cat /tmp/mptunnel-baseline-mptcp-server.pid)"; then
    append_skipped_result "$case_name" "mptcp" "MPTCP HTTP server failed to start"
    stop_baselines
    return 0
  fi
  set +e
  local output exit_code
  output="$(exec_in client "timeout $((curl_timeout + 10))s python3 /workspace/lab/mptcp_http.py download --label '${case_name}' --target 172.31.10.30:${baseline_mptcp_port} --path '${large_http_path}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}'" 2>/dev/null)"
  exit_code="$?"
  set -e
  if [[ -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    printf '{"case":"%s","protocol":"mptcp","status":"fail","exit_code":%s}\n' "$case_name" "$exit_code" >> "$result_file"
  fi
  stop_baselines
}

record_mixed_probe_case() {
  local case_name="$1"
  set +e
  local output client_log server_log
  output="$(exec_in client "python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}'" 2>/dev/null)"
  local exit_code="$?"
  client_log="$(exec_in client "for file in /tmp/mptunnel-client-*.log; do [ -f \"\$file\" ] || continue; echo \"== \$(basename \"\$file\") ==\"; tail -n '${log_tail_lines}' \"\$file\"; done | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  server_log="$(exec_in server "tail -n '${log_tail_lines}' /tmp/mptunnel-server.log 2>/dev/null | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  set -e
  ROW="$output" \
  EXIT_CODE="$exit_code" \
  CLIENT_LOG="$client_log" \
  SERVER_LOG="$server_log" \
  LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
  LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
  LOG_TAIL_BYTES="$log_tail_bytes" \
  python3 - "$case_name" <<'PY' >> "$result_file"
import json
import os
import sys

case = sys.argv[1]
raw = os.environ.get("ROW", "")
try:
    row = json.loads(raw) if raw else {}
except json.JSONDecodeError:
    row = {"case": case, "protocol": "mixed", "status": "fail", "raw_output": raw}
if not row:
    try:
        exit_code = int(os.environ.get("EXIT_CODE", "124"))
    except ValueError:
        exit_code = 124
    row = {
        "case": case,
        "protocol": "mixed",
        "status": "fail",
        "exit_code": exit_code,
    }
lab_diag = os.environ.get("LAB_DIAG", "").lower() in ("1", "true", "yes")
lab_perf = os.environ.get("LAB_PERF", "").lower() in ("1", "true", "yes")
try:
    log_tail_bytes = int(os.environ.get("LOG_TAIL_BYTES", "4000"))
except ValueError:
    log_tail_bytes = 4000
if row.get("status") != "ok" or lab_diag or lab_perf:
    for env_name, field in (
        ("CLIENT_LOG", "client_log_tail"),
        ("SERVER_LOG", "server_log_tail"),
    ):
        value = os.environ.get(env_name, "")
        if value:
            row[field] = value[-log_tail_bytes:]
print(json.dumps(row, sort_keys=True))
PY
}

run_mixed_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  record_mixed_probe_case "$case_name"
}

run_mixed_saturated_case() {
  local case_name="$1"
  local saturated_path="$2"
  shift 2
  start_client "$case_name" "$@"
  start_saturation_path "$saturated_path"
  record_mixed_probe_case "$case_name"
  stop_saturation
}

run_mixed_flapping_case() {
  local case_name="mptunnel_mixed_multipath_flapping_links"
  start_client "$case_name" "$tcp_all $udp_all"
  start_random_flapping
  record_mixed_probe_case "$case_name"
  stop_random_flapping
}

run_mixed_ideal_case() {
  local case_name="$1"
  local ideal_path="$2"
  shift 2
  start_client "$case_name" "$@"
  case "$ideal_path" in
    lowlat)
      exec_netem client ideal-lowlat
      exec_netem server ideal-lowlat
      ;;
    balanced)
      exec_netem client ideal-balanced
      exec_netem server ideal-balanced
      ;;
    fat)
      exec_netem client ideal-fat
      exec_netem server ideal-fat
      ;;
    poor)
      exec_netem client ideal-poor
      exec_netem server ideal-poor
      ;;
    *)
      echo "unknown ideal path: $ideal_path" >&2
      return 2
      ;;
  esac
  record_mixed_probe_case "$case_name"
  apply_netem apply
}

matrix_case_name() {
  local bits="$1"
  local bandwidth_label latency_label loss_label

  if [[ "${bits:0:1}" == "1" ]]; then
    bandwidth_label="bw_good"
  else
    bandwidth_label="bw_poor"
  fi
  if [[ "${bits:1:1}" == "1" ]]; then
    latency_label="lat_good"
  else
    latency_label="lat_poor"
  fi
  if [[ "${bits:2:1}" == "1" ]]; then
    loss_label="loss_good"
  else
    loss_label="loss_poor"
  fi

  printf 'mptunnel_matrix_%s_%s_%s' "$bandwidth_label" "$latency_label" "$loss_label"
}

run_matrix_case() {
  local bits="$1"
  local case_name
  case_name="$(matrix_case_name "$bits")"

  start_client_with_netem "$case_name" "matrix-b${bits}" "$tcp_lowlat $udp_lowlat"
  record_mixed_probe_case "$case_name"
  apply_netem apply
}

run_mixed_failover_case() {
  local case_name="mptunnel_mixed_multipath_failover_blackhole_fat"
  local output exit_code
  local started_file="/tmp/mptunnel-mixed.started"
  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f /tmp/mptunnel-mixed.out /tmp/mptunnel-mixed.status /tmp/mptunnel-mixed.pid '${started_file}'"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${started_file}' > /tmp/mptunnel-mixed.out 2>/tmp/mptunnel-mixed.err; echo \$? >/tmp/mptunnel-mixed.status) & echo \$! >/tmp/mptunnel-mixed.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-mixed.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-mixed.status ]; then echo 124 >/tmp/mptunnel-mixed.status; fi"
  output="$(exec_in client "cat /tmp/mptunnel-mixed.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-mixed.status 2>/dev/null || echo 124")"
  if [[ -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    printf '{"case":"%s","protocol":"mixed","status":"fail","exit_code":%s}\n' "$case_name" "$exit_code" >> "$result_file"
  fi
  apply_netem apply
}

run_mixed_latency_spike_case() {
  local case_name="mptunnel_mixed_multipath_latency_spike_fat"
  local output exit_code
  local started_file="/tmp/mptunnel-mixed-spike.started"
  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f /tmp/mptunnel-mixed-spike.out /tmp/mptunnel-mixed-spike.status /tmp/mptunnel-mixed-spike.pid '${started_file}'"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${started_file}' > /tmp/mptunnel-mixed-spike.out 2>/tmp/mptunnel-mixed-spike.err; echo \$? >/tmp/mptunnel-mixed-spike.status) & echo \$! >/tmp/mptunnel-mixed-spike.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_latency_spike_fat
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-mixed-spike.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-mixed-spike.status ]; then echo 124 >/tmp/mptunnel-mixed-spike.status; fi"
  output="$(exec_in client "cat /tmp/mptunnel-mixed-spike.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-mixed-spike.status 2>/dev/null || echo 124")"
  if [[ -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    printf '{"case":"%s","protocol":"mixed","status":"fail","exit_code":%s}\n' "$case_name" "$exit_code" >> "$result_file"
  fi
  apply_netem apply
}

run_failover_case() {
  local case_name="mptunnel_tcp_multipath_failover_blackhole_fat"
  local output exit_code
  local started_file="/tmp/mptunnel-failover.started"
  start_client "$case_name" "$tcp_all"
  exec_in client "rm -f /tmp/mptunnel-failover.out /tmp/mptunnel-failover.status /tmp/mptunnel-failover.pid '${started_file}'"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-failover.out 2>/tmp/mptunnel-failover.err; echo \$? >/tmp/mptunnel-failover.status) & echo \$! >/tmp/mptunnel-failover.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-failover.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-failover.status ]; then echo 124 >/tmp/mptunnel-failover.status; fi"
  output="$(exec_in client "cat /tmp/mptunnel-failover.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-failover.status 2>/dev/null || echo 124")"
  if [[ "$exit_code" == "0" && -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    local probe_stderr
    probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-failover.err 2>/dev/null | tail -c 4000 || true")"
    append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
  fi
  apply_netem apply
}

run_latency_spike_case() {
  local case_name="mptunnel_tcp_multipath_latency_spike_fat"
  local output exit_code
  local started_file="/tmp/mptunnel-spike.started"
  start_client "$case_name" "$tcp_all"
  exec_in client "rm -f /tmp/mptunnel-spike.out /tmp/mptunnel-spike.status /tmp/mptunnel-spike.pid '${started_file}'"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-spike.out 2>/tmp/mptunnel-spike.err; echo \$? >/tmp/mptunnel-spike.status) & echo \$! >/tmp/mptunnel-spike.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_latency_spike_fat
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-spike.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-spike.status ]; then echo 124 >/tmp/mptunnel-spike.status; fi"
  output="$(exec_in client "cat /tmp/mptunnel-spike.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-spike.status 2>/dev/null || echo 124")"
  if [[ "$exit_code" == "0" && -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    local probe_stderr
    probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-spike.err 2>/dev/null | tail -c 4000 || true")"
    append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
  fi
  apply_netem apply
}

mkdir -p "$result_dir"
: > "$result_file"

docker compose version >/dev/null
if [[ "$build_product" == "1" ]]; then
  build_mptunnel_binary
fi
if [[ "$build_lab_images" == "1" ]]; then
  compose build
fi
compose up -d --remove-orphans
trap cleanup EXIT

start_target_services
apply_netem apply
start_server

if [[ "${MPTUNNEL_LAB_USE_PATH_HINTS:-0}" == "1" ]]; then
  tcp_lowlat="--path 'tcp://172.31.10.20:${server_port}?srtt-ms=20&rate-mbps=30&low-latency=true'"
  tcp_balanced="--path 'tcp://172.31.15.20:${server_port}?srtt-ms=80&rate-mbps=120'"
  tcp_fat="--path 'tcp://172.31.20.20:${server_port}?srtt-ms=180&rate-mbps=300'"
  tcp_poor="--path 'tcp://172.31.30.20:${server_port}?srtt-ms=420&jitter-ms=120&rate-mbps=8&expensive=true'"
  udp_lowlat="--path 'udp://172.31.10.20:${server_port}?srtt-ms=20&rate-mbps=30&low-latency=true'"
  udp_balanced="--path 'udp://172.31.15.20:${server_port}?srtt-ms=80&rate-mbps=120'"
  udp_fat="--path 'udp://172.31.20.20:${server_port}?srtt-ms=180&rate-mbps=300'"
  udp_poor="--path 'udp://172.31.30.20:${server_port}?srtt-ms=420&jitter-ms=120&rate-mbps=8&expensive=true'"
else
  tcp_lowlat="--path 'tcp://172.31.10.20:${server_port}'"
  tcp_balanced="--path 'tcp://172.31.15.20:${server_port}'"
  tcp_fat="--path 'tcp://172.31.20.20:${server_port}'"
  tcp_poor="--path 'tcp://172.31.30.20:${server_port}'"
  udp_lowlat="--path 'udp://172.31.10.20:${server_port}'"
  udp_balanced="--path 'udp://172.31.15.20:${server_port}'"
  udp_fat="--path 'udp://172.31.20.20:${server_port}'"
  udp_poor="--path 'udp://172.31.30.20:${server_port}'"
fi
tcp_all="${tcp_lowlat} ${tcp_balanced} ${tcp_fat} ${tcp_poor}"
udp_all="${udp_lowlat} ${udp_balanced} ${udp_fat} ${udp_poor}"

if should_run_case "direct_low_latency"; then
  run_unproxied_download_probe_case "direct_low_latency" "tcp" "172.31.10.30:8080"
fi
if should_run_case "direct_balanced"; then
  run_unproxied_download_probe_case "direct_balanced" "tcp" "172.31.15.30:8080"
fi
if should_run_case "direct_cross_continent_high_bandwidth"; then
  run_unproxied_download_probe_case "direct_cross_continent_high_bandwidth" "tcp" "172.31.20.30:8080"
fi
if should_run_case "direct_poor_internet"; then
  run_unproxied_download_probe_case "direct_poor_internet" "tcp" "172.31.30.30:8080"
fi

if should_run_case "baseline_vmess_tcp_single_balanced"; then
  run_vmess_baseline_case "baseline_vmess_tcp_single_balanced" "172.31.15.20"
fi

if should_run_case "baseline_hysteria2_udp_single_balanced"; then
  run_hysteria2_baseline_case "baseline_hysteria2_udp_single_balanced" "172.31.15.20"
fi

if should_run_case "baseline_mptcp_tcp_multipath_all"; then
  run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_all"
fi

if should_run_case "mptunnel_tcp_single_low_latency"; then
  start_client "tcp_single_low_latency" "$tcp_lowlat"
  run_tcp_download_probe_case "mptunnel_tcp_single_low_latency"
fi

if should_run_case "mptunnel_tcp_single_balanced"; then
  start_client "tcp_single_balanced" "$tcp_balanced"
  run_tcp_download_probe_case "mptunnel_tcp_single_balanced"
fi

if should_run_case "mptunnel_tcp_single_cross_continent_high_bandwidth"; then
  start_client "tcp_single_cross_continent_high_bandwidth" "$tcp_fat"
  run_tcp_download_probe_case "mptunnel_tcp_single_cross_continent_high_bandwidth"
fi

if should_run_case "mptunnel_tcp_single_poor_internet"; then
  start_client "tcp_single_poor_internet" "$tcp_poor"
  run_tcp_download_probe_case "mptunnel_tcp_single_poor_internet"
fi

if should_run_case "mptunnel_tcp_multipath_all"; then
  start_client "tcp_multipath_all" "$tcp_all"
  run_tcp_download_probe_case "mptunnel_tcp_multipath_all"
fi

if should_run_case "mptunnel_udp_stream_single_low_latency"; then
  start_client "udp_stream_single_low_latency" "$udp_lowlat"
  run_tcp_download_probe_case "mptunnel_udp_stream_single_low_latency"
fi

if should_run_case "mptunnel_udp_stream_single_balanced"; then
  start_client "udp_stream_single_balanced" "$udp_balanced"
  run_tcp_download_probe_case "mptunnel_udp_stream_single_balanced"
fi

if should_run_case "mptunnel_udp_stream_single_cross_continent_high_bandwidth"; then
  start_client "udp_stream_single_cross_continent_high_bandwidth" "$udp_fat"
  run_tcp_download_probe_case "mptunnel_udp_stream_single_cross_continent_high_bandwidth"
fi

if should_run_case "mptunnel_udp_stream_multipath_all"; then
  start_client "udp_stream_multipath_all" "$udp_all"
  run_tcp_download_probe_case "mptunnel_udp_stream_multipath_all"
fi

if should_run_case "mptunnel_reliable_mixed_single_low_latency"; then
  start_client "reliable_mixed_single_low_latency" "$tcp_lowlat $udp_lowlat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_single_low_latency"
fi

if should_run_case "mptunnel_reliable_mixed_single_balanced"; then
  start_client "reliable_mixed_single_balanced" "$tcp_balanced $udp_balanced"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_single_balanced"
fi

if should_run_case "mptunnel_reliable_mixed_multipath_all"; then
  start_client "reliable_mixed_multipath_all" "$tcp_all $udp_all"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_multipath_all"
fi

if should_run_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat"; then
  start_client "reliable_mixed_tcp_lowlat_udp_fat" "$tcp_lowlat $udp_fat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat"
fi

if should_run_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat"; then
  start_client "reliable_mixed_tcp_fat_udp_lowlat" "$tcp_fat $udp_lowlat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat"
fi

if should_run_case "mptunnel_tun_tcp_single_low_latency"; then
  run_tun_download_case "mptunnel_tun_tcp_single_low_latency" "$tcp_lowlat"
fi

if should_run_case "mptunnel_tun_tcp_single_balanced"; then
  run_tun_download_case "mptunnel_tun_tcp_single_balanced" "$tcp_balanced"
fi

if should_run_case "mptunnel_tun_udp_stream_single_low_latency"; then
  run_tun_download_case "mptunnel_tun_udp_stream_single_low_latency" "$udp_lowlat"
fi

if should_run_case "mptunnel_tun_udp_stream_single_balanced"; then
  run_tun_download_case "mptunnel_tun_udp_stream_single_balanced" "$udp_balanced"
fi

if should_run_case "mptunnel_tun_mixed_multipath_all"; then
  run_tun_download_case "mptunnel_tun_mixed_multipath_all" "$tcp_all $udp_all"
fi

if should_run_case "mptunnel_udp_single_low_latency"; then
  run_udp_case "mptunnel_udp_single_low_latency" "$udp_lowlat"
fi
if should_run_case "mptunnel_udp_single_balanced"; then
  run_udp_case "mptunnel_udp_single_balanced" "$udp_balanced"
fi
if should_run_case "mptunnel_udp_single_cross_continent_high_bandwidth"; then
  run_udp_case "mptunnel_udp_single_cross_continent_high_bandwidth" "$udp_fat"
fi
if should_run_case "mptunnel_udp_single_poor_internet"; then
  run_udp_case "mptunnel_udp_single_poor_internet" "$udp_poor"
fi
if should_run_case "mptunnel_udp_multipath_all"; then
  run_udp_case "mptunnel_udp_multipath_all" "$udp_all"
fi
if should_run_case "mptunnel_udp_target_over_tcp_single_low_latency"; then
  run_udp_case "mptunnel_udp_target_over_tcp_single_low_latency" "$tcp_lowlat"
fi
if should_run_case "mptunnel_udp_target_over_tcp_single_balanced"; then
  run_udp_case "mptunnel_udp_target_over_tcp_single_balanced" "$tcp_balanced"
fi
if should_run_case "mptunnel_udp_target_over_tcp_multipath_all"; then
  run_udp_case "mptunnel_udp_target_over_tcp_multipath_all" "$tcp_all"
fi

if should_run_case "mptunnel_mixed_single_low_latency"; then
  run_mixed_case "mptunnel_mixed_single_low_latency" "$tcp_lowlat $udp_lowlat"
fi
if should_run_case "mptunnel_mixed_single_balanced"; then
  run_mixed_case "mptunnel_mixed_single_balanced" "$tcp_balanced $udp_balanced"
fi
if should_run_case "mptunnel_mixed_tcp_lowlat_udp_fat"; then
  run_mixed_case "mptunnel_mixed_tcp_lowlat_udp_fat" "$tcp_lowlat $udp_fat"
fi
if should_run_case "mptunnel_mixed_tcp_fat_udp_lowlat"; then
  run_mixed_case "mptunnel_mixed_tcp_fat_udp_lowlat" "$tcp_fat $udp_lowlat"
fi
if should_run_case "mptunnel_mixed_multipath_all"; then
  run_mixed_case "mptunnel_mixed_multipath_all" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_ideal_lowlat"; then
  run_mixed_ideal_case "mptunnel_mixed_multipath_ideal_lowlat" "lowlat" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_ideal_balanced"; then
  run_mixed_ideal_case "mptunnel_mixed_multipath_ideal_balanced" "balanced" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_ideal_fat"; then
  run_mixed_ideal_case "mptunnel_mixed_multipath_ideal_fat" "fat" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_saturate_lowlat"; then
  run_mixed_saturated_case "mptunnel_mixed_multipath_saturate_lowlat" "lowlat" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_saturate_balanced"; then
  run_mixed_saturated_case "mptunnel_mixed_multipath_saturate_balanced" "balanced" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_saturate_fat"; then
  run_mixed_saturated_case "mptunnel_mixed_multipath_saturate_fat" "fat" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_saturate_poor"; then
  run_mixed_saturated_case "mptunnel_mixed_multipath_saturate_poor" "poor" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_mixed_multipath_flapping_links"; then
  run_mixed_flapping_case
fi
if should_run_case "mptunnel_mixed_multipath_failover_blackhole_fat"; then
  run_mixed_failover_case
fi
if should_run_case "mptunnel_mixed_multipath_latency_spike_fat"; then
  run_mixed_latency_spike_case
fi

for matrix_bits in 000 001 010 011 100 101 110 111; do
  matrix_case="$(matrix_case_name "$matrix_bits")"
  if should_run_case "$matrix_case"; then
    run_matrix_case "$matrix_bits"
  fi
done

if should_run_case "mptunnel_tcp_multipath_failover_blackhole_fat"; then
  run_failover_case
fi
if should_run_case "mptunnel_tcp_multipath_latency_spike_fat"; then
  run_latency_spike_case
fi

echo "$result_file"
python3 - "$result_file" <<'PY'
import json
import sys

failed = []
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line_number, line in enumerate(handle, 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("status") not in {"ok", "loss"}:
            failed.append((line_number, row.get("case", "unknown"), row.get("status")))

if failed:
    for line_number, case, status in failed:
        print(
            f"failed experiment row {line_number}: {case} status={status}",
            file=sys.stderr,
        )
    sys.exit(1)
PY

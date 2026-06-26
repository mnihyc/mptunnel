#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

compose_file="${COMPOSE_FILE:-lab/docker-compose.yml}"
result_dir="${RESULT_DIR:-lab/results}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_file="${RESULT_FILE:-$result_dir/heterogeneous-$timestamp.jsonl}"
file_mib="${FILE_MIB:-128}"
proxy_port="${PROXY_PORT:-1080}"
server_port="${SERVER_PORT:-7443}"
curl_timeout="${CURL_TIMEOUT_SECONDS:-120}"
udp_count="${UDP_COUNT:-60}"
udp_payload_bytes="${UDP_PAYLOAD_BYTES:-512}"
udp_timeout_ms="${UDP_TIMEOUT_MS:-2500}"
failover_after="${FAILOVER_AFTER_SECONDS:-2}"
path_probe_interval_ms="${PATH_PROBE_INTERVAL_MS:-10000}"
path_probe_timeout_ms="${PATH_PROBE_TIMEOUT_MS:-5000}"
build_product="${BUILD_PRODUCT:-1}"
build_lab_images="${BUILD_LAB_IMAGES:-1}"
case_filter="${CASE_FILTER:-}"
secret="${MPTUNNEL_LAB_SECRET:-mptunnel-lab-secret-change-me-32-bytes-minimum}"

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
    -e MPTUNNEL_LAB_LOWLAT_LOSS="${MPTUNNEL_LAB_LOWLAT_LOSS:-0.01%}" \
    -e MPTUNNEL_LAB_FAT_RATE="${MPTUNNEL_LAB_FAT_RATE:-300mbit}" \
    -e MPTUNNEL_LAB_FAT_DELAY="${MPTUNNEL_LAB_FAT_DELAY:-180ms}" \
    -e MPTUNNEL_LAB_FAT_JITTER="${MPTUNNEL_LAB_FAT_JITTER:-20ms}" \
    -e MPTUNNEL_LAB_FAT_LOSS="${MPTUNNEL_LAB_FAT_LOSS:-0.10%}" \
    -e MPTUNNEL_LAB_POOR_RATE="${MPTUNNEL_LAB_POOR_RATE:-8mbit}" \
    -e MPTUNNEL_LAB_POOR_DELAY="${MPTUNNEL_LAB_POOR_DELAY:-420ms}" \
    -e MPTUNNEL_LAB_POOR_JITTER="${MPTUNNEL_LAB_POOR_JITTER:-120ms}" \
    -e MPTUNNEL_LAB_POOR_LOSS="${MPTUNNEL_LAB_POOR_LOSS:-3.00%}" \
    -e MPTUNNEL_LAB_BLACKHOLE_LOSS="${MPTUNNEL_LAB_BLACKHOLE_LOSS:-100%}" \
    "$service" bash -lc "/workspace/lab/configure-netem.sh '$mode'"
}

append_curl_result() {
  local case_name="$1"
  local protocol="$2"
  local status="$3"
  local exit_code="$4"
  local http_code="$5"
  local time_s="$6"
  local goodput_mbps="$7"

  printf '{"case":"%s","protocol":"%s","status":"%s","exit_code":%s,"http_code":%s,"time_s":%s,"goodput_mbps":%s}\n' \
    "$case_name" "$protocol" "$status" "$exit_code" "$http_code" "$time_s" "$goodput_mbps" \
    >> "$result_file"
}

parse_and_record_curl() {
  local case_name="$1"
  local protocol="$2"
  local exit_code="$3"
  local output="$4"
  local time_s speed_bytes http_code goodput_mbps

  if [[ "$exit_code" == "0" ]]; then
    read -r time_s speed_bytes http_code <<< "$output"
    goodput_mbps="$(awk -v speed="$speed_bytes" 'BEGIN {printf "%.3f", speed * 8 / 1000000}')"
    append_curl_result "$case_name" "$protocol" "ok" "$exit_code" "$http_code" "$time_s" "$goodput_mbps"
  else
    append_curl_result "$case_name" "$protocol" "fail" "$exit_code" "null" "0" "0"
  fi
}

run_curl_case() {
  local case_name="$1"
  local protocol="$2"
  local url="$3"
  local proxy_arg="${4:-}"
  local output exit_code

  set +e
  output="$(exec_in client "timeout ${curl_timeout}s curl -sS --fail --location --output /dev/null --write-out '%{time_total} %{speed_download} %{http_code}' ${proxy_arg} '${url}'" 2>/dev/null)"
  exit_code="$?"
  set -e
  parse_and_record_curl "$case_name" "$protocol" "$exit_code" "$output"
}

stop_process() {
  local service="$1"
  local pid_file="$2"
  exec_in "$service" "if [ -f '$pid_file' ]; then kill \$(cat '$pid_file') >/dev/null 2>&1 || true; rm -f '$pid_file'; fi" \
    >/dev/null 2>&1 || true
}

stop_client() {
  stop_process client /tmp/mptunnel-client.pid
}

stop_server() {
  stop_process server /tmp/mptunnel-server.pid
}

cleanup() {
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

should_run_case() {
  local case_name="$1"
  local pattern

  if [[ -z "$case_filter" ]]; then
    return 0
  fi

  IFS=',' read -r -a patterns <<< "$case_filter"
  for pattern in "${patterns[@]}"; do
    if [[ "$case_name" == $pattern ]]; then
      return 0
    fi
  done
  return 1
}

start_target_services() {
  exec_in target "mkdir -p /tmp/mptunnel-lab && dd if=/dev/zero of=/tmp/mptunnel-lab/large.bin bs=1M count='${file_mib}' status=none"
  exec_in target "if [ -f /tmp/mptunnel-http.pid ]; then kill \$(cat /tmp/mptunnel-http.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-http.pid; fi"
  exec_in target "if [ -f /tmp/mptunnel-udp-echo.pid ]; then kill \$(cat /tmp/mptunnel-udp-echo.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-udp-echo.pid; fi"
  exec_in target "python3 -m http.server 8080 --bind 0.0.0.0 --directory /tmp/mptunnel-lab >/tmp/mptunnel-http.log 2>&1 & echo \$! >/tmp/mptunnel-http.pid"
  exec_in target "python3 /workspace/lab/udp_echo.py --bind 0.0.0.0:9090 >/tmp/mptunnel-udp-echo.log 2>&1 & echo \$! >/tmp/mptunnel-udp-echo.pid"
}

start_server() {
  stop_server
  exec_in server "\
    MPTUNNEL_LOG=info /workspace/target/release/mptunnel \
      --secret '${secret}' \
      server \
      --bind-path tcp://172.31.10.20:${server_port} \
      --bind-path tcp://172.31.20.20:${server_port} \
      --bind-path tcp://172.31.30.20:${server_port} \
      --bind-path udp://172.31.10.20:${server_port} \
      --bind-path udp://172.31.20.20:${server_port} \
      --bind-path udp://172.31.30.20:${server_port} \
      --outbound direct \
      >/tmp/mptunnel-server.log 2>&1 & echo \$! >/tmp/mptunnel-server.pid"
  sleep 1
  exec_in server "kill -0 \$(cat /tmp/mptunnel-server.pid)"
}

start_client() {
  local profile="$1"
  shift
  local path_args="$*"
  stop_client
  exec_in client "\
    MPTUNNEL_LOG=info /workspace/target/release/mptunnel \
      --secret '${secret}' \
      client \
      --ingress socks5 \
      --listen 127.0.0.1:${proxy_port} \
      --path-probe-interval-ms '${path_probe_interval_ms}' \
      --path-probe-timeout-ms '${path_probe_timeout_ms}' \
      ${path_args} \
      >/tmp/mptunnel-client-${profile}.log 2>&1 & echo \$! >/tmp/mptunnel-client.pid"
  sleep 1
  exec_in client "kill -0 \$(cat /tmp/mptunnel-client.pid)"
}

run_udp_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  set +e
  local output
  output="$(exec_in client "python3 /workspace/lab/socks5_udp_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:9090 --count '${udp_count}' --payload-bytes '${udp_payload_bytes}' --timeout-ms '${udp_timeout_ms}'" 2>/dev/null)"
  local exit_code="$?"
  set -e
  if [[ "$exit_code" == "0" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    printf '{"case":"%s","protocol":"udp","status":"fail","exit_code":%s}\n' "$case_name" "$exit_code" >> "$result_file"
  fi
}

run_failover_case() {
  local case_name="mptunnel_tcp_multipath_failover_blackhole_fat"
  local output exit_code
  start_client "$case_name" "$tcp_all"
  exec_in client "rm -f /tmp/mptunnel-failover.out /tmp/mptunnel-failover.status /tmp/mptunnel-failover.pid"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path /large.bin --failover-after '${failover_after}' --timeout '${curl_timeout}' > /tmp/mptunnel-failover.out 2>/tmp/mptunnel-failover.err; echo \$? >/tmp/mptunnel-failover.status) & echo \$! >/tmp/mptunnel-failover.pid"
  sleep "$failover_after"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-failover.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-failover.status ]; then echo 124 >/tmp/mptunnel-failover.status; fi"
  output="$(exec_in client "cat /tmp/mptunnel-failover.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-failover.status 2>/dev/null || echo 124")"
  if [[ "$exit_code" == "0" && -n "$output" ]]; then
    printf '%s\n' "$output" >> "$result_file"
  else
    parse_and_record_curl "$case_name" "tcp" "$exit_code" "$output"
  fi
  apply_netem apply
}

mkdir -p "$result_dir"
: > "$result_file"

docker compose version >/dev/null
if [[ "$build_product" == "1" ]]; then
  cargo build --release --bin mptunnel
fi
if [[ "$build_lab_images" == "1" ]]; then
  compose build
fi
compose up -d --remove-orphans
trap cleanup EXIT

start_target_services
apply_netem apply
start_server

tcp_lowlat="--path 'tcp://172.31.10.20:${server_port}?srtt-ms=20&rate-mbps=30&low-latency=true'"
tcp_fat="--path 'tcp://172.31.20.20:${server_port}?srtt-ms=180&rate-mbps=300'"
tcp_poor="--path 'tcp://172.31.30.20:${server_port}?srtt-ms=420&jitter-ms=120&rate-mbps=8&expensive=true'"
tcp_all="${tcp_lowlat} ${tcp_fat} ${tcp_poor}"

udp_lowlat="--path 'udp://172.31.10.20:${server_port}?srtt-ms=20&rate-mbps=30&low-latency=true'"
udp_fat="--path 'udp://172.31.20.20:${server_port}?srtt-ms=180&rate-mbps=300'"
udp_poor="--path 'udp://172.31.30.20:${server_port}?srtt-ms=420&jitter-ms=120&rate-mbps=8&expensive=true'"
udp_all="${udp_lowlat} ${udp_fat} ${udp_poor}"

if should_run_case "direct_low_latency"; then
  run_curl_case "direct_low_latency" "tcp" "http://172.31.10.30:8080/large.bin"
fi
if should_run_case "direct_cross_continent_high_bandwidth"; then
  run_curl_case "direct_cross_continent_high_bandwidth" "tcp" "http://172.31.20.30:8080/large.bin"
fi
if should_run_case "direct_poor_internet"; then
  run_curl_case "direct_poor_internet" "tcp" "http://172.31.30.30:8080/large.bin"
fi

if should_run_case "mptunnel_tcp_single_low_latency"; then
  start_client "tcp_single_low_latency" "$tcp_lowlat"
  run_curl_case "mptunnel_tcp_single_low_latency" "tcp" "http://172.31.40.30:8080/large.bin" "--socks5-hostname 127.0.0.1:${proxy_port}"
fi

if should_run_case "mptunnel_tcp_single_cross_continent_high_bandwidth"; then
  start_client "tcp_single_cross_continent_high_bandwidth" "$tcp_fat"
  run_curl_case "mptunnel_tcp_single_cross_continent_high_bandwidth" "tcp" "http://172.31.40.30:8080/large.bin" "--socks5-hostname 127.0.0.1:${proxy_port}"
fi

if should_run_case "mptunnel_tcp_single_poor_internet"; then
  start_client "tcp_single_poor_internet" "$tcp_poor"
  run_curl_case "mptunnel_tcp_single_poor_internet" "tcp" "http://172.31.40.30:8080/large.bin" "--socks5-hostname 127.0.0.1:${proxy_port}"
fi

if should_run_case "mptunnel_tcp_multipath_all"; then
  start_client "tcp_multipath_all" "$tcp_all"
  run_curl_case "mptunnel_tcp_multipath_all" "tcp" "http://172.31.40.30:8080/large.bin" "--socks5-hostname 127.0.0.1:${proxy_port}"
fi

if should_run_case "mptunnel_udp_single_low_latency"; then
  run_udp_case "mptunnel_udp_single_low_latency" "$udp_lowlat"
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

if should_run_case "mptunnel_tcp_multipath_failover_blackhole_fat"; then
  run_failover_case
fi

echo "$result_file"

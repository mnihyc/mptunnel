#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"
source "$script_dir/result-paths.sh"
mkdir -p .tmp/lab .tmp/python-cache .tmp/system
export PYTHONPYCACHEPREFIX="$repo_root/.tmp/python-cache"
export TMPDIR="$repo_root/.tmp/system"

flag_enabled() {
  case "${1,,}" in
    1|true|yes) return 0 ;;
    *) return 1 ;;
  esac
}

validate_build_decision() {
  local name="$1"
  local value="$2"
  if [[ "$value" != "0" && "$value" != "1" ]]; then
    echo "$name must be 0 or 1" >&2
    exit 2
  fi
}

hysteria_bandwidth_from_netem_rate() {
  local value="${1,,}"
  if [[ "$value" =~ ^([0-9]+([.][0-9]+)?)([kmgt]?)bit$ ]]; then
    printf '%s %sbps\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[3]}"
    return
  fi
  if [[ "$value" =~ ^[0-9]+([.][0-9]+)?[[:space:]]*[kmgt]?bps$ ]]; then
    printf '%s\n' "$value"
    return
  fi
  echo "cannot express netem rate as Hysteria2 bandwidth: $1" >&2
  return 2
}

elapsed_seconds_between_ns() {
  local started_ns="$1"
  local stopped_ns="$2"
  local elapsed_ns=$((stopped_ns - started_ns))
  if (( elapsed_ns < 0 )); then
    return 2
  fi
  printf '%d.%09d\n' "$((elapsed_ns / 1000000000))" "$((elapsed_ns % 1000000000))"
}

monotonic_time_ns() {
  python3 -c 'import time; print(time.monotonic_ns())'
}

lab_lock_file=".tmp/lab/compose.lock"
exec {lab_lock_fd}>"$lab_lock_file"
if ! flock -n "$lab_lock_fd"; then
  echo "another mptunnel Compose lab run holds $lab_lock_file" >&2
  exit 75
fi

compose_file="${COMPOSE_FILE:-lab/docker-compose.yml}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -n "${RESULT_DIR:-}" ]]; then
  result_dir="$(normalize_lab_result_path "$RESULT_DIR")"
else
  result_dir="$(normalize_lab_result_path "run-${timestamp}-$$")"
fi
result_file="$(normalize_lab_result_path "${RESULT_FILE:-$result_dir/heterogeneous-$timestamp.jsonl}")"
object_mib="${MPTUNNEL_LAB_OBJECT_MIB:-1024}"
if [[ ! "$object_mib" =~ ^[1-9][0-9]*$ ]]; then
  echo "MPTUNNEL_LAB_OBJECT_MIB must be a positive integer" >&2
  exit 2
fi
large_http_path="${MPTUNNEL_LAB_LARGE_HTTP_PATH:-/large.bin}"
small_http_path="${MPTUNNEL_LAB_SMALL_HTTP_PATH:-/small.bin}"
small_object_kib="${MPTUNNEL_LAB_SMALL_OBJECT_KIB:-32}"
load_duration_seconds="${MPTUNNEL_LAB_LOAD_DURATION_SECONDS:-30}"
bulk_connections="${MPTUNNEL_LAB_BULK_CONNECTIONS:-2}"
bulk_interactive_epoch_seconds=5
bulk_interactive_loss_percent=(3 8 5 6 10 3 5 8)
bulk_interactive_duration_seconds=$((${#bulk_interactive_loss_percent[@]} * bulk_interactive_epoch_seconds))
bulk_interactive_probe_timeout_seconds=$((bulk_interactive_duration_seconds + 20))
bulk_interactive_rate="500mbit"
bulk_interactive_delay="50ms"
bulk_interactive_jitter="20ms"
bulk_interactive_initial_loss="3%"
bulk_interactive_transition_complete_lateness_ms=250
bulk_interactive_transition_command_timeout_seconds=2
bulk_interactive_dynamic_loss_json="$(
  PYTHONPATH="$script_dir" python3 -c \
    'import json; from result_enrichment import BULK_INTERACTIVE_DYNAMIC_LOSS_CONDITION as c; print(json.dumps(c, separators=(",", ":"), sort_keys=True))'
)"
bulk_interactive_schedule_events_json=""
bulk_interactive_schedule_event_count=0
tcp_carrier_qos_cohort="${MPTUNNEL_LAB_TCP_CARRIER_QOS_COHORT:-0}"
tcp_carrier_qos_duration_seconds=30
tcp_carrier_qos_workers=3
tcp_carrier_qos_probe_timeout_seconds=60
tcp_carrier_qos_object_mib=4096
tcp_carrier_qos_http_path="/tcp-carrier-qos.bin"
tcp_per_flow_qos_rate="${MPTUNNEL_LAB_TCP_PER_FLOW_QOS_RATE:-500mbit}"
tcp_shared_bottleneck_rate="${MPTUNNEL_LAB_TCP_SHARED_BOTTLENECK_RATE:-200mbit}"
scale_seed="${MPTUNNEL_LAB_SCALE_SEED:-mptunnel-scale-links}"
scale_epoch_seconds=6
scale_epoch_count=5
scale_load_duration_seconds=$((scale_epoch_seconds * scale_epoch_count + 15))
scale_rate_bands=(access gigabit multi-gigabit)
browser_batch_size=10
browser_batch_period_ms=3000
browser_batch_deadline_ms=3000
browser_load_concurrency=20
browser_load_duration_seconds=60
browser_load_path="/browser.bin"
proxy_port="${PROXY_PORT:-1080}"
baseline_proxy_port="${BASELINE_PROXY_PORT:-1090}"
server_port="${SERVER_PORT:-7443}"
port_hop_first_port="$((server_port + 1))"
port_hop_last_port="$((server_port + 3))"
if (( port_hop_last_port > 65535 )); then
  echo "SERVER_PORT leaves no room for the three-port QUIC migration lab range" >&2
  exit 2
fi
port_hop_forwarding=0
baseline_vmess_port="${BASELINE_VMESS_PORT:-18443}"
baseline_hysteria2_port="${BASELINE_HYSTERIA2_PORT:-18444}"
baseline_mptcp_port="${BASELINE_MPTCP_PORT:-18081}"
baseline_lock_file="$script_dir/baseline-lock.json"
baseline_lock_sha256="$(sha256sum "$baseline_lock_file" | awk '{print $1}')"
baseline_tool_command="env MPTUNNEL_LAB_BASELINE_LOCK_SHA256=${baseline_lock_sha256} bash /workspace/lab/baseline-tools.sh"
curl_timeout="${CURL_TIMEOUT_SECONDS:-120}"
upload_process_timeout_seconds="$(
  LOAD_DURATION_SECONDS="$load_duration_seconds" \
  CURL_TIMEOUT_SECONDS="$curl_timeout" \
    python3 -c 'import math, os
load = float(os.environ["LOAD_DURATION_SECONDS"])
timeout = float(os.environ["CURL_TIMEOUT_SECONDS"])
active = timeout if load <= 0 else min(load, timeout)
print(max(1, math.ceil(active + timeout + 10.0)))'
)"
scale_upload_process_timeout_seconds="$(
  LOAD_DURATION_SECONDS="$scale_load_duration_seconds" \
  CURL_TIMEOUT_SECONDS="$curl_timeout" \
    python3 -c 'import math, os
load = float(os.environ["LOAD_DURATION_SECONDS"])
timeout = float(os.environ["CURL_TIMEOUT_SECONDS"])
print(max(1, math.ceil(load + timeout + 10.0)))'
)"
mptcp_evidence_interval_seconds="${MPTUNNEL_LAB_MPTCP_EVIDENCE_INTERVAL_SECONDS:-1.0}"
mptcp_evidence_max_duration_seconds="${MPTUNNEL_LAB_MPTCP_EVIDENCE_MAX_DURATION_SECONDS:-$((upload_process_timeout_seconds + 5))}"
udp_payload_bytes="${UDP_PAYLOAD_BYTES:-512}"
udp_timeout_ms="${UDP_TIMEOUT_MS:-2500}"
tcp_echo_payload_bytes="${TCP_ECHO_PAYLOAD_BYTES:-64}"
tcp_echo_timeout_ms="${TCP_ECHO_TIMEOUT_MS:-5000}"
tcp_echo_interval_ms="${TCP_ECHO_INTERVAL_MS:-500}"
tcp_upload_target_port="${TCP_UPLOAD_TARGET_PORT:-10023}"
tcp_sink_progress_file="/dev/shm/mptunnel-tcp-sink-progress.json"
failover_after="${FAILOVER_AFTER_SECONDS:-2}"
failover_profile="${MPTUNNEL_LAB_FAILOVER_PROFILE:-fat}"
failover_tx_trigger_bytes="${MPTUNNEL_LAB_FAILOVER_TX_TRIGGER_BYTES:-${MPTUNNEL_LAB_FAILOVER_FAT_TX_TRIGGER_BYTES:-0}}"
case "$failover_profile" in
  lowlat)
    failover_client_address="172.31.10.10"
    failover_server_address="172.31.10.20"
    ;;
  balanced)
    failover_client_address="172.31.15.10"
    failover_server_address="172.31.15.20"
    ;;
  fat)
    failover_client_address="172.31.20.10"
    failover_server_address="172.31.20.20"
    ;;
  poor)
    failover_client_address="172.31.30.10"
    failover_server_address="172.31.30.20"
    ;;
  *)
    echo "MPTUNNEL_LAB_FAILOVER_PROFILE must be lowlat, balanced, fat, or poor" >&2
    exit 2
    ;;
esac
failover_trigger_timeout_seconds="${MPTUNNEL_LAB_FAILOVER_TRIGGER_TIMEOUT_SECONDS:-60}"
failover_trigger_poll_interval_seconds="${MPTUNNEL_LAB_FAILOVER_TRIGGER_POLL_INTERVAL_SECONDS:-0.02}"
build_product="${BUILD_PRODUCT:-1}"
build_lab_images="${BUILD_LAB_IMAGES:-1}"
validate_build_decision BUILD_PRODUCT "$build_product"
validate_build_decision BUILD_LAB_IMAGES "$build_lab_images"
client_runtime="${MPTUNNEL_LAB_CLIENT_RUNTIME:-native}"
wine_prefix="${MPTUNNEL_LAB_WINE_PREFIX:-.tmp/lab/wine}"
host_build_root="${MPTUNNEL_LAB_BUILD_ROOT:-target}"
printf -v wine_prefix_shell '%q' "$wine_prefix"
case "$client_runtime" in
  native)
    client_target="$(rustc -vV | sed -n 's/^host: //p')"
    client_binary_host="${host_build_root}/release/mptunnel"
    client_binary_container="/workspace/target/release/mptunnel"
    ;;
  wine)
    client_target="x86_64-pc-windows-gnu"
    client_binary_host="${host_build_root}/${client_target}/release/mptunnel.exe"
    client_binary_container="/workspace/target/${client_target}/release/mptunnel.exe"
    ;;
  *)
    echo "MPTUNNEL_LAB_CLIENT_RUNTIME must be native or wine" >&2
    exit 2
    ;;
esac
# One public diagnostic switch controls both the optimized feature build and
# the private runtime event switch consumed by the instrumented binary.
lab_diagnostics="${MPTUNNEL_LAB_DIAGNOSTICS:-0}"
if flag_enabled "$lab_diagnostics"; then
  export MPTUNNEL_LAB_DIAG=1
else
  export MPTUNNEL_LAB_DIAG=0
fi
lab_perf="${MPTUNNEL_LAB_PERF:-0}"
lab_perf_samples="${MPTUNNEL_LAB_PERF_SAMPLES:-0}"
lab_perf_interval_ms="${MPTUNNEL_LAB_PERF_INTERVAL_MS:-1000}"
container_stats="${MPTUNNEL_LAB_CONTAINER_STATS:-1}"
container_stats_interval="${MPTUNNEL_LAB_CONTAINER_STATS_INTERVAL_SECONDS:-1}"
management_snapshots="${MPTUNNEL_LAB_MANAGEMENT_SNAPSHOTS:-0}"
management_snapshot_interval="${MPTUNNEL_LAB_MANAGEMENT_SNAPSHOT_INTERVAL_SECONDS:-1}"
management_snapshot_port="${MPTUNNEL_LAB_MANAGEMENT_PORT:-17600}"
management_token="${MPTUNNEL_LAB_MANAGEMENT_TOKEN:-mptunnel-lab-management-token}"
fail_on_bad_status="${MPTUNNEL_LAB_FAIL_ON_BAD_STATUS:-1}"
require_competitor_baselines="${MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES:-0}"
default_netem_mode="${MPTUNNEL_LAB_NETEM_MODE:-apply}"
internet_seed="${MPTUNNEL_LAB_INTERNET_SEED:-mptunnel-random-internet-v1}"
internet_schedule_file="${MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE:-}"
internet_schedule_sha256="${MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256:-}"
internet_load_queue_delay="${MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY:-100ms}"
case "$default_netem_mode" in
  apply) ;;
  *)
    if [[ ! "$default_netem_mode" =~ ^internet-five-path-epoch-[0-9]+$ \
      && ! "$default_netem_mode" =~ ^internet-five-path-load-coupled-epoch-[0-9]+$ ]]; then
      echo "MPTUNNEL_LAB_NETEM_MODE must be apply, internet-five-path-epoch-N, or internet-five-path-load-coupled-epoch-N" >&2
      exit 2
    fi
    ;;
esac
if [[ -z "$internet_seed" ]]; then
  echo "MPTUNNEL_LAB_INTERNET_SEED must not be empty" >&2
  exit 2
fi
if [[ -n "$internet_schedule_file" || -n "$internet_schedule_sha256" ]]; then
  if [[ -z "$internet_schedule_file" \
    || ! "$internet_schedule_sha256" =~ ^[0-9a-f]{64}$ ]]; then
    echo "MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE and a lowercase SHA-256 identity must be provided together" >&2
    exit 2
  fi
fi
case "${MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES:-0}" in
  1|true|True|TRUE|yes|Yes|YES) internet_include_outages=1 ;;
  0|false|False|FALSE|no|No|NO) internet_include_outages=0 ;;
  *)
    echo "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES must be 0/1, false/true, or no/yes" >&2
    exit 2
    ;;
esac
lab_log_level="${MPTUNNEL_LAB_LOG:-info}"
case "${MPTUNNEL_LAB_COLLECT_LOGS:-auto}" in
  auto)
    if flag_enabled "${MPTUNNEL_LAB_DIAG:-0}" || flag_enabled "$lab_perf"; then
      collect_logs="1"
    else
      collect_logs="0"
    fi
    ;;
  *)
    collect_logs="${MPTUNNEL_LAB_COLLECT_LOGS}"
    ;;
esac
if flag_enabled "$lab_perf"; then
  log_tail_bytes="${MPTUNNEL_LAB_LOG_TAIL_BYTES:-16000}"
  log_tail_lines="${MPTUNNEL_LAB_LOG_TAIL_LINES:-240}"
else
  log_tail_bytes="${MPTUNNEL_LAB_LOG_TAIL_BYTES:-4000}"
  log_tail_lines="${MPTUNNEL_LAB_LOG_TAIL_LINES:-120}"
fi
shared_transport_secret="${MPTUNNEL_LAB_SHARED_TRANSPORT_SECRET:-1}"
if flag_enabled "$shared_transport_secret"; then
  mptunnel_transport_profile="shared-secret"
else
  mptunnel_transport_profile="standard"
fi
mptunnel_protocol_version="$(sed -nE 's/^const VERSION: u8 = ([0-9]+);$/\1/p' src/protocol/codec.rs)"
IFS=$'\t' read -r mptunnel_expected_protocol_version mptunnel_carrier_presentation < <(
  MPTUNNEL_TRANSPORT_PROFILE="$mptunnel_transport_profile" \
    PYTHONPATH="$script_dir" python3 - <<'PY'
import os

from result_enrichment import (
    MPTUNNEL_PROTOCOL_VERSION,
    mptunnel_carrier_presentation,
)

presentation = mptunnel_carrier_presentation(
    os.environ["MPTUNNEL_TRANSPORT_PROFILE"]
)
print(f"{MPTUNNEL_PROTOCOL_VERSION}\t{presentation}")
PY
)
if [[ "$mptunnel_protocol_version" != "$mptunnel_expected_protocol_version" ]]; then
  echo "lab evidence supports only MPP wire protocol v${mptunnel_expected_protocol_version}; found ${mptunnel_protocol_version:-unknown}" >&2
  exit 2
fi
source_commit="$(git rev-parse --verify HEAD)"
if [[ -n "$(git status --porcelain --untracked-files=normal -- .)" ]]; then
  source_tree_dirty=true
else
  source_tree_dirty=false
fi
if flag_enabled "$lab_diagnostics"; then
  mptunnel_build_features='["lab-diagnostics"]'
else
  mptunnel_build_features='[]'
fi
result_reproducibility=""
host_snapshot_file="$result_dir/host-snapshot.json"
host_snapshot_sha256=""
if [[ ! "$log_tail_bytes" =~ ^[0-9]+$ ]] || (( log_tail_bytes < 1 )); then
  echo "MPTUNNEL_LAB_LOG_TAIL_BYTES must be a positive integer" >&2
  exit 2
fi
# Embedded tails cross execve's per-string limit before the total ARG_MAX
# limit. Full diagnostic logs remain available as per-case artifact files.
if (( log_tail_bytes > 60000 )); then
  echo "warning: clamping embedded log tail to 60000 bytes; full logs remain in RESULT_DIR" >&2
  log_tail_bytes=60000
fi
case_filter="${CASE_FILTER:-}"
client_start_settle_seconds="${CLIENT_START_SETTLE_SECONDS:-${CLIENT_SETTLE_SECONDS:-2}}"
client_stop_settle_seconds="${CLIENT_STOP_SETTLE_SECONDS:-${CLIENT_SETTLE_SECONDS:-2}}"
client_start_timeout_seconds="${MPTUNNEL_LAB_CLIENT_START_TIMEOUT_SECONDS:-15}"
if [[ ! "$client_start_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "MPTUNNEL_LAB_CLIENT_START_TIMEOUT_SECONDS must be a positive integer" >&2
  exit 2
fi
isolate_cases="${ISOLATE_CASES:-1}"
isolate_containers="${ISOLATE_CONTAINERS_PER_CASE:-1}"
if [[ -n "${MPTUNNEL_LAB_SECRET:-}" ]]; then
  secret="$MPTUNNEL_LAB_SECRET"
else
  secret="$(python3 -c 'import uuid; print(uuid.uuid4())')"
fi
if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required to generate the ephemeral MPTUNNEL lab TLS identity" >&2
  exit 2
fi
lab_identity_dir="$repo_root/.tmp/lab/identity-$$"
lab_identity_container_dir="/workspace/.tmp/lab/identity-$$"
mkdir -p "$lab_identity_dir"
umask 077
printf '%s' "$secret" > "$lab_identity_dir/credential.key"
printf '%s' "$management_token" > "$lab_identity_dir/management.token"
openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
  -subj "/CN=mptunnel.test" \
  -addext "subjectAltName=DNS:mptunnel.test" \
  -addext "basicConstraints=critical,CA:FALSE" \
  -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=serverAuth" \
  -keyout "$lab_identity_dir/tls-private-key.pem" \
  -out "$lab_identity_dir/tls-certificate.pem" >/dev/null 2>&1
if flag_enabled "$shared_transport_secret"; then
  openssl rand -out "$lab_identity_dir/transport-secret.raw" 32
fi
server_credential_path="$lab_identity_container_dir/credential.key"
server_management_token_path="$lab_identity_container_dir/management.token"
server_tls_certificate_path="$lab_identity_container_dir/tls-certificate.pem"
server_tls_private_key_path="$lab_identity_container_dir/tls-private-key.pem"
server_transport_secret_path="$lab_identity_container_dir/transport-secret.raw"
if [[ "$client_runtime" == "wine" ]]; then
  client_identity_dir="Z:\\workspace\\.tmp\\lab\\identity-$$"
  client_credential_path="${client_identity_dir}\\credential.key"
  client_management_token_path="${client_identity_dir}\\management.token"
  client_tls_certificate_path="${client_identity_dir}\\tls-certificate.pem"
  client_transport_secret_path="${client_identity_dir}\\transport-secret.raw"
else
  client_credential_path="$server_credential_path"
  client_management_token_path="$server_management_token_path"
  client_tls_certificate_path="$server_tls_certificate_path"
  client_transport_secret_path="$server_transport_secret_path"
fi
baseline_uuid="${BASELINE_UUID:-$(SECRET="$secret" python3 -c 'import os, uuid; print(uuid.uuid5(uuid.NAMESPACE_URL, os.environ["SECRET"]))')}"
saturate_protocol="${MPTUNNEL_LAB_SATURATE_PROTOCOL:-udp}"
saturate_udp_packet_bytes="${MPTUNNEL_LAB_SATURATE_UDP_PACKET_BYTES:-1200}"
saturate_tcp_parallel="${MPTUNNEL_LAB_SATURATE_TCP_PARALLEL:-4}"
saturate_lowlat_bandwidth="${MPTUNNEL_LAB_SATURATE_LOWLAT_BANDWIDTH:-70M}"
saturate_balanced_bandwidth="${MPTUNNEL_LAB_SATURATE_BALANCED_BANDWIDTH:-180M}"
saturate_fat_bandwidth="${MPTUNNEL_LAB_SATURATE_FAT_BANDWIDTH:-450M}"
saturate_poor_bandwidth="${MPTUNNEL_LAB_SATURATE_POOR_BANDWIDTH:-45M}"
flap_min_seconds="${MPTUNNEL_LAB_FLAP_MIN_SECONDS:-1}"
flap_max_seconds="${MPTUNNEL_LAB_FLAP_MAX_SECONDS:-4}"
flap_initial_stable_seconds="${MPTUNNEL_LAB_FLAP_INITIAL_STABLE_SECONDS:-10}"
flap_modes="${MPTUNNEL_LAB_FLAP_MODES:-apply,spike-lowlat,spike-balanced,spike-fat,spike-poor,blackhole-lowlat,blackhole-balanced,blackhole-fat,blackhole-poor}"
flap_seed="${MPTUNNEL_LAB_FLAP_SEED:-}"
flap_seed_source=""
flapper_pid=""
flapper_pgid=""
flapper_stop_file=""
flapper_done_file=""
flapper_probe_gate_file=""
flapper_probe_finished_file=""
flapper_trace_file=""
flapper_started_unix_ms=""
flapper_started_monotonic_ms=""
flapper_stop_requested_offset_ms=""
flapper_probe_started_unix_seconds=""
flapper_worker_exit_code=""
flapper_restore_exit_code=""
active_telemetry_case=""
active_telemetry_pid=""
case_telemetry_pid=""
case_management_pid=""
active_mptcp_evidence_case=""
active_client_config_artifact=""
active_bulk_interactive_probe_pid_file=""
active_bulk_interactive_schedule_pids=()
active_bulk_interactive_schedule_services=()
active_bulk_interactive_schedule_pid_files=()
active_bulk_interactive_cancel_file=""
active_bulk_interactive_host_files=()

scale_lab_netem_value() {
  python3 - "$1" "$2" <<'PY'
import re
import sys

value = sys.argv[1]
factor = float(sys.argv[2])
match = re.match(r"^([0-9]+(?:\.[0-9]+)?)(.*)$", value)
if not match:
    print(value)
    raise SystemExit(0)
scaled = float(match.group(1)) * factor
if scaled.is_integer():
    number = str(int(scaled))
else:
    number = f"{scaled:.6f}".rstrip("0").rstrip(".")
print(f"{number}{match.group(2)}")
PY
}

monotonic_milliseconds() {
  printf '%d\n' "$(($(monotonic_time_ns) / 1000000))"
}

normalize_monotonic_timens_offset() {
  python3 -c 'import json, sys

matches = []
for line in sys.stdin:
    fields = line.split()
    if fields and fields[0] == "monotonic":
        matches.append(fields)
if len(matches) != 1 or len(matches[0]) != 3:
    raise SystemExit(2)
try:
    seconds = int(matches[0][1])
    nanoseconds = int(matches[0][2])
except ValueError:
    raise SystemExit(2)
if not 0 <= nanoseconds < 1_000_000_000:
    raise SystemExit(2)
print(json.dumps(
    {"seconds": seconds, "nanoseconds": nanoseconds},
    separators=(",", ":"),
))'
}

balanced_rate_for_mildloss="${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}"
balanced_delay_for_mildloss="${MPTUNNEL_LAB_BALANCED_DELAY:-80ms}"
balanced_jitter_for_mildloss="${MPTUNNEL_LAB_BALANCED_JITTER:-10ms}"
mildloss_rate_for_netem="${MPTUNNEL_LAB_MILDLOSS_RATE:-$(scale_lab_netem_value "$balanced_rate_for_mildloss" 0.5)}"
mildloss_delay_for_netem="${MPTUNNEL_LAB_MILDLOSS_DELAY:-$(scale_lab_netem_value "$balanced_delay_for_mildloss" 2)}"
mildloss_jitter_for_netem="${MPTUNNEL_LAB_MILDLOSS_JITTER:-$balanced_jitter_for_mildloss}"

compose() {
  docker compose -f "$compose_file" "$@"
}

compose_up() {
  compose up --no-build -d --remove-orphans
}

exec_in() {
  local service="$1"
  shift
  compose exec -T "$service" bash -lc "$*"
}

exec_netem() {
  local service="$1"
  local mode="$2"
  local timeout_seconds="${3:-}"
  local -a netem_command=(docker compose -f "$compose_file")
  if [[ -n "$timeout_seconds" ]]; then
    netem_command=(
      timeout --signal=TERM --kill-after=1s "${timeout_seconds}s"
      docker compose -f "$compose_file"
    )
  fi
  "${netem_command[@]}" exec -T \
    -e MPTUNNEL_LAB_LOWLAT_RATE="${MPTUNNEL_LAB_LOWLAT_RATE:-80mbit}" \
    -e MPTUNNEL_LAB_LOWLAT_DELAY="${MPTUNNEL_LAB_LOWLAT_DELAY:-20ms}" \
    -e MPTUNNEL_LAB_LOWLAT_JITTER="${MPTUNNEL_LAB_LOWLAT_JITTER:-2ms}" \
    -e MPTUNNEL_LAB_LOWLAT_LOSS="${MPTUNNEL_LAB_LOWLAT_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_BALANCED_RATE="${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}" \
    -e MPTUNNEL_LAB_BALANCED_DELAY="${MPTUNNEL_LAB_BALANCED_DELAY:-80ms}" \
    -e MPTUNNEL_LAB_BALANCED_JITTER="${MPTUNNEL_LAB_BALANCED_JITTER:-10ms}" \
    -e MPTUNNEL_LAB_BALANCED_LOSS="${MPTUNNEL_LAB_BALANCED_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_MILDLOSS_RATE="$mildloss_rate_for_netem" \
    -e MPTUNNEL_LAB_MILDLOSS_DELAY="$mildloss_delay_for_netem" \
    -e MPTUNNEL_LAB_MILDLOSS_JITTER="$mildloss_jitter_for_netem" \
    -e MPTUNNEL_LAB_MILDLOSS_LOSS="${MPTUNNEL_LAB_MILDLOSS_LOSS:-0.10%}" \
    -e MPTUNNEL_LAB_FAT_RATE="${MPTUNNEL_LAB_FAT_RATE:-500mbit}" \
    -e MPTUNNEL_LAB_FAT_DELAY="${MPTUNNEL_LAB_FAT_DELAY:-180ms}" \
    -e MPTUNNEL_LAB_FAT_JITTER="${MPTUNNEL_LAB_FAT_JITTER:-20ms}" \
    -e MPTUNNEL_LAB_FAT_LOSS="${MPTUNNEL_LAB_FAT_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_TCP_PER_FLOW_QOS_RATE="$tcp_per_flow_qos_rate" \
    -e MPTUNNEL_LAB_TCP_SHARED_BOTTLENECK_RATE="$tcp_shared_bottleneck_rate" \
    -e MPTUNNEL_LAB_POOR_RATE="${MPTUNNEL_LAB_POOR_RATE:-50mbit}" \
    -e MPTUNNEL_LAB_POOR_DELAY="${MPTUNNEL_LAB_POOR_DELAY:-420ms}" \
    -e MPTUNNEL_LAB_POOR_JITTER="${MPTUNNEL_LAB_POOR_JITTER:-120ms}" \
    -e MPTUNNEL_LAB_POOR_LOSS="${MPTUNNEL_LAB_POOR_LOSS:-10.00%}" \
    -e MPTUNNEL_LAB_IDEAL_LOSS="${MPTUNNEL_LAB_IDEAL_LOSS:-0.00%}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_RATE="${MPTUNNEL_LAB_MATRIX_GOOD_RATE:-500mbit}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_RATE="${MPTUNNEL_LAB_MATRIX_POOR_RATE:-50mbit}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_DELAY="${MPTUNNEL_LAB_MATRIX_GOOD_DELAY:-50ms}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_DELAY="${MPTUNNEL_LAB_MATRIX_POOR_DELAY:-250ms}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_JITTER="${MPTUNNEL_LAB_MATRIX_GOOD_JITTER:-5ms}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_JITTER="${MPTUNNEL_LAB_MATRIX_POOR_JITTER:-60ms}" \
    -e MPTUNNEL_LAB_MATRIX_GOOD_LOSS="${MPTUNNEL_LAB_MATRIX_GOOD_LOSS:-1.00%}" \
    -e MPTUNNEL_LAB_MATRIX_POOR_LOSS="${MPTUNNEL_LAB_MATRIX_POOR_LOSS:-15.00%}" \
    -e MPTUNNEL_LAB_SCALE_SEED="$scale_seed" \
    -e MPTUNNEL_LAB_INTERNET_SEED="$internet_seed" \
    -e MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES="$internet_include_outages" \
    -e MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE="$internet_schedule_file" \
    -e MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256="$internet_schedule_sha256" \
    -e MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY="$internet_load_queue_delay" \
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
  local -a feature_args=()
  if flag_enabled "$lab_diagnostics"; then
    feature_args=(--features lab-diagnostics)
  fi
  CARGO_TARGET_DIR="$host_build_root" \
    cargo build --release --locked --bin mptunnel "${feature_args[@]}"
  if [[ "$client_runtime" == "wine" ]]; then
    CARGO_TARGET_DIR="$host_build_root" \
      cargo build --release --locked --target "$client_target" --bin mptunnel "${feature_args[@]}"
  fi
}

require_prebuilt_product() {
  [[ -x "${host_build_root}/release/mptunnel" ]] || {
    echo "BUILD_PRODUCT=0 requires an executable server binary at ${host_build_root}/release/mptunnel" >&2
    return 2
  }
  [[ -x "$client_binary_host" ]] || {
    echo "BUILD_PRODUCT=0 requires an executable client binary at $client_binary_host" >&2
    return 2
  }
}

client_mptunnel_command() {
  if [[ "$client_runtime" == "wine" ]]; then
    printf 'env WINEDEBUG=-all WINEPREFIX=%q wine %q' \
      "$wine_prefix" "$client_binary_container"
  else
    printf '%q' "$client_binary_container"
  fi
}

prepare_client_runtime() {
  if [[ "$client_runtime" != "wine" ]]; then
    return 0
  fi
  exec_in client "command -v wine >/dev/null || { echo 'Wine client runtime requires MPTUNNEL_LAB_INSTALL_WINE=1 when building the lab image' >&2; exit 2; }; test -x '$client_binary_container'"
  exec_in client "if [ ! -d ${wine_prefix_shell}/drive_c ]; then if ! timeout ${client_start_timeout_seconds}s env WINEDEBUG=-all WINEPREFIX=${wine_prefix_shell} wineboot --init >/tmp/mptunnel-wineboot.log 2>&1; then echo 'timed out initializing the Wine client runtime' >&2; tail -n 80 /tmp/mptunnel-wineboot.log >&2 || true; exit 1; fi; if ! timeout ${client_start_timeout_seconds}s env WINEDEBUG=-all WINEPREFIX=${wine_prefix_shell} wineserver -w; then echo 'timed out waiting for Wine initialization to stop' >&2; exit 1; fi; fi"
}

capture_host_snapshot() {
  local -a lab_container_ids=()
  local -a snapshot_command=(
    python3 "$script_dir/host_snapshot.py" capture
    --repo-root "$repo_root"
    --output "$host_snapshot_file"
  )
  local container_id
  mapfile -t lab_container_ids < <(compose ps -q)
  if (( ${#lab_container_ids[@]} < 3 )); then
    echo "unable to identify all running lab containers for host validity" >&2
    exit 2
  fi
  for container_id in "${lab_container_ids[@]}"; do
    snapshot_command+=(--exclude-container-id "$container_id")
  done
  "${snapshot_command[@]}"
  host_snapshot_sha256="$(sha256sum "$host_snapshot_file" | awk '{print $1}')"
}

refresh_result_reproducibility() {
  local server_target server_sha256 client_sha256 runtime_version
  server_target="$(rustc -vV | sed -n 's/^host: //p')"
  server_sha256="$(sha256sum "${host_build_root}/release/mptunnel" | awk '{print $1}')"
  client_sha256="$(sha256sum "$client_binary_host" | awk '{print $1}')"
  runtime_version="$client_runtime"
  if [[ "$client_runtime" == "wine" ]]; then
    runtime_version="$(exec_in client 'wine --version 2>/dev/null')"
  fi
  result_reproducibility="$(
    SOURCE_COMMIT="$source_commit" \
    SOURCE_TREE_DIRTY="$source_tree_dirty" \
    MPTUNNEL_BUILD_FEATURES="$mptunnel_build_features" \
    MPTUNNEL_PROTOCOL_VERSION="$mptunnel_protocol_version" \
    MPTUNNEL_CARRIER_PRESENTATION="$mptunnel_carrier_presentation" \
    MPTUNNEL_TRANSPORT_PROFILE="$mptunnel_transport_profile" \
    MPTUNNEL_CLIENT_RUNTIME="$client_runtime" \
    MPTUNNEL_CLIENT_RUNTIME_VERSION="$runtime_version" \
    MPTUNNEL_CLIENT_TARGET="$client_target" \
    MPTUNNEL_CLIENT_SHA256="$client_sha256" \
    MPTUNNEL_SERVER_TARGET="$server_target" \
    MPTUNNEL_SERVER_SHA256="$server_sha256" \
    HOST_SNAPSHOT_FILE="$host_snapshot_file" \
    HOST_SNAPSHOT_SHA256="$host_snapshot_sha256" \
    LAB_SCRIPT_DIR="$script_dir" \
      python3 - <<'PY'
import json
import os
import sys

sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import load_host_reproducibility

_, host_fields = load_host_reproducibility(
    os.environ["HOST_SNAPSHOT_FILE"],
    os.environ["HOST_SNAPSHOT_SHA256"],
)

identity = {
    "source_commit": os.environ["SOURCE_COMMIT"],
    "source_tree_dirty": os.environ["SOURCE_TREE_DIRTY"] == "true",
    "mptunnel_build_profile": "release",
    "mptunnel_build_features": json.loads(os.environ["MPTUNNEL_BUILD_FEATURES"]),
    "mptunnel_protocol_version": int(os.environ["MPTUNNEL_PROTOCOL_VERSION"]),
    "mptunnel_carrier_presentation": os.environ[
        "MPTUNNEL_CARRIER_PRESENTATION"
    ],
    "mptunnel_transport_profile": os.environ["MPTUNNEL_TRANSPORT_PROFILE"],
    "mptunnel_client_runtime": os.environ["MPTUNNEL_CLIENT_RUNTIME"],
    "mptunnel_client_runtime_version": os.environ["MPTUNNEL_CLIENT_RUNTIME_VERSION"],
    "mptunnel_client_target": os.environ["MPTUNNEL_CLIENT_TARGET"],
    "mptunnel_client_sha256": os.environ["MPTUNNEL_CLIENT_SHA256"],
    "mptunnel_server_target": os.environ["MPTUNNEL_SERVER_TARGET"],
    "mptunnel_server_sha256": os.environ["MPTUNNEL_SERVER_SHA256"],
}
identity.update(host_fields)
print(json.dumps(identity, separators=(",", ":"), sort_keys=True))
PY
  )"
}

write_run_manifest() {
  local client_container_id server_container_id target_container_id
  local client_image_id server_image_id target_image_id
  client_container_id="$(compose ps -q client)"
  server_container_id="$(compose ps -q server)"
  target_container_id="$(compose ps -q target)"
  client_image_id="$(docker inspect -f '{{.Image}}' "$client_container_id")"
  server_image_id="$(docker inspect -f '{{.Image}}' "$server_container_id")"
  target_image_id="$(docker inspect -f '{{.Image}}' "$target_container_id")"
  compose config | REPO_ROOT="$repo_root" python3 -c \
    'import os, sys; sys.stdout.write(sys.stdin.read().replace(os.environ["REPO_ROOT"], "."))' \
    > "$result_dir/compose-config.yaml"
  RESULT_REPRODUCIBILITY="$result_reproducibility" \
  RESULT_FILE="$result_file" \
  CASE_FILTER_VALUE="$case_filter" \
  OBJECT_MIB="$object_mib" \
  LOAD_DURATION_SECONDS="$load_duration_seconds" \
  UPLOAD_COMPLETION_TIMEOUT_SECONDS="$curl_timeout" \
  BULK_CONNECTIONS="$bulk_connections" \
  BULK_INTERACTIVE_DYNAMIC_LOSS="$bulk_interactive_dynamic_loss_json" \
  FAILOVER_AFTER_SECONDS="$failover_after" \
  FAILOVER_PROFILE="$failover_profile" \
  FAILOVER_TX_TRIGGER_BYTES="$failover_tx_trigger_bytes" \
  BUILD_PRODUCT="$build_product" \
  BUILD_LAB_IMAGES="$build_lab_images" \
  ISOLATE_CASES_VALUE="$isolate_cases" \
  ISOLATE_CONTAINERS_VALUE="$isolate_containers" \
  CLIENT_SETTLE_SECONDS="$client_start_settle_seconds" \
  CLIENT_START_TIMEOUT_SECONDS="$client_start_timeout_seconds" \
  LAB_DIAGNOSTICS_VALUE="$lab_diagnostics" \
  LAB_PERF_VALUE="$lab_perf" \
  CONTAINER_STATS_VALUE="$container_stats" \
  MANAGEMENT_SNAPSHOTS_VALUE="$management_snapshots" \
  USE_PATH_HINTS_VALUE="${MPTUNNEL_LAB_USE_PATH_HINTS:-0}" \
  REQUIRE_COMPETITOR_BASELINES_VALUE="$require_competitor_baselines" \
  CLIENT_IMAGE_ID="$client_image_id" \
  SERVER_IMAGE_ID="$server_image_id" \
  TARGET_IMAGE_ID="$target_image_id" \
  HOST_SNAPSHOT_FILE="$host_snapshot_file" \
  HOST_SNAPSHOT_SHA256="$host_snapshot_sha256" \
  DOCKER_VERSION="$(docker version --format '{{.Client.Version}}')" \
  COMPOSE_VERSION="$(docker compose version --short)" \
  BASELINE_LOCK_FILE="$script_dir/baseline-lock.json" \
  BASELINE_LOCK_SHA256="$baseline_lock_sha256" \
  LAB_SCRIPT_DIR="$script_dir" \
    python3 - "$result_dir/run-manifest.json" <<'PY'
import os
import sys
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import write_run_manifest

write_run_manifest(sys.argv[1], os.environ)
PY
}

toml_string() {
  python3 - "$1" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1]))
PY
}

toml_array_from_args() {
  python3 - "$@" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1:]))
PY
}

toml_named_paths_from_args() {
  python3 - "$@" <<'PY'
import json
import sys

paths = [
    f'{{ name = {json.dumps(f"path-{index}")}, endpoint = {json.dumps(endpoint)} }}'
    for index, endpoint in enumerate(sys.argv[1:], start=1)
]
print(f"[{', '.join(paths)}]")
PY
}

path_args_to_named_path_array() {
  local path_args="$1"
  python3 - "$path_args" <<'PY'
import json
import shlex
import sys

tokens = shlex.split(sys.argv[1])
endpoints = []
index = 0
while index < len(tokens):
    token = tokens[index]
    if token != "--path":
        raise SystemExit(f"unsupported mptunnel lab path argument: {token}")
    if index + 1 >= len(tokens):
        raise SystemExit("--path requires an endpoint")
    endpoints.append(tokens[index + 1])
    index += 2

if not endpoints:
    raise SystemExit("lab mptunnel client config requires at least one endpoint")

paths = [
    f'{{ name = {json.dumps(f"path-{index}")}, endpoint = {json.dumps(endpoint)} }}'
    for index, endpoint in enumerate(endpoints, start=1)
]
print(f"[{', '.join(paths)}]")
PY
}

resource_config_toml() {
  local env_name key value lines=""
  local -a mappings=(
    MPTUNNEL_MAX_FRAME_BYTES:max_frame_bytes
    MPTUNNEL_MAX_PAYLOAD_BYTES:max_payload_bytes
    MPTUNNEL_MAX_ACK_RANGES:max_ack_ranges
    MPTUNNEL_MAX_PATHS:max_paths
    MPTUNNEL_MAX_STREAMS:max_streams
    MPTUNNEL_MAX_QUIC_CONCURRENT_BIDI_STREAMS:max_quic_concurrent_bidi_streams
    MPTUNNEL_MAX_STREAM_WINDOW_BYTES:max_stream_window_bytes
    MPTUNNEL_MAX_REPAIR_BYTES:max_repair_bytes
    MPTUNNEL_MAX_REORDER_BYTES:max_reorder_bytes
    MPTUNNEL_MAX_REINJECTION_CACHE_CHUNKS:max_reinjection_cache_chunks
    MPTUNNEL_MAX_REORDER_BUFFER_CHUNKS:max_reorder_buffer_chunks
    MPTUNNEL_MAX_RETAINED_RECEIVE_RANGES:max_retained_receive_ranges
    MPTUNNEL_MAX_DATAGRAM_QUEUE_BYTES:max_datagram_queue_bytes
    MPTUNNEL_MAX_PATH_FLIGHT_BYTES:max_path_flight_bytes
    MPTUNNEL_MAX_RELIABLE_RELAY_CHUNK_BYTES:max_reliable_relay_chunk_bytes
    MPTUNNEL_TCP_PATH_HEARTBEAT_INTERVAL_S:tcp_path_heartbeat_interval_s
    MPTUNNEL_TCP_PATH_HEARTBEAT_TIMEOUT_S:tcp_path_heartbeat_timeout_s
    MPTUNNEL_QUIC_PATH_KEEP_ALIVE_INTERVAL_S:quic_path_keep_alive_interval_s
    MPTUNNEL_QUIC_PATH_IDLE_TIMEOUT_S:quic_path_idle_timeout_s
  )
  for mapping in "${mappings[@]}"; do
    env_name="${mapping%%:*}"
    key="${mapping#*:}"
    value="${!env_name:-}"
    if [[ -n "$value" ]]; then
      if [[ "$key" == *_s ]]; then
        if [[ ! "$value" =~ ^(0|[1-9][0-9]*)(\.[0-9]+)?$ ]]; then
          echo "$env_name must be non-negative decimal seconds when passed into lab config" >&2
          return 2
        fi
      elif [[ ! "$value" =~ ^[0-9]+$ ]]; then
        echo "$env_name must be an unsigned integer when passed into lab config" >&2
        return 2
      fi
      lines+="${key} = ${value}"$'\n'
    fi
  done
  if [[ -n "$lines" ]]; then
    printf '[resources]\n%s\n' "$lines"
  fi
}

probe_config_toml() {
  if [[ -n "${PATH_PROBE_INTERVAL_S:-}" ]]; then
    if [[ ! "${PATH_PROBE_INTERVAL_S}" =~ ^(0|[1-9][0-9]*)(\.[0-9]+)?$ ]]; then
      echo "PATH_PROBE_INTERVAL_S must be non-negative decimal seconds" >&2
      return 2
    fi
    printf 'path_probe_interval_s = %s\n' "$PATH_PROBE_INTERVAL_S"
  fi
  if [[ -n "${PATH_PROBE_TIMEOUT_S:-}" ]]; then
    if [[ ! "${PATH_PROBE_TIMEOUT_S}" =~ ^(0|[1-9][0-9]*)(\.[0-9]+)?$ ]]; then
      echo "PATH_PROBE_TIMEOUT_S must be non-negative decimal seconds" >&2
      return 2
    fi
    printf 'path_probe_timeout_s = %s\n' "$PATH_PROBE_TIMEOUT_S"
  fi
}

mptunnel_lab_env_prefix() {
  local role="$1"
  printf 'MPTUNNEL_LAB_DIAG=%q MPTUNNEL_LAB_DIAG_EVENTS=%q MPTUNNEL_LAB_PERF=%q MPTUNNEL_LAB_PERF_SAMPLES=%q MPTUNNEL_LAB_ROLE=%q MPTUNNEL_LAB_PERF_INTERVAL_MS=%q ' \
    "${MPTUNNEL_LAB_DIAG:-0}" \
    "${MPTUNNEL_LAB_DIAG_EVENTS:-}" \
    "$lab_perf" \
    "$lab_perf_samples" \
    "$role" \
    "$lab_perf_interval_ms"
}

write_in() {
  local service="$1"
  local path="$2"
  local content="$3"
  local quoted_path
  printf -v quoted_path '%q' "$path"
  printf '%s' "$content" | compose exec -T "$service" bash -lc "cat > $quoted_path"
}

record_config_checksum() {
  local artifact_name="$1"
  local output="$result_dir/$artifact_name"
  local checksum checksum_file checksum_tmp
  checksum="$(sha256sum "$output" | awk '{print $1}')"
  checksum_file="$result_dir/config-sha256.txt"
  checksum_tmp="${checksum_file}.tmp"
  awk -v name="$artifact_name" '$2 != name' "$checksum_file" > "$checksum_tmp"
  printf '%s  %s\n' "$checksum" "$artifact_name" >> "$checksum_tmp"
  mv "$checksum_tmp" "$checksum_file"
}

persist_redacted_config() {
  local service="$1"
  local source_path="$2"
  local artifact_name="$3"
  local output="$result_dir/$artifact_name"
  exec_in "$service" "sed -E 's/^(secret|token) = .*/\1 = \"<redacted>\"/' '$source_path'" > "$output"
  record_config_checksum "$artifact_name"
}

retain_active_client_config_for_case() {
  local case_name="$1"
  local artifact_name output
  if [[ -z "$active_client_config_artifact" || ! -f "$active_client_config_artifact" ]]; then
    return 0
  fi
  artifact_name="config-client-$(case_artifact_name "$case_name").toml"
  output="$result_dir/$artifact_name"
  if [[ "$active_client_config_artifact" != "$output" ]]; then
    cp "$active_client_config_artifact" "$output"
  fi
  record_config_checksum "$artifact_name"
}

validate_mptunnel_config_in() {
  local service="$1"
  local path="$2"
  local command="/workspace/target/release/mptunnel"
  if [[ "$service" == "client" ]]; then
    command="$(client_mptunnel_command)"
  fi
  exec_in "$service" "$command --config '$path' --check-config"
}

wait_for_client_proxy() {
  local log_path="$1"
  local port_hex
  printf -v port_hex '%04X' "$proxy_port"
  exec_in client "pid=\$(cat /tmp/mptunnel-client.pid); deadline=\$((SECONDS + $client_start_timeout_seconds)); while ! awk -v port=':${port_hex}' '\$2 ~ port \"\$\" && \$4 == \"0A\" { found=1 } END { exit !found }' /proc/net/tcp /proc/net/tcp6; do if ! kill -0 \"\$pid\" >/dev/null 2>&1; then echo 'mptunnel client exited before the SOCKS listener became ready' >&2; tail -n 80 '$log_path' >&2 || true; exit 1; fi; if [ \$SECONDS -ge \$deadline ]; then echo 'timed out waiting for the mptunnel SOCKS listener' >&2; tail -n 80 '$log_path' >&2 || true; exit 1; fi; sleep 0.05; done"
}

management_config_toml() {
  if ! flag_enabled "$management_snapshots"; then
    return 0
  fi
  if [[ ! "$management_snapshot_port" =~ ^[0-9]+$ ]] || (( management_snapshot_port < 1 || management_snapshot_port > 65535 )); then
    echo "MPTUNNEL_LAB_MANAGEMENT_PORT must be an integer from 1 through 65535" >&2
    return 2
  fi
  local token_path="$1"
  local token_path_json
  token_path_json="$(toml_string "$token_path")"
  printf '[management]\nlisten = ["127.0.0.1:%s"]\ntoken = { from = "file", path = %s }\n' \
    "$management_snapshot_port" "$token_path_json"
}

shared_transport_secret_toml() {
  local path="$1"
  if flag_enabled "$shared_transport_secret"; then
    printf 'transport_secret = { from = "file", path = %s }\n' "$(toml_string "$path")"
  fi
}

server_config_toml() {
  local log_level_json credential_path_json certificate_path_json private_key_path_json
  local paths resources management transport_security
  log_level_json="$(toml_string "$lab_log_level")"
  credential_path_json="$(toml_string "$server_credential_path")"
  certificate_path_json="$(toml_string "$server_tls_certificate_path")"
  private_key_path_json="$(toml_string "$server_tls_private_key_path")"
  transport_security="$(shared_transport_secret_toml "$server_transport_secret_path")"
  paths="$(toml_named_paths_from_args \
    "tcp://172.31.10.20:${server_port}" \
    "tcp://172.31.15.20:${server_port}" \
    "tcp://172.31.16.20:${server_port}" \
    "tcp://172.31.20.20:${server_port}" \
    "tcp://172.31.30.20:${server_port}" \
    "tcp://172.31.41.20:${server_port}" \
    "tcp://172.31.42.20:${server_port}" \
    "tcp://172.31.43.20:${server_port}" \
    "tcp://172.31.44.20:${server_port}" \
    "tcp://172.31.45.20:${server_port}" \
    "quic://172.31.10.20:${server_port}" \
    "quic://172.31.15.20:${server_port}" \
    "quic://172.31.16.20:${server_port}" \
    "quic://172.31.20.20:${server_port}" \
    "quic://172.31.30.20:${server_port}" \
    "quic://172.31.51.20:${server_port}" \
    "quic://172.31.52.20:${server_port}" \
    "quic://172.31.53.20:${server_port}" \
    "quic://172.31.54.20:${server_port}" \
    "quic://172.31.55.20:${server_port}" \
    "quic://172.31.56.20:${server_port}" \
    "quic://172.31.57.20:${server_port}" \
    "quic://172.31.58.20:${server_port}" \
    "quic://172.31.59.20:${server_port}" \
    "quic://172.31.60.20:${server_port}")"
  resources="$(resource_config_toml)"
  management="$(management_config_toml "$server_management_token_path")"
  if [[ -n "$resources" ]]; then
    resources="${resources}"$'\n\n'
  fi
  if [[ -n "$management" ]]; then
    management="${management}"$'\n'
  fi
  cat <<EOF
[logging]
level = ${log_level_json}

${resources}${management}[[credentials]]
credential_id = "lab"
principal_id = "lab"
secret = { from = "file", path = ${credential_path_json} }

[[inbounds]]
name = "lab-mpp-in"
protocol = "mpp"
paths = ${paths}

[inbounds.security]
credential_ids = ["lab"]
tls_certificate_chain = { from = "file", path = ${certificate_path_json} }
tls_private_key = { from = "file", path = ${private_key_path_json} }
${transport_security}

[[outbounds]]
name = "lab-direct"
protocol = "direct"

[routing]

[[routing.rules]]
name = "allow-lab-private-targets"
inbounds = ["lab-mpp-in"]
principal_ids = ["lab"]
destination_cidrs = ["172.31.0.0/16"]
networks = ["tcp", "udp"]
decision = "allow-restricted"
outbound = "lab-direct"
EOF
}

socks_client_config_toml() {
  local path_args="$1"
  local log_level_json credential_path_json certificate_path_json
  local listen paths resources probe management transport_security
  log_level_json="$(toml_string "$lab_log_level")"
  credential_path_json="$(toml_string "$client_credential_path")"
  certificate_path_json="$(toml_string "$client_tls_certificate_path")"
  transport_security="$(shared_transport_secret_toml "$client_transport_secret_path")"
  listen="$(toml_array_from_args "127.0.0.1:${proxy_port}")"
  paths="$(path_args_to_named_path_array "$path_args")"
  resources="$(resource_config_toml)"
  probe="$(probe_config_toml)"
  management="$(management_config_toml "$client_management_token_path")"
  if [[ -n "$resources" ]]; then
    resources="${resources}"$'\n\n'
  fi
  if [[ -n "$probe" ]]; then
    probe="${probe}"$'\n'
  fi
  if [[ -n "$management" ]]; then
    management="${management}"$'\n'
  fi
  cat <<EOF
[logging]
level = ${log_level_json}

${resources}${management}[[credentials]]
credential_id = "lab"
principal_id = "lab"
secret = { from = "file", path = ${credential_path_json} }

[[inbounds]]
name = "lab-socks"
protocol = "socks5"
listen = ${listen}

[[outbounds]]
name = "lab-mpp-out"
protocol = "mpp"
paths = ${paths}
${probe}
[outbounds.security]
credential_id = "lab"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "file", path = ${certificate_path_json} }
${transport_security}

[[outbounds]]
name = "lab-client-direct"
protocol = "direct"

[routing]

[[routing.rules]]
name = "allow-lab-client-direct-control"
inbounds = ["lab-socks"]
destination_cidrs = ["172.31.15.30/32"]
networks = ["tcp", "udp"]
decision = "allow-restricted"
outbound = "lab-client-direct"

[[routing.rules]]
name = "allow-lab-private-targets"
inbounds = ["lab-socks"]
destination_cidrs = ["172.31.0.0/16"]
networks = ["tcp", "udp"]
decision = "allow-restricted"
outbound = "lab-mpp-out"
EOF
}

tun_client_config_toml() {
  local path_args="$1"
  local log_level_json credential_path_json certificate_path_json
  local paths resources probe management transport_security
  log_level_json="$(toml_string "$lab_log_level")"
  credential_path_json="$(toml_string "$client_credential_path")"
  certificate_path_json="$(toml_string "$client_tls_certificate_path")"
  transport_security="$(shared_transport_secret_toml "$client_transport_secret_path")"
  paths="$(path_args_to_named_path_array "$path_args")"
  resources="$(resource_config_toml)"
  probe="$(probe_config_toml)"
  management="$(management_config_toml "$client_management_token_path")"
  if [[ -n "$resources" ]]; then
    resources="${resources}"$'\n\n'
  fi
  if [[ -n "$probe" ]]; then
    probe="${probe}"$'\n'
  fi
  if [[ -n "$management" ]]; then
    management="${management}"$'\n'
  fi
  cat <<EOF
[logging]
level = ${log_level_json}

${resources}${management}[[credentials]]
credential_id = "lab"
principal_id = "lab"
secret = { from = "file", path = ${credential_path_json} }

[[inbounds]]
name = "lab-tun"
protocol = "tun"
interface_name = "mptun0"
ipv4 = "10.88.0.1"
ipv4_prefix = 24

[[outbounds]]
name = "lab-mpp-out"
protocol = "mpp"
paths = ${paths}
${probe}
[outbounds.security]
credential_id = "lab"
tls_server_name = "mptunnel.test"
tls_pinned_certificate = { from = "file", path = ${certificate_path_json} }
${transport_security}

[routing]

[[routing.rules]]
name = "allow-lab-private-targets"
inbounds = ["lab-tun"]
destination_cidrs = ["172.31.0.0/16"]
networks = ["tcp", "udp"]
decision = "allow-restricted"
outbound = "lab-mpp-out"
EOF
}

case_artifact_name() {
  local raw="$1"
  printf '%s' "$raw" | tr -c 'A-Za-z0-9_.=-' '_'
}

mptcp_evidence_file_for_case() {
  local case_name="$1"
  printf '%s/mptcp-evidence-%s.jsonl' "$result_dir" "$(case_artifact_name "$case_name")"
}

mptcp_evidence_container_file_for_case() {
  local case_name="$1"
  local service="$2"
  printf '/tmp/mptunnel-mptcp-evidence-%s-%s.jsonl' "$(case_artifact_name "$case_name")" "$service"
}

mptcp_evidence_container_stop_file_for_case() {
  local case_name="$1"
  local service="$2"
  printf '/tmp/mptunnel-mptcp-evidence-%s-%s.stop' "$(case_artifact_name "$case_name")" "$service"
}

mptcp_evidence_container_pid_file_for_case() {
  local case_name="$1"
  local service="$2"
  printf '/tmp/mptunnel-mptcp-evidence-%s-%s.pid' "$(case_artifact_name "$case_name")" "$service"
}

record_mptcp_evidence_error() {
  local case_name="$1"
  local service="$2"
  local error="$3"
  SERVICE="$service" ERROR="$error" python3 - "$(mptcp_evidence_file_for_case "$case_name")" <<'PY'
import json
import os
import sys

with open(sys.argv[1], "a", encoding="utf-8") as handle:
    print(json.dumps({
        "kind": "sampler_error",
        "schema_version": 1,
        "service": os.environ["SERVICE"],
        "error": os.environ["ERROR"][-2000:],
    }, sort_keys=True), file=handle)
PY
}

start_mptcp_evidence() {
  local case_name="$1"
  local host_file service sample_file stop_file pid_file start_error
  host_file="$(mptcp_evidence_file_for_case "$case_name")"
  : > "$host_file"
  for service in client target; do
    sample_file="$(mptcp_evidence_container_file_for_case "$case_name" "$service")"
    stop_file="$(mptcp_evidence_container_stop_file_for_case "$case_name" "$service")"
    pid_file="$(mptcp_evidence_container_pid_file_for_case "$case_name" "$service")"
    if ! start_error="$(exec_in "$service" "rm -f '${sample_file}' '${stop_file}' '${pid_file}'; python3 /workspace/lab/mptcp_evidence.py sample --service '${service}' --output '${sample_file}' --stop-file '${stop_file}' --interval '${mptcp_evidence_interval_seconds}' --max-duration '${mptcp_evidence_max_duration_seconds}' >/tmp/mptunnel-mptcp-evidence-${service}.log 2>&1 & echo \$! > '${pid_file}'; test -s '${pid_file}'" 2>&1)"; then
      record_mptcp_evidence_error "$case_name" "$service" "sampler start failed: ${start_error}"
    fi
  done
  active_mptcp_evidence_case="$case_name"
}

stop_mptcp_evidence() {
  local case_name="$1"
  local host_file service sample_file stop_file pid_file tmp_file sampler_error
  host_file="$(mptcp_evidence_file_for_case "$case_name")"
  for service in client target; do
    sample_file="$(mptcp_evidence_container_file_for_case "$case_name" "$service")"
    stop_file="$(mptcp_evidence_container_stop_file_for_case "$case_name" "$service")"
    pid_file="$(mptcp_evidence_container_pid_file_for_case "$case_name" "$service")"
    exec_in "$service" "touch '${stop_file}'; if [ -s '${pid_file}' ]; then pid=\$(cat '${pid_file}'); deadline=\$((SECONDS + 3)); while kill -0 \"\$pid\" >/dev/null 2>&1 && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; if kill -0 \"\$pid\" >/dev/null 2>&1; then kill -TERM \"\$pid\" >/dev/null 2>&1 || true; fi; fi" >/dev/null 2>&1 || true
    tmp_file="${host_file}.${service}.tmp"
    if exec_in "$service" "test -s '${sample_file}' && cat '${sample_file}'" > "$tmp_file" 2>/dev/null; then
      cat "$tmp_file" >> "$host_file"
    else
      sampler_error="$(exec_in "$service" "tail -c 2000 /tmp/mptunnel-mptcp-evidence-${service}.log 2>/dev/null || true" 2>/dev/null || true)"
      record_mptcp_evidence_error "$case_name" "$service" "sampler produced no artifact: ${sampler_error}"
    fi
    rm -f "$tmp_file"
    exec_in "$service" "rm -f '${sample_file}' '${stop_file}' '${pid_file}'" >/dev/null 2>&1 || true
  done
  if [[ "$active_mptcp_evidence_case" == "$case_name" ]]; then
    active_mptcp_evidence_case=""
  fi
}

mptcp_evidence_summary() {
  local case_name="$1"
  local evidence_file
  evidence_file="$(mptcp_evidence_file_for_case "$case_name")"
  python3 "$script_dir/mptcp_evidence.py" summarize \
    --input "$evidence_file" \
    --artifact "$evidence_file" 2>/dev/null || printf '{"collection_ok":false,"aggregation_evidence":"unavailable"}'
}

telemetry_file_for_case() {
  local case_name="$1"
  printf '%s/container-stats-%s.jsonl' "$result_dir" "$(case_artifact_name "$case_name")"
}

management_snapshot_file_for_case() {
  local case_name="$1"
  printf '%s/management-snapshots-%s.jsonl' "$result_dir" "$(case_artifact_name "$case_name")"
}

netdev_snapshot_file_for_case() {
  local case_name="$1"
  local phase="$2"
  printf '%s/netdev-%s-%s.json' "$result_dir" "$(case_artifact_name "$case_name")" "$phase"
}

qdisc_snapshot_file_for_case() {
  local case_name="$1"
  local phase="$2"
  printf '%s/qdisc-%s-%s.txt' "$result_dir" "$(case_artifact_name "$case_name")" "$phase"
}

capture_qdisc_snapshot() {
  local case_name="$1"
  local phase="$2"
  local output tmp service
  output="$(qdisc_snapshot_file_for_case "$case_name" "$phase")"
  tmp="${output}.tmp"
  : > "$tmp"
  for service in client server target; do
    printf '[%s]\n' "$service" >> "$tmp"
    exec_netem "$service" show >> "$tmp"
  done
  mv "$tmp" "$output"
}

failover_trigger_file_for_case() {
  local case_name="$1"
  printf '%s/failover-trigger-%s.json' "$result_dir" "$(case_artifact_name "$case_name")"
}

telemetry_stop_file_for_case() {
  local case_name="$1"
  printf '%s/container-stats-%s.stop' "$result_dir" "$(case_artifact_name "$case_name")"
}

telemetry_enabled() {
  case "$container_stats" in
    0|false|FALSE|no|NO) return 1 ;;
    *) return 0 ;;
  esac
}

management_snapshots_enabled() {
  flag_enabled "$management_snapshots"
}

log_collection_enabled() {
  case "$collect_logs" in
    0|false|FALSE|no|NO) return 1 ;;
    *) return 0 ;;
  esac
}

start_case_telemetry() {
  local case_name="$1"
  case_telemetry_pid=""
  case_management_pid=""
  retain_active_client_config_for_case "$case_name"
  capture_qdisc_snapshot "$case_name" before
  if ! telemetry_enabled && ! management_snapshots_enabled; then
    return 0
  fi
  local telemetry_file management_file stop_file before_file after_file
  telemetry_file="$(telemetry_file_for_case "$case_name")"
  management_file="$(management_snapshot_file_for_case "$case_name")"
  stop_file="$(telemetry_stop_file_for_case "$case_name")"
  before_file="$(netdev_snapshot_file_for_case "$case_name" before)"
  after_file="$(netdev_snapshot_file_for_case "$case_name" after)"
  rm -f "$telemetry_file" "$management_file" "$stop_file" "$before_file" "$after_file"
  if telemetry_enabled; then
    python3 "$script_dir/container_stats.py" snapshot \
      --compose-file "$compose_file" \
      > "$before_file" 2>/dev/null || true
    python3 "$script_dir/container_stats.py" sample \
      --compose-file "$compose_file" \
      --case "$case_name" \
      --output "$telemetry_file" \
      --stop-file "$stop_file" \
      --interval "$container_stats_interval" \
      >/dev/null 2>&1 &
    case_telemetry_pid="$!"
  fi
  if management_snapshots_enabled; then
    python3 "$script_dir/management_snapshots.py" \
      --compose-file "$compose_file" \
      --case "$case_name" \
      --output "$management_file" \
      --stop-file "$stop_file" \
      --interval "$management_snapshot_interval" \
      --port "$management_snapshot_port" \
      --token "$management_token" \
      >/dev/null 2>&1 &
    case_management_pid="$!"
  fi
  active_telemetry_case="$case_name"
  active_telemetry_pid="$case_telemetry_pid"
}

stop_case_telemetry() {
  local case_name="$1"
  local sampler_pid="${2:-}"
  if ! telemetry_enabled && ! management_snapshots_enabled; then
    capture_qdisc_snapshot "$case_name" after
    return 0
  fi
  touch "$(telemetry_stop_file_for_case "$case_name")" >/dev/null 2>&1 || true
  if [[ -n "$sampler_pid" ]]; then
    wait "$sampler_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$case_management_pid" ]]; then
    wait "$case_management_pid" >/dev/null 2>&1 || true
    case_management_pid=""
  fi
  if telemetry_enabled; then
    python3 "$script_dir/container_stats.py" snapshot \
      --compose-file "$compose_file" \
      > "$(netdev_snapshot_file_for_case "$case_name" after)" 2>/dev/null || true
  fi
  capture_qdisc_snapshot "$case_name" after
  if [[ "$active_telemetry_case" == "$case_name" ]]; then
    active_telemetry_case=""
    active_telemetry_pid=""
  fi
}

case_telemetry_summary() {
  local case_name="$1"
  if ! telemetry_enabled; then
    printf '{}'
    return 0
  fi
  local telemetry_file before_file after_file
  telemetry_file="$(telemetry_file_for_case "$case_name")"
  before_file="$(netdev_snapshot_file_for_case "$case_name" before)"
  after_file="$(netdev_snapshot_file_for_case "$case_name" after)"
  python3 "$script_dir/container_stats.py" summarize \
    --input "$telemetry_file" \
    --netdev-before "$before_file" \
    --netdev-after "$after_file" \
    2>/dev/null || printf '{}'
}

case_log_artifacts_summary() {
  local case_name="$1"
  if ! log_collection_enabled; then
    printf '{}'
    return 0
  fi

  local case_artifact service output tmp
  case_artifact="$(case_artifact_name "$case_name")"
  for service in client server target; do
    output="${result_dir}/logs-${case_artifact}-${service}.log"
    tmp="${output}.tmp"
    if exec_in "$service" "for file in /tmp/mptunnel*.log; do [ -f \"\$file\" ] || continue; echo \"== ${service}:\$(basename \"\$file\") ==\"; cat \"\$file\"; done" > "$tmp" 2>/dev/null; then
      if [[ -s "$tmp" ]]; then
        mv "$tmp" "$output"
      else
        rm -f "$tmp" "$output"
      fi
    else
      rm -f "$tmp"
    fi
  done

  CASE_ARTIFACT="$case_artifact" RESULT_DIR="$result_dir" python3 - <<'PY'
import json
import os
from pathlib import Path

result_dir = Path(os.environ["RESULT_DIR"])
case_artifact = os.environ["CASE_ARTIFACT"]
services = {}
for service in ("client", "server", "target"):
    path = result_dir / f"logs-{case_artifact}-{service}.log"
    if path.exists() and path.stat().st_size > 0:
        services[service] = {"file": str(path), "bytes": path.stat().st_size}
print(json.dumps({"services": services}, separators=(",", ":"), sort_keys=True) if services else "{}")
PY
}

append_row_with_telemetry() {
  local case_name="$1"
  local row_json="$2"
  local protocol="${3:-}"
  local mptunnel_row="${4:-0}"
  local mptcp_evidence_json="${5:-}"
  local baseline_identity_json="${6:-}"
  local telemetry_json log_artifacts_json
  telemetry_json="$(case_telemetry_summary "$case_name")"
  log_artifacts_json="$(case_log_artifacts_summary "$case_name")"
  ROW="$row_json" \
    PROTOCOL="$protocol" \
    MPTUNNEL_ROW="$mptunnel_row" \
    LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
    LAB_DIAG_EVENTS="${MPTUNNEL_LAB_DIAG_EVENTS:-}" \
    LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
    TELEMETRY="$telemetry_json" \
    LOG_ARTIFACTS="$log_artifacts_json" \
    MPTCP_EVIDENCE="$mptcp_evidence_json" \
    BASELINE_IDENTITY="$baseline_identity_json" \
    RESULT_REPRODUCIBILITY="$result_reproducibility" \
    LAB_SCRIPT_DIR="$script_dir" \
    python3 - "$case_name" <<'PY' >> "$result_file"
import json
import os
import sys

case = sys.argv[1]
raw = os.environ.get("ROW", "")
try:
    row = json.loads(raw) if raw else {}
except json.JSONDecodeError:
    row = {"case": case, "raw_output": raw}
if not row:
    row = {"case": case}
row.setdefault("case", case)
protocol = os.environ.get("PROTOCOL", "")
if protocol:
    row["protocol"] = protocol
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import enrich_instrumentation_for_scope
enrich_instrumentation_for_scope(
    row,
    os.environ.get("MPTUNNEL_ROW", ""),
    os.environ.get("LAB_DIAG", ""),
    os.environ.get("LAB_PERF", ""),
    os.environ.get("LAB_DIAG_EVENTS", ""),
)
try:
    telemetry = json.loads(os.environ.get("TELEMETRY", "{}"))
except json.JSONDecodeError:
    telemetry = {}
if telemetry:
    row["container_telemetry"] = telemetry
try:
    sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
    from result_enrichment import enrich_traffic_overhead
    enrich_traffic_overhead(row, telemetry)
except Exception as exc:
    row["traffic_overhead_error"] = str(exc)
try:
    log_artifacts = json.loads(os.environ.get("LOG_ARTIFACTS", "{}"))
except json.JSONDecodeError:
    log_artifacts = {}
if log_artifacts:
    row["log_artifacts"] = log_artifacts
if os.environ.get("MPTCP_EVIDENCE", ""):
    try:
        row["mptcp_runtime_evidence"] = json.loads(os.environ["MPTCP_EVIDENCE"])
    except json.JSONDecodeError as exc:
        row["mptcp_runtime_evidence"] = {
            "collection_ok": False,
            "error": f"invalid evidence summary: {exc}",
        }
if log_artifacts or row.get("status") not in ("ok", "loss"):
    try:
        sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
        from diagnostic_buckets import analyze_row
        row["diagnostic_failure_buckets"] = analyze_row(row, log_artifacts, telemetry)
    except Exception as exc:
        row["diagnostic_failure_buckets_error"] = str(exc)
from result_enrichment import enrich_reproducibility
enrich_reproducibility(row, os.environ["RESULT_REPRODUCIBILITY"])
from result_enrichment import enrich_baseline_identity
enrich_baseline_identity(row, os.environ.get("BASELINE_IDENTITY", ""))
print(json.dumps(row, sort_keys=True))
PY
}

append_skipped_result() {
  local case_name="$1"
  local protocol="$2"
  local reason="$3"
  local status="skipped"
  if flag_enabled "$require_competitor_baselines"; then
    case "$case_name" in
      baseline_vmess_*|baseline_hysteria2_*|baseline_mptcp_*) status="fail" ;;
    esac
  fi
  CASE_NAME="$case_name" PROTOCOL="$protocol" REASON="$reason" STATUS="$status" \
    RESULT_REPRODUCIBILITY="$result_reproducibility" LAB_SCRIPT_DIR="$script_dir" \
    python3 - <<'PY' >> "$result_file"
import json
import os
import sys

row = {
    "case": os.environ["CASE_NAME"],
    "protocol": os.environ["PROTOCOL"],
    "status": os.environ["STATUS"],
    "reason": os.environ["REASON"],
}
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import enrich_reproducibility
enrich_reproducibility(row, os.environ["RESULT_REPRODUCIBILITY"])
print(json.dumps(row, sort_keys=True))
PY
}

append_download_probe_result() {
  local case_name="$1"
  local exit_code="$2"
  local output="$3"
  local probe_stderr="$4"
  local mptunnel_row="${5:-1}"
  local fallback_protocol="${6:-tcp}"
  local baseline_identity_json="${7:-}"
  local client_log server_log

  client_log="$(exec_in client "for file in /tmp/mptunnel-client-*.log; do [ -f \"\$file\" ] || continue; echo \"== \$(basename \"\$file\") ==\"; tail -n '${log_tail_lines}' \"\$file\"; done | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  server_log="$(exec_in server "tail -n '${log_tail_lines}' /tmp/mptunnel-server.log 2>/dev/null | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"

  ROW="$output" \
  EXIT_CODE="$exit_code" \
  PROBE_STDERR="$probe_stderr" \
  CLIENT_LOG="$client_log" \
	  SERVER_LOG="$server_log" \
	  LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
	  LAB_DIAG_EVENTS="${MPTUNNEL_LAB_DIAG_EVENTS:-}" \
	  LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
	  MPTUNNEL_ROW="$mptunnel_row" \
	  FALLBACK_PROTOCOL="$fallback_protocol" \
	  LOG_TAIL_BYTES="$log_tail_bytes" \
	  TELEMETRY="$(case_telemetry_summary "$case_name")" \
	  LOG_ARTIFACTS="$(case_log_artifacts_summary "$case_name")" \
	  RESULT_REPRODUCIBILITY="$result_reproducibility" \
	  BASELINE_IDENTITY="$baseline_identity_json" \
	  LAB_SCRIPT_DIR="$script_dir" \
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
        "protocol": os.environ.get("FALLBACK_PROTOCOL", "tcp"),
        "status": "fail",
        "exit_code": exit_code,
    }
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import enrich_instrumentation_for_scope
lab_diag, lab_perf = enrich_instrumentation_for_scope(
    row,
    os.environ.get("MPTUNNEL_ROW", ""),
    os.environ.get("LAB_DIAG", ""),
    os.environ.get("LAB_PERF", ""),
    os.environ.get("LAB_DIAG_EVENTS", ""),
)
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
try:
    telemetry = json.loads(os.environ.get("TELEMETRY", "{}"))
except json.JSONDecodeError:
    telemetry = {}
if telemetry:
    row["container_telemetry"] = telemetry
try:
    sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
    from result_enrichment import enrich_traffic_overhead
    enrich_traffic_overhead(row, telemetry)
except Exception as exc:
    row["traffic_overhead_error"] = str(exc)
try:
    log_artifacts = json.loads(os.environ.get("LOG_ARTIFACTS", "{}"))
except json.JSONDecodeError:
    log_artifacts = {}
if log_artifacts:
    row["log_artifacts"] = log_artifacts
if log_artifacts or row.get("status") not in ("ok", "loss"):
    try:
        sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
        from diagnostic_buckets import analyze_row
        row["diagnostic_failure_buckets"] = analyze_row(row, log_artifacts, telemetry)
    except Exception as exc:
        row["diagnostic_failure_buckets_error"] = str(exc)
from result_enrichment import enrich_reproducibility
enrich_reproducibility(row, os.environ["RESULT_REPRODUCIBILITY"])
from result_enrichment import enrich_baseline_identity
enrich_baseline_identity(row, os.environ.get("BASELINE_IDENTITY", ""))
print(json.dumps(row, sort_keys=True))
PY
}

run_unproxied_download_probe_case() {
  local case_name="$1"
  local protocol="$2"
  local target="$3"
  local mptunnel_row="${4:-0}"
  local out_file="/tmp/mptunnel-probe-${case_name}.out"
  local err_file="/tmp/mptunnel-probe-${case_name}.err"
  local telemetry_pid
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output probe_stderr
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout $((curl_timeout + 10))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --protocol '${protocol}' --target '${target}' --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" "$mptunnel_row" "$protocol"
}

run_tcp_download_probe_case() {
  local case_name="$1"
  local probe_load_duration="${2:-$load_duration_seconds}"
  local probe_workers="${3:-$bulk_connections}"
  local request_lifecycle="${4:-duration}"
  local synchronized_start="${5:-0}"
  local probe_path="${6:-$large_http_path}"
  local probe_timeout="${7:-$curl_timeout}"
  local probe_target="${8:-172.31.40.30:8080}"
  local out_file="/tmp/mptunnel-probe-${case_name}.out"
  local err_file="/tmp/mptunnel-probe-${case_name}.err"
  local telemetry_pid synchronized_start_arg="" probe_process_timeout
  if flag_enabled "$synchronized_start"; then
    synchronized_start_arg=" --synchronized-start"
  fi
  probe_process_timeout="$(
    python3 - "$probe_timeout" "$probe_load_duration" "$synchronized_start" <<'PY'
import math
import sys

setup_timeout = float(sys.argv[1])
load_duration = float(sys.argv[2])
synchronized = sys.argv[3].lower() in {"1", "true", "yes"}
print(math.ceil(setup_timeout + (load_duration if synchronized else 0.0) + 10.0))
PY
  )"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output probe_stderr
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout ${probe_process_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target '${probe_target}' --path '${probe_path}' --failover-after -1 --timeout '${probe_timeout}' --load-duration '${probe_load_duration}' --parallel-downloads '${probe_workers}' --request-lifecycle '${request_lifecycle}'${synchronized_start_arg} >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
}

append_upload_probe_result() {
  local case_name="$1"
  local exit_code="$2"
  local output="$3"
  local probe_stderr="$4"
  local mptunnel_row="${5:-1}"
  local fallback_protocol="${6:-tcp-upload}"
  local observer_elapsed_seconds="${7:-}"
  local observer_freeze_exit_code="${8:-0}"
  local mptcp_evidence_json="${9:-}"
  local baseline_identity_json="${10:-}"
  local client_log server_log target_sink_observer

  client_log="$(exec_in client "for file in /tmp/mptunnel-client-*.log; do [ -f \"\$file\" ] || continue; echo \"== \$(basename \"\$file\") ==\"; tail -n '${log_tail_lines}' \"\$file\"; done | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  server_log="$(exec_in server "tail -n '${log_tail_lines}' /tmp/mptunnel-server.log 2>/dev/null | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  target_sink_observer="$(exec_in target "cat '${tcp_sink_progress_file}' 2>/dev/null || true")"

  ROW="$output" \
  EXIT_CODE="$exit_code" \
  PROBE_STDERR="$probe_stderr" \
  CLIENT_LOG="$client_log" \
  SERVER_LOG="$server_log" \
  TARGET_SINK_OBSERVER="$target_sink_observer" \
  TARGET_OBSERVER_ELAPSED_SECONDS="$observer_elapsed_seconds" \
  TARGET_OBSERVER_FREEZE_EXIT_CODE="$observer_freeze_exit_code" \
  MPTCP_EVIDENCE="$mptcp_evidence_json" \
	  LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
	  LAB_DIAG_EVENTS="${MPTUNNEL_LAB_DIAG_EVENTS:-}" \
	  LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
	  MPTUNNEL_ROW="$mptunnel_row" \
	  FALLBACK_PROTOCOL="$fallback_protocol" \
	  LOG_TAIL_BYTES="$log_tail_bytes" \
	  TELEMETRY="$(case_telemetry_summary "$case_name")" \
	  LOG_ARTIFACTS="$(case_log_artifacts_summary "$case_name")" \
	  RESULT_REPRODUCIBILITY="$result_reproducibility" \
	  BASELINE_IDENTITY="$baseline_identity_json" \
	  LAB_SCRIPT_DIR="$script_dir" \
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
        "protocol": os.environ.get("FALLBACK_PROTOCOL", "tcp-upload"),
        "status": "fail",
        "exit_code": exit_code,
    }
try:
    observer_freeze_exit_code = int(
        os.environ.get("TARGET_OBSERVER_FREEZE_EXIT_CODE", "0")
    )
except ValueError:
    observer_freeze_exit_code = 1
if observer_freeze_exit_code != 0:
    row["upload_observer_freeze_exit_code"] = observer_freeze_exit_code
try:
    if observer_freeze_exit_code != 0:
        raise RuntimeError("target sink did not quiesce cleanly")
    sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
    from result_enrichment import enrich_upload_target_observer
    enrich_upload_target_observer(
        row,
        os.environ.get("TARGET_SINK_OBSERVER", ""),
        os.environ.get("TARGET_OBSERVER_ELAPSED_SECONDS", ""),
    )
except Exception as exc:
    row["upload_observer_error"] = str(exc)
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import enrich_instrumentation_for_scope
lab_diag, lab_perf = enrich_instrumentation_for_scope(
    row,
    os.environ.get("MPTUNNEL_ROW", ""),
    os.environ.get("LAB_DIAG", ""),
    os.environ.get("LAB_PERF", ""),
    os.environ.get("LAB_DIAG_EVENTS", ""),
)
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
try:
    telemetry = json.loads(os.environ.get("TELEMETRY", "{}"))
except json.JSONDecodeError:
    telemetry = {}
if telemetry:
    row["container_telemetry"] = telemetry
try:
    sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
    from result_enrichment import enrich_traffic_overhead
    enrich_traffic_overhead(row, telemetry)
except Exception as exc:
    row["traffic_overhead_error"] = str(exc)
try:
    log_artifacts = json.loads(os.environ.get("LOG_ARTIFACTS", "{}"))
except json.JSONDecodeError:
    log_artifacts = {}
if log_artifacts:
    row["log_artifacts"] = log_artifacts
if os.environ.get("MPTCP_EVIDENCE", ""):
    try:
        row["mptcp_runtime_evidence"] = json.loads(os.environ["MPTCP_EVIDENCE"])
    except json.JSONDecodeError as exc:
        row["mptcp_runtime_evidence"] = {
            "collection_ok": False,
            "error": f"invalid evidence summary: {exc}",
        }
if log_artifacts or row.get("status") not in ("ok", "loss"):
    try:
        sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
        from diagnostic_buckets import analyze_row
        row["diagnostic_failure_buckets"] = analyze_row(row, log_artifacts, telemetry)
    except Exception as exc:
        row["diagnostic_failure_buckets_error"] = str(exc)
from result_enrichment import enrich_reproducibility
enrich_reproducibility(row, os.environ["RESULT_REPRODUCIBILITY"])
from result_enrichment import enrich_baseline_identity
enrich_baseline_identity(row, os.environ.get("BASELINE_IDENTITY", ""))
print(json.dumps(row, sort_keys=True))
PY
}

run_unproxied_upload_probe_case() {
  local case_name="$1"
  local target="$2"
  local mptunnel_row="${3:-0}"
  local protocol="${4:-tcp-upload}"
  local out_file="/tmp/mptunnel-upload-${case_name}.out"
  local err_file="/tmp/mptunnel-upload-${case_name}.err"
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  restart_target_tcp_sink
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output probe_stderr
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout ${upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --protocol '${protocol}' --target '${target}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-uploads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" "$mptunnel_row" "$protocol" "$observer_elapsed_seconds" "$observer_freeze_exit_code"
}

run_tcp_upload_probe_case() {
  local case_name="$1"
  local probe_load_duration="${2:-$load_duration_seconds}"
  local probe_workers="${3:-$bulk_connections}"
  local synchronized_start="${4:-0}"
  local probe_timeout="${5:-$curl_timeout}"
  local probe_target="${6:-172.31.40.30:${tcp_upload_target_port}}"
  local out_file="/tmp/mptunnel-upload-${case_name}.out"
  local err_file="/tmp/mptunnel-upload-${case_name}.err"
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  local synchronized_start_arg="" probe_process_timeout
  if flag_enabled "$synchronized_start"; then
    synchronized_start_arg=" --synchronized-start"
  fi
  probe_process_timeout="$(
    python3 - \
      "$probe_timeout" \
      "$probe_load_duration" \
      "$synchronized_start" <<'PY'
import math
import sys

setup_timeout = float(sys.argv[1])
load_duration = float(sys.argv[2])
synchronized = sys.argv[3].lower() in {"1", "true", "yes"}
active = setup_timeout if load_duration <= 0 else min(load_duration, setup_timeout)
setup = setup_timeout if synchronized else 0.0
print(max(1, math.ceil(setup + active + setup_timeout + 10.0)))
PY
  )"
  restart_target_tcp_sink
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output probe_stderr
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout ${probe_process_timeout}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target '${probe_target}' --failover-after -1 --timeout '${probe_timeout}' --load-duration '${probe_load_duration}' --parallel-uploads '${probe_workers}'${synchronized_start_arg} >'${out_file}' 2>'${err_file}'"
  local exit_code="$?"
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 1 "tcp-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code"
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
  if [[ "$client_runtime" == "wine" ]]; then
    if ! exec_in client "WINEDEBUG=-all WINEPREFIX=${wine_prefix_shell} wineserver -k >/dev/null 2>&1 || true; timeout ${client_start_timeout_seconds}s env WINEDEBUG=-all WINEPREFIX=${wine_prefix_shell} wineserver -w" >/dev/null 2>&1; then
      exec_in client "pkill -KILL -x wineserver >/dev/null 2>&1 || true" \
        >/dev/null 2>&1 || true
    fi
  fi
  active_client_config_artifact=""
  sleep "$client_stop_settle_seconds"
}

stop_server() {
  stop_server_port_forwarding
  stop_process server /tmp/mptunnel-server.pid
}

stop_server_port_forwarding() {
  exec_in server "\
    while iptables -t nat -C PREROUTING -p udp \
      --dport '${port_hop_first_port}:${port_hop_last_port}' \
      -j REDIRECT --to-ports '${server_port}' >/dev/null 2>&1; do \
      iptables -t nat -D PREROUTING -p udp \
        --dport '${port_hop_first_port}:${port_hop_last_port}' \
        -j REDIRECT --to-ports '${server_port}'; \
    done" >/dev/null 2>&1 || true
}

start_server_port_forwarding() {
  stop_server_port_forwarding
  exec_in server "\
    iptables -t nat -A PREROUTING -p udp \
      --dport '${port_hop_first_port}:${port_hop_last_port}' \
      -j REDIRECT --to-ports '${server_port}'"
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
    if [[ -n "$flapper_started_monotonic_ms" ]]; then
      flapper_stop_requested_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
    fi
    local stop_deadline=$((SECONDS + 10))
    while [[ ! -f "$flapper_done_file" ]] && (( SECONDS < stop_deadline )); do
      sleep 0.05
    done
    if [[ ! -f "$flapper_done_file" ]]; then
      kill -TERM -- "-$flapper_pgid" >/dev/null 2>&1 || true
      sleep 0.1
      kill -KILL -- "-$flapper_pgid" >/dev/null 2>&1 || true
      wait "$flapper_pid" >/dev/null 2>&1 || true
      flapper_worker_exit_code=124
    elif wait "$flapper_pid" >/dev/null 2>&1; then
      flapper_worker_exit_code=0
    else
      flapper_worker_exit_code=$?
    fi
  fi
  if [[ -n "$flapper_stop_file" ]]; then
    rm -f "$flapper_stop_file"
  fi
  if [[ -n "$flapper_done_file" ]]; then
    rm -f "$flapper_done_file"
  fi
  if [[ -n "$flapper_probe_gate_file" ]]; then
    rm -f "$flapper_probe_gate_file"
  fi
  if [[ -n "$flapper_probe_finished_file" ]]; then
    rm -f "$flapper_probe_finished_file"
  fi
  flapper_pid=""
  flapper_pgid=""
  flapper_stop_file=""
  flapper_done_file=""
  flapper_probe_gate_file=""
  flapper_probe_finished_file=""
  flapper_restore_exit_code=0
  for service in client server target; do
    if ! exec_netem "$service" apply >/dev/null 2>&1; then
      flapper_restore_exit_code=1
    fi
  done
}

flapping_result_metadata() {
  if [[ -z "$flapper_trace_file" ]]; then
    printf '{}'
    return 0
  fi

  local -a anchor_args=()
  if [[ -n "$flapper_probe_started_unix_seconds" ]]; then
    anchor_args+=(--probe-started-unix-seconds "$flapper_probe_started_unix_seconds")
  fi
  if [[ -n "$flapper_started_unix_ms" ]]; then
    anchor_args+=(--schedule-origin-unix-ms "$flapper_started_unix_ms")
  fi
  if [[ -n "$flapper_started_monotonic_ms" ]]; then
    anchor_args+=(--schedule-origin-monotonic-ms "$flapper_started_monotonic_ms")
  fi
  if [[ -n "$flapper_stop_requested_offset_ms" ]]; then
    anchor_args+=(--stop-requested-offset-ms "$flapper_stop_requested_offset_ms")
  fi
  if [[ -n "$flapper_worker_exit_code" ]]; then
    anchor_args+=(--worker-exit-code "$flapper_worker_exit_code")
  fi
  if [[ -n "$flapper_restore_exit_code" ]]; then
    anchor_args+=(--restore-exit-code "$flapper_restore_exit_code")
  fi
  python3 "$script_dir/flapping_schedule.py" metadata \
    --seed "$flap_seed" \
    --seed-source "$flap_seed_source" \
    --modes "$flap_modes" \
    --min-seconds "$flap_min_seconds" \
    --max-seconds "$flap_max_seconds" \
    --initial-stable-seconds "$flap_initial_stable_seconds" \
    --trace "$flapper_trace_file" \
    "${anchor_args[@]}"
}

cleanup_active_bulk_interactive_probe() {
  local index schedule_pid schedule_service schedule_pid_file
  if [[ -n "$active_bulk_interactive_cancel_file" ]]; then
    : > "$active_bulk_interactive_cancel_file"
  fi
  for index in "${!active_bulk_interactive_schedule_services[@]}"; do
    schedule_service="${active_bulk_interactive_schedule_services[$index]}"
    schedule_pid_file="${active_bulk_interactive_schedule_pid_files[$index]}"
    exec_in "$schedule_service" "schedule_pid=\$(cat '${schedule_pid_file}' 2>/dev/null || true); if [[ \"\$schedule_pid\" =~ ^[0-9]+$ ]]; then for child_pid in \$(pgrep -P \"\$schedule_pid\" 2>/dev/null || true); do kill -TERM -- \"-\$child_pid\" >/dev/null 2>&1 || true; done; kill -TERM \"\$schedule_pid\" >/dev/null 2>&1 || true; deadline=\$((SECONDS + 2)); while kill -0 \"\$schedule_pid\" >/dev/null 2>&1 && (( SECONDS < deadline )); do sleep 0.02; done; if kill -0 \"\$schedule_pid\" >/dev/null 2>&1; then for child_pid in \$(pgrep -P \"\$schedule_pid\" 2>/dev/null || true); do kill -KILL -- \"-\$child_pid\" >/dev/null 2>&1 || true; done; kill -KILL \"\$schedule_pid\" >/dev/null 2>&1 || true; fi; fi" \
      >/dev/null 2>&1 || true
  done
  for schedule_pid in "${active_bulk_interactive_schedule_pids[@]}"; do
    kill "$schedule_pid" >/dev/null 2>&1 || true
  done
  for schedule_pid in "${active_bulk_interactive_schedule_pids[@]}"; do
    wait "$schedule_pid" >/dev/null 2>&1 || true
  done
  active_bulk_interactive_schedule_pids=()
  active_bulk_interactive_schedule_services=()
  active_bulk_interactive_schedule_pid_files=()
  active_bulk_interactive_cancel_file=""
  if [[ -n "$active_bulk_interactive_probe_pid_file" ]]; then
    exec_in client "probe_pid=\$(cat '${active_bulk_interactive_probe_pid_file}' 2>/dev/null || true); if [[ \"\$probe_pid\" =~ ^[0-9]+$ ]]; then pkill -TERM -P \"\$probe_pid\" >/dev/null 2>&1 || true; kill \"\$probe_pid\" >/dev/null 2>&1 || true; fi; rm -f '${active_bulk_interactive_probe_pid_file}'" \
      >/dev/null 2>&1 || true
  fi
  if (( ${#active_bulk_interactive_host_files[@]} > 0 )); then
    rm -f "${active_bulk_interactive_host_files[@]}"
  fi
  active_bulk_interactive_probe_pid_file=""
  active_bulk_interactive_host_files=()
}

cleanup() {
  cleanup_active_bulk_interactive_probe
  stop_random_flapping
  if [[ -n "$active_mptcp_evidence_case" ]]; then
    stop_mptcp_evidence "$active_mptcp_evidence_case"
    active_mptcp_evidence_case=""
  fi
  if [[ -n "$active_telemetry_case" ]]; then
    stop_case_telemetry "$active_telemetry_case" "$active_telemetry_pid"
    active_telemetry_case=""
    active_telemetry_pid=""
  fi
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
  if [[ "$mode" == "asymmetric" ]]; then
    apply_asymmetric_netem
  elif [[ "$mode" =~ ^internet-five-path-epoch-([0-9]+)$ ]]; then
    exec_netem client "${mode}-client"
    exec_netem server "${mode}-server"
    exec_netem target "${mode}-server"
  elif [[ "$mode" =~ ^internet-five-path-load-coupled-epoch-([0-9]+)$ ]]; then
    exec_netem client "${mode}-client"
    exec_netem server "${mode}-server"
    exec_netem target "${mode}-server"
  elif [[ "$mode" =~ ^scale-(access|gigabit|multi-gigabit)-epoch-0$ ]]; then
    prepare_path_variation_initial_epoch "${BASH_REMATCH[1]}"
    exec_netem target clear >/dev/null
  else
    exec_netem client "$mode"
    exec_netem server "$mode"
    exec_netem target "$mode"
  fi
}

apply_asymmetric_netem() {
  exec_netem client clear >/dev/null
  exec_netem server clear >/dev/null
  exec_netem target clear >/dev/null
  exec_netem client asymmetric-client >/dev/null
  exec_netem server asymmetric-server >/dev/null
}

apply_failover_blackhole() {
  exec_netem client "blackhole-${failover_profile}"
  exec_netem server "blackhole-${failover_profile}"
}

mark_client_failover_injection() {
  local marker_file="$1"
  exec_in client "python3 -c 'import time; print(time.monotonic())' > '${marker_file}'"
}

wait_for_tcp_failover_trigger() {
  local case_name="$1"
  local marker_file="$2"
  local endpoint="$3"
  local address="$4"
  local trigger_file trigger_output trigger_status
  trigger_file="$(failover_trigger_file_for_case "$case_name")"
  rm -f "$trigger_file"
  if [[ "$failover_tx_trigger_bytes" == "0" ]]; then
    sleep "$failover_after"
  else
    set +e
    trigger_output="$(
      exec_in "$endpoint" "python3 /workspace/lab/wait_interface_counter.py --address '${address}' --counter tx_bytes --delta-bytes '${failover_tx_trigger_bytes}' --min-wait '${failover_after}' --timeout '${failover_trigger_timeout_seconds}' --interval '${failover_trigger_poll_interval_seconds}'"
    )"
    trigger_status="$?"
    set -e
    printf '%s\n' "$trigger_output" > "$trigger_file"
    if [[ "$trigger_status" != "0" ]]; then
      case_log_artifacts_summary "$case_name" >/dev/null || true
      echo "${failover_profile}-path payload trigger failed for ${case_name}: ${trigger_output}" >&2
      return "$trigger_status"
    fi
  fi
  mark_client_failover_injection "$marker_file"
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
  local probe_gate_file="$1"
  local probe_finished_file="$2"
  local min_seconds="$flap_min_seconds"
  local max_seconds="$flap_max_seconds"
  local initial_stable_seconds="$flap_initial_stable_seconds"
  if ! [[ "$min_seconds" =~ ^[0-9]+$ \
    && "$max_seconds" =~ ^[0-9]+$ \
    && "$initial_stable_seconds" =~ ^[0-9]+$ ]]; then
    echo "MPTUNNEL_LAB_FLAP_MIN_SECONDS, MPTUNNEL_LAB_FLAP_MAX_SECONDS, and" \
      "MPTUNNEL_LAB_FLAP_INITIAL_STABLE_SECONDS must be non-negative integers" >&2
    return 2
  fi
  if (( min_seconds < 1 )); then
    min_seconds=1
  fi
  if (( max_seconds < min_seconds )); then
    max_seconds="$min_seconds"
  fi

  if [[ -n "$flapper_pid" || -n "$flapper_stop_file" ]]; then
    stop_random_flapping
  fi
  if [[ -z "$flap_seed" ]]; then
    flap_seed="$(python3 -c 'import secrets; print(secrets.randbits(64))')"
    flap_seed_source="generated"
  elif [[ -z "$flap_seed_source" ]]; then
    flap_seed_source="configured"
  fi
  flapper_stop_file="${result_dir}/flapper-${timestamp}-$$.stop"
  flapper_done_file="${result_dir}/flapper-${timestamp}-$$.done"
  flapper_probe_gate_file="$probe_gate_file"
  flapper_probe_finished_file="$probe_finished_file"
  flapper_trace_file="${result_dir}/flapper-${timestamp}-$$-trace.jsonl"
  flapper_stop_requested_offset_ms=""
  flapper_worker_exit_code=""
  flapper_restore_exit_code=""
  flapper_probe_started_unix_seconds=""
  flapper_started_unix_ms=""
  flapper_started_monotonic_ms=""
  rm -f "$flapper_stop_file" "$flapper_done_file" "$flapper_trace_file"
  python3 "$script_dir/flapping_schedule.py" choose \
    --seed "$flap_seed" \
    --modes "$flap_modes" \
    --min-seconds "$min_seconds" \
    --max-seconds "$max_seconds" \
    --index 0 >/dev/null
  set -m
  (
    # Docker Compose may inspect the controlling terminal even with exec -T.
    # Keep the job-controlled worker runnable while it applies netem modes.
    trap '' TTOU TTIN TSTP
    trap 'touch "$flapper_done_file"' EXIT
    while [[ ! -f "$flapper_probe_gate_file" && ! -f "$flapper_probe_finished_file" && ! -f "$flapper_stop_file" ]]; do
      sleep 0.01
    done
    if [[ -f "$flapper_stop_file" || -f "$flapper_probe_finished_file" ]]; then
      exit 0
    fi
    mapfile -t probe_anchor < "$flapper_probe_gate_file"
    flapper_probe_started_unix_seconds="${probe_anchor[0]}"
    flapper_started_monotonic_ms="${probe_anchor[1]}"
    flapper_started_unix_ms="${probe_anchor[2]}"
    initial_hold_deadline_ms="$(($(monotonic_milliseconds) + initial_stable_seconds * 1000))"
    while [[ ! -f "$flapper_stop_file" && ! -f "$flapper_probe_finished_file" ]] \
      && (( $(monotonic_milliseconds) < initial_hold_deadline_ms )); do
      sleep 0.05
    done
    event_index=0
    planned_offset_seconds=0
    while [[ ! -f "$flapper_stop_file" && ! -f "$flapper_probe_finished_file" ]]; do
      schedule_row="$(python3 "$script_dir/flapping_schedule.py" choose \
        --seed "$flap_seed" \
        --modes "$flap_modes" \
        --min-seconds "$min_seconds" \
        --max-seconds "$max_seconds" \
        --index "$event_index")"
      IFS=$'\t' read -r selected_index mode sleep_seconds <<< "$schedule_row"
      if [[ -f "$flapper_stop_file" || -f "$flapper_probe_finished_file" ]]; then
        break
      fi
      event_start_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
      client_apply_start_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
      # One event is one complete condition epoch. Restore the declared
      # baseline before applying the selected condition so random history
      # cannot silently turn the default single-link handover case into an
      # unbounded cumulative all-link outage.
      if exec_netem client apply >/dev/null 2>&1 \
        && exec_netem client "$mode" >/dev/null 2>&1; then
        client_command_exit_code=0
      else
        client_command_exit_code=$?
      fi
      client_apply_end_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
      server_apply_start_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
      if exec_netem server apply >/dev/null 2>&1 \
        && exec_netem server "$mode" >/dev/null 2>&1; then
        server_command_exit_code=0
      else
        server_command_exit_code=$?
      fi
      server_apply_end_offset_ms="$(($(monotonic_milliseconds) - flapper_started_monotonic_ms))"
      printf '{"index":%s,"planned_offset_seconds":%s,"mode":"%s","hold_seconds":%s,"event_start_offset_ms":%s,"client_apply_start_offset_ms":%s,"client_apply_end_offset_ms":%s,"client_command_exit_code":%s,"server_apply_start_offset_ms":%s,"server_apply_end_offset_ms":%s,"server_command_exit_code":%s}\n' \
        "$selected_index" \
        "$planned_offset_seconds" \
        "$mode" \
        "$sleep_seconds" \
        "$event_start_offset_ms" \
        "$client_apply_start_offset_ms" \
        "$client_apply_end_offset_ms" \
        "$client_command_exit_code" \
        "$server_apply_start_offset_ms" \
        "$server_apply_end_offset_ms" \
        "$server_command_exit_code" >> "$flapper_trace_file"
      event_index=$((event_index + 1))
      planned_offset_seconds=$((planned_offset_seconds + sleep_seconds))
      hold_deadline_ms="$(($(monotonic_milliseconds) + sleep_seconds * 1000))"
      while [[ ! -f "$flapper_stop_file" && ! -f "$flapper_probe_finished_file" ]] && (( $(monotonic_milliseconds) < hold_deadline_ms )); do
        sleep 0.05
      done
    done
  ) </dev/null &
  flapper_pid="$!"
  flapper_pgid="$flapper_pid"
  set +m
}

should_run_case() {
  local case_name="$1"
  local pattern

  if [[ -n "$case_filter" ]]; then
    IFS=',' read -r -a patterns <<< "$case_filter"
    local selected=0
    for pattern in "${patterns[@]}"; do
      # CASE_FILTER supports shell-style globs, for example mptunnel_mixed_*.
      # shellcheck disable=SC2254
      case "$case_name" in
        $pattern) selected=1; break ;;
      esac
    done
    if [[ "$selected" != "1" ]]; then
      return 1
    fi
  fi

  if [[ "$client_runtime" == "wine" && "$case_name" == mptunnel_tun_* ]]; then
    return 1
  fi
  return 0
}

validate_client_runtime_case_filter() {
  if [[ "$client_runtime" != "wine" || -z "$case_filter" ]]; then
    return 0
  fi
  local pattern tun_case
  local -a tun_cases=(
    mptunnel_tun_tcp_single_low_latency
    mptunnel_tun_tcp_single_balanced
    mptunnel_tun_udp_stream_single_low_latency
    mptunnel_tun_udp_stream_single_balanced
    mptunnel_tun_mixed_multipath_all
    mptunnel_tun_tcp_single_low_latency_upload
    mptunnel_tun_tcp_single_balanced_upload
    mptunnel_tun_udp_stream_single_low_latency_upload
    mptunnel_tun_udp_stream_single_balanced_upload
    mptunnel_tun_mixed_multipath_all_upload
    mptunnel_tun_app_bypass_balanced
    mptunnel_tun_app_bypass_balanced_upload
  )
  IFS=',' read -r -a patterns <<< "$case_filter"
  for pattern in "${patterns[@]}"; do
    for tun_case in "${tun_cases[@]}"; do
      # shellcheck disable=SC2254
      case "$tun_case" in
        $pattern)
          echo "Wine client runtime cannot run TUN case selected by CASE_FILTER: $tun_case" >&2
          return 2
          ;;
      esac
    done
  done
}

restart_target_tcp_sink() {
  local transport="${1:-tcp}"
  local socket_arg=""
  case "$transport" in
    tcp) ;;
    mptcp) socket_arg="--mptcp" ;;
    *)
      echo "unknown target sink transport: $transport" >&2
      return 2
      ;;
  esac
  exec_in target "sink_process_active() { local state; [ -r \"/proc/\$pid/stat\" ] || return 1; state=\$(awk '{print \$3}' \"/proc/\$pid/stat\" 2>/dev/null) || return 1; [ \"\$state\" != Z ] && kill -0 \"\$pid\" >/dev/null 2>&1; }; if [ -f /tmp/mptunnel-tcp-sink.pid ]; then pid=\$(cat /tmp/mptunnel-tcp-sink.pid 2>/dev/null || true); if [[ \"\$pid\" =~ ^[0-9]+$ ]] && sink_process_active; then kill -TERM \"\$pid\" >/dev/null 2>&1 || true; deadline=\$((SECONDS + 7)); while sink_process_active && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; if sink_process_active; then kill -KILL \"\$pid\" >/dev/null 2>&1 || true; deadline=\$((SECONDS + 1)); while sink_process_active && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; sink_process_active && exit 1; fi; fi; rm -f /tmp/mptunnel-tcp-sink.pid; fi" || return
  exec_in target "rm -f '${tcp_sink_progress_file}'; python3 /workspace/lab/tcp_sink.py ${socket_arg} --bind 0.0.0.0:${tcp_upload_target_port} --progress-file '${tcp_sink_progress_file}' >/tmp/mptunnel-tcp-sink.log 2>&1 & echo \$! >/tmp/mptunnel-tcp-sink.pid" || return
  exec_in target "pid=\$(cat /tmp/mptunnel-tcp-sink.pid); sink_process_active() { local state; [ -r \"/proc/\$pid/stat\" ] || return 1; state=\$(awk '{print \$3}' \"/proc/\$pid/stat\" 2>/dev/null) || return 1; [ \"\$state\" != Z ] && kill -0 \"\$pid\" >/dev/null 2>&1; }; deadline=\$((SECONDS + 5)); while { [ ! -s '${tcp_sink_progress_file}' ] || ! sink_process_active; } && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -s '${tcp_sink_progress_file}'; sink_process_active"
}

freeze_target_tcp_sink() {
  exec_in target "sink_process_active() { local state; [ -r \"/proc/\$pid/stat\" ] || return 1; state=\$(awk '{print \$3}' \"/proc/\$pid/stat\" 2>/dev/null) || return 1; [ \"\$state\" != Z ] && kill -0 \"\$pid\" >/dev/null 2>&1; }; test -f /tmp/mptunnel-tcp-sink.pid; pid=\$(cat /tmp/mptunnel-tcp-sink.pid); [[ \"\$pid\" =~ ^[0-9]+$ ]]; sink_process_active; kill -TERM \"\$pid\"; deadline=\$((SECONDS + 7)); while sink_process_active && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; if sink_process_active; then kill -KILL \"\$pid\" >/dev/null 2>&1 || true; deadline=\$((SECONDS + 1)); while sink_process_active && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; rm -f /tmp/mptunnel-tcp-sink.pid; exit 1; fi; rm -f /tmp/mptunnel-tcp-sink.pid; python3 - '${tcp_sink_progress_file}' <<'PY'
import json
import sys

with open(sys.argv[1], encoding='utf-8') as handle:
    snapshot = json.load(handle)
if snapshot.get('version') != 2:
    raise SystemExit('target sink did not write snapshot v2')
if snapshot.get('quiesced') is not True or snapshot.get('finalized') is not True:
    raise SystemExit('target sink snapshot is not finalized')
PY"
}

start_target_services() {
  exec_in target "mkdir -p /tmp/mptunnel-lab && truncate -s '${object_mib}M' /tmp/mptunnel-lab/large.bin"
  if flag_enabled "$tcp_carrier_qos_cohort"; then
    exec_in target "truncate -s '${tcp_carrier_qos_object_mib}M' '/tmp/mptunnel-lab${tcp_carrier_qos_http_path}'"
  fi
  exec_in target "dd if=/dev/zero of=/tmp/mptunnel-lab/small.bin bs=1K count='${small_object_kib}' status=none && truncate -s 1M '/tmp/mptunnel-lab${browser_load_path}' && printf 'mptunnel lab target\\n' >/tmp/mptunnel-lab/index.html"
  exec_in target "if [ -f /tmp/mptunnel-http.pid ]; then kill \$(cat /tmp/mptunnel-http.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-http.pid; fi"
  exec_in target "if [ -f /tmp/mptunnel-udp-echo.pid ]; then kill \$(cat /tmp/mptunnel-udp-echo.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-udp-echo.pid; fi"
  exec_in target "if [ -f /tmp/mptunnel-tcp-echo.pid ]; then kill \$(cat /tmp/mptunnel-tcp-echo.pid) >/dev/null 2>&1 || true; rm -f /tmp/mptunnel-tcp-echo.pid; fi"
  exec_in target "python3 -m http.server 8080 --bind 0.0.0.0 --directory /tmp/mptunnel-lab >/tmp/mptunnel-http.log 2>&1 & echo \$! >/tmp/mptunnel-http.pid"
  exec_in target "python3 /workspace/lab/udp_echo.py --bind 0.0.0.0:9090 >/tmp/mptunnel-udp-echo.log 2>&1 & echo \$! >/tmp/mptunnel-udp-echo.pid"
  exec_in target "python3 /workspace/lab/tcp_echo.py --bind 0.0.0.0:10022 >/tmp/mptunnel-tcp-echo.log 2>&1 & echo \$! >/tmp/mptunnel-tcp-echo.pid"
  restart_target_tcp_sink
}

start_server() {
  stop_server
  local config_path="/tmp/mptunnel-server.toml"
  write_in server "$config_path" "$(server_config_toml)"
  validate_mptunnel_config_in server "$config_path"
  persist_redacted_config server "$config_path" config-server.toml
  exec_in server "\
    $(mptunnel_lab_env_prefix server) \
    /workspace/target/release/mptunnel --config '${config_path}' \
      >/tmp/mptunnel-server.log 2>&1 & echo \$! >/tmp/mptunnel-server.pid"
  sleep 1
  exec_in server "kill -0 \$(cat /tmp/mptunnel-server.pid)"
  if [[ "$port_hop_forwarding" == "1" ]]; then
    start_server_port_forwarding
  fi
}

start_client_with_netem() {
  local profile="$1"
  local netem_mode="$2"
  shift 2
  local path_args="$*"
  if [[ "$isolate_cases" == "1" ]]; then
    stop_client
    if [[ "$isolate_containers" == "1" ]]; then
      stop_server
      compose down --remove-orphans >/dev/null 2>&1 || true
      compose_up >/dev/null
    fi
    prepare_client_runtime
    apply_netem "$netem_mode"
    start_target_services
    start_server
  else
    stop_client
    apply_netem "$netem_mode"
  fi
  local config_path="/tmp/mptunnel-client-${profile}.toml"
  local client_command
  client_command="$(client_mptunnel_command)"
  write_in client "$config_path" "$(socks_client_config_toml "$path_args")"
  validate_mptunnel_config_in client "$config_path"
  persist_redacted_config client "$config_path" \
    "config-client-$(case_artifact_name "$profile").toml"
  active_client_config_artifact="$result_dir/config-client-$(case_artifact_name "$profile").toml"
  exec_in client "\
    $(mptunnel_lab_env_prefix client) \
    ${client_command} --config '${config_path}' \
      >/tmp/mptunnel-client-${profile}.log 2>&1 & echo \$! >/tmp/mptunnel-client.pid"
  wait_for_client_proxy "/tmp/mptunnel-client-${profile}.log"
  sleep "$client_start_settle_seconds"
  exec_in client "kill -0 \$(cat /tmp/mptunnel-client.pid)"
}

start_client() {
  local profile="$1"
  shift
  start_client_with_netem "$profile" "$default_netem_mode" "$@"
}

start_tun_client() {
  if [[ "$client_runtime" != "native" ]]; then
    echo "TUN lab cases require MPTUNNEL_LAB_CLIENT_RUNTIME=native" >&2
    return 2
  fi
  local profile="$1"
  shift
  local path_args="$*"
  if [[ "$isolate_cases" == "1" ]]; then
    stop_client
    if [[ "$isolate_containers" == "1" ]]; then
      stop_server
      compose down --remove-orphans >/dev/null 2>&1 || true
      compose_up >/dev/null
    fi
    apply_netem "$default_netem_mode"
    start_target_services
    start_server
  else
    stop_client
    apply_netem "$default_netem_mode"
  fi
  local config_path="/tmp/mptunnel-client-${profile}.toml"
  write_in client "$config_path" "$(tun_client_config_toml "$path_args")"
  validate_mptunnel_config_in client "$config_path"
  persist_redacted_config client "$config_path" \
    "config-client-$(case_artifact_name "$profile").toml"
  active_client_config_artifact="$result_dir/config-client-$(case_artifact_name "$profile").toml"
  exec_in client "\
    $(mptunnel_lab_env_prefix client) \
    /workspace/target/release/mptunnel --config '${config_path}' \
      >/tmp/mptunnel-client-${profile}.log 2>&1 & echo \$! >/tmp/mptunnel-client.pid"
  sleep "$client_start_settle_seconds"
  exec_in client "kill -0 \$(cat /tmp/mptunnel-client.pid)"
  exec_in client "deadline=\$((SECONDS + 10)); while ! ip link show mptun0 >/dev/null 2>&1 && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; ip link show mptun0 >/dev/null"
  exec_in client "ip route replace 172.31.40.30/32 dev mptun0"
}

run_tun_download_case() {
  local case_name="$1"
  shift
  start_tun_client "$case_name" "$@"
  run_unproxied_download_probe_case "$case_name" "tun" "172.31.40.30:8080" 1
}

run_tun_upload_case() {
  local case_name="$1"
  shift
  start_tun_client "$case_name" "$@"
  run_unproxied_upload_probe_case "$case_name" "172.31.40.30:${tcp_upload_target_port}" 1 "tun-upload"
}

run_tun_bypass_download_case() {
  local case_name="$1"
  shift
  start_tun_client "$case_name" "$@"
  # start_tun_client installs only the 172.31.40.30/32 TUN route. This target
  # remains on the ordinary client routing table and is therefore a negative
  # control for traffic intentionally bypassing the active tunnel.
  run_unproxied_download_probe_case \
    "$case_name" "tun-app-bypass" "172.31.15.30:8080" 0
}

run_tun_bypass_upload_case() {
  local case_name="$1"
  shift
  start_tun_client "$case_name" "$@"
  run_unproxied_upload_probe_case \
    "$case_name" "172.31.15.30:${tcp_upload_target_port}" 0 "tun-app-bypass-upload"
}

run_udp_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  local telemetry_pid
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output
  output="$(exec_in client "python3 /workspace/lab/socks5_udp_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:9090 --load-duration '${load_duration_seconds}' --payload-bytes '${udp_payload_bytes}' --timeout-ms '${udp_timeout_ms}'" 2>/dev/null)"
  local exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  set -e
  if [[ "$exit_code" == "0" ]]; then
    append_row_with_telemetry "$case_name" "$output" "" 1
  else
    append_row_with_telemetry "$case_name" "{\"case\":\"$case_name\",\"protocol\":\"udp\",\"status\":\"fail\",\"exit_code\":$exit_code}" "" 1
  fi
}

prepare_baseline_case() {
  local netem_mode="$1"
  stop_baselines
  stop_client
  if [[ "$isolate_cases" == "1" && "$isolate_containers" == "1" ]]; then
    stop_server
    compose down --remove-orphans >/dev/null 2>&1 || true
    compose_up >/dev/null
  else
    stop_server
  fi
  apply_netem "$netem_mode"
  start_target_services
}

prepare_baseline_profile() {
  local netem_mode="$1"
  prepare_baseline_case "$netem_mode"
}

run_baseline_download_probe_case() {
  local case_name="$1"
  local protocol="$2"
  local proxy_port_arg="$3"
  local baseline_identity_json="${4:-}"
  local out_file="/tmp/mptunnel-baseline-${case_name}.out"
  local err_file="/tmp/mptunnel-baseline-${case_name}.err"
  local output probe_stderr exit_code
  local telemetry_pid
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout $((curl_timeout + 10))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port_arg} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  if [[ -n "$output" ]]; then
    append_row_with_telemetry "$case_name" "$output" "$protocol" 0 "" "$baseline_identity_json"
  else
    append_download_probe_result "$case_name" "$exit_code" "" "$probe_stderr" 0 "$protocol" "$baseline_identity_json"
  fi
}

apply_bulk_interactive_baseline_profile() {
  local remote_service="$1"
  local service
  for service in client "$remote_service"; do
    MPTUNNEL_LAB_BALANCED_RATE="$bulk_interactive_rate" \
    MPTUNNEL_LAB_BALANCED_DELAY="$bulk_interactive_delay" \
    MPTUNNEL_LAB_BALANCED_JITTER="$bulk_interactive_jitter" \
    MPTUNNEL_LAB_BALANCED_LOSS="$bulk_interactive_initial_loss" \
      exec_netem "$service" apply-balanced >/dev/null
  done
}

start_bulk_interactive_loss_scheduler() {
  local role="$1"
  local service="$2"
  local probe_started_file="$3"
  local probe_finished_file="$4"
  local cancel_file="$5"
  local ready_file="$6"
  local status_file="$7"
  local pid_file="$8"
  local result_prefix="$9"
  local log_file="${10}"
  local losses_csv
  losses_csv="$(IFS=,; printf '%s' "${bulk_interactive_loss_percent[*]}")"
  compose exec -T \
    -e MPTUNNEL_LAB_BALANCED_RATE="$bulk_interactive_rate" \
    -e MPTUNNEL_LAB_BALANCED_DELAY="$bulk_interactive_delay" \
    -e MPTUNNEL_LAB_BALANCED_JITTER="$bulk_interactive_jitter" \
    -e MPTUNNEL_LAB_SCHEDULE_ROLE="$role" \
    -e MPTUNNEL_LAB_SCHEDULE_SERVICE="$service" \
    -e MPTUNNEL_LAB_SCHEDULE_STARTED_FILE="$probe_started_file" \
    -e MPTUNNEL_LAB_SCHEDULE_FINISHED_FILE="$probe_finished_file" \
    -e MPTUNNEL_LAB_SCHEDULE_CANCEL_FILE="$cancel_file" \
    -e MPTUNNEL_LAB_SCHEDULE_READY_FILE="$ready_file" \
    -e MPTUNNEL_LAB_SCHEDULE_STATUS_FILE="$status_file" \
    -e MPTUNNEL_LAB_SCHEDULE_PID_FILE="$pid_file" \
    -e MPTUNNEL_LAB_SCHEDULE_RESULT_PREFIX="$result_prefix" \
    -e MPTUNNEL_LAB_SCHEDULE_EPOCH_MS="$((bulk_interactive_epoch_seconds * 1000))" \
    -e MPTUNNEL_LAB_SCHEDULE_DURATION_MS="$((bulk_interactive_duration_seconds * 1000))" \
    -e MPTUNNEL_LAB_SCHEDULE_LATENESS_MS="$bulk_interactive_transition_complete_lateness_ms" \
    -e MPTUNNEL_LAB_SCHEDULE_COMMAND_TIMEOUT_S="$bulk_interactive_transition_command_timeout_seconds" \
    -e MPTUNNEL_LAB_SCHEDULE_LOSSES="$losses_csv" \
    "$service" bash -lc \
    "exec /workspace/lab/configure-netem.sh bulk-interactive-loss-schedule" \
    >"$log_file" 2>&1 &
  bulk_interactive_scheduler_wrapper_pid="$!"
}

wait_for_bulk_interactive_schedulers() {
  local client_status_file="$1"
  local remote_status_file="$2"
  local deadline=$((SECONDS + bulk_interactive_probe_timeout_seconds + 10))
  while [[ ! -f "$client_status_file" || ! -f "$remote_status_file" ]]; do
    if (( SECONDS >= deadline )); then
      return 124
    fi
    sleep 0.01
  done
}

collect_bulk_interactive_dynamic_loss_schedule() {
  local client_status_file="$1"
  local remote_status_file="$2"
  local client_result_prefix="$3"
  local remote_result_prefix="$4"
  local epoch loss_percent planned_offset_ms event_json client_json remote_json
  local status_fields client_exit remote_exit client_completed remote_completed
  local schedule_status=0

  bulk_interactive_schedule_events_json=""
  bulk_interactive_schedule_event_count=0
  status_fields="$(python3 - "$client_status_file" "$remote_status_file" <<'PY'
import json
import sys

values = []
for path in sys.argv[1:]:
    try:
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        values.extend((int(data["exit_code"]), data["completed_offset_ms"]))
    except (OSError, TypeError, ValueError, KeyError, json.JSONDecodeError):
        values.extend((2, None))
print("\t".join("null" if value is None else str(value) for value in values))
PY
)"
  IFS=$'\t' read -r client_exit client_completed remote_exit remote_completed \
    <<< "$status_fields"
  if [[ "$client_exit" == "124" || "$remote_exit" == "124" ]]; then
    schedule_status=124
  elif [[ "$client_exit" != "0" || "$remote_exit" != "0" ]]; then
    schedule_status=1
  fi
  for epoch in "${!bulk_interactive_loss_percent[@]}"; do
    loss_percent="${bulk_interactive_loss_percent[$epoch]}"
    planned_offset_ms=$((epoch * bulk_interactive_epoch_seconds * 1000))
    client_json="$(cat "${client_result_prefix}-epoch-${epoch}.json" 2>/dev/null || true)"
    remote_json="$(cat "${remote_result_prefix}-epoch-${epoch}.json" 2>/dev/null || true)"
    if [[ -z "$client_json" || -z "$remote_json" ]]; then
      if [[ "$schedule_status" == "0" ]]; then
        schedule_status=1
      fi
      break
    fi
    printf -v event_json '{"index":%s,"loss_percent":%s,"planned_offset_ms":%s,"endpoints":{"client-egress":%s,"remote-egress":%s}}' \
      "$epoch" \
      "$loss_percent" \
      "$planned_offset_ms" \
      "$client_json" \
      "$remote_json"
    if [[ -n "$bulk_interactive_schedule_events_json" ]]; then
      bulk_interactive_schedule_events_json+=","
    fi
    bulk_interactive_schedule_events_json+="$event_json"
    bulk_interactive_schedule_event_count=$((bulk_interactive_schedule_event_count + 1))
  done
  bulk_interactive_schedule_completed_offset_ms=""
  if [[ "$client_completed" =~ ^[0-9]+$ && "$remote_completed" =~ ^[0-9]+$ ]]; then
    if (( client_completed > remote_completed )); then
      bulk_interactive_schedule_completed_offset_ms="$client_completed"
    else
      bulk_interactive_schedule_completed_offset_ms="$remote_completed"
    fi
  fi
  return "$schedule_status"
}

bulk_interactive_dynamic_loss_metadata() {
  local probe_started_monotonic_ms="${1:-null}"
  local schedule_exit_code="$2"
  local schedule_completed_offset_ms="${3:-null}"
  local host_time_namespace_id="${4:-}"
  local client_time_namespace_id="${5:-}"
  local host_monotonic_offset_json="${6:-null}"
  local client_monotonic_offset_json="${7:-null}"
  local topology_mode="$8"
  local role_service_mapping_json="$9"
  local endpoint_clocks_json="${10:-null}"
  local trace_complete=false
  if [[ "$schedule_exit_code" == "0" ]] && \
     [[ "$host_time_namespace_id" =~ ^time:\[[0-9]+\]$ \
       && "$client_time_namespace_id" =~ ^time:\[[0-9]+\]$ \
       && "$host_monotonic_offset_json" != "null" \
       && "$host_monotonic_offset_json" == "$client_monotonic_offset_json" \
       && "$endpoint_clocks_json" != "null" ]] && \
     (( bulk_interactive_schedule_event_count == ${#bulk_interactive_loss_percent[@]} )) && \
     (( schedule_completed_offset_ms >= bulk_interactive_duration_seconds * 1000 )); then
    trace_complete=true
  fi
  printf '{"condition":%s,"probe_started_monotonic_ms":%s,"schedule_origin":"probe-started-file-clock-monotonic-ms","clock_name":"CLOCK_MONOTONIC","host_time_namespace_id":"%s","client_time_namespace_id":"%s","host_monotonic_offset":%s,"client_monotonic_offset":%s,"endpoint_clocks":%s,"topology_mode":"%s","dynamic_role_to_service":%s,"schedule_exit_code":%s,"schedule_completed_offset_ms":%s,"applied_event_count":%s,"events":[%s],"trace_complete":%s}\n' \
    "$bulk_interactive_dynamic_loss_json" \
    "$probe_started_monotonic_ms" \
    "$host_time_namespace_id" \
    "$client_time_namespace_id" \
    "$host_monotonic_offset_json" \
    "$client_monotonic_offset_json" \
    "$endpoint_clocks_json" \
    "$topology_mode" \
    "$role_service_mapping_json" \
    "$schedule_exit_code" \
    "$schedule_completed_offset_ms" \
    "$bulk_interactive_schedule_event_count" \
    "$bulk_interactive_schedule_events_json" \
    "$trace_complete"
}

run_bulk_interactive_probe_case() {
  local case_name="$1"
  local proxy_port_arg="$2"
  local mptunnel_row="${3:-1}"
  local baseline_identity_json="${4:-}"
  local probe_mode="${5:-socks5}"
  local remote_service="${6:-server}"
  local protocol_override="${7:-}"
  local out_file="/tmp/mptunnel-bulk-interactive-${case_name}.out"
  local err_file="/tmp/mptunnel-bulk-interactive-${case_name}.err"
  local probe_pid_file="/tmp/mptunnel-bulk-interactive-${case_name}.pid"
  local probe_launch_relative=".tmp/lab/bulk-interactive-${case_name}-${timestamp}-$$.launch"
  local probe_gate_relative=".tmp/lab/bulk-interactive-${case_name}-${timestamp}-$$.started"
  local probe_finished_relative=".tmp/lab/bulk-interactive-${case_name}-${timestamp}-$$.finished"
  local probe_status_relative=".tmp/lab/bulk-interactive-${case_name}-${timestamp}-$$.status"
  local scheduler_base_relative=".tmp/lab/bulk-interactive-${case_name}-${timestamp}-$$-schedule"
  local scheduler_cancel_relative="${scheduler_base_relative}.cancel"
  local client_scheduler_ready_relative="${scheduler_base_relative}-client.ready"
  local remote_scheduler_ready_relative="${scheduler_base_relative}-remote.ready"
  local client_scheduler_status_relative="${scheduler_base_relative}-client.status"
  local remote_scheduler_status_relative="${scheduler_base_relative}-remote.status"
  local client_scheduler_pid_relative="${scheduler_base_relative}-client.pid"
  local remote_scheduler_pid_relative="${scheduler_base_relative}-remote.pid"
  local client_scheduler_result_relative="${scheduler_base_relative}-client"
  local remote_scheduler_result_relative="${scheduler_base_relative}-remote"
  local probe_launch_file="${repo_root}/${probe_launch_relative}"
  local probe_gate_file="${repo_root}/${probe_gate_relative}"
  local probe_finished_file="${repo_root}/${probe_finished_relative}"
  local probe_status_file="${repo_root}/${probe_status_relative}"
  local scheduler_cancel_file="${repo_root}/${scheduler_cancel_relative}"
  local client_scheduler_ready_file="${repo_root}/${client_scheduler_ready_relative}"
  local remote_scheduler_ready_file="${repo_root}/${remote_scheduler_ready_relative}"
  local client_scheduler_status_file="${repo_root}/${client_scheduler_status_relative}"
  local remote_scheduler_status_file="${repo_root}/${remote_scheduler_status_relative}"
  local probe_launch_container_file="/workspace/${probe_launch_relative}"
  local probe_gate_container_file="/workspace/${probe_gate_relative}"
  local probe_finished_container_file="/workspace/${probe_finished_relative}"
  local probe_status_container_file="/workspace/${probe_status_relative}"
  local scheduler_cancel_container_file="/workspace/${scheduler_cancel_relative}"
  local client_scheduler_ready_container_file="/workspace/${client_scheduler_ready_relative}"
  local remote_scheduler_ready_container_file="/workspace/${remote_scheduler_ready_relative}"
  local client_scheduler_status_container_file="/workspace/${client_scheduler_status_relative}"
  local remote_scheduler_status_container_file="/workspace/${remote_scheduler_status_relative}"
  local client_scheduler_pid_container_file="/workspace/${client_scheduler_pid_relative}"
  local remote_scheduler_pid_container_file="/workspace/${remote_scheduler_pid_relative}"
  local client_scheduler_result_container_prefix="/workspace/${client_scheduler_result_relative}"
  local remote_scheduler_result_container_prefix="/workspace/${remote_scheduler_result_relative}"
  local output probe_stderr exit_code telemetry_pid schedule_exit_code
  local schedule_completed_offset_ms=""
  local dynamic_loss_metadata_json=""
  local host_time_namespace_id client_time_namespace_id remote_time_namespace_id
  local host_monotonic_offset_json client_monotonic_offset_json
  local remote_monotonic_offset_json endpoint_clocks_json
  local bulk_interactive_scheduler_wrapper_pid scheduler_ready_deadline schedule_pid
  local clock_preflight_error=""
  local probe_target_ip probe_route_arguments topology_mode
  local role_service_mapping_json
  local -a probe_anchor=()

  case "$probe_mode:$remote_service" in
    socks5:server)
      probe_target_ip="172.31.40.30"
      probe_route_arguments="--proxy 127.0.0.1:${proxy_port_arg}"
      topology_mode="proxy"
      role_service_mapping_json='{"client-egress":"client","remote-egress":"server"}'
      ;;
    direct:target)
      probe_target_ip="172.31.15.30"
      probe_route_arguments="--mode direct"
      topology_mode="direct"
      role_service_mapping_json='{"client-egress":"client","remote-egress":"target"}'
      ;;
    *)
      echo "unsupported bulk-interactive topology: $probe_mode/$remote_service" >&2
      return 2
      ;;
  esac

  bulk_interactive_schedule_events_json=""
  bulk_interactive_schedule_event_count=0
  apply_bulk_interactive_baseline_profile "$remote_service"
  mkdir -p "$(dirname "$probe_gate_file")"
  rm -f \
    "$probe_launch_file" "$probe_gate_file" "$probe_finished_file" \
    "$probe_status_file" "${probe_status_file}.tmp" "$scheduler_cancel_file" \
    "$client_scheduler_ready_file" "$remote_scheduler_ready_file" \
    "$client_scheduler_status_file" "$remote_scheduler_status_file"
  exec_in client "rm -f '${out_file}' '${err_file}' '${probe_pid_file}'"
  host_time_namespace_id="$(readlink /proc/self/ns/time 2>/dev/null || true)"
  client_time_namespace_id="$(
    exec_in client "readlink /proc/self/ns/time" 2>/dev/null || true
  )"
  remote_time_namespace_id="$(
    exec_in "$remote_service" "readlink /proc/self/ns/time" 2>/dev/null || true
  )"
  host_monotonic_offset_json="$(
    normalize_monotonic_timens_offset </proc/self/timens_offsets 2>/dev/null || true
  )"
  client_monotonic_offset_json="$(
    exec_in client "cat /proc/self/timens_offsets" 2>/dev/null \
      | normalize_monotonic_timens_offset 2>/dev/null || true
  )"
  remote_monotonic_offset_json="$(
    exec_in "$remote_service" "cat /proc/self/timens_offsets" 2>/dev/null \
      | normalize_monotonic_timens_offset 2>/dev/null || true
  )"
  printf -v endpoint_clocks_json '{"client-egress":{"service":"client","time_namespace_id":"%s","monotonic_offset":%s},"remote-egress":{"service":"%s","time_namespace_id":"%s","monotonic_offset":%s}}' \
    "$client_time_namespace_id" "${client_monotonic_offset_json:-null}" \
    "$remote_service" "$remote_time_namespace_id" \
    "${remote_monotonic_offset_json:-null}"
  if [[ ! "$host_time_namespace_id" =~ ^time:\[[0-9]+\]$ \
    || ! "$client_time_namespace_id" =~ ^time:\[[0-9]+\]$ \
    || ! "$remote_time_namespace_id" =~ ^time:\[[0-9]+\]$ ]]; then
    clock_preflight_error="time namespace provenance invalid"
  elif [[ -z "$host_monotonic_offset_json" \
    || -z "$client_monotonic_offset_json" \
    || -z "$remote_monotonic_offset_json" ]]; then
    clock_preflight_error="effective monotonic offset unavailable"
  elif [[ "$host_monotonic_offset_json" != "$client_monotonic_offset_json" \
    || "$host_monotonic_offset_json" != "$remote_monotonic_offset_json" ]]; then
    clock_preflight_error="effective monotonic offset mismatch"
  fi
  if [[ -n "$clock_preflight_error" ]]; then
    schedule_exit_code=78
    dynamic_loss_metadata_json="$(bulk_interactive_dynamic_loss_metadata \
      null "$schedule_exit_code" null \
      "$host_time_namespace_id" "$client_time_namespace_id" \
      "${host_monotonic_offset_json:-null}" \
      "${client_monotonic_offset_json:-null}" \
      "$topology_mode" "$role_service_mapping_json" \
      "$endpoint_clocks_json")"
    append_mixed_probe_result \
      "$case_name" 78 "" "" "$clock_preflight_error" \
      "$mptunnel_row" "$baseline_identity_json" "$dynamic_loss_metadata_json" \
      "$protocol_override"
    return 0
  fi
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  active_telemetry_case="$case_name"
  active_telemetry_pid="$telemetry_pid"
  active_bulk_interactive_probe_pid_file="$probe_pid_file"
  active_bulk_interactive_cancel_file="$scheduler_cancel_file"
  active_bulk_interactive_host_files=(
    "$probe_launch_file" "$probe_gate_file" "$probe_finished_file"
    "$probe_status_file" "${probe_status_file}.tmp"
    "$scheduler_cancel_file"
    "$client_scheduler_ready_file" "$remote_scheduler_ready_file"
    "$client_scheduler_status_file" "$remote_scheduler_status_file"
    "${repo_root}/${client_scheduler_pid_relative}"
    "${repo_root}/${remote_scheduler_pid_relative}"
    "${repo_root}/${client_scheduler_result_relative}.log"
    "${repo_root}/${remote_scheduler_result_relative}.log"
  )
  local epoch
  for epoch in "${!bulk_interactive_loss_percent[@]}"; do
    active_bulk_interactive_host_files+=(
      "${repo_root}/${client_scheduler_result_relative}-epoch-${epoch}.json"
      "${repo_root}/${remote_scheduler_result_relative}-epoch-${epoch}.json"
    )
  done
  rm -f "${active_bulk_interactive_host_files[@]}"
  exec_in client "(while [[ ! -f '${probe_launch_container_file}' ]]; do sleep 0.01; done; if timeout $((bulk_interactive_probe_timeout_seconds + 10))s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --workload-mode bulk-interactive ${probe_route_arguments} --http-target ${probe_target_ip}:8080 --tcp-echo-target ${probe_target_ip}:10022 --bulk-path '${large_http_path}' --failover-after -1 --timeout '${bulk_interactive_probe_timeout_seconds}' --load-duration '${bulk_interactive_duration_seconds}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${probe_gate_container_file}' --finished-file '${probe_finished_container_file}' >'${out_file}' 2>'${err_file}'; then probe_exit=0; else probe_exit=\$?; fi; if [[ ! -f '${probe_finished_container_file}' ]]; then python3 - '${probe_finished_container_file}' <<'PY'
import os
import sys
import time
path = sys.argv[1]
temporary = f\"{path}.tmp-{os.getpid()}\"
with open(temporary, \"w\", encoding=\"utf-8\") as handle:
    handle.write(f\"{time.monotonic_ns() // 1_000_000}\\n\")
os.replace(temporary, path)
PY
fi; printf '%s\n' \"\$probe_exit\" > '${probe_status_container_file}.tmp'; mv '${probe_status_container_file}.tmp' '${probe_status_container_file}') </dev/null >/dev/null 2>&1 & echo \$! > '${probe_pid_file}'"

  start_bulk_interactive_loss_scheduler \
    client-egress client "$probe_gate_container_file" \
    "$probe_finished_container_file" "$scheduler_cancel_container_file" \
    "$client_scheduler_ready_container_file" \
    "$client_scheduler_status_container_file" \
    "$client_scheduler_pid_container_file" \
    "$client_scheduler_result_container_prefix" \
    "${repo_root}/${client_scheduler_result_relative}.log"
  active_bulk_interactive_schedule_pids=("$bulk_interactive_scheduler_wrapper_pid")
  active_bulk_interactive_schedule_services=(client)
  active_bulk_interactive_schedule_pid_files=("$client_scheduler_pid_container_file")
  start_bulk_interactive_loss_scheduler \
    remote-egress "$remote_service" "$probe_gate_container_file" \
    "$probe_finished_container_file" "$scheduler_cancel_container_file" \
    "$remote_scheduler_ready_container_file" \
    "$remote_scheduler_status_container_file" \
    "$remote_scheduler_pid_container_file" \
    "$remote_scheduler_result_container_prefix" \
    "${repo_root}/${remote_scheduler_result_relative}.log"
  active_bulk_interactive_schedule_pids+=("$bulk_interactive_scheduler_wrapper_pid")
  active_bulk_interactive_schedule_services+=("$remote_service")
  active_bulk_interactive_schedule_pid_files+=("$remote_scheduler_pid_container_file")

  scheduler_ready_deadline=$((SECONDS + 10))
  while [[ ! -f "$client_scheduler_ready_file" \
    || ! -f "$remote_scheduler_ready_file" ]] \
    && (( SECONDS < scheduler_ready_deadline )); do
    sleep 0.01
  done
  schedule_exit_code=78
  if [[ -f "$client_scheduler_ready_file" \
    && -f "$remote_scheduler_ready_file" ]]; then
    : > "$probe_launch_file"
  else
    : > "$scheduler_cancel_file"
    cleanup_active_bulk_interactive_probe
    stop_case_telemetry "$case_name" "$telemetry_pid"
    active_telemetry_case=""
    active_telemetry_pid=""
    dynamic_loss_metadata_json="$(bulk_interactive_dynamic_loss_metadata \
      null "$schedule_exit_code" null \
      "$host_time_namespace_id" "$client_time_namespace_id" \
      "${host_monotonic_offset_json:-null}" \
      "${client_monotonic_offset_json:-null}" \
      "$topology_mode" "$role_service_mapping_json" \
      "$endpoint_clocks_json")"
    apply_netem "$default_netem_mode"
    append_mixed_probe_result \
      "$case_name" 78 "" "" "endpoint loss scheduler readiness failed" \
      "$mptunnel_row" "$baseline_identity_json" "$dynamic_loss_metadata_json" \
      "$protocol_override"
    return 0
  fi

  local gate_deadline=$((SECONDS + 10))
  while [[ ! -f "$probe_gate_file" && ! -f "$probe_status_file" ]] \
    && (( SECONDS < gate_deadline )); do
    sleep 0.01
  done
  if [[ -f "$probe_gate_file" ]]; then
    mapfile -t probe_anchor < "$probe_gate_file"
    if (( ${#probe_anchor[@]} == 3 )); then
      if wait_for_bulk_interactive_schedulers \
        "$client_scheduler_status_file" "$remote_scheduler_status_file"; then
        if collect_bulk_interactive_dynamic_loss_schedule \
          "$client_scheduler_status_file" "$remote_scheduler_status_file" \
          "${repo_root}/${client_scheduler_result_relative}" \
          "${repo_root}/${remote_scheduler_result_relative}"; then
          schedule_exit_code=0
        else
          schedule_exit_code="$?"
        fi
        schedule_completed_offset_ms="$bulk_interactive_schedule_completed_offset_ms"
      else
        schedule_exit_code="$?"
      fi
    else
      schedule_exit_code=2
    fi
  fi

  for schedule_pid in "${active_bulk_interactive_schedule_pids[@]}"; do
    wait "$schedule_pid" >/dev/null 2>&1 || true
  done
  active_bulk_interactive_schedule_pids=()
  active_bulk_interactive_schedule_services=()
  active_bulk_interactive_schedule_pid_files=()

  wait_for_case_probe \
    "$probe_status_file" "$probe_pid_file" "$bulk_interactive_probe_timeout_seconds"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  active_telemetry_case=""
  active_telemetry_pid=""
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  exit_code="$(cat "$probe_status_file" 2>/dev/null || echo 124)"
  dynamic_loss_metadata_json="$(bulk_interactive_dynamic_loss_metadata \
    "${probe_anchor[1]:-}" \
    "$schedule_exit_code" \
    "$schedule_completed_offset_ms" \
    "$host_time_namespace_id" "$client_time_namespace_id" \
    "${host_monotonic_offset_json:-null}" \
    "${client_monotonic_offset_json:-null}" \
    "$topology_mode" "$role_service_mapping_json" \
    "$endpoint_clocks_json")"
  apply_netem "$default_netem_mode"
  append_mixed_probe_result \
    "$case_name" "$exit_code" "$output" "" "$probe_stderr" \
    "$mptunnel_row" "$baseline_identity_json" "$dynamic_loss_metadata_json" \
    "$protocol_override"
  cleanup_active_bulk_interactive_probe
}

run_raw_tcp_bulk_interactive_case() {
  local case_name="baseline_raw_tcp_bulk_interactive_balanced"
  local probe_status=0
  local service pid_file
  prepare_baseline_case "$default_netem_mode"
  stop_client
  stop_server
  for service in client server; do
    pid_file="/tmp/mptunnel-${service}.pid"
    exec_in "$service" "deadline=\$((SECONDS + 5)); while pgrep -f '[m]ptunnel.*--config' >/dev/null && (( SECONDS < deadline )); do sleep 0.05; done; test ! -e '${pid_file}'; ! pgrep -f '[m]ptunnel.*--config' >/dev/null"
  done
  if run_bulk_interactive_probe_case \
    "$case_name" "" 0 "" direct target raw-tcp; then
    :
  else
    probe_status="$?"
  fi
  if [[ "$isolate_cases" != "1" ]]; then
    start_server
  fi
  return "$probe_status"
}

run_baseline_upload_probe_case() {
  local case_name="$1"
  local protocol="$2"
  local proxy_port_arg="$3"
  local baseline_identity_json="${4:-}"
  local out_file="/tmp/mptunnel-baseline-upload-${case_name}.out"
  local err_file="/tmp/mptunnel-baseline-upload-${case_name}.err"
  local output probe_stderr exit_code
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  restart_target_tcp_sink
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout ${upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --protocol '${protocol}-upload' --proxy 127.0.0.1:${proxy_port_arg} --target 172.31.40.30:${tcp_upload_target_port} --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-uploads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  exit_code="$?"
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 0 "${protocol}-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code" "" "$baseline_identity_json"
}

ensure_baseline_tool() {
  local service="$1"
  local tool="$2"
  exec_in "$service" "$baseline_tool_command 'ensure-${tool}'"
}

capture_baseline_identity() {
  local tool="$1"
  local client_identity server_identity
  client_identity="$(exec_in client "$baseline_tool_command 'identity-${tool}'")"
  server_identity="$(exec_in server "$baseline_tool_command 'identity-${tool}'")"
  CLIENT_IDENTITY="$client_identity" SERVER_IDENTITY="$server_identity" TOOL="$tool" \
  BASELINE_LOCK_FILE="$baseline_lock_file" BASELINE_LOCK_SHA256="$baseline_lock_sha256" \
  LAB_SCRIPT_DIR="$script_dir" \
    python3 - <<'PY'
import json
import os
import sys

tool = os.environ["TOOL"]
client = json.loads(os.environ["CLIENT_IDENTITY"])
server = json.loads(os.environ["SERVER_IDENTITY"])
lock_path = os.environ["BASELINE_LOCK_FILE"]
lock_sha256 = os.environ["BASELINE_LOCK_SHA256"]
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import load_baseline_lock
locked_tool = load_baseline_lock(lock_path, lock_sha256)["tools"][tool]
if client.get("tool") != tool or server.get("tool") != tool:
    raise ValueError("baseline endpoint identity names do not match")
for endpoint in (client, server):
    architecture = endpoint.get("architecture")
    asset = locked_tool["assets"].get(architecture, {})
    if endpoint.get("release") != locked_tool["release"]:
        raise ValueError("baseline endpoint release does not match the run lock")
    if (
        endpoint.get("asset_name") != asset.get("name")
        or endpoint.get("asset_sha256") != asset.get("sha256")
    ):
        raise ValueError("baseline endpoint asset does not match the run lock")
identity = {
    "tool": tool,
    "lock_sha256": lock_sha256,
    "client": client,
    "server": server,
}
print(json.dumps(identity, separators=(",", ":"), sort_keys=True))
PY
}

run_vmess_baseline_case() {
  local case_name="$1"
  local server_ip="$2"
  local netem_mode="${3:-$default_netem_mode}"
  local workload_mode="${4:-download}"
  prepare_baseline_profile "$netem_mode"
  if ! ensure_baseline_tool server xray || ! ensure_baseline_tool client xray; then
    append_skipped_result "$case_name" "vmess" "xray baseline binary unavailable"
    return 0
  fi
  exec_in server "bash /workspace/lab/baseline-tools.sh write-xray-server '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}'"
  exec_in client "bash /workspace/lab/baseline-tools.sh write-xray-client '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}' 127.0.0.1 '${baseline_proxy_port}'"
  exec_in server "$baseline_tool_command run-xray-server >/tmp/mptunnel-baseline-vmess-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-server.pid"
  exec_in client "$baseline_tool_command run-xray-client >/tmp/mptunnel-baseline-vmess-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-client.pid)"; then
    append_skipped_result "$case_name" "vmess" "xray baseline failed to start"
    stop_baselines
    return 0
  fi
  local baseline_identity_json
  if ! baseline_identity_json="$(capture_baseline_identity xray)"; then
    append_skipped_result "$case_name" "vmess" "could not verify the running xray executables"
    stop_baselines
    return 0
  fi
  if [[ "$workload_mode" == "bulk-interactive" ]]; then
    run_bulk_interactive_probe_case \
      "$case_name" "$baseline_proxy_port" 0 "$baseline_identity_json"
  else
    run_baseline_download_probe_case "$case_name" "vmess" "$baseline_proxy_port" "$baseline_identity_json"
  fi
  stop_baselines
}

run_vmess_baseline_upload_case() {
  local case_name="$1"
  local server_ip="$2"
  local netem_mode="${3:-$default_netem_mode}"
  prepare_baseline_profile "$netem_mode"
  if ! ensure_baseline_tool server xray || ! ensure_baseline_tool client xray; then
    append_skipped_result "$case_name" "vmess-upload" "xray baseline binary unavailable"
    return 0
  fi
  exec_in server "bash /workspace/lab/baseline-tools.sh write-xray-server '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}'"
  exec_in client "bash /workspace/lab/baseline-tools.sh write-xray-client '${baseline_uuid}' '${server_ip}' '${baseline_vmess_port}' 127.0.0.1 '${baseline_proxy_port}'"
  exec_in server "$baseline_tool_command run-xray-server >/tmp/mptunnel-baseline-vmess-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-server.pid"
  exec_in client "$baseline_tool_command run-xray-client >/tmp/mptunnel-baseline-vmess-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-vmess-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-vmess-client.pid)"; then
    append_skipped_result "$case_name" "vmess-upload" "xray baseline failed to start"
    stop_baselines
    return 0
  fi
  local baseline_identity_json
  if ! baseline_identity_json="$(capture_baseline_identity xray)"; then
    append_skipped_result "$case_name" "vmess-upload" "could not verify the running xray executables"
    stop_baselines
    return 0
  fi
  run_baseline_upload_probe_case "$case_name" "vmess" "$baseline_proxy_port" "$baseline_identity_json"
  stop_baselines
}

run_hysteria2_baseline_case() {
  local case_name="$1"
  local server_ip="$2"
  local netem_mode="${3:-$default_netem_mode}"
  local brutal_up="${4:-}"
  local brutal_down="${5:-$brutal_up}"
  local workload_mode="${6:-download}"
  prepare_baseline_profile "$netem_mode"
  if ! ensure_baseline_tool server hysteria2 || ! ensure_baseline_tool client hysteria2; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 baseline binary unavailable"
    return 0
  fi
  if ! exec_in server "bash /workspace/lab/baseline-tools.sh write-hysteria-server '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}'"; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 TLS certificate generation unavailable"
    return 0
  fi
  exec_in client "bash /workspace/lab/baseline-tools.sh write-hysteria-client '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}' 127.0.0.1 '${baseline_proxy_port}' '${brutal_up}' '${brutal_down}'"
  exec_in server "$baseline_tool_command run-hysteria-server >/tmp/mptunnel-baseline-hysteria-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-server.pid"
  exec_in client "$baseline_tool_command run-hysteria-client >/tmp/mptunnel-baseline-hysteria-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-client.pid)"; then
    append_skipped_result "$case_name" "hysteria2" "hysteria2 baseline failed to start"
    stop_baselines
    return 0
  fi
  local baseline_identity_json
  if ! baseline_identity_json="$(capture_baseline_identity hysteria2)"; then
    append_skipped_result "$case_name" "hysteria2" "could not verify the running hysteria2 executables"
    stop_baselines
    return 0
  fi
  if [[ "$workload_mode" == "bulk-interactive" ]]; then
    run_bulk_interactive_probe_case \
      "$case_name" "$baseline_proxy_port" 0 "$baseline_identity_json"
  else
    run_baseline_download_probe_case "$case_name" "hysteria2" "$baseline_proxy_port" "$baseline_identity_json"
  fi
  stop_baselines
}

run_hysteria2_baseline_upload_case() {
  local case_name="$1"
  local server_ip="$2"
  local netem_mode="${3:-$default_netem_mode}"
  local brutal_up="${4:-}"
  local brutal_down="${5:-$brutal_up}"
  prepare_baseline_profile "$netem_mode"
  if ! ensure_baseline_tool server hysteria2 || ! ensure_baseline_tool client hysteria2; then
    append_skipped_result "$case_name" "hysteria2-upload" "hysteria2 baseline binary unavailable"
    return 0
  fi
  if ! exec_in server "bash /workspace/lab/baseline-tools.sh write-hysteria-server '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}'"; then
    append_skipped_result "$case_name" "hysteria2-upload" "hysteria2 TLS certificate generation unavailable"
    return 0
  fi
  exec_in client "bash /workspace/lab/baseline-tools.sh write-hysteria-client '${baseline_uuid}' '${server_ip}' '${baseline_hysteria2_port}' 127.0.0.1 '${baseline_proxy_port}' '${brutal_up}' '${brutal_down}'"
  exec_in server "$baseline_tool_command run-hysteria-server >/tmp/mptunnel-baseline-hysteria-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-server.pid"
  exec_in client "$baseline_tool_command run-hysteria-client >/tmp/mptunnel-baseline-hysteria-client.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-hysteria-client.pid"
  sleep 1
  if ! exec_in server "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-server.pid)" || \
     ! exec_in client "kill -0 \$(cat /tmp/mptunnel-baseline-hysteria-client.pid)"; then
    append_skipped_result "$case_name" "hysteria2-upload" "hysteria2 baseline failed to start"
    stop_baselines
    return 0
  fi
  local baseline_identity_json
  if ! baseline_identity_json="$(capture_baseline_identity hysteria2)"; then
    append_skipped_result "$case_name" "hysteria2-upload" "could not verify the running hysteria2 executables"
    stop_baselines
    return 0
  fi
  run_baseline_upload_probe_case "$case_name" "hysteria2" "$baseline_proxy_port" "$baseline_identity_json"
  stop_baselines
}

configure_mptcp_endpoints() {
  local service="$1"
  local role="$2"
  shift 2
  if ! exec_in "$service" "ip mptcp limits set subflows 4 add_addr_accepted 4 && ip mptcp endpoint flush"; then
    echo "${service}: could not set MPTCP limits and flush old endpoints" >&2
    return 1
  fi
  case "$role" in
    accept)
      return 0
      ;;
    signal) ;;
    *)
      echo "${service}: unknown MPTCP endpoint role ${role}" >&2
      return 2
      ;;
  esac
  local id=1
  local addr dev
  for addr in "$@"; do
    if ! dev="$(exec_in "$service" "ip -o addr show | awk -v ip='${addr}' '\$4 ~ \"^\" ip \"/\" {print \$2; exit}'")" || [[ -z "$dev" ]]; then
      echo "${service}: no interface owns requested MPTCP address ${addr}" >&2
      return 1
    fi
    if ! exec_in "$service" "ip mptcp endpoint add '${addr}' dev '${dev}' id '${id}' signal"; then
      echo "${service}: failed to add MPTCP endpoint ${addr} on ${dev} with id ${id}" >&2
      return 1
    fi
    if ! exec_in "$service" "ip mptcp endpoint show id '${id}' | awk -v ip='${addr}' '\$1 == ip {found=1} END {exit !found}'"; then
      echo "${service}: kernel endpoint table did not retain ${addr} with id ${id}" >&2
      return 1
    fi
    id=$((id + 1))
  done
  return 0
}

check_mptcp_baseline_case() {
  local case_name="$1"
  local protocol="$2"
  if ! exec_in client "python3 /workspace/lab/mptcp_http.py check" || \
     ! exec_in target "python3 /workspace/lab/mptcp_http.py check"; then
    append_skipped_result "$case_name" "$protocol" "kernel MPTCP sockets unavailable"
    return 1
  fi
  # The initial connection owns the .10 path. The target advertises the four
  # remaining directly connected addresses, so the client route selects the
  # matching local source for exactly four additional subflows. Advertising
  # every address as signal+subflow+fullmesh would request a race-selected
  # 5x5 mesh rather than the five-path topology this baseline measures.
  local endpoint_error
  if ! endpoint_error="$(configure_mptcp_endpoints client accept 2>&1)"; then
    endpoint_error="${endpoint_error//$'\n'/; }"
    append_skipped_result "$case_name" "$protocol" "client MPTCP endpoint configuration failed: ${endpoint_error: -1500}"
    return 1
  fi
  if ! endpoint_error="$(configure_mptcp_endpoints target signal 172.31.15.30 172.31.16.30 172.31.20.30 172.31.30.30 2>&1)"; then
    endpoint_error="${endpoint_error//$'\n'/; }"
    append_skipped_result "$case_name" "$protocol" "target MPTCP endpoint configuration failed: ${endpoint_error: -1500}"
    return 1
  fi
  return 0
}

run_mptcp_baseline_case() {
  local case_name="$1"
  local netem_mode="${2:-$default_netem_mode}"
  prepare_baseline_case "$netem_mode"
  if ! check_mptcp_baseline_case "$case_name" "mptcp"; then
    return 0
  fi
  exec_in target "python3 /workspace/lab/mptcp_http.py serve --bind 0.0.0.0:${baseline_mptcp_port} --file /tmp/mptunnel-lab/large.bin >/tmp/mptunnel-baseline-mptcp-server.log 2>&1 & echo \$! >/tmp/mptunnel-baseline-mptcp-server.pid"
  sleep 1
  if ! exec_in target "kill -0 \$(cat /tmp/mptunnel-baseline-mptcp-server.pid)"; then
    append_skipped_result "$case_name" "mptcp" "MPTCP HTTP server failed to start"
    stop_baselines
    return 0
  fi
  local telemetry_pid mptcp_evidence_json
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  start_mptcp_evidence "$case_name"
  set +e
  local output exit_code
  output="$(exec_in client "timeout $((curl_timeout + 10))s python3 /workspace/lab/mptcp_http.py download --label '${case_name}' --target 172.31.10.30:${baseline_mptcp_port} --path '${large_http_path}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}'" 2>/dev/null)"
  exit_code="$?"
  stop_mptcp_evidence "$case_name"
  mptcp_evidence_json="$(mptcp_evidence_summary "$case_name")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  set -e
  if [[ -n "$output" ]]; then
    append_row_with_telemetry "$case_name" "$output" "mptcp" 0 "$mptcp_evidence_json"
  else
    append_row_with_telemetry "$case_name" "{\"case\":\"$case_name\",\"protocol\":\"mptcp\",\"status\":\"fail\",\"exit_code\":$exit_code}" "mptcp" 0 "$mptcp_evidence_json"
  fi
  stop_baselines
}

run_mptcp_baseline_upload_case() {
  local case_name="$1"
  local netem_mode="${2:-$default_netem_mode}"
  prepare_baseline_case "$netem_mode"
  if ! check_mptcp_baseline_case "$case_name" "mptcp-upload"; then
    return 0
  fi

  # Both endpoints use MPTCP sockets; the finalized sink snapshot supplies the
  # same exact metric-v4 receiver authority as every other upload cohort.
  if ! restart_target_tcp_sink mptcp; then
    append_skipped_result "$case_name" "mptcp-upload" "MPTCP upload sink failed to start"
    stop_baselines
    return 0
  fi
  local out_file="/tmp/mptunnel-baseline-upload-${case_name}.out"
  local err_file="/tmp/mptunnel-baseline-upload-${case_name}.err"
  local output probe_stderr exit_code mptcp_evidence_json
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  start_mptcp_evidence "$case_name"
  set +e
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "rm -f '${out_file}' '${err_file}'; timeout ${upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --protocol 'mptcp-upload' --mptcp --target 172.31.10.30:${tcp_upload_target_port} --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-uploads '${bulk_connections}' >'${out_file}' 2>'${err_file}'"
  exit_code="$?"
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_mptcp_evidence "$case_name"
  mptcp_evidence_json="$(mptcp_evidence_summary "$case_name")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${out_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -n 80 '${err_file}' 2>/dev/null | tail -c 4000 || true")"
  set -e
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 0 "mptcp-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code" "$mptcp_evidence_json"
  stop_baselines
}

append_mixed_probe_result() {
  local case_name="$1"
  local exit_code="$2"
  local output="$3"
  local flapping_metadata_json="${4:-}"
  local probe_stderr="${5:-}"
  local mptunnel_row="${6:-1}"
  local baseline_identity_json="${7:-}"
  local dynamic_loss_metadata_json="${8:-}"
  local protocol_override="${9:-}"
  local client_log server_log

  client_log="$(exec_in client "for file in /tmp/mptunnel-client-*.log; do [ -f \"\$file\" ] || continue; echo \"== \$(basename \"\$file\") ==\"; tail -n '${log_tail_lines}' \"\$file\"; done | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"
  server_log="$(exec_in server "tail -n '${log_tail_lines}' /tmp/mptunnel-server.log 2>/dev/null | tail -c '${log_tail_bytes}'" 2>/dev/null || true)"

  ROW="$output" \
  EXIT_CODE="$exit_code" \
  CLIENT_LOG="$client_log" \
  SERVER_LOG="$server_log" \
  LAB_DIAG="${MPTUNNEL_LAB_DIAG:-0}" \
  LAB_DIAG_EVENTS="${MPTUNNEL_LAB_DIAG_EVENTS:-}" \
  LAB_PERF="${MPTUNNEL_LAB_PERF:-0}" \
  MPTUNNEL_ROW="$mptunnel_row" \
  LOG_TAIL_BYTES="$log_tail_bytes" \
  TELEMETRY="$(case_telemetry_summary "$case_name")" \
  LOG_ARTIFACTS="$(case_log_artifacts_summary "$case_name")" \
  FLAPPING_METADATA="$flapping_metadata_json" \
  PROBE_STDERR="$probe_stderr" \
  RESULT_REPRODUCIBILITY="$result_reproducibility" \
  BASELINE_IDENTITY="$baseline_identity_json" \
  BULK_INTERACTIVE_DYNAMIC_LOSS_METADATA="$dynamic_loss_metadata_json" \
  PROTOCOL_OVERRIDE="$protocol_override" \
  LAB_SCRIPT_DIR="$script_dir" \
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
protocol_override = os.environ.get("PROTOCOL_OVERRIDE", "")
if protocol_override:
    row["protocol"] = protocol_override
sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
from result_enrichment import enrich_instrumentation_for_scope
lab_diag, lab_perf = enrich_instrumentation_for_scope(
    row,
    os.environ.get("MPTUNNEL_ROW", ""),
    os.environ.get("LAB_DIAG", ""),
    os.environ.get("LAB_PERF", ""),
    os.environ.get("LAB_DIAG_EVENTS", ""),
)
raw_flapping = os.environ.get("FLAPPING_METADATA", "")
if raw_flapping:
    try:
        flapping = json.loads(raw_flapping)
        if isinstance(flapping, dict) and flapping:
            sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
            from flapping_schedule import attach_metadata_to_result
            attach_metadata_to_result(row, flapping)
    except json.JSONDecodeError as exc:
        row["probe_status_before_flapping_validation"] = row.get("status")
        row["status"] = "fail"
        row["failure_reason"] = "flapping metadata is invalid"
        row["flapping_metadata_error"] = str(exc)
raw_dynamic_loss = os.environ.get("BULK_INTERACTIVE_DYNAMIC_LOSS_METADATA", "")
if raw_dynamic_loss:
    try:
        dynamic_loss = json.loads(raw_dynamic_loss)
        if not isinstance(dynamic_loss, dict):
            raise ValueError("dynamic-loss metadata is not an object")
        row["bulk_interactive_dynamic_loss"] = dynamic_loss
        from result_enrichment import (
            validate_bulk_interactive_dynamic_loss_metadata,
            validate_bulk_interactive_probe_route,
        )
        validate_bulk_interactive_dynamic_loss_metadata(dynamic_loss)
        validate_bulk_interactive_probe_route(row, dynamic_loss)
    except (json.JSONDecodeError, ValueError) as exc:
        row["probe_status_before_dynamic_loss_validation"] = row.get("status")
        row["status"] = "fail"
        row["failure_reason"] = "bulk-interactive dynamic-loss trace is invalid"
        row["bulk_interactive_dynamic_loss_error"] = str(exc)
elif row.get("workload_mode") == "bulk-interactive":
    row["probe_status_before_dynamic_loss_validation"] = row.get("status")
    row["status"] = "fail"
    row["failure_reason"] = "bulk-interactive dynamic-loss trace is missing"
try:
    log_tail_bytes = int(os.environ.get("LOG_TAIL_BYTES", "4000"))
except ValueError:
    log_tail_bytes = 4000
probe_stderr = os.environ.get("PROBE_STDERR", "")
if row.get("status") != "ok" and probe_stderr:
    row["probe_stderr_tail"] = probe_stderr[-log_tail_bytes:]
if row.get("status") != "ok" or lab_diag or lab_perf:
    for env_name, field in (
        ("CLIENT_LOG", "client_log_tail"),
        ("SERVER_LOG", "server_log_tail"),
    ):
        value = os.environ.get(env_name, "")
        if value:
            row[field] = value[-log_tail_bytes:]
try:
    telemetry = json.loads(os.environ.get("TELEMETRY", "{}"))
except json.JSONDecodeError:
    telemetry = {}
if telemetry:
    row["container_telemetry"] = telemetry
try:
    sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
    from result_enrichment import enrich_traffic_overhead
    enrich_traffic_overhead(row, telemetry)
except Exception as exc:
    row["traffic_overhead_error"] = str(exc)
try:
    log_artifacts = json.loads(os.environ.get("LOG_ARTIFACTS", "{}"))
except json.JSONDecodeError:
    log_artifacts = {}
if log_artifacts:
    row["log_artifacts"] = log_artifacts
if log_artifacts or row.get("status") not in ("ok", "loss"):
    try:
        sys.path.insert(0, os.environ["LAB_SCRIPT_DIR"])
        from diagnostic_buckets import analyze_row
        row["diagnostic_failure_buckets"] = analyze_row(row, log_artifacts, telemetry)
    except Exception as exc:
        row["diagnostic_failure_buckets_error"] = str(exc)
from result_enrichment import enrich_reproducibility
enrich_reproducibility(row, os.environ["RESULT_REPRODUCIBILITY"])
from result_enrichment import enrich_baseline_identity
enrich_baseline_identity(row, os.environ.get("BASELINE_IDENTITY", ""))
print(json.dumps(row, sort_keys=True))
PY
}

record_mixed_probe_case() {
  local case_name="$1"
  local telemetry_pid
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output
  output="$(exec_in client "python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}'" 2>/dev/null)"
  local exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  set -e
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 1
}

run_direct_mixed_case() {
  local case_name="$1"
  local target_ip="$2"
  local netem_mode="${3:-$default_netem_mode}"
  if [[ "$isolate_cases" == "1" ]]; then
    stop_client
    if [[ "$isolate_containers" == "1" ]]; then
      stop_server
      compose down --remove-orphans >/dev/null 2>&1 || true
      compose_up >/dev/null
    fi
    apply_netem "$netem_mode"
    start_target_services
    sleep 1
  else
    stop_client
    apply_netem "$netem_mode"
  fi

  local telemetry_pid
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  set +e
  local output
  output="$(exec_in client "python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --mode direct --http-target '${target_ip}:8080' --udp-target '${target_ip}:9090' --tcp-echo-target '${target_ip}:10022' --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}'" 2>/dev/null)"
  local exit_code="$?"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  set -e
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 0
}

prepare_seeded_direct_case() {
  # Legacy fixed profiles are deterministic without resetting their random
  # process. Seeded netem is part of a paired experiment, so every direct
  # subject must start from the same empty queue and PRNG state as proxies and
  # mptunnel subjects.
  if [[ "$default_netem_mode" =~ ^internet-five-path-epoch-[0-9]+$ \
    || "$default_netem_mode" =~ ^internet-five-path-load-coupled-epoch-[0-9]+$ ]]; then
    prepare_baseline_case "$default_netem_mode"
  fi
}

run_direct_download_case() {
  local case_name="$1"
  local target="$2"
  prepare_seeded_direct_case
  run_unproxied_download_probe_case "$case_name" "tcp" "$target"
}

run_direct_upload_case() {
  local case_name="$1"
  local target="$2"
  prepare_seeded_direct_case
  run_unproxied_upload_probe_case "$case_name" "$target"
}

run_direct_unconstrained_download_case() {
  local case_name="$1"
  local target="$2"
  prepare_baseline_case unconstrained
  run_unproxied_download_probe_case "$case_name" "tcp" "$target"
  apply_netem apply
}

run_direct_unconstrained_upload_case() {
  local case_name="$1"
  local target="$2"
  prepare_baseline_case unconstrained
  run_unproxied_upload_probe_case "$case_name" "$target"
  apply_netem apply
}

run_client_direct_download_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  # The proxy and MPP carriers remain active, but this exact destination is
  # selected by the client's first routing rule and must use ordinary direct
  # egress without entering the MPP server.
  run_tcp_download_probe_case \
    "$case_name" \
    "$load_duration_seconds" \
    "$bulk_connections" \
    duration \
    0 \
    "$large_http_path" \
    "$curl_timeout" \
    "172.31.15.30:8080"
}

run_client_direct_upload_case() {
  local case_name="$1"
  shift
  start_client "$case_name" "$@"
  run_tcp_upload_probe_case \
    "$case_name" \
    "$load_duration_seconds" \
    "$bulk_connections" \
    0 \
    "$curl_timeout" \
    "172.31.15.30:${tcp_upload_target_port}"
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
  local output exit_code telemetry_pid flapping_metadata_json probe_stderr
  local output_file="/tmp/mptunnel-mixed-flapping.out"
  local error_file="/tmp/mptunnel-mixed-flapping.err"
  local probe_pid_file="/tmp/mptunnel-mixed-flapping.pid"
  local probe_gate_relative=".tmp/lab/flapper-${timestamp}-$$-probe.started"
  local probe_finished_relative=".tmp/lab/flapper-${timestamp}-$$-probe.finished"
  local probe_status_relative=".tmp/lab/flapper-${timestamp}-$$-probe.status"
  local probe_gate_file="${repo_root}/${probe_gate_relative}"
  local probe_finished_file="${repo_root}/${probe_finished_relative}"
  local probe_status_file="${repo_root}/${probe_status_relative}"
  local probe_gate_container_file="/workspace/${probe_gate_relative}"
  local probe_finished_container_file="/workspace/${probe_finished_relative}"
  local probe_status_container_file="/workspace/${probe_status_relative}"
  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f '${output_file}' '${error_file}' '${probe_pid_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  active_telemetry_case="$case_name"
  active_telemetry_pid="$telemetry_pid"
  mkdir -p "$(dirname "$probe_gate_file")"
  rm -f "$probe_gate_file" "$probe_finished_file" "$probe_status_file" "${probe_status_file}.tmp"
  start_random_flapping "$probe_gate_file" "$probe_finished_file"
  exec_in client "(set +e; timeout ${curl_timeout}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${probe_gate_container_file}' > '${output_file}' 2> '${error_file}'; probe_exit=\$?; : > '${probe_finished_container_file}'; printf '%s\n' \"\$probe_exit\" > '${probe_status_container_file}.tmp'; mv '${probe_status_container_file}.tmp' '${probe_status_container_file}') & echo \$! > '${probe_pid_file}'"
  local gate_deadline=$((SECONDS + 10))
  while [[ ! -f "$probe_gate_file" && ! -f "$probe_status_file" ]] && (( SECONDS < gate_deadline )); do
    sleep 0.01
  done
  if [[ -f "$probe_gate_file" ]]; then
    mapfile -t probe_anchor < "$probe_gate_file"
    flapper_probe_started_unix_seconds="${probe_anchor[0]}"
    flapper_started_monotonic_ms="${probe_anchor[1]}"
    flapper_started_unix_ms="${probe_anchor[2]}"
  elif [[ ! -f "$probe_status_file" ]]; then
    exec_in client "probe_pid=\$(cat '${probe_pid_file}' 2>/dev/null || true); if [ -n \"\$probe_pid\" ]; then pkill -TERM -P \"\$probe_pid\" >/dev/null 2>&1 || true; kill \"\$probe_pid\" >/dev/null 2>&1 || true; fi; true"
    : > "$probe_finished_file"
    printf '124\n' > "$probe_status_file"
  fi
  local probe_deadline=$((SECONDS + curl_timeout + 5))
  while [[ ! -f "$probe_status_file" ]] && (( SECONDS < probe_deadline )); do
    sleep 0.05
  done
  if [[ ! -f "$probe_status_file" ]]; then
    exec_in client "probe_pid=\$(cat '${probe_pid_file}' 2>/dev/null || true); if [ -n \"\$probe_pid\" ]; then pkill -TERM -P \"\$probe_pid\" >/dev/null 2>&1 || true; kill \"\$probe_pid\" >/dev/null 2>&1 || true; fi; true"
    : > "$probe_finished_file"
    printf '124\n' > "$probe_status_file"
  fi
  stop_random_flapping
  stop_case_telemetry "$case_name" "$telemetry_pid"
  active_telemetry_case=""
  active_telemetry_pid=""
  output="$(exec_in client "cat '${output_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -c '${log_tail_bytes}' '${error_file}' 2>/dev/null || true")"
  exit_code="$(cat "$probe_status_file" 2>/dev/null || echo 124)"
  rm -f "$probe_status_file" "${probe_status_file}.tmp"
  flapping_metadata_json="$(flapping_result_metadata)"
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "$flapping_metadata_json" "$probe_stderr" 1
}

prepare_path_variation_initial_epoch() {
  local rate_band="$1"
  local client_pid server_pid client_exit_code server_exit_code

  set +e
  exec_netem client "scale-${rate_band}-epoch-0-client" >/dev/null &
  client_pid="$!"
  exec_netem server "scale-${rate_band}-epoch-0-server" >/dev/null &
  server_pid="$!"
  wait "$client_pid"
  client_exit_code="$?"
  wait "$server_pid"
  server_exit_code="$?"
  set -e
  if [[ "$client_exit_code" != "0" || "$server_exit_code" != "0" ]]; then
    echo "failed to establish the ${rate_band} initial path conditions" >&2
    return 1
  fi
}

run_path_variation_schedule() {
  local trace_file="$1"
  local rate_band="$2"
  local origin_ms event_start_ms client_start_ms client_end_ms
  local server_start_ms server_end_ms hold_deadline_ms
  local client_profiles server_profiles client_exit_code server_exit_code
  local client_profiles_file server_profiles_file client_pid server_pid epoch preconditioned

  : > "$trace_file"
  origin_ms="$(monotonic_milliseconds)"
  for ((epoch = 0; epoch < scale_epoch_count; epoch++)); do
    event_start_ms="$(($(monotonic_milliseconds) - origin_ms))"
    client_start_ms="$(($(monotonic_milliseconds) - origin_ms))"
    server_start_ms="$(($(monotonic_milliseconds) - origin_ms))"
    if [[ "$epoch" == "0" ]]; then
      preconditioned=1
      client_profiles="$(python3 "$script_dir/path_variation.py" profiles --seed "$scale_seed" --epoch "$epoch" --direction client --rate-band "$rate_band")"
      server_profiles="$(python3 "$script_dir/path_variation.py" profiles --seed "$scale_seed" --epoch "$epoch" --direction server --rate-band "$rate_band")"
      client_exit_code=0
      server_exit_code=0
      client_end_ms="$(($(monotonic_milliseconds) - origin_ms))"
      server_end_ms="$client_end_ms"
    else
      preconditioned=0
      client_profiles_file="$(mktemp "${trace_file}.client.XXXXXX")"
      server_profiles_file="$(mktemp "${trace_file}.server.XXXXXX")"
      set +e
      exec_netem client "scale-${rate_band}-epoch-${epoch}-client" > "$client_profiles_file" &
      client_pid="$!"
      exec_netem server "scale-${rate_band}-epoch-${epoch}-server" > "$server_profiles_file" &
      server_pid="$!"
      wait "$client_pid"
      client_exit_code="$?"
      client_end_ms="$(($(monotonic_milliseconds) - origin_ms))"
      wait "$server_pid"
      server_exit_code="$?"
      server_end_ms="$(($(monotonic_milliseconds) - origin_ms))"
      set -e
      client_profiles="$(<"$client_profiles_file")"
      server_profiles="$(<"$server_profiles_file")"
      rm -f -- "$client_profiles_file" "$server_profiles_file"
    fi

    CLIENT_PROFILES="$client_profiles" \
    SERVER_PROFILES="$server_profiles" \
    python3 - \
      "$epoch" \
      "$event_start_ms" \
      "$client_start_ms" \
      "$client_end_ms" \
      "$client_exit_code" \
      "$server_start_ms" \
      "$server_end_ms" \
      "$server_exit_code" \
      "$preconditioned" <<'PY' >> "$trace_file"
import json
import os
import sys


def profiles(name):
    try:
        value = json.loads(os.environ.get(name, ""))
    except json.JSONDecodeError:
        value = []
    return value


fields = [int(value) for value in sys.argv[1:]]
print(
    json.dumps(
        {
            "epoch": fields[0],
            "event_start_offset_ms": fields[1],
            "client_apply_start_offset_ms": fields[2],
            "client_apply_end_offset_ms": fields[3],
            "client_exit_code": fields[4],
            "server_apply_start_offset_ms": fields[5],
            "server_apply_end_offset_ms": fields[6],
            "server_exit_code": fields[7],
            "preconditioned": bool(fields[8]),
            "client_profiles": profiles("CLIENT_PROFILES"),
            "server_profiles": profiles("SERVER_PROFILES"),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
)
PY
    if [[ "$client_exit_code" != "0" || "$server_exit_code" != "0" ]]; then
      return 1
    fi
    hold_deadline_ms="$(($(monotonic_milliseconds) + scale_epoch_seconds * 1000))"
    while (( $(monotonic_milliseconds) < hold_deadline_ms )); do
      sleep 0.05
    done
  done
}

path_variation_metadata() {
  local trace_file="$1"
  local rate_band="$2"
  python3 "$script_dir/path_variation.py" metadata \
    --seed "$scale_seed" \
    --rate-band "$rate_band" \
    --trace "$trace_file" \
    --expected-epochs "$scale_epoch_count"
}

attach_path_variation_metadata() {
  local output="$1"
  local metadata="$2"
  local schedule_exit_code="$3"
  ROW="$output" METADATA="$metadata" SCHEDULE_EXIT_CODE="$schedule_exit_code" \
    python3 - <<'PY'
import json
import os

try:
    row = json.loads(os.environ.get("ROW", ""))
except json.JSONDecodeError:
    row = {"status": "fail", "raw_output": os.environ.get("ROW", "")}
try:
    metadata = json.loads(os.environ.get("METADATA", ""))
except json.JSONDecodeError as exc:
    metadata = {"trace_complete": False, "trace_error": str(exc)}
schedule_exit_code = int(os.environ.get("SCHEDULE_EXIT_CODE", "1"))
metadata["schedule_exit_code"] = schedule_exit_code
row["path_variation"] = metadata
if schedule_exit_code != 0 or not metadata.get("trace_complete", False):
    row.setdefault("probe_status_before_path_variation_validation", row.get("status"))
    row["status"] = "fail"
    row["failure_reason"] = "path variation schedule was incomplete"
print(json.dumps(row, separators=(",", ":"), sort_keys=True))
PY
}

wait_for_case_probe() {
  local status_file="$1"
  local probe_pid_file="$2"
  local probe_duration_seconds="${3:-$scale_load_duration_seconds}"
  local deadline=$((SECONDS + probe_duration_seconds + 20))
  while [[ ! -f "$status_file" ]] && (( SECONDS < deadline )); do
    sleep 0.05
  done
  if [[ ! -f "$status_file" ]]; then
    exec_in client "probe_pid=\$(cat '${probe_pid_file}' 2>/dev/null || true); if [ -n \"\$probe_pid\" ]; then pkill -TERM -P \"\$probe_pid\" >/dev/null 2>&1 || true; kill \"\$probe_pid\" >/dev/null 2>&1 || true; fi; true"
    printf '124\n' > "$status_file"
  fi
}

run_browser_probe_case() {
  local case_name="$1"
  local object_path="$2"
  local probe_duration_seconds="$3"
  shift 3
  local probe_arguments="$*"
  local output_file="/tmp/${case_name}.out"
  local error_file="/tmp/${case_name}.err"
  local probe_pid_file="/tmp/${case_name}.pid"
  local started_file="/tmp/${case_name}.started"
  local status_relative=".tmp/lab/${case_name}-${timestamp}-$$.status"
  local status_file="${repo_root}/${status_relative}"
  local status_container_file="/workspace/${status_relative}"
  local telemetry_pid output probe_stderr exit_code
  local probe_process_timeout_seconds
  local probe_wait_timeout_seconds

  probe_process_timeout_seconds="$((probe_duration_seconds + 15))"
  if [[ " ${probe_arguments} " == *" --browser-full-load "* ]]; then
    probe_process_timeout_seconds="$(
      LOAD_DURATION_SECONDS="$probe_duration_seconds" \
      COMPLETION_TIMEOUT_SECONDS="$curl_timeout" \
        python3 -c 'import math, os
load = float(os.environ["LOAD_DURATION_SECONDS"])
completion = float(os.environ["COMPLETION_TIMEOUT_SECONDS"])
print(max(1, math.ceil(load + completion + 10.0)))'
    )"
  fi
  probe_wait_timeout_seconds="$((probe_process_timeout_seconds + 5))"

  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f '${output_file}' '${error_file}' '${probe_pid_file}' '${started_file}'"
  rm -f "$status_file" "${status_file}.tmp"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(set +e; timeout ${probe_process_timeout_seconds}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --browser-only --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --small-path '${object_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${probe_duration_seconds}' ${probe_arguments} --started-file '${started_file}' > '${output_file}' 2> '${error_file}'; probe_exit=\$?; printf '%s\n' \"\$probe_exit\" > '${status_container_file}.tmp'; mv '${status_container_file}.tmp' '${status_container_file}') & echo \$! > '${probe_pid_file}'"
  wait_for_case_probe "$status_file" "$probe_pid_file" "$probe_wait_timeout_seconds"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${output_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -c '${log_tail_bytes}' '${error_file}' 2>/dev/null || true")"
  exit_code="$(cat "$status_file" 2>/dev/null || echo 124)"
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "$probe_stderr" 1
  rm -f "$status_file" "${status_file}.tmp"
}

run_browser_batch_case() {
  run_browser_probe_case \
    "mptunnel_browser_concurrent_batches" \
    "$small_http_path" \
    "$load_duration_seconds" \
    --small-response-budget-ms "$browser_batch_deadline_ms" \
    --small-batch-size "$browser_batch_size" \
    --small-batch-period-ms "$browser_batch_period_ms" \
    --require-small-response-budget
}

run_browser_full_load_case() {
  run_browser_probe_case \
    "mptunnel_browser_full_load" \
    "$browser_load_path" \
    "$browser_load_duration_seconds" \
    --browser-full-load \
    --small-batch-size "$browser_load_concurrency"
}

run_varying_links_download_case() {
  local rate_band="$1"
  local rate_band_label="${rate_band//-/_}"
  local case_name="mptunnel_twenty_varying_links_${rate_band_label}_download"
  local output_file="/tmp/${case_name}.out"
  local error_file="/tmp/${case_name}.err"
  local probe_pid_file="/tmp/${case_name}.pid"
  local started_file="/tmp/${case_name}.started"
  local status_relative=".tmp/lab/${case_name}-${timestamp}-$$.status"
  local status_file="${repo_root}/${status_relative}"
  local status_container_file="/workspace/${status_relative}"
  local trace_file="${result_dir}/path-variation-${case_name}.jsonl"
  local telemetry_pid output probe_stderr exit_code schedule_exit_code
  local metadata metadata_exit_code

  start_client_with_netem "$case_name" "scale-${rate_band}-epoch-0" "$mixed_scale_all"
  exec_in client "rm -f '${output_file}' '${error_file}' '${probe_pid_file}' '${started_file}'"
  rm -f "$status_file" "${status_file}.tmp" "$trace_file"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(set +e; timeout $((scale_load_duration_seconds + 15))s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after -1 --timeout '${curl_timeout}' --load-duration '${scale_load_duration_seconds}' --parallel-downloads '${bulk_connections}' --started-file '${started_file}' > '${output_file}' 2> '${error_file}'; probe_exit=\$?; printf '%s\n' \"\$probe_exit\" > '${status_container_file}.tmp'; mv '${status_container_file}.tmp' '${status_container_file}') & echo \$! > '${probe_pid_file}'"
  if exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"; then
    set +e
    run_path_variation_schedule "$trace_file" "$rate_band"
    schedule_exit_code="$?"
    set -e
  else
    schedule_exit_code=124
  fi
  wait_for_case_probe "$status_file" "$probe_pid_file"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${output_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -c '${log_tail_bytes}' '${error_file}' 2>/dev/null || true")"
  exit_code="$(cat "$status_file" 2>/dev/null || echo 124)"
  set +e
  metadata="$(path_variation_metadata "$trace_file" "$rate_band")"
  metadata_exit_code="$?"
  set -e
  if [[ "$metadata_exit_code" != "0" && "$schedule_exit_code" == "0" ]]; then
    schedule_exit_code="$metadata_exit_code"
  fi
  output="$(attach_path_variation_metadata "$output" "$metadata" "$schedule_exit_code")"
  append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
  rm -f "$status_file" "${status_file}.tmp"
  apply_netem apply
}

run_varying_links_upload_case() {
  local rate_band="$1"
  local rate_band_label="${rate_band//-/_}"
  local case_name="mptunnel_twenty_varying_links_${rate_band_label}_upload"
  local output_file="/tmp/${case_name}.out"
  local error_file="/tmp/${case_name}.err"
  local probe_pid_file="/tmp/${case_name}.pid"
  local started_file="/tmp/${case_name}.started"
  local status_relative=".tmp/lab/${case_name}-${timestamp}-$$.status"
  local status_file="${repo_root}/${status_relative}"
  local status_container_file="/workspace/${status_relative}"
  local trace_file="${result_dir}/path-variation-${case_name}.jsonl"
  local telemetry_pid output probe_stderr exit_code schedule_exit_code
  local metadata metadata_exit_code observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code

  start_client_with_netem "$case_name" "scale-${rate_band}-epoch-0" "$mixed_scale_all"
  restart_target_tcp_sink
  exec_in client "rm -f '${output_file}' '${error_file}' '${probe_pid_file}' '${started_file}'"
  rm -f "$status_file" "${status_file}.tmp" "$trace_file"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "(set +e; timeout ${scale_upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:${tcp_upload_target_port} --failover-after -1 --timeout '${curl_timeout}' --load-duration '${scale_load_duration_seconds}' --parallel-uploads '${bulk_connections}' --started-file '${started_file}' > '${output_file}' 2> '${error_file}'; probe_exit=\$?; printf '%s\n' \"\$probe_exit\" > '${status_container_file}.tmp'; mv '${status_container_file}.tmp' '${status_container_file}') & echo \$! > '${probe_pid_file}'"
  if exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"; then
    set +e
    run_path_variation_schedule "$trace_file" "$rate_band"
    schedule_exit_code="$?"
    set -e
  else
    schedule_exit_code=124
  fi
  wait_for_case_probe "$status_file" "$probe_pid_file" "$scale_upload_process_timeout_seconds"
  set +e
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  set -e
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat '${output_file}' 2>/dev/null || true")"
  probe_stderr="$(exec_in client "tail -c '${log_tail_bytes}' '${error_file}' 2>/dev/null || true")"
  exit_code="$(cat "$status_file" 2>/dev/null || echo 124)"
  set +e
  metadata="$(path_variation_metadata "$trace_file" "$rate_band")"
  metadata_exit_code="$?"
  set -e
  if [[ "$metadata_exit_code" != "0" && "$schedule_exit_code" == "0" ]]; then
    schedule_exit_code="$metadata_exit_code"
  fi
  output="$(attach_path_variation_metadata "$output" "$metadata" "$schedule_exit_code")"
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 1 "tcp-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code"
  rm -f "$status_file" "${status_file}.tmp"
  apply_netem apply
}

run_asymmetric_download_case() {
  local case_name="$1"
  shift
  start_client_with_netem "$case_name" asymmetric "$@"
  run_tcp_download_probe_case "$case_name"
  apply_netem apply
}

run_asymmetric_upload_case() {
  local case_name="$1"
  shift
  start_client_with_netem "$case_name" asymmetric "$@"
  run_tcp_upload_probe_case "$case_name"
  apply_netem apply
}

netem_mode_for_equal_profile() {
  local ideal_path="$1"
  case "$ideal_path" in
    lowlat)
      printf 'ideal-all-lowlat\n'
      ;;
    balanced)
      printf 'ideal-all-balanced\n'
      ;;
    fat)
      printf 'ideal-all-fat\n'
      ;;
    poor)
      printf 'ideal-all-poor\n'
      ;;
    unconstrained)
      printf 'unconstrained\n'
      ;;
    *)
      echo "unknown equal profile: $ideal_path" >&2
      return 2
      ;;
  esac
}

run_reliable_ideal_download_case() {
  local case_name="$1"
  local ideal_path="$2"
  local netem_mode
  shift 2
  netem_mode="$(netem_mode_for_equal_profile "$ideal_path")"
  start_client_with_netem "$case_name" "$netem_mode" "$@"
  run_tcp_download_probe_case "$case_name"
  apply_netem apply
}

run_reliable_ideal_upload_case() {
  local case_name="$1"
  local ideal_path="$2"
  local netem_mode
  shift 2
  netem_mode="$(netem_mode_for_equal_profile "$ideal_path")"
  start_client_with_netem "$case_name" "$netem_mode" "$@"
  run_tcp_upload_probe_case "$case_name"
  apply_netem apply
}

run_tcp_carrier_qos_case() {
  local regime="$1"
  local netem_mode="$2"
  local topology="$3"
  local direction="$4"
  local case_name="mptunnel_tcp_carrier_qos_${regime}_${topology}_${direction}"
  local endpoint
  case "$topology" in
    range_1_1)
      endpoint="--path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=1'"
      ;;
    range_1_3)
      endpoint="--path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=3'"
      ;;
    range_3_3)
      endpoint="--path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=3'"
      ;;
    three_endpoints_1_1)
      endpoint="--path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=1' --path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=1' --path 'tcp://172.31.20.20:${server_port}?max-tcp-carriers=1'"
      ;;
    *)
      echo "unknown TCP carrier QoS topology: $topology" >&2
      return 2
      ;;
  esac

  start_client_with_netem "$case_name" "$netem_mode" "$endpoint"
  case "$direction" in
    download)
      run_tcp_download_probe_case \
        "$case_name" \
        "$tcp_carrier_qos_duration_seconds" \
        "$tcp_carrier_qos_workers" \
        fixed \
        1 \
        "$tcp_carrier_qos_http_path" \
        "$tcp_carrier_qos_probe_timeout_seconds"
      ;;
    upload)
      run_tcp_upload_probe_case \
        "$case_name" \
        "$tcp_carrier_qos_duration_seconds" \
        "$tcp_carrier_qos_workers" \
        1 \
        "$tcp_carrier_qos_probe_timeout_seconds"
      ;;
    *)
      echo "unknown TCP carrier QoS direction: $direction" >&2
      return 2
      ;;
  esac
  apply_netem apply
}

start_quic_port_hop_client() {
  local case_name="$1"
  shift
  port_hop_forwarding=1
  if [[ "$isolate_cases" != "1" ]]; then
    start_server_port_forwarding
  fi
  if ! start_client_with_netem "$case_name" unconstrained "$@"; then
    port_hop_forwarding=0
    return 1
  fi
  port_hop_forwarding=0
}

finish_quic_port_hop_case() {
  stop_client
  stop_server_port_forwarding
  apply_netem apply
}

run_quic_port_hop_download_case() {
  local case_name="mptunnel_udp_stream_single_unconstrained_port_hopping"
  local endpoint="--path 'quic://172.31.10.20:${port_hop_first_port}-${port_hop_last_port}?port-rotation-interval-s=5'"
  start_quic_port_hop_client "$case_name" "$endpoint"
  run_tcp_download_probe_case "$case_name"
  finish_quic_port_hop_case
}

run_quic_port_hop_upload_case() {
  local case_name="mptunnel_udp_stream_single_unconstrained_port_hopping_upload"
  local endpoint="--path 'quic://172.31.10.20:${port_hop_first_port}-${port_hop_last_port}?port-rotation-interval-s=5'"
  start_quic_port_hop_client "$case_name" "$endpoint"
  run_tcp_upload_probe_case "$case_name"
  finish_quic_port_hop_case
}

run_mixed_ideal_case() {
  local case_name="$1"
  local ideal_path="$2"
  local netem_mode
  shift 2
  netem_mode="$(netem_mode_for_equal_profile "$ideal_path")"
  start_client_with_netem "$case_name" "$netem_mode" "$@"
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

run_matrix_upload_case() {
  local bits="$1"
  local base_name case_name
  base_name="$(matrix_case_name "$bits")"
  case_name="${base_name}_upload"

  start_client_with_netem "$case_name" "matrix-b${bits}" "$tcp_lowlat $udp_lowlat"
  run_tcp_upload_probe_case "$case_name"
  apply_netem apply
}

run_mixed_failover_case() {
  local case_name="mptunnel_mixed_multipath_failover_blackhole_${failover_profile}"
  local output exit_code
  local telemetry_pid
  local started_file="/tmp/mptunnel-mixed.started"
  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f /tmp/mptunnel-mixed.out /tmp/mptunnel-mixed.status /tmp/mptunnel-mixed.pid '${started_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${started_file}' > /tmp/mptunnel-mixed.out 2>/tmp/mptunnel-mixed.err; echo \$? >/tmp/mptunnel-mixed.status) & echo \$! >/tmp/mptunnel-mixed.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-mixed.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-mixed.status ]; then echo 124 >/tmp/mptunnel-mixed.status; fi"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-mixed.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-mixed.status 2>/dev/null || echo 124")"
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 1
  apply_netem apply
}

run_mixed_latency_spike_case() {
  local case_name="mptunnel_mixed_multipath_latency_spike_fat"
  local output exit_code
  local telemetry_pid
  local started_file="/tmp/mptunnel-mixed-spike.started"
  start_client "$case_name" "$tcp_all $udp_all"
  exec_in client "rm -f /tmp/mptunnel-mixed-spike.out /tmp/mptunnel-mixed-spike.status /tmp/mptunnel-mixed-spike.pid '${started_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/mixed_workload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --http-target 172.31.40.30:8080 --udp-target 172.31.40.30:9090 --tcp-echo-target 172.31.40.30:10022 --bulk-path '${large_http_path}' --small-path '${small_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --udp-payload-bytes '${udp_payload_bytes}' --udp-timeout-ms '${udp_timeout_ms}' --tcp-echo-payload-bytes '${tcp_echo_payload_bytes}' --tcp-echo-timeout-ms '${tcp_echo_timeout_ms}' --tcp-echo-interval-ms '${tcp_echo_interval_ms}' --started-file '${started_file}' > /tmp/mptunnel-mixed-spike.out 2>/tmp/mptunnel-mixed-spike.err; echo \$? >/tmp/mptunnel-mixed-spike.status) & echo \$! >/tmp/mptunnel-mixed-spike.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_latency_spike_fat
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-mixed-spike.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-mixed-spike.status ]; then echo 124 >/tmp/mptunnel-mixed-spike.status; fi"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-mixed-spike.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-mixed-spike.status 2>/dev/null || echo 124")"
  append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 1
  apply_netem apply
}

run_failover_case() {
  local case_name="mptunnel_tcp_multipath_failover_blackhole_${failover_profile}"
  local output exit_code
  local telemetry_pid
  local started_file="/tmp/mptunnel-failover.started"
  local failover_marker_file="/tmp/mptunnel-failover.trigger"
  local probe_failover_after="$failover_after"
  if [[ "$failover_tx_trigger_bytes" != "0" ]]; then
    probe_failover_after="-1"
  fi
  start_client "$case_name" "$tcp_all"
  exec_in client "rm -f /tmp/mptunnel-failover.out /tmp/mptunnel-failover.status /tmp/mptunnel-failover.pid '${started_file}' '${failover_marker_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after '${probe_failover_after}' --failover-marker-file '${failover_marker_file}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-failover.out 2>/tmp/mptunnel-failover.err; echo \$? >/tmp/mptunnel-failover.status) & echo \$! >/tmp/mptunnel-failover.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  wait_for_tcp_failover_trigger "$case_name" "$failover_marker_file" server "$failover_server_address"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-failover.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-failover.status ]; then echo 124 >/tmp/mptunnel-failover.status; fi"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-failover.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-failover.status 2>/dev/null || echo 124")"
  if [[ "$exit_code" == "0" && -n "$output" ]]; then
    append_row_with_telemetry "$case_name" "$output" "" 1
  else
    local probe_stderr
    probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-failover.err 2>/dev/null | tail -c 4000 || true")"
    append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
  fi
  apply_netem apply
}

run_upload_failover_case() {
  local case_name="mptunnel_tcp_multipath_failover_blackhole_${failover_profile}_upload"
  local output exit_code
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  local started_file="/tmp/mptunnel-upload-failover.started"
  local failover_marker_file="/tmp/mptunnel-upload-failover.trigger"
  local probe_failover_after="$failover_after"
  if [[ "$failover_tx_trigger_bytes" != "0" ]]; then
    probe_failover_after="-1"
  fi
  start_client "$case_name" "$tcp_all"
  restart_target_tcp_sink
  exec_in client "rm -f /tmp/mptunnel-upload-failover.out /tmp/mptunnel-upload-failover.status /tmp/mptunnel-upload-failover.pid '${started_file}' '${failover_marker_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "(timeout ${upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:${tcp_upload_target_port} --failover-after '${probe_failover_after}' --failover-marker-file '${failover_marker_file}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-uploads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-upload-failover.out 2>/tmp/mptunnel-upload-failover.err; echo \$? >/tmp/mptunnel-upload-failover.status) & echo \$! >/tmp/mptunnel-upload-failover.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  wait_for_tcp_failover_trigger "$case_name" "$failover_marker_file" client "$failover_client_address"
  apply_failover_blackhole
  exec_in client "deadline=\$((SECONDS + ${upload_process_timeout_seconds} + 5)); while [ ! -f /tmp/mptunnel-upload-failover.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-upload-failover.status ]; then echo 124 >/tmp/mptunnel-upload-failover.status; fi"
  set +e
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  set -e
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-upload-failover.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-upload-failover.status 2>/dev/null || echo 124")"
  local probe_stderr
  probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-upload-failover.err 2>/dev/null | tail -c 4000 || true")"
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 1 "tcp-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code"
  apply_netem apply
}

run_latency_spike_case() {
  local case_name="mptunnel_tcp_multipath_latency_spike_fat"
  local output exit_code
  local telemetry_pid
  local started_file="/tmp/mptunnel-spike.started"
  start_client "$case_name" "$tcp_all"
  exec_in client "rm -f /tmp/mptunnel-spike.out /tmp/mptunnel-spike.status /tmp/mptunnel-spike.pid '${started_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  exec_in client "(timeout ${curl_timeout}s python3 /workspace/lab/failover_download_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:8080 --path '${large_http_path}' --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-downloads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-spike.out 2>/tmp/mptunnel-spike.err; echo \$? >/tmp/mptunnel-spike.status) & echo \$! >/tmp/mptunnel-spike.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_latency_spike_fat
  exec_in client "deadline=\$((SECONDS + ${curl_timeout} + 5)); while [ ! -f /tmp/mptunnel-spike.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-spike.status ]; then echo 124 >/tmp/mptunnel-spike.status; fi"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-spike.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-spike.status 2>/dev/null || echo 124")"
  if [[ "$exit_code" == "0" && -n "$output" ]]; then
    append_row_with_telemetry "$case_name" "$output" "" 1
  else
    local probe_stderr
    probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-spike.err 2>/dev/null | tail -c 4000 || true")"
    append_download_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr"
  fi
  apply_netem apply
}

run_upload_latency_spike_case() {
  local case_name="mptunnel_tcp_multipath_latency_spike_fat_upload"
  local output exit_code
  local telemetry_pid observer_started_ns observer_stopped_ns
  local observer_elapsed_seconds observer_freeze_exit_code
  local started_file="/tmp/mptunnel-upload-spike.started"
  start_client "$case_name" "$tcp_all"
  restart_target_tcp_sink
  exec_in client "rm -f /tmp/mptunnel-upload-spike.out /tmp/mptunnel-upload-spike.status /tmp/mptunnel-upload-spike.pid '${started_file}'"
  start_case_telemetry "$case_name"
  telemetry_pid="$case_telemetry_pid"
  observer_started_ns="$(monotonic_time_ns)"
  exec_in client "(timeout ${upload_process_timeout_seconds}s python3 /workspace/lab/bulk_upload_probe.py --label '${case_name}' --proxy 127.0.0.1:${proxy_port} --target 172.31.40.30:${tcp_upload_target_port} --failover-after '${failover_after}' --timeout '${curl_timeout}' --load-duration '${load_duration_seconds}' --parallel-uploads '${bulk_connections}' --started-file '${started_file}' > /tmp/mptunnel-upload-spike.out 2>/tmp/mptunnel-upload-spike.err; echo \$? >/tmp/mptunnel-upload-spike.status) & echo \$! >/tmp/mptunnel-upload-spike.pid"
  exec_in client "deadline=\$((SECONDS + 10)); while [ ! -f '${started_file}' ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.05; done; test -f '${started_file}'"
  sleep "$failover_after"
  apply_latency_spike_fat
  exec_in client "deadline=\$((SECONDS + ${upload_process_timeout_seconds} + 5)); while [ ! -f /tmp/mptunnel-upload-spike.status ] && [ \$SECONDS -lt \$deadline ]; do sleep 0.5; done; if [ ! -f /tmp/mptunnel-upload-spike.status ]; then echo 124 >/tmp/mptunnel-upload-spike.status; fi"
  set +e
  freeze_target_tcp_sink
  observer_freeze_exit_code="$?"
  set -e
  observer_stopped_ns="$(monotonic_time_ns)"
  observer_elapsed_seconds="$(elapsed_seconds_between_ns "$observer_started_ns" "$observer_stopped_ns")"
  stop_case_telemetry "$case_name" "$telemetry_pid"
  output="$(exec_in client "cat /tmp/mptunnel-upload-spike.out 2>/dev/null || true")"
  exit_code="$(exec_in client "cat /tmp/mptunnel-upload-spike.status 2>/dev/null || echo 124")"
  local probe_stderr
  probe_stderr="$(exec_in client "tail -n 80 /tmp/mptunnel-upload-spike.err 2>/dev/null | tail -c 4000 || true")"
  append_upload_probe_result "$case_name" "$exit_code" "$output" "$probe_stderr" 1 "tcp-upload" "$observer_elapsed_seconds" "$observer_freeze_exit_code"
  apply_netem apply
}

validate_client_runtime_case_filter
mkdir -p "$result_dir"
: > "$result_file"
: > "$result_dir/config-sha256.txt"

docker compose version >/dev/null
if [[ "$build_product" == "1" ]]; then
  build_mptunnel_binary
else
  require_prebuilt_product
fi
if [[ "$build_lab_images" == "1" ]]; then
  compose build
fi
compose_up
trap cleanup EXIT
prepare_client_runtime
capture_host_snapshot
refresh_result_reproducibility
write_run_manifest

start_target_services
apply_netem "$default_netem_mode"
start_server

tcp_carrier_max="${MPTUNNEL_LAB_TCP_CARRIER_MAX:-}"
tcp_carrier_query=""
tcp_carrier_hint_query=""
if [[ -n "$tcp_carrier_max" ]]; then
  if [[ ! "$tcp_carrier_max" =~ ^[1-9][0-9]*$ ]]; then
    echo "MPTUNNEL_LAB_TCP_CARRIER_MAX must be a positive integer" >&2
    exit 2
  fi
  tcp_carrier_query="?max-tcp-carriers=${tcp_carrier_max}"
  tcp_carrier_hint_query="&max-tcp-carriers=${tcp_carrier_max}"
fi

tcp_endpoint_lowlat="--path 'tcp://172.31.10.20:${server_port}${tcp_carrier_query}'"
tcp_endpoint_balanced="--path 'tcp://172.31.15.20:${server_port}${tcp_carrier_query}'"
tcp_endpoint_mildloss="--path 'tcp://172.31.16.20:${server_port}${tcp_carrier_query}'"
tcp_endpoint_fat="--path 'tcp://172.31.20.20:${server_port}${tcp_carrier_query}'"
tcp_endpoint_poor="--path 'tcp://172.31.30.20:${server_port}${tcp_carrier_query}'"
udp_endpoint_lowlat="--path 'quic://172.31.10.20:${server_port}'"
udp_endpoint_balanced="--path 'quic://172.31.15.20:${server_port}'"
udp_endpoint_mildloss="--path 'quic://172.31.16.20:${server_port}'"
udp_endpoint_fat="--path 'quic://172.31.20.20:${server_port}'"
udp_endpoint_poor="--path 'quic://172.31.30.20:${server_port}'"

if [[ "${MPTUNNEL_LAB_USE_PATH_HINTS:-0}" == "1" ]]; then
  tcp_lowlat="--path 'tcp://172.31.10.20:${server_port}?initial-srtt-s=0.02&initial-rate-mbps=80${tcp_carrier_hint_query}'"
  tcp_balanced="--path 'tcp://172.31.15.20:${server_port}?initial-srtt-s=0.08&initial-rate-mbps=200${tcp_carrier_hint_query}'"
  tcp_mildloss="--path 'tcp://172.31.16.20:${server_port}?initial-srtt-s=0.16&initial-rate-mbps=100${tcp_carrier_hint_query}'"
  tcp_fat="--path 'tcp://172.31.20.20:${server_port}?initial-srtt-s=0.18&initial-rate-mbps=500${tcp_carrier_hint_query}'"
  tcp_poor="--path 'tcp://172.31.30.20:${server_port}?initial-srtt-s=0.42&initial-rttvar-s=0.12&initial-rate-mbps=50&expensive=true${tcp_carrier_hint_query}'"
  udp_lowlat="--path 'quic://172.31.10.20:${server_port}?initial-srtt-s=0.02&initial-rate-mbps=80'"
  udp_balanced="--path 'quic://172.31.15.20:${server_port}?initial-srtt-s=0.08&initial-rate-mbps=200'"
  udp_mildloss="--path 'quic://172.31.16.20:${server_port}?initial-srtt-s=0.16&initial-rate-mbps=100'"
  udp_fat="--path 'quic://172.31.20.20:${server_port}?initial-srtt-s=0.18&initial-rate-mbps=500'"
  udp_poor="--path 'quic://172.31.30.20:${server_port}?initial-srtt-s=0.42&initial-rttvar-s=0.12&initial-rate-mbps=50&expensive=true'"
else
  tcp_lowlat="--path 'tcp://172.31.10.20:${server_port}${tcp_carrier_query}'"
  tcp_balanced="--path 'tcp://172.31.15.20:${server_port}${tcp_carrier_query}'"
  tcp_mildloss="--path 'tcp://172.31.16.20:${server_port}${tcp_carrier_query}'"
  tcp_fat="--path 'tcp://172.31.20.20:${server_port}${tcp_carrier_query}'"
  tcp_poor="--path 'tcp://172.31.30.20:${server_port}${tcp_carrier_query}'"
  udp_lowlat="--path 'quic://172.31.10.20:${server_port}'"
  udp_balanced="--path 'quic://172.31.15.20:${server_port}'"
  udp_mildloss="--path 'quic://172.31.16.20:${server_port}'"
  udp_fat="--path 'quic://172.31.20.20:${server_port}'"
  udp_poor="--path 'quic://172.31.30.20:${server_port}'"
fi
tcp_all="${tcp_lowlat} ${tcp_balanced} ${tcp_mildloss} ${tcp_fat} ${tcp_poor}"
udp_all="${udp_lowlat} ${udp_balanced} ${udp_mildloss} ${udp_fat} ${udp_poor}"
tcp_equal_all="${tcp_endpoint_lowlat} ${tcp_endpoint_balanced} ${tcp_endpoint_mildloss} ${tcp_endpoint_fat} ${tcp_endpoint_poor}"
udp_equal_all="${udp_endpoint_lowlat} ${udp_endpoint_balanced} ${udp_endpoint_mildloss} ${udp_endpoint_fat} ${udp_endpoint_poor}"
mixed_equal_all="${tcp_endpoint_lowlat} ${tcp_endpoint_balanced} ${udp_endpoint_mildloss} ${udp_endpoint_fat} ${udp_endpoint_poor}"
scale_tcp_carrier_max="${MPTUNNEL_LAB_SCALE_TCP_CARRIER_MAX:-$tcp_carrier_max}"
scale_tcp_carrier_query=""
if [[ -n "$scale_tcp_carrier_max" ]]; then
  if [[ ! "$scale_tcp_carrier_max" =~ ^[1-9][0-9]*$ ]]; then
    echo "MPTUNNEL_LAB_SCALE_TCP_CARRIER_MAX must be a positive integer" >&2
    exit 2
  fi
  scale_tcp_carrier_query="?max-tcp-carriers=${scale_tcp_carrier_max}"
fi
tcp_scale_all="--path 'tcp://172.31.10.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.15.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.16.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.20.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.30.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.41.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.42.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.43.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.44.20:${server_port}${scale_tcp_carrier_query}' \
--path 'tcp://172.31.45.20:${server_port}${scale_tcp_carrier_query}'"
udp_scale_all="--path 'quic://172.31.51.20:${server_port}' \
--path 'quic://172.31.52.20:${server_port}' \
--path 'quic://172.31.53.20:${server_port}' \
--path 'quic://172.31.54.20:${server_port}' \
--path 'quic://172.31.55.20:${server_port}' \
--path 'quic://172.31.56.20:${server_port}' \
--path 'quic://172.31.57.20:${server_port}' \
--path 'quic://172.31.58.20:${server_port}' \
--path 'quic://172.31.59.20:${server_port}' \
--path 'quic://172.31.60.20:${server_port}'"
mixed_scale_all="${tcp_scale_all} ${udp_scale_all}"

if should_run_case "direct_low_latency"; then
  run_direct_download_case "direct_low_latency" "172.31.10.30:8080"
fi
if should_run_case "direct_balanced"; then
  run_direct_download_case "direct_balanced" "172.31.15.30:8080"
fi
if should_run_case "direct_cross_continent_high_bandwidth"; then
  run_direct_download_case "direct_cross_continent_high_bandwidth" "172.31.20.30:8080"
fi
if should_run_case "direct_poor_internet"; then
  run_direct_download_case "direct_poor_internet" "172.31.30.30:8080"
fi
if should_run_case "direct_upload_low_latency"; then
  run_direct_upload_case "direct_upload_low_latency" "172.31.10.30:${tcp_upload_target_port}"
fi
if should_run_case "direct_upload_balanced"; then
  run_direct_upload_case "direct_upload_balanced" "172.31.15.30:${tcp_upload_target_port}"
fi
if should_run_case "direct_upload_cross_continent_high_bandwidth"; then
  run_direct_upload_case "direct_upload_cross_continent_high_bandwidth" "172.31.20.30:${tcp_upload_target_port}"
fi
if should_run_case "direct_upload_poor_internet"; then
  run_direct_upload_case "direct_upload_poor_internet" "172.31.30.30:${tcp_upload_target_port}"
fi
if should_run_case "direct_unconstrained"; then
  run_direct_unconstrained_download_case "direct_unconstrained" "172.31.10.30:8080"
fi
if should_run_case "direct_upload_unconstrained"; then
  run_direct_unconstrained_upload_case "direct_upload_unconstrained" "172.31.10.30:${tcp_upload_target_port}"
fi

if should_run_case "direct_mixed_low_latency"; then
  run_direct_mixed_case "direct_mixed_low_latency" "172.31.10.30"
fi
if should_run_case "direct_mixed_balanced"; then
  run_direct_mixed_case "direct_mixed_balanced" "172.31.15.30"
fi
if should_run_case "baseline_raw_tcp_bulk_interactive_balanced"; then
  run_raw_tcp_bulk_interactive_case
fi
if should_run_case "direct_mixed_cross_continent_high_bandwidth"; then
  run_direct_mixed_case "direct_mixed_cross_continent_high_bandwidth" "172.31.20.30"
fi
if should_run_case "direct_mixed_poor_internet"; then
  run_direct_mixed_case "direct_mixed_poor_internet" "172.31.30.30"
fi
if should_run_case "direct_mixed_unconstrained"; then
  run_direct_mixed_case "direct_mixed_unconstrained" "172.31.10.30" unconstrained
  apply_netem apply
fi

if should_run_case "mptunnel_client_direct_balanced"; then
  run_client_direct_download_case \
    "mptunnel_client_direct_balanced" "$tcp_all $udp_all"
fi
if should_run_case "mptunnel_client_direct_balanced_upload"; then
  run_client_direct_upload_case \
    "mptunnel_client_direct_balanced_upload" "$tcp_all $udp_all"
fi

if should_run_case "baseline_vmess_tcp_single_balanced"; then
  run_vmess_baseline_case "baseline_vmess_tcp_single_balanced" "172.31.15.20"
fi

if should_run_case "baseline_vmess_tcp_bulk_interactive_balanced"; then
  run_vmess_baseline_case \
    "baseline_vmess_tcp_bulk_interactive_balanced" \
    "172.31.15.20" \
    "$default_netem_mode" \
    bulk-interactive
fi

if should_run_case "baseline_vmess_tcp_single_cross_continent_high_bandwidth"; then
  run_vmess_baseline_case "baseline_vmess_tcp_single_cross_continent_high_bandwidth" "172.31.20.20"
fi

if should_run_case "baseline_vmess_tcp_single_balanced_upload"; then
  run_vmess_baseline_upload_case "baseline_vmess_tcp_single_balanced_upload" "172.31.15.20"
fi

if should_run_case "baseline_vmess_tcp_single_cross_continent_high_bandwidth_upload"; then
  run_vmess_baseline_upload_case "baseline_vmess_tcp_single_cross_continent_high_bandwidth_upload" "172.31.20.20"
fi
if should_run_case "baseline_vmess_tcp_single_unconstrained"; then
  run_vmess_baseline_case "baseline_vmess_tcp_single_unconstrained" "172.31.10.20" unconstrained
fi
if should_run_case "baseline_vmess_tcp_single_unconstrained_upload"; then
  run_vmess_baseline_upload_case "baseline_vmess_tcp_single_unconstrained_upload" "172.31.10.20" unconstrained
fi

if should_run_case "baseline_hysteria2_udp_single_balanced"; then
  hysteria_up_rate="$(hysteria_bandwidth_from_netem_rate \
    "${MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE:-${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}}")"
  hysteria_down_rate="$(hysteria_bandwidth_from_netem_rate \
    "${MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE:-${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}}")"
  run_hysteria2_baseline_case \
    "baseline_hysteria2_udp_single_balanced" \
    "172.31.15.20" \
    "$default_netem_mode" \
    "$hysteria_up_rate" \
    "$hysteria_down_rate"
fi

if should_run_case "baseline_hysteria2_udp_bulk_interactive_balanced"; then
  hysteria_up_rate="$(hysteria_bandwidth_from_netem_rate \
    "$bulk_interactive_rate")"
  hysteria_down_rate="$(hysteria_bandwidth_from_netem_rate \
    "$bulk_interactive_rate")"
  run_hysteria2_baseline_case \
    "baseline_hysteria2_udp_bulk_interactive_balanced" \
    "172.31.15.20" \
    "$default_netem_mode" \
    "$hysteria_up_rate" \
    "$hysteria_down_rate" \
    bulk-interactive
fi

if should_run_case "baseline_hysteria2_udp_single_balanced_autonomous"; then
  run_hysteria2_baseline_case \
    "baseline_hysteria2_udp_single_balanced_autonomous" \
    "172.31.15.20" \
    "$default_netem_mode"
fi

if should_run_case "baseline_hysteria2_udp_single_cross_continent_high_bandwidth"; then
  hysteria_rate="$(hysteria_bandwidth_from_netem_rate "${MPTUNNEL_LAB_FAT_RATE:-500mbit}")"
  run_hysteria2_baseline_case "baseline_hysteria2_udp_single_cross_continent_high_bandwidth" "172.31.20.20" apply "$hysteria_rate" "$hysteria_rate"
fi

if should_run_case "baseline_hysteria2_udp_single_balanced_upload"; then
  hysteria_up_rate="$(hysteria_bandwidth_from_netem_rate \
    "${MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE:-${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}}")"
  hysteria_down_rate="$(hysteria_bandwidth_from_netem_rate \
    "${MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE:-${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}}")"
  run_hysteria2_baseline_upload_case \
    "baseline_hysteria2_udp_single_balanced_upload" \
    "172.31.15.20" \
    "$default_netem_mode" \
    "$hysteria_up_rate" \
    "$hysteria_down_rate"
fi

if should_run_case "baseline_hysteria2_udp_single_cross_continent_high_bandwidth_upload"; then
  hysteria_rate="$(hysteria_bandwidth_from_netem_rate "${MPTUNNEL_LAB_FAT_RATE:-500mbit}")"
  run_hysteria2_baseline_upload_case "baseline_hysteria2_udp_single_cross_continent_high_bandwidth_upload" "172.31.20.20" apply "$hysteria_rate" "$hysteria_rate"
fi
if should_run_case "baseline_hysteria2_udp_single_unconstrained"; then
  run_hysteria2_baseline_case "baseline_hysteria2_udp_single_unconstrained" "172.31.10.20" unconstrained
fi
if should_run_case "baseline_hysteria2_udp_single_unconstrained_upload"; then
  run_hysteria2_baseline_upload_case "baseline_hysteria2_udp_single_unconstrained_upload" "172.31.10.20" unconstrained
fi

if should_run_case "baseline_vmess_tcp_single_asymmetric_download_reference"; then
  run_vmess_baseline_case \
    "baseline_vmess_tcp_single_asymmetric_download_reference" \
    "172.31.10.20" \
    asymmetric
fi
if should_run_case "baseline_hysteria2_udp_single_asymmetric_download_reference"; then
  run_hysteria2_baseline_case \
    "baseline_hysteria2_udp_single_asymmetric_download_reference" \
    "172.31.10.20" \
    asymmetric \
    "20 mbps" \
    "200 mbps"
fi
if should_run_case "baseline_vmess_tcp_single_asymmetric_upload_reference"; then
  run_vmess_baseline_upload_case \
    "baseline_vmess_tcp_single_asymmetric_upload_reference" \
    "172.31.10.20" \
    asymmetric
fi
if should_run_case "baseline_hysteria2_udp_single_asymmetric_upload_reference"; then
  run_hysteria2_baseline_upload_case \
    "baseline_hysteria2_udp_single_asymmetric_upload_reference" \
    "172.31.10.20" \
    asymmetric \
    "20 mbps" \
    "200 mbps"
fi

if should_run_case "baseline_mptcp_tcp_multipath_all"; then
  run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_all"
fi
if should_run_case "baseline_mptcp_tcp_multipath_all_upload"; then
  run_mptcp_baseline_upload_case "baseline_mptcp_tcp_multipath_all_upload"
fi
if should_run_case "baseline_mptcp_tcp_multipath_unconstrained"; then
  run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_unconstrained" unconstrained
fi
if should_run_case "baseline_mptcp_tcp_multipath_equal_fat"; then
  run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_equal_fat" ideal-all-fat
fi
if should_run_case "baseline_mptcp_tcp_multipath_equal_fat_upload"; then
  run_mptcp_baseline_upload_case "baseline_mptcp_tcp_multipath_equal_fat_upload" ideal-all-fat
fi

if should_run_case "mptunnel_tcp_single_asymmetric_download_reference"; then
  run_asymmetric_download_case \
    "mptunnel_tcp_single_asymmetric_download_reference" \
    "$tcp_endpoint_lowlat"
fi
if should_run_case "mptunnel_tcp_multipath_asymmetric_download"; then
  run_asymmetric_download_case \
    "mptunnel_tcp_multipath_asymmetric_download" \
    "$tcp_endpoint_lowlat $tcp_endpoint_balanced"
fi
if should_run_case "mptunnel_tcp_single_asymmetric_upload_reference"; then
  run_asymmetric_upload_case \
    "mptunnel_tcp_single_asymmetric_upload_reference" \
    "$tcp_endpoint_balanced"
fi
if should_run_case "mptunnel_tcp_multipath_asymmetric_upload"; then
  run_asymmetric_upload_case \
    "mptunnel_tcp_multipath_asymmetric_upload" \
    "$tcp_endpoint_lowlat $tcp_endpoint_balanced"
fi
if should_run_case "mptunnel_browser_concurrent_batches"; then
  run_browser_batch_case
fi
if should_run_case "mptunnel_browser_full_load"; then
  run_browser_full_load_case
fi
for scale_rate_band in "${scale_rate_bands[@]}"; do
  scale_rate_band_label="${scale_rate_band//-/_}"
  scale_download_case="mptunnel_twenty_varying_links_${scale_rate_band_label}_download"
  scale_upload_case="mptunnel_twenty_varying_links_${scale_rate_band_label}_upload"
  if should_run_case "$scale_download_case"; then
    run_varying_links_download_case "$scale_rate_band"
  fi
  if should_run_case "$scale_upload_case"; then
    run_varying_links_upload_case "$scale_rate_band"
  fi
done

if should_run_case "mptunnel_tcp_single_low_latency"; then
  start_client "tcp_single_low_latency" "$tcp_lowlat"
  run_tcp_download_probe_case "mptunnel_tcp_single_low_latency"
fi

if should_run_case "mptunnel_tcp_single_balanced"; then
  start_client "tcp_single_balanced" "$tcp_balanced"
  run_tcp_download_probe_case "mptunnel_tcp_single_balanced"
fi

if should_run_case "mptunnel_tcp_bulk_interactive_balanced"; then
  start_client "tcp_bulk_interactive_balanced" "$tcp_balanced"
  run_bulk_interactive_probe_case \
    "mptunnel_tcp_bulk_interactive_balanced" "$proxy_port"
fi

if should_run_case "mptunnel_quic_bulk_interactive_balanced"; then
  start_client "quic_bulk_interactive_balanced" "$udp_balanced"
  run_bulk_interactive_probe_case \
    "mptunnel_quic_bulk_interactive_balanced" "$proxy_port"
fi

if should_run_case "mptunnel_tcp_quic_bulk_interactive_balanced"; then
  start_client \
    "tcp_quic_bulk_interactive_balanced" "$tcp_balanced $udp_balanced"
  run_bulk_interactive_probe_case \
    "mptunnel_tcp_quic_bulk_interactive_balanced" "$proxy_port"
fi

if should_run_case "mptunnel_tcp_single_cross_continent_high_bandwidth"; then
  start_client "tcp_single_cross_continent_high_bandwidth" "$tcp_fat"
  run_tcp_download_probe_case "mptunnel_tcp_single_cross_continent_high_bandwidth"
fi

if should_run_case "mptunnel_tcp_single_poor_internet"; then
  start_client "tcp_single_poor_internet" "$tcp_poor"
  run_tcp_download_probe_case "mptunnel_tcp_single_poor_internet"
fi
if should_run_case "mptunnel_tcp_single_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_tcp_single_unconstrained" "unconstrained" "$tcp_endpoint_lowlat"
fi
if should_run_case "mptunnel_tcp_single_unconstrained_range_1_1"; then
  run_reliable_ideal_download_case \
    "mptunnel_tcp_single_unconstrained_range_1_1" \
    "unconstrained" \
    "--path 'tcp://172.31.10.20:${server_port}?max-tcp-carriers=1'"
fi

if should_run_case "mptunnel_tcp_multipath_all"; then
  start_client "tcp_multipath_all" "$tcp_all"
  run_tcp_download_probe_case "mptunnel_tcp_multipath_all"
fi
if should_run_case "mptunnel_tcp_multipath_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_tcp_multipath_unconstrained" "unconstrained" "$tcp_equal_all"
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
if should_run_case "mptunnel_udp_stream_single_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_udp_stream_single_unconstrained" "unconstrained" "$udp_endpoint_lowlat"
fi
if should_run_case "mptunnel_udp_stream_single_unconstrained_port_hopping"; then
  run_quic_port_hop_download_case
fi

if should_run_case "mptunnel_udp_stream_multipath_all"; then
  start_client "udp_stream_multipath_all" "$udp_all"
  run_tcp_download_probe_case "mptunnel_udp_stream_multipath_all"
fi
if should_run_case "mptunnel_udp_stream_multipath_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_udp_stream_multipath_unconstrained" "unconstrained" "$udp_equal_all"
fi

if should_run_case "mptunnel_reliable_mixed_single_low_latency"; then
  start_client "reliable_mixed_single_low_latency" "$tcp_lowlat $udp_lowlat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_single_low_latency"
fi

if should_run_case "mptunnel_reliable_mixed_single_balanced"; then
  start_client "reliable_mixed_single_balanced" "$tcp_balanced $udp_balanced"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_single_balanced"
fi
if should_run_case "mptunnel_reliable_mixed_single_cross_continent_high_bandwidth"; then
  start_client "reliable_mixed_single_cross_continent_high_bandwidth" "$tcp_fat $udp_fat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_single_cross_continent_high_bandwidth"
fi
if should_run_case "mptunnel_reliable_mixed_single_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_reliable_mixed_single_unconstrained" "unconstrained" "$tcp_endpoint_lowlat $udp_endpoint_lowlat"
fi
if should_run_case "mptunnel_reliable_mixed_single_equal_fat"; then
  run_reliable_ideal_download_case "mptunnel_reliable_mixed_single_equal_fat" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"
fi
if should_run_case "mptunnel_reliable_mixed_two_links_equal_fat"; then
  run_reliable_ideal_download_case \
    "mptunnel_reliable_mixed_two_links_equal_fat" \
    "fat" \
    "$tcp_endpoint_lowlat $udp_endpoint_lowlat $tcp_endpoint_fat $udp_endpoint_fat"
fi

if should_run_case "mptunnel_reliable_mixed_multipath_all"; then
  start_client "reliable_mixed_multipath_all" "$tcp_all $udp_all"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_multipath_all"
fi
if should_run_case "mptunnel_reliable_mixed_multipath_unconstrained"; then
  run_reliable_ideal_download_case "mptunnel_reliable_mixed_multipath_unconstrained" "unconstrained" "$tcp_equal_all $udp_equal_all"
fi
if should_run_case "mptunnel_reliable_mixed_paired_multipath_equal_fat"; then
  run_reliable_ideal_download_case "mptunnel_reliable_mixed_paired_multipath_equal_fat" "fat" "$tcp_equal_all $udp_equal_all"
fi

for equal_profile in lowlat balanced fat unconstrained; do
  if should_run_case "mptunnel_tcp_multipath_equal_${equal_profile}"; then
    run_reliable_ideal_download_case "mptunnel_tcp_multipath_equal_${equal_profile}" "$equal_profile" "$tcp_equal_all"
  fi
  if should_run_case "mptunnel_udp_stream_multipath_equal_${equal_profile}"; then
    run_reliable_ideal_download_case "mptunnel_udp_stream_multipath_equal_${equal_profile}" "$equal_profile" "$udp_equal_all"
  fi
  if should_run_case "mptunnel_reliable_mixed_multipath_equal_${equal_profile}"; then
    run_reliable_ideal_download_case "mptunnel_reliable_mixed_multipath_equal_${equal_profile}" "$equal_profile" "$mixed_equal_all"
  fi
done

if should_run_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat"; then
  start_client "reliable_mixed_tcp_lowlat_udp_fat" "$tcp_lowlat $udp_fat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat"
fi

if should_run_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat"; then
  start_client "reliable_mixed_tcp_fat_udp_lowlat" "$tcp_fat $udp_lowlat"
  run_tcp_download_probe_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat"
fi

if should_run_case "mptunnel_tcp_single_low_latency_upload"; then
  start_client "tcp_single_low_latency_upload" "$tcp_lowlat"
  run_tcp_upload_probe_case "mptunnel_tcp_single_low_latency_upload"
fi

if should_run_case "mptunnel_tcp_single_balanced_upload"; then
  start_client "tcp_single_balanced_upload" "$tcp_balanced"
  run_tcp_upload_probe_case "mptunnel_tcp_single_balanced_upload"
fi

if should_run_case "mptunnel_tcp_single_cross_continent_high_bandwidth_upload"; then
  start_client "tcp_single_cross_continent_high_bandwidth_upload" "$tcp_fat"
  run_tcp_upload_probe_case "mptunnel_tcp_single_cross_continent_high_bandwidth_upload"
fi

if should_run_case "mptunnel_tcp_single_poor_internet_upload"; then
  start_client "tcp_single_poor_internet_upload" "$tcp_poor"
  run_tcp_upload_probe_case "mptunnel_tcp_single_poor_internet_upload"
fi
if should_run_case "mptunnel_tcp_single_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_tcp_single_unconstrained_upload" "unconstrained" "$tcp_endpoint_lowlat"
fi
if should_run_case "mptunnel_tcp_single_unconstrained_range_1_1_upload"; then
  run_reliable_ideal_upload_case \
    "mptunnel_tcp_single_unconstrained_range_1_1_upload" \
    "unconstrained" \
    "--path 'tcp://172.31.10.20:${server_port}?max-tcp-carriers=1'"
fi
if should_run_case "mptunnel_tcp_single_unconstrained_range_1_3_upload"; then
  run_reliable_ideal_upload_case \
    "mptunnel_tcp_single_unconstrained_range_1_3_upload" \
    "unconstrained" \
    "--path 'tcp://172.31.10.20:${server_port}?max-tcp-carriers=3'"
fi

if should_run_case "mptunnel_tcp_multipath_all_upload"; then
  start_client "tcp_multipath_all_upload" "$tcp_all"
  run_tcp_upload_probe_case "mptunnel_tcp_multipath_all_upload"
fi
if should_run_case "mptunnel_tcp_multipath_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_tcp_multipath_unconstrained_upload" "unconstrained" "$tcp_equal_all"
fi

if should_run_case "mptunnel_udp_stream_single_low_latency_upload"; then
  start_client "udp_stream_single_low_latency_upload" "$udp_lowlat"
  run_tcp_upload_probe_case "mptunnel_udp_stream_single_low_latency_upload"
fi

if should_run_case "mptunnel_udp_stream_single_balanced_upload"; then
  start_client "udp_stream_single_balanced_upload" "$udp_balanced"
  run_tcp_upload_probe_case "mptunnel_udp_stream_single_balanced_upload"
fi

if should_run_case "mptunnel_udp_stream_single_cross_continent_high_bandwidth_upload"; then
  start_client "udp_stream_single_cross_continent_high_bandwidth_upload" "$udp_fat"
  run_tcp_upload_probe_case "mptunnel_udp_stream_single_cross_continent_high_bandwidth_upload"
fi
if should_run_case "mptunnel_udp_stream_single_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_udp_stream_single_unconstrained_upload" "unconstrained" "$udp_endpoint_lowlat"
fi
if should_run_case "mptunnel_udp_stream_single_unconstrained_port_hopping_upload"; then
  run_quic_port_hop_upload_case
fi

if should_run_case "mptunnel_udp_stream_multipath_all_upload"; then
  start_client "udp_stream_multipath_all_upload" "$udp_all"
  run_tcp_upload_probe_case "mptunnel_udp_stream_multipath_all_upload"
fi
if should_run_case "mptunnel_udp_stream_multipath_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_udp_stream_multipath_unconstrained_upload" "unconstrained" "$udp_equal_all"
fi

if should_run_case "mptunnel_reliable_mixed_single_low_latency_upload"; then
  start_client "reliable_mixed_single_low_latency_upload" "$tcp_lowlat $udp_lowlat"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_single_low_latency_upload"
fi

if should_run_case "mptunnel_reliable_mixed_single_balanced_upload"; then
  start_client "reliable_mixed_single_balanced_upload" "$tcp_balanced $udp_balanced"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_single_balanced_upload"
fi
if should_run_case "mptunnel_reliable_mixed_single_cross_continent_high_bandwidth_upload"; then
  start_client "reliable_mixed_single_cross_continent_high_bandwidth_upload" "$tcp_fat $udp_fat"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_single_cross_continent_high_bandwidth_upload"
fi
if should_run_case "mptunnel_reliable_mixed_single_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_reliable_mixed_single_unconstrained_upload" "unconstrained" "$tcp_endpoint_lowlat $udp_endpoint_lowlat"
fi
if should_run_case "mptunnel_reliable_mixed_single_equal_fat_upload"; then
  run_reliable_ideal_upload_case "mptunnel_reliable_mixed_single_equal_fat_upload" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"
fi
if should_run_case "mptunnel_reliable_mixed_two_links_equal_fat_upload"; then
  run_reliable_ideal_upload_case \
    "mptunnel_reliable_mixed_two_links_equal_fat_upload" \
    "fat" \
    "$tcp_endpoint_lowlat $udp_endpoint_lowlat $tcp_endpoint_fat $udp_endpoint_fat"
fi
if should_run_case "mptunnel_reliable_mixed_family_contention_equal_fat_upload"; then
  # UDP-first ordering makes one QUIC Service compete with one TCP Service;
  # the second TCP endpoint exists only to expose cross-family admission bugs.
  run_reliable_ideal_upload_case "mptunnel_reliable_mixed_family_contention_equal_fat_upload" "fat" "$udp_endpoint_fat $tcp_endpoint_lowlat $tcp_endpoint_balanced"
fi

if should_run_case "mptunnel_reliable_mixed_multipath_all_upload"; then
  start_client "reliable_mixed_multipath_all_upload" "$tcp_all $udp_all"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_multipath_all_upload"
fi
if should_run_case "mptunnel_reliable_mixed_multipath_unconstrained_upload"; then
  run_reliable_ideal_upload_case "mptunnel_reliable_mixed_multipath_unconstrained_upload" "unconstrained" "$tcp_equal_all $udp_equal_all"
fi
if should_run_case "mptunnel_reliable_mixed_paired_multipath_equal_fat_upload"; then
  run_reliable_ideal_upload_case "mptunnel_reliable_mixed_paired_multipath_equal_fat_upload" "fat" "$tcp_equal_all $udp_equal_all"
fi

for equal_profile in lowlat balanced fat unconstrained; do
  if should_run_case "mptunnel_tcp_multipath_equal_${equal_profile}_upload"; then
    run_reliable_ideal_upload_case "mptunnel_tcp_multipath_equal_${equal_profile}_upload" "$equal_profile" "$tcp_equal_all"
  fi
  if should_run_case "mptunnel_udp_stream_multipath_equal_${equal_profile}_upload"; then
    run_reliable_ideal_upload_case "mptunnel_udp_stream_multipath_equal_${equal_profile}_upload" "$equal_profile" "$udp_equal_all"
  fi
  if should_run_case "mptunnel_reliable_mixed_multipath_equal_${equal_profile}_upload"; then
    run_reliable_ideal_upload_case "mptunnel_reliable_mixed_multipath_equal_${equal_profile}_upload" "$equal_profile" "$mixed_equal_all"
  fi
done

if should_run_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat_upload"; then
  start_client "reliable_mixed_tcp_lowlat_udp_fat_upload" "$tcp_lowlat $udp_fat"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_tcp_lowlat_udp_fat_upload"
fi

if should_run_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat_upload"; then
  start_client "reliable_mixed_tcp_fat_udp_lowlat_upload" "$tcp_fat $udp_lowlat"
  run_tcp_upload_probe_case "mptunnel_reliable_mixed_tcp_fat_udp_lowlat_upload"
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

if should_run_case "mptunnel_tun_tcp_single_low_latency_upload"; then
  run_tun_upload_case "mptunnel_tun_tcp_single_low_latency_upload" "$tcp_lowlat"
fi

if should_run_case "mptunnel_tun_tcp_single_balanced_upload"; then
  run_tun_upload_case "mptunnel_tun_tcp_single_balanced_upload" "$tcp_balanced"
fi

if should_run_case "mptunnel_tun_udp_stream_single_low_latency_upload"; then
  run_tun_upload_case "mptunnel_tun_udp_stream_single_low_latency_upload" "$udp_lowlat"
fi

if should_run_case "mptunnel_tun_udp_stream_single_balanced_upload"; then
  run_tun_upload_case "mptunnel_tun_udp_stream_single_balanced_upload" "$udp_balanced"
fi

if should_run_case "mptunnel_tun_mixed_multipath_all_upload"; then
  run_tun_upload_case "mptunnel_tun_mixed_multipath_all_upload" "$tcp_all $udp_all"
fi

if should_run_case "mptunnel_tun_app_bypass_balanced"; then
  run_tun_bypass_download_case \
    "mptunnel_tun_app_bypass_balanced" "$tcp_all $udp_all"
fi

if should_run_case "mptunnel_tun_app_bypass_balanced_upload"; then
  run_tun_bypass_upload_case \
    "mptunnel_tun_app_bypass_balanced_upload" "$tcp_all $udp_all"
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
if should_run_case "mptunnel_mixed_single_cross_continent_high_bandwidth"; then
  run_mixed_case \
    "mptunnel_mixed_single_cross_continent_high_bandwidth" \
    "$tcp_fat $udp_fat"
fi
if should_run_case "mptunnel_mixed_two_links_lowlat_fat"; then
  run_mixed_case \
    "mptunnel_mixed_two_links_lowlat_fat" \
    "$tcp_lowlat $udp_lowlat $tcp_fat $udp_fat"
fi
if should_run_case "mptunnel_mixed_single_unconstrained"; then
  run_mixed_ideal_case "mptunnel_mixed_single_unconstrained" "unconstrained" "$tcp_endpoint_lowlat $udp_endpoint_lowlat"
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
if should_run_case "mptunnel_mixed_multipath_unconstrained"; then
  run_mixed_ideal_case "mptunnel_mixed_multipath_unconstrained" "unconstrained" "$tcp_equal_all $udp_equal_all"
fi
for equal_profile in lowlat balanced fat unconstrained; do
  if should_run_case "mptunnel_mixed_tcp_multipath_equal_${equal_profile}"; then
    run_mixed_ideal_case "mptunnel_mixed_tcp_multipath_equal_${equal_profile}" "$equal_profile" "$tcp_equal_all"
  fi
  if should_run_case "mptunnel_mixed_udp_multipath_equal_${equal_profile}"; then
    run_mixed_ideal_case "mptunnel_mixed_udp_multipath_equal_${equal_profile}" "$equal_profile" "$udp_equal_all"
  fi
  if should_run_case "mptunnel_mixed_multipath_equal_${equal_profile}"; then
    run_mixed_ideal_case "mptunnel_mixed_multipath_equal_${equal_profile}" "$equal_profile" "$mixed_equal_all"
  fi
done
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
if should_run_case "mptunnel_mixed_multipath_failover_blackhole_${failover_profile}"; then
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
  matrix_upload_case="${matrix_case}_upload"
  if should_run_case "$matrix_upload_case"; then
    run_matrix_upload_case "$matrix_bits"
  fi
done

if flag_enabled "$tcp_carrier_qos_cohort"; then
  for qos_profile in \
    "per_flow_qos:tcp-per-flow-qos" \
    "shared_bottleneck:tcp-shared-bottleneck" \
    "unconstrained:unconstrained"; do
    IFS=':' read -r qos_regime qos_netem_mode <<< "$qos_profile"
    for qos_direction in download upload; do
      for qos_topology in range_1_1 range_1_3 range_3_3 three_endpoints_1_1; do
        qos_case_name="mptunnel_tcp_carrier_qos_${qos_regime}_${qos_topology}_${qos_direction}"
        if should_run_case "$qos_case_name"; then
          run_tcp_carrier_qos_case \
            "$qos_regime" \
            "$qos_netem_mode" \
            "$qos_topology" \
            "$qos_direction"
        fi
      done
    done
  done
fi

if should_run_case "mptunnel_tcp_multipath_failover_blackhole_${failover_profile}"; then
  run_failover_case
fi
if should_run_case "mptunnel_tcp_multipath_failover_blackhole_${failover_profile}_upload"; then
  run_upload_failover_case
fi
if should_run_case "mptunnel_tcp_multipath_latency_spike_fat"; then
  run_latency_spike_case
fi
if should_run_case "mptunnel_tcp_multipath_latency_spike_fat_upload"; then
  run_upload_latency_spike_case
fi

echo "$result_file"
FAIL_ON_BAD_STATUS="$fail_on_bad_status" python3 - "$result_file" <<'PY'
import json
import os
import sys

fail_on_bad_status = os.environ.get("FAIL_ON_BAD_STATUS", "1").lower() not in {
    "0",
    "false",
    "no",
}
failed = []
with open(sys.argv[1], "r", encoding="utf-8") as handle:
    for line_number, line in enumerate(handle, 1):
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("status") not in {"ok", "loss", "skipped"}:
            failed.append((line_number, row.get("case", "unknown"), row.get("status")))

if failed:
    for line_number, case, status in failed:
        print(
            f"failed lab result row {line_number}: {case} status={status}",
            file=sys.stderr,
        )
if failed and fail_on_bad_status:
    sys.exit(1)
PY

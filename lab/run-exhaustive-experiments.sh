#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"

profile="${EXPERIMENT_PROFILE:-standard}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_root="${RESULT_ROOT:-lab/results/exhaustive-${timestamp}}"

case "$profile" in
  smoke)
    default_load_duration="10"
    default_bulk_connections="1"
    default_failover_after="2"
    default_repeats="1"
    default_curl_timeout="90"
    ;;
  standard)
    default_load_duration="30 60"
    default_bulk_connections="1 2"
    default_failover_after="2 5"
    default_repeats="1"
    default_curl_timeout="240"
    ;;
  exhaustive)
    default_load_duration="30 60 120"
    default_bulk_connections="1 2 4"
    default_failover_after="2 5"
    default_repeats="2"
    default_curl_timeout="480"
    ;;
  custom)
    default_load_duration="${LOAD_DURATION_MATRIX:?LOAD_DURATION_MATRIX is required for custom profile}"
    default_bulk_connections="${BULK_CONNECTIONS_MATRIX:?BULK_CONNECTIONS_MATRIX is required for custom profile}"
    default_failover_after="${FAILOVER_AFTER_MATRIX:?FAILOVER_AFTER_MATRIX is required for custom profile}"
    default_repeats="${REPEATS:?REPEATS is required for custom profile}"
    default_curl_timeout="${CURL_TIMEOUT_SECONDS:-240}"
    ;;
  *)
    echo "unknown EXPERIMENT_PROFILE: $profile" >&2
    echo "valid profiles: smoke, standard, exhaustive, custom" >&2
    exit 2
    ;;
esac

load_duration_matrix="${LOAD_DURATION_MATRIX:-$default_load_duration}"
bulk_connections_matrix="${BULK_CONNECTIONS_MATRIX:-$default_bulk_connections}"
failover_after_matrix="${FAILOVER_AFTER_MATRIX:-$default_failover_after}"
repeats="${REPEATS:-$default_repeats}"
curl_timeout="${CURL_TIMEOUT_SECONDS:-$default_curl_timeout}"
object_mib="${MPTUNNEL_LAB_OBJECT_MIB:-1024}"
udp_payload_bytes="${UDP_PAYLOAD_BYTES:-512}"
udp_timeout_ms="${UDP_TIMEOUT_MS:-2500}"
case_filter="${CASE_FILTER:-}"

mkdir -p "$result_root"

manifest_file="${result_root}/manifest.jsonl"
: > "$manifest_file"

read -r -a load_durations <<< "$load_duration_matrix"
read -r -a bulk_connections_values <<< "$bulk_connections_matrix"
read -r -a failover_afters <<< "$failover_after_matrix"

first_run=1
for load_duration in "${load_durations[@]}"; do
  for bulk_connections in "${bulk_connections_values[@]}"; do
    for failover_after in "${failover_afters[@]}"; do
      for repeat in $(seq 1 "$repeats"); do
        run_id="dur${load_duration}s-bulk${bulk_connections}-fail${failover_after}s-r${repeat}"
        result_file="${result_root}/${run_id}.jsonl"
        printf '{"run_id":"%s","object_mib":%s,"load_duration_seconds":%s,"bulk_connections":%s,"failover_after_seconds":%s,"repeat":%s,"result_file":"%s"}\n' \
          "$run_id" "$object_mib" "$load_duration" "$bulk_connections" "$failover_after" "$repeat" "$result_file" >> "$manifest_file"
        echo "running ${run_id}"
        BUILD_PRODUCT="$first_run" \
          BUILD_LAB_IMAGES="$first_run" \
          MPTUNNEL_LAB_OBJECT_MIB="$object_mib" \
          MPTUNNEL_LAB_LOAD_DURATION_SECONDS="$load_duration" \
          MPTUNNEL_LAB_BULK_CONNECTIONS="$bulk_connections" \
          UDP_PAYLOAD_BYTES="$udp_payload_bytes" \
          UDP_TIMEOUT_MS="$udp_timeout_ms" \
          FAILOVER_AFTER_SECONDS="$failover_after" \
          CURL_TIMEOUT_SECONDS="$curl_timeout" \
          CASE_FILTER="$case_filter" \
          RESULT_FILE="$result_file" \
          "$script_dir/run-heterogeneous-ablation.sh"
        first_run=0
      done
    done
  done
done

"$script_dir/summarize-results.py" "$result_root"/*.jsonl > "${result_root}/summary.md"
"$script_dir/summarize-results.py" --format json "$result_root"/*.jsonl > "${result_root}/summary.json"

echo "${result_root}/summary.md"

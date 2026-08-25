#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
cd "$repo_root"
source "$script_dir/result-paths.sh"
mkdir -p .tmp/python-cache .tmp/system
export PYTHONPYCACHEPREFIX="$repo_root/.tmp/python-cache"
export TMPDIR="$repo_root/.tmp/system"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
result_root="$(normalize_lab_result_path "${RESULT_ROOT:-random-internet-${timestamp}-$$}")"
schedule_script="$script_dir/internet_condition_schedule.py"
runner="$script_dir/run-heterogeneous-ablation.sh"
seed="${MPTUNNEL_LAB_INTERNET_SEED:-mptunnel-random-internet-v1}"
epoch_count="${MPTUNNEL_LAB_INTERNET_EPOCHS:-7}"
requested_workload_set="${MPTUNNEL_LAB_INTERNET_WORKLOAD_SET:-wide}"
smoke_case_filter="direct_balanced,baseline_vmess_tcp_single_balanced,baseline_hysteria2_udp_single_balanced,baseline_mptcp_tcp_multipath_all,mptunnel_tcp_single_balanced,mptunnel_udp_stream_single_balanced,mptunnel_tcp_multipath_all,mptunnel_udp_stream_multipath_all,mptunnel_udp_single_balanced,mptunnel_udp_multipath_all"
wide_case_filter="${smoke_case_filter},direct_upload_balanced,direct_mixed_balanced,baseline_vmess_tcp_single_balanced_upload,baseline_hysteria2_udp_single_balanced_upload,baseline_mptcp_tcp_multipath_all_upload,mptunnel_client_direct_balanced,mptunnel_client_direct_balanced_upload,mptunnel_tcp_single_balanced_upload,mptunnel_udp_stream_single_balanced_upload,mptunnel_tcp_multipath_all_upload,mptunnel_udp_stream_multipath_all_upload,mptunnel_reliable_mixed_single_balanced,mptunnel_reliable_mixed_single_balanced_upload,mptunnel_reliable_mixed_multipath_all,mptunnel_reliable_mixed_multipath_all_upload,mptunnel_udp_target_over_tcp_multipath_all,mptunnel_mixed_single_balanced,mptunnel_mixed_multipath_all,mptunnel_tun_tcp_single_balanced,mptunnel_tun_tcp_single_balanced_upload,mptunnel_tun_udp_stream_single_balanced,mptunnel_tun_udp_stream_single_balanced_upload,mptunnel_tun_mixed_multipath_all,mptunnel_tun_mixed_multipath_all_upload,mptunnel_tun_app_bypass_balanced,mptunnel_tun_app_bypass_balanced_upload"
case "$requested_workload_set" in
  smoke|wide) ;;
  *)
    echo "MPTUNNEL_LAB_INTERNET_WORKLOAD_SET must be smoke or wide" >&2
    exit 2
    ;;
esac
if [[ -n "${CASE_FILTER:-}" ]]; then
  workload_set="custom"
  case_filter="$CASE_FILTER"
elif [[ "$requested_workload_set" == "smoke" ]]; then
  workload_set="smoke"
  case_filter="$smoke_case_filter"
else
  workload_set="wide"
  case_filter="$wide_case_filter"
fi
build_product_first="${BUILD_PRODUCT:-1}"
build_lab_images_first="${BUILD_LAB_IMAGES:-1}"

if [[ -z "$seed" ]]; then
  echo "MPTUNNEL_LAB_INTERNET_SEED must not be empty" >&2
  exit 2
fi
if [[ ! "$epoch_count" =~ ^[1-9][0-9]*$ ]]; then
  echo "MPTUNNEL_LAB_INTERNET_EPOCHS must be an integer from 1 through 100000" >&2
  exit 2
fi
epoch_count=$((10#$epoch_count))
if (( epoch_count > 100000 )); then
  echo "MPTUNNEL_LAB_INTERNET_EPOCHS must be an integer from 1 through 100000" >&2
  exit 2
fi
for build_flag_name in build_product_first build_lab_images_first; do
  build_flag_value="${!build_flag_name}"
  if [[ "$build_flag_value" != "0" && "$build_flag_value" != "1" ]]; then
    echo "BUILD_PRODUCT and BUILD_LAB_IMAGES must be 0 or 1" >&2
    exit 2
  fi
done
case "${MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES:-0}" in
  1|true|True|TRUE|yes|Yes|YES)
    include_outages=1
    outage_args=(--include-outages)
    ;;
  0|false|False|FALSE|no|No|NO)
    include_outages=0
    outage_args=()
    ;;
  *)
    echo "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES must be 0/1, false/true, or no/yes" >&2
    exit 2
    ;;
esac

mkdir -p "$result_root"
schedule_file="$result_root/internet-condition-schedule.json"
container_schedule_file="/workspace/${schedule_file#./}"
schedule_metadata_file="$result_root/internet-condition-metadata.json"
experiment_manifest_file="$result_root/random-internet-manifest.json"
for artifact in "$schedule_file" "$schedule_metadata_file" "$experiment_manifest_file"; do
  if [[ -e "$artifact" ]]; then
    echo "refusing to overwrite existing random-Internet evidence: $artifact" >&2
    exit 2
  fi
done

python3 "$schedule_script" generate \
  --seed "$seed" \
  --epochs "$epoch_count" \
  "${outage_args[@]}" > "$schedule_file"
python3 "$schedule_script" validate --schedule "$schedule_file" >/dev/null
python3 "$schedule_script" metadata --schedule "$schedule_file" \
  > "$schedule_metadata_file"

schedule_sha256="$(python3 - "$schedule_metadata_file" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["schedule_sha256"])
PY
)"
generator_sha256="$(sha256sum "$schedule_script" | awk '{print $1}')"

SCHEDULE_FILE="$schedule_file" \
SCHEDULE_METADATA_FILE="$schedule_metadata_file" \
RESULT_ROOT_VALUE="$result_root" \
CASE_FILTER_VALUE="$case_filter" \
WORKLOAD_SET_VALUE="$workload_set" \
GENERATOR_SHA256="$generator_sha256" \
RUNNER_PATH="${runner#"$repo_root/"}" \
  python3 - "$experiment_manifest_file" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone

with open(os.environ["SCHEDULE_FILE"], encoding="utf-8") as source:
    schedule = json.load(source)
with open(os.environ["SCHEDULE_METADATA_FILE"], encoding="utf-8") as source:
    metadata = json.load(source)

balanced_prefix = "172.31.15"
rates = {}
for row in schedule["rows"]:
    if row["subnet_prefix"] == balanced_prefix:
        rates[(row["epoch"], row["direction"])] = row["rate"]

result_root = os.environ["RESULT_ROOT_VALUE"]
manifest = {
    "schema_version": 1,
    "kind": "mptunnel.lab.random-internet-matrix",
    "created_utc": datetime.now(timezone.utc).isoformat(),
    "schedule": {
        "file": "internet-condition-schedule.json",
        "metadata_file": "internet-condition-metadata.json",
        "generator_sha256": os.environ["GENERATOR_SHA256"],
        **metadata,
    },
    "runner": os.environ["RUNNER_PATH"],
    "case_filter": os.environ["CASE_FILTER_VALUE"],
    "workload_set": os.environ["WORKLOAD_SET_VALUE"],
    "subjects": os.environ["CASE_FILTER_VALUE"].split(","),
    "require_valid_host_default": True,
    "require_competitor_baselines_default": True,
    "epochs": [
        {
            "epoch": epoch,
            "netem_mode": f"internet-five-path-epoch-{epoch}",
            "result_dir": f"{result_root}/epoch-{epoch:04d}",
            "hysteria_balanced_client_rate": rates[(epoch, "client")],
            "hysteria_balanced_server_rate": rates[(epoch, "server")],
        }
        for epoch in range(schedule["epoch_count"])
    ],
}
with open(sys.argv[1], "w", encoding="utf-8") as destination:
    json.dump(manifest, destination, indent=2, sort_keys=True)
    destination.write("\n")
PY

first_run=1
for ((epoch = 0; epoch < epoch_count; epoch++)); do
  run_id="epoch-$(printf '%04d' "$epoch")"
  run_result_dir="$result_root/$run_id"
  netem_mode="internet-five-path-epoch-${epoch}"
  # Netem shapes egress. Hysteria's client-side Brutal declaration therefore
  # uses the client row for `up` and the server row for `down`.
  read -r hysteria_client_rate hysteria_server_rate < <(
    PYTHONPATH="$script_dir" python3 - "$schedule_file" "$epoch" <<'PY'
import sys
from pathlib import Path

from internet_condition_schedule import load_schedule, rows_for

schedule = load_schedule(Path(sys.argv[1]))
epoch = int(sys.argv[2])
rates = {}
for direction in ("client", "server"):
    matching = [
        row["rate"]
        for row in rows_for(schedule, epoch, direction)
        if row["subnet_prefix"] == "172.31.15"
    ]
    if len(matching) != 1:
        raise SystemExit(f"expected one balanced {direction} schedule row")
    rates[direction] = matching[0]
print(rates["client"], rates["server"])
PY
  )

  if [[ "$first_run" == "1" ]]; then
    build_product="$build_product_first"
    build_lab_images="$build_lab_images_first"
  else
    build_product=0
    build_lab_images=0
  fi

  echo "running random Internet epoch $((epoch + 1))/${epoch_count} (${netem_mode})"
  BUILD_PRODUCT="$build_product" \
  BUILD_LAB_IMAGES="$build_lab_images" \
  MPTUNNEL_LAB_NETEM_MODE="$netem_mode" \
  MPTUNNEL_LAB_INTERNET_SEED="$seed" \
  MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES="$include_outages" \
  MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE="$container_schedule_file" \
  MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256="$schedule_sha256" \
  MPTUNNEL_LAB_INTERNET_GENERATOR_SHA256="$generator_sha256" \
  MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE="$hysteria_client_rate" \
  MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE="$hysteria_server_rate" \
  MPTUNNEL_LAB_TCP_CARRIER_MAX="${MPTUNNEL_LAB_TCP_CARRIER_MAX:-1}" \
  MPTUNNEL_LAB_REQUIRE_VALID_HOST="${MPTUNNEL_LAB_REQUIRE_VALID_HOST:-1}" \
  MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES="${MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES:-1}" \
  MPTUNNEL_LAB_FAIL_ON_BAD_STATUS="${MPTUNNEL_LAB_FAIL_ON_BAD_STATUS:-1}" \
  CASE_FILTER="$case_filter" \
  RESULT_DIR="$run_result_dir" \
  RESULT_FILE="$run_result_dir/results.jsonl" \
    "$runner"

  EXPECTED_NETEM_MODE="$netem_mode" \
  EXPECTED_SEED="$seed" \
  EXPECTED_INCLUDE_OUTAGES="$include_outages" \
  EXPECTED_SCHEDULE_FILE="$container_schedule_file" \
  EXPECTED_SCHEDULE_SHA256="$schedule_sha256" \
  EXPECTED_GENERATOR_SHA256="$generator_sha256" \
  EXPECTED_HYSTERIA_CLIENT_RATE="$hysteria_client_rate" \
  EXPECTED_HYSTERIA_SERVER_RATE="$hysteria_server_rate" \
    python3 - "$run_result_dir/run-manifest.json" <<'PY'
import json
import os
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    manifest = json.load(source)
overrides = manifest["safe_environment_overrides"]
expected = {
    "MPTUNNEL_LAB_NETEM_MODE": os.environ["EXPECTED_NETEM_MODE"],
    "MPTUNNEL_LAB_INTERNET_SEED": os.environ["EXPECTED_SEED"],
    "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES": os.environ[
        "EXPECTED_INCLUDE_OUTAGES"
    ],
    "MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE": os.environ[
        "EXPECTED_SCHEDULE_FILE"
    ],
    "MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256": os.environ[
        "EXPECTED_SCHEDULE_SHA256"
    ],
    "MPTUNNEL_LAB_INTERNET_GENERATOR_SHA256": os.environ[
        "EXPECTED_GENERATOR_SHA256"
    ],
    "MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE": os.environ[
        "EXPECTED_HYSTERIA_CLIENT_RATE"
    ],
    "MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE": os.environ[
        "EXPECTED_HYSTERIA_SERVER_RATE"
    ],
}
for key, value in expected.items():
    if overrides.get(key) != value:
        raise SystemExit(f"run manifest did not retain {key}={value!r}")
PY
  first_run=0
done

"$script_dir/summarize-results.py" "$result_root"/epoch-*/results.jsonl \
  > "$result_root/summary.md"
"$script_dir/summarize-results.py" --format json \
  "$result_root"/epoch-*/results.jsonl > "$result_root/summary.json"

echo "$experiment_manifest_file"
echo "$result_root/summary.md"

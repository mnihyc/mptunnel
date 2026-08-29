#!/usr/bin/env bash
set -euo pipefail

mode="${1:-apply}"

scale_netem_value() {
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

netem_limit_packets() {
  python3 - "$@" <<'PY'
import math
import re
import sys


def parse_rate_bps(value):
    match = re.fullmatch(
        r"([0-9]+(?:\.[0-9]+)?)([kmgt]?)(?:bit|bps)?", value.lower()
    )
    if match is None:
        raise SystemExit(f"unsupported netem rate: {value}")
    scale = {
        "": 1,
        "k": 1_000,
        "m": 1_000_000,
        "g": 1_000_000_000,
        "t": 1_000_000_000_000,
    }
    return float(match.group(1)) * scale[match.group(2)]


def parse_seconds(value):
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)(ns|us|ms|s)", value.lower())
    if match is None:
        raise SystemExit(f"unsupported netem duration: {value}")
    scale = {"ns": 1e-9, "us": 1e-6, "ms": 1e-3, "s": 1.0}
    return float(match.group(1)) * scale[match.group(2)]


rate_bps = parse_rate_bps(sys.argv[1])
delay_seconds = parse_seconds(sys.argv[2])
jitter_seconds = parse_seconds(sys.argv[3])
if len(sys.argv) == 5:
    # In load-coupled mode the child must hold the fixed propagation process
    # plus one explicit full-size-packet queue horizon behind its HTB parent.
    queue_seconds = parse_seconds(sys.argv[4])
    horizon_seconds = delay_seconds + 3.0 * jitter_seconds + queue_seconds
    bdp_packets = math.ceil(rate_bps * horizon_seconds / 8.0 / 1500.0)
    print(max(1, bdp_packets))
    raise SystemExit(0)
# Netem's packet limit must hold the emulated delay-rate product. Include a
# three-sigma jitter horizon and two BDPs of headroom so queue overflow does
# not become an undocumented loss model.
horizon_seconds = delay_seconds + 3.0 * jitter_seconds
bdp_packets = math.ceil(rate_bps * horizon_seconds / 8.0 / 1500.0)
print(max(1000, 2 * bdp_packets + 256))
PY
}

internet_load_burst_bytes() {
  python3 - "$1" <<'PY'
import math
import re
import sys

value = sys.argv[1]
match = re.fullmatch(
    r"([0-9]+(?:\.[0-9]+)?)([kmgt]?)(?:bit|bps)?", value.lower()
)
if match is None:
    raise SystemExit(f"unsupported Internet load-coupled rate: {value}")
scale = {
    "": 1,
    "k": 1_000,
    "m": 1_000_000,
    "g": 1_000_000_000,
    "t": 1_000_000_000_000,
}
rate_bps = float(match.group(1)) * scale[match.group(2)]
# A two-millisecond token reservoir prevents HTB timer granularity from
# becoming an accidental low-rate bottleneck.  It changes only the first two
# milliseconds of a burst; sustained capacity remains the schedule's rate.
print(max(1514, math.ceil(rate_bps * 0.002 / 8.0)))
PY
}

lowlat_rate="${MPTUNNEL_LAB_LOWLAT_RATE:-80mbit}"
lowlat_delay="${MPTUNNEL_LAB_LOWLAT_DELAY:-20ms}"
lowlat_jitter="${MPTUNNEL_LAB_LOWLAT_JITTER:-2ms}"
lowlat_loss="${MPTUNNEL_LAB_LOWLAT_LOSS:-1.00%}"

balanced_rate="${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}"
balanced_delay="${MPTUNNEL_LAB_BALANCED_DELAY:-80ms}"
balanced_jitter="${MPTUNNEL_LAB_BALANCED_JITTER:-10ms}"
balanced_loss="${MPTUNNEL_LAB_BALANCED_LOSS:-1.00%}"
mildloss_rate="${MPTUNNEL_LAB_MILDLOSS_RATE:-$(scale_netem_value "$balanced_rate" 0.5)}"
mildloss_delay="${MPTUNNEL_LAB_MILDLOSS_DELAY:-$(scale_netem_value "$balanced_delay" 2)}"
mildloss_jitter="${MPTUNNEL_LAB_MILDLOSS_JITTER:-$balanced_jitter}"
mildloss_loss="${MPTUNNEL_LAB_MILDLOSS_LOSS:-0.10%}"

fat_rate="${MPTUNNEL_LAB_FAT_RATE:-500mbit}"
fat_delay="${MPTUNNEL_LAB_FAT_DELAY:-180ms}"
fat_jitter="${MPTUNNEL_LAB_FAT_JITTER:-20ms}"
fat_loss="${MPTUNNEL_LAB_FAT_LOSS:-1.00%}"
tcp_per_flow_qos_rate="${MPTUNNEL_LAB_TCP_PER_FLOW_QOS_RATE:-500mbit}"
tcp_shared_bottleneck_rate="${MPTUNNEL_LAB_TCP_SHARED_BOTTLENECK_RATE:-200mbit}"

poor_rate="${MPTUNNEL_LAB_POOR_RATE:-50mbit}"
poor_delay="${MPTUNNEL_LAB_POOR_DELAY:-420ms}"
poor_jitter="${MPTUNNEL_LAB_POOR_JITTER:-120ms}"
poor_loss="${MPTUNNEL_LAB_POOR_LOSS:-10.00%}"
ideal_loss="${MPTUNNEL_LAB_IDEAL_LOSS:-0.00%}"
matrix_good_rate="${MPTUNNEL_LAB_MATRIX_GOOD_RATE:-500mbit}"
matrix_poor_rate="${MPTUNNEL_LAB_MATRIX_POOR_RATE:-50mbit}"
matrix_good_delay="${MPTUNNEL_LAB_MATRIX_GOOD_DELAY:-50ms}"
matrix_poor_delay="${MPTUNNEL_LAB_MATRIX_POOR_DELAY:-250ms}"
matrix_good_jitter="${MPTUNNEL_LAB_MATRIX_GOOD_JITTER:-5ms}"
matrix_poor_jitter="${MPTUNNEL_LAB_MATRIX_POOR_JITTER:-60ms}"
matrix_good_loss="${MPTUNNEL_LAB_MATRIX_GOOD_LOSS:-1.00%}"
matrix_poor_loss="${MPTUNNEL_LAB_MATRIX_POOR_LOSS:-15.00%}"
scale_seed="${MPTUNNEL_LAB_SCALE_SEED:-mptunnel-scale-links}"
internet_seed="${MPTUNNEL_LAB_INTERNET_SEED:-mptunnel-random-internet-v1}"
internet_include_outages="${MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES:-0}"
internet_schedule_script="${MPTUNNEL_LAB_INTERNET_SCHEDULE_SCRIPT:-/workspace/lab/internet_condition_schedule.py}"
internet_schedule_file="${MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE:-}"
internet_schedule_sha256="${MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256:-}"
internet_load_queue_delay="${MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY:-100ms}"

scale_subnet_prefixes=(
  172.31.10 172.31.15 172.31.16 172.31.20 172.31.30
  172.31.41 172.31.42 172.31.43 172.31.44 172.31.45
  172.31.51 172.31.52 172.31.53 172.31.54 172.31.55
  172.31.56 172.31.57 172.31.58 172.31.59 172.31.60
)

blackhole_loss="${MPTUNNEL_LAB_BLACKHOLE_LOSS:-100%}"
spike_fat_rate="${MPTUNNEL_LAB_SPIKE_FAT_RATE:-20mbit}"
spike_fat_delay="${MPTUNNEL_LAB_SPIKE_FAT_DELAY:-900ms}"
spike_fat_jitter="${MPTUNNEL_LAB_SPIKE_FAT_JITTER:-250ms}"
spike_fat_loss="${MPTUNNEL_LAB_SPIKE_FAT_LOSS:-10.00%}"
spike_lowlat_rate="${MPTUNNEL_LAB_SPIKE_LOWLAT_RATE:-10mbit}"
spike_lowlat_delay="${MPTUNNEL_LAB_SPIKE_LOWLAT_DELAY:-650ms}"
spike_lowlat_jitter="${MPTUNNEL_LAB_SPIKE_LOWLAT_JITTER:-180ms}"
spike_lowlat_loss="${MPTUNNEL_LAB_SPIKE_LOWLAT_LOSS:-10.00%}"
spike_balanced_rate="${MPTUNNEL_LAB_SPIKE_BALANCED_RATE:-25mbit}"
spike_balanced_delay="${MPTUNNEL_LAB_SPIKE_BALANCED_DELAY:-500ms}"
spike_balanced_jitter="${MPTUNNEL_LAB_SPIKE_BALANCED_JITTER:-140ms}"
spike_balanced_loss="${MPTUNNEL_LAB_SPIKE_BALANCED_LOSS:-15.00%}"
spike_poor_rate="${MPTUNNEL_LAB_SPIKE_POOR_RATE:-2mbit}"
spike_poor_delay="${MPTUNNEL_LAB_SPIKE_POOR_DELAY:-1200ms}"
spike_poor_jitter="${MPTUNNEL_LAB_SPIKE_POOR_JITTER:-350ms}"
spike_poor_loss="${MPTUNNEL_LAB_SPIKE_POOR_LOSS:-25.00%}"

interface_for_subnet() {
  local subnet_prefix="$1"
  ip -o -4 addr show scope global \
    | awk -v prefix="$subnet_prefix" '$4 ~ "^" prefix "\\." {print $2; exit}'
}

apply_profile_to_interface() {
  local operation="$1"
  local iface="$2"
  local rate="$3"
  local delay="$4"
  local jitter="$5"
  local loss="$6"
  local limit_packets

  limit_packets="${MPTUNNEL_LAB_NETEM_LIMIT_PACKETS:-$(
    netem_limit_packets "$rate" "$delay" "$jitter"
  )}"
  if [[ ! "$limit_packets" =~ ^[1-9][0-9]*$ ]]; then
    echo "MPTUNNEL_LAB_NETEM_LIMIT_PACKETS must be a positive integer" >&2
    exit 2
  fi

  case "$jitter" in
    0|0ms|0us|0ns|0s)
      tc qdisc "$operation" dev "$iface" root netem \
        limit "$limit_packets" \
        rate "$rate" \
        delay "$delay" \
        loss "$loss"
      ;;
    *)
      tc qdisc "$operation" dev "$iface" root netem \
        limit "$limit_packets" \
        rate "$rate" \
        delay "$delay" "$jitter" distribution normal \
        loss "$loss"
      ;;
  esac
}

apply_profile() {
  local subnet_prefix="$1"
  local rate="$2"
  local delay="$3"
  local jitter="$4"
  local loss="$5"
  local iface

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  apply_profile_to_interface replace "$iface" "$rate" "$delay" "$jitter" "$loss"
}

apply_scale_profile() {
  local operation="$1"
  local subnet_prefix="$2"
  local rate="$3"
  local delay="$4"
  local jitter="$5"
  local loss="$6"
  local iface

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    echo "scale topology is missing subnet ${subnet_prefix}.0/24" >&2
    return 1
  fi

  apply_profile_to_interface "$operation" "$iface" "$rate" "$delay" "$jitter" "$loss"
}

change_balanced_observed() {
  local iface address apply_exit readback_exit readback_output root_line evidence

  iface="$(interface_for_subnet "172.31.15" || true)"
  if [[ -z "$iface" ]]; then
    printf '69\t69\t-\n'
    return 69
  fi

  apply_exit=0
  if apply_profile_to_interface \
    change "$iface" \
    "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$balanced_loss"; then
    :
  else
    apply_exit="$?"
  fi

  readback_exit=0
  readback_output=""
  if readback_output="$(tc -s -d qdisc show dev "$iface")"; then
    :
  else
    readback_exit="$?"
  fi
  address="$(
    ip -o -4 addr show dev "$iface" scope global \
      | awk '$4 ~ /^172[.]31[.]15[.]/ {print $4; exit}' \
      || true
  )"
  root_line="$(
    printf '%s\n' "$readback_output" \
      | awk '$1 == "qdisc" && $2 == "netem" && $0 ~ /(^|[[:space:]])root([[:space:]]|$)/ {print; exit}'
  )"
  evidence="-"
  if [[ "$readback_exit" == "0" && -n "$address" && -n "$root_line" ]]; then
    if evidence="$(
      printf 'interface=%s\naddress=%s\n%s\n' "$iface" "$address" "$root_line" \
        | base64 -w0
    )"; then
      :
    else
      readback_exit="$?"
      evidence="-"
    fi
  elif [[ "$readback_exit" == "0" ]]; then
    readback_exit=65
  fi

  printf '%s\t%s\t%s\n' "$apply_exit" "$readback_exit" "$evidence"
  [[ "$apply_exit" == "0" && "$readback_exit" == "0" ]]
}

apply_profile_all() {
  local rate="$1"
  local delay="$2"
  local jitter="$3"
  local loss="$4"

  apply_profile "172.31.10" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.15" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.16" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.20" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.30" "$rate" "$delay" "$jitter" "$loss"
}

apply_scale_epoch() {
  local epoch="$1"
  local direction="$2"
  local rate_band="$3"
  local operation profile_json pid failed
  local -a profile_pids=()
  if [[ "$epoch" == "0" ]]; then
    operation="replace"
  else
    # Preserve in-flight packets while changing the declared path condition.
    # Replacing the qdisc would inject an undeclared simultaneous link reset.
    operation="change"
  fi
  profile_json="$(python3 /workspace/lab/path_variation.py profiles \
    --seed "$scale_seed" \
    --epoch "$epoch" \
    --direction "$direction" \
    --rate-band "$rate_band")"
  while IFS=$'\t' read -r subnet_prefix rate delay jitter loss; do
    apply_scale_profile \
      "$operation" "$subnet_prefix" "$rate" "$delay" "$jitter" "$loss" &
    profile_pids+=("$!")
  done < <(python3 /workspace/lab/path_variation.py profiles \
    --seed "$scale_seed" \
    --epoch "$epoch" \
    --direction "$direction" \
    --rate-band "$rate_band" \
    --format tsv)
  failed=0
  for pid in "${profile_pids[@]}"; do
    if ! wait "$pid"; then
      failed=1
    fi
  done
  if [[ "$failed" != "0" ]]; then
    return 1
  fi
  printf '%s\n' "$profile_json"
}

internet_contract_error() {
  echo "invalid seeded Internet-condition schedule: $*" >&2
  return 2
}

internet_percentage_is_valid() {
  local token="$1"
  local numeric whole fraction=""

  # Keep tc arguments bounded as well as syntactically constrained. The
  # generator emits percentages, never probability fractions.
  if [[ ${#token} -gt 32 \
    || ! "$token" =~ ^[0-9]{1,3}([.][0-9]+)?%$ ]]; then
    return 1
  fi
  numeric="${token%%%}"
  whole="${numeric%%.*}"
  if [[ "$numeric" == *.* ]]; then
    fraction="${numeric#*.}"
  fi
  if (( 10#$whole > 100 )); then
    return 1
  fi
  if (( 10#$whole == 100 )) && [[ "$fraction" =~ [1-9] ]]; then
    return 1
  fi
}

internet_rate_is_valid() {
  local token="$1"
  if [[ ${#token} -gt 32 \
    || ! "$token" =~ ^([0-9]+)([.][0-9]+)?([kmgt]?)(bit|bps)$ ]]; then
    return 1
  fi
  # A syntactically valid zero rate is not a usable netem rate.
  [[ "${BASH_REMATCH[1]}${BASH_REMATCH[2]:-}" =~ [1-9] ]]
}

internet_duration_is_valid() {
  local token="$1"
  [[ ${#token} -le 32 \
    && "$token" =~ ^[0-9]+([.][0-9]+)?(ns|us|ms|s)$ ]]
}

internet_positive_duration_is_valid() {
  local token="$1"
  internet_duration_is_valid "$token" || return 1
  [[ "${BASH_REMATCH[0]}" =~ [1-9] ]]
}

internet_netem_seed_is_valid() {
  local token="$1"
  [[ "$token" =~ ^[0-9]+$ && ${#token} -le 10 ]] || return 1
  (( 10#$token >= 1 && 10#$token <= 4294967295 ))
}

require_netem_seed_support() {
  local iface="$1"
  local help_text

  # `help` is parsed before qdisc mutation, so this detects both the iproute2
  # spelling and its support without perturbing a link. Reproducible random
  # loss is part of this mode's contract; silently omitting seed would make
  # nominally paired protocol runs experience different packet loss.
  help_text="$(tc qdisc add dev "$iface" root netem help 2>&1 || true)"
  if [[ ! "$help_text" =~ (^|[[:space:]])seed([[:space:]]|$) ]]; then
    echo "tc netem does not advertise seed support; seeded Internet-condition runs require an iproute2/kernel netem combination with 'seed SEED' support" >&2
    return 2
  fi
}

apply_internet_profile_to_interface() {
  local operation="$1"
  local iface="$2"
  local rate="$3"
  local delay="$4"
  local jitter="$5"
  local delay_correlation="$6"
  local loss="$7"
  local loss_correlation="$8"
  local reorder="$9"
  local reorder_correlation="${10}"
  local duplicate="${11}"
  local corrupt="${12}"
  local netem_seed="${13}"
  local outage="${14}"
  local limit_packets="${15}"
  local effective_loss="$loss"
  local -a qdisc_args

  if [[ "$outage" == "1" ]]; then
    effective_loss="100%"
  fi

  qdisc_args=(
    tc qdisc "$operation" dev "$iface" root netem
    limit "$limit_packets"
    rate "$rate"
  )
  case "$jitter" in
    0|0ms|0us|0ns|0s)
      qdisc_args+=(delay "$delay")
      ;;
    *)
      qdisc_args+=(
        delay "$delay" "$jitter" "$delay_correlation" distribution normal
      )
      ;;
  esac
  qdisc_args+=(
    loss random "$effective_loss" "$loss_correlation"
    reorder "$reorder" "$reorder_correlation"
    duplicate "$duplicate"
    corrupt "$corrupt"
    seed "$netem_seed"
  )
  if ! "${qdisc_args[@]}"; then
    echo "failed to apply seeded tc netem profile on $iface; tc advertised seed support, but the running kernel must also support seeded netem" >&2
    return 2
  fi
}

apply_load_coupled_internet_profile_to_interface() {
  local iface="$1"
  local rate="$2"
  local delay="$3"
  local jitter="$4"
  local delay_correlation="$5"
  local loss="$6"
  local loss_correlation="$7"
  local reorder="$8"
  local reorder_correlation="$9"
  local duplicate="${10}"
  local corrupt="${11}"
  local netem_seed="${12}"
  local outage="${13}"
  local limit_packets="${14}"
  local burst_bytes="${15}"
  local effective_loss="$loss"
  local -a netem_args

  if [[ "$outage" == "1" ]]; then
    effective_loss="100%"
  fi

  # HTB owns only the aggregate scheduled rate.  The netem child retains the
  # seeded propagation/jitter/loss process and is deliberately finite.  At
  # offered load below the class rate there is no token backlog.  Sustained
  # excess load makes packets wait behind the class, increasing observed
  # delay variation until the child limit produces queue-overflow loss.
  # Recreate the hierarchy so each paired subject starts with empty tokens,
  # an empty queue, and the same seeded netem packet process.
  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root handle 1: htb default 10
  tc class add dev "$iface" parent 1: classid 1:10 htb \
    rate "$rate" ceil "$rate" \
    burst "${burst_bytes}b" cburst "${burst_bytes}b" quantum 1514

  netem_args=(
    tc qdisc add dev "$iface" parent 1:10 handle 10: netem
    limit "$limit_packets"
  )
  case "$jitter" in
    0|0ms|0us|0ns|0s)
      netem_args+=(delay "$delay")
      ;;
    *)
      netem_args+=(
        delay "$delay" "$jitter" "$delay_correlation" distribution normal
      )
      ;;
  esac
  netem_args+=(
    loss random "$effective_loss" "$loss_correlation"
    reorder "$reorder" "$reorder_correlation"
    duplicate "$duplicate"
    corrupt "$corrupt"
    seed "$netem_seed"
  )
  if ! "${netem_args[@]}"; then
    echo "failed to apply load-coupled seeded tc profile on $iface" >&2
    return 2
  fi
}

apply_internet_five_path_epoch() {
  local epoch="$1"
  local direction="$2"
  local queue_model="${3:-static}"
  local operation="replace" schedule_tsv line subnet_prefix iface limit_packets
  local burst_bytes
  local -a fields=()
  local -a outage_args=()
  local -a schedule_args=()
  local -a subnets=() rates=() delays=() jitters=()
  local -a delay_correlations=() losses=() loss_correlations=()
  local -a reorders=() reorder_correlations=() duplicates=() corruptions=()
  local -a netem_seeds=() outages=() ifaces=() limits=() bursts=()
  local -A seen_subnets=()
  local row_count=0 index

  case "$queue_model" in
    static) ;;
    load-coupled)
      if ! internet_positive_duration_is_valid "$internet_load_queue_delay"; then
        internet_contract_error \
          "MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY must be a positive duration"
        return
      fi
      ;;
    *)
      internet_contract_error "unsupported Internet queue model"
      return
      ;;
  esac

  # Every matrix subject is an independent schedule replay. The runner tears
  # its containers down between epochs, and reapplying a subject must reset
  # both the queue and netem's seeded packet process. Epoch number is therefore
  # not qdisc lifecycle state. Dynamic in-flight transitions use the separate
  # flapping lab; load-coupled queue occupancy is local to one subject.

  case "$internet_include_outages" in
    0) ;;
    1) outage_args=(--include-outages) ;;
    *)
      internet_contract_error \
        "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES must be 0 or 1"
      return
      ;;
  esac

  if [[ -n "$internet_schedule_file" ]]; then
    if [[ ! "$internet_schedule_sha256" =~ ^[0-9a-f]{64}$ ]]; then
      internet_contract_error \
        "artifact replay requires MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256"
      return
    fi
    schedule_args=(
      replay
      --schedule "$internet_schedule_file"
      --expect-sha256 "$internet_schedule_sha256"
      --epoch "$epoch"
      --direction "$direction"
      --format tsv
    )
  else
    if [[ -n "$internet_schedule_sha256" ]]; then
      internet_contract_error \
        "schedule identity was provided without MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE"
      return
    fi
    schedule_args=(
      render
      --seed "$internet_seed"
      --epoch "$epoch"
      --direction "$direction"
      --topology five-path
      --format tsv
      "${outage_args[@]}"
    )
  fi

  if ! schedule_tsv="$(python3 "$internet_schedule_script" "${schedule_args[@]}")"; then
    echo "failed to render seeded Internet-condition schedule" >&2
    return 2
  fi
  if [[ -z "$schedule_tsv" ]]; then
    internet_contract_error "generator returned no rows"
    return
  fi

  # Parse into one array per column and validate every row before touching a
  # qdisc. No schedule value is interpreted as shell source.
  while IFS= read -r line; do
    fields=()
    IFS=$'\t' read -r -a fields <<< "$line"
    if [[ ${#fields[@]} -ne 13 ]]; then
      internet_contract_error \
        "row $((row_count + 1)) has ${#fields[@]} columns; expected 13"
      return
    fi
    subnet_prefix="${fields[0]}"
    case "$subnet_prefix" in
      172.31.10|172.31.15|172.31.16|172.31.20|172.31.30) ;;
      *)
        internet_contract_error \
          "row $((row_count + 1)) has an unexpected subnet prefix"
        return
        ;;
    esac
    if [[ -n "${seen_subnets[$subnet_prefix]:-}" ]]; then
      internet_contract_error "duplicate subnet prefix $subnet_prefix"
      return
    fi
    seen_subnets[$subnet_prefix]=1
    if ! internet_rate_is_valid "${fields[1]}"; then
      internet_contract_error "row $((row_count + 1)) has an invalid rate"
      return
    fi
    if ! internet_duration_is_valid "${fields[2]}" \
      || ! internet_duration_is_valid "${fields[3]}"; then
      internet_contract_error \
        "row $((row_count + 1)) has an invalid delay or jitter"
      return
    fi
    for index in 4 5 6 7 8 9 10; do
      if ! internet_percentage_is_valid "${fields[$index]}"; then
        internet_contract_error \
          "row $((row_count + 1)) column $((index + 1)) is not a 0..100% token"
        return
      fi
    done
    if ! internet_netem_seed_is_valid "${fields[11]}"; then
      internet_contract_error \
        "row $((row_count + 1)) has an invalid or zero uint32 netem seed"
      return
    fi
    if [[ "${fields[12]}" != "0" && "${fields[12]}" != "1" ]]; then
      internet_contract_error "row $((row_count + 1)) has an invalid outage flag"
      return
    fi

    subnets+=("$subnet_prefix")
    rates+=("${fields[1]}")
    delays+=("${fields[2]}")
    jitters+=("${fields[3]}")
    delay_correlations+=("${fields[4]}")
    losses+=("${fields[5]}")
    loss_correlations+=("${fields[6]}")
    reorders+=("${fields[7]}")
    reorder_correlations+=("${fields[8]}")
    duplicates+=("${fields[9]}")
    corruptions+=("${fields[10]}")
    netem_seeds+=("${fields[11]}")
    outages+=("${fields[12]}")
    ((row_count += 1))
  done <<< "$schedule_tsv"

  if [[ "$row_count" -ne 5 ]]; then
    internet_contract_error \
      "five-path topology returned $row_count unique rows; expected 5"
    return
  fi

  # Resolve all interfaces and queue limits before applying the first row, so
  # topology or generator mistakes cannot leave a knowingly partial profile.
  for ((index = 0; index < row_count; index += 1)); do
    iface="$(interface_for_subnet "${subnets[$index]}")"
    if [[ -z "$iface" ]]; then
      internet_contract_error \
        "five-path topology is missing subnet ${subnets[$index]}.0/24"
      return
    fi
    if [[ "$queue_model" == "load-coupled" ]]; then
      limit_packets="$(netem_limit_packets \
        "${rates[$index]}" "${delays[$index]}" "${jitters[$index]}" \
        "$internet_load_queue_delay")"
      burst_bytes="$(internet_load_burst_bytes "${rates[$index]}")"
    else
      limit_packets="${MPTUNNEL_LAB_NETEM_LIMIT_PACKETS:-$(
        netem_limit_packets \
          "${rates[$index]}" "${delays[$index]}" "${jitters[$index]}"
      )}"
      burst_bytes=""
    fi
    if [[ ! "$limit_packets" =~ ^[1-9][0-9]*$ ]]; then
      echo "MPTUNNEL_LAB_NETEM_LIMIT_PACKETS must be a positive integer" >&2
      return 2
    fi
    ifaces+=("$iface")
    limits+=("$limit_packets")
    bursts+=("$burst_bytes")
  done

  require_netem_seed_support "${ifaces[0]}"
  for ((index = 0; index < row_count; index += 1)); do
    if [[ "$queue_model" == "load-coupled" ]]; then
      apply_load_coupled_internet_profile_to_interface \
        "${ifaces[$index]}" \
        "${rates[$index]}" "${delays[$index]}" "${jitters[$index]}" \
        "${delay_correlations[$index]}" \
        "${losses[$index]}" "${loss_correlations[$index]}" \
        "${reorders[$index]}" "${reorder_correlations[$index]}" \
        "${duplicates[$index]}" "${corruptions[$index]}" \
        "${netem_seeds[$index]}" "${outages[$index]}" \
        "${limits[$index]}" "${bursts[$index]}"
    else
      apply_internet_profile_to_interface \
        "$operation" "${ifaces[$index]}" \
        "${rates[$index]}" "${delays[$index]}" "${jitters[$index]}" \
        "${delay_correlations[$index]}" \
        "${losses[$index]}" "${loss_correlations[$index]}" \
        "${reorders[$index]}" "${reorder_correlations[$index]}" \
        "${duplicates[$index]}" "${corruptions[$index]}" \
        "${netem_seeds[$index]}" "${outages[$index]}" "${limits[$index]}"
    fi
  done
}

apply_tcp_per_flow_qos() {
  local subnet_prefix="$1"
  local maxrate="$2"
  local iface limit_packets flow_limit_packets aggregate_rate

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  # The root models only deterministic propagation. Its queue must hold the
  # aggregate delay-rate product of the largest configured carrier cohort;
  # the child fq qdisc then applies maxrate independently to each TCP flow.
  aggregate_rate="$(scale_netem_value "$maxrate" 3)"
  limit_packets="${MPTUNNEL_LAB_NETEM_LIMIT_PACKETS:-$(
    netem_limit_packets "$aggregate_rate" "$fat_delay" "0ms"
  )}"
  flow_limit_packets="$(
    netem_limit_packets "$maxrate" "$fat_delay" "0ms"
  )"
  if [[ ! "$limit_packets" =~ ^[1-9][0-9]*$ ]]; then
    echo "MPTUNNEL_LAB_NETEM_LIMIT_PACKETS must be a positive integer" >&2
    exit 2
  fi
  if [[ ! "$flow_limit_packets" =~ ^[1-9][0-9]*$ ]]; then
    echo "derived TCP per-flow queue limit must be a positive integer" >&2
    exit 2
  fi

  # Recreate the hierarchy so a preceding shared-bottleneck profile cannot
  # leave a child qdisc with different semantics.
  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root handle 1: netem \
    limit "$limit_packets" \
    delay "$fat_delay"
  tc qdisc add dev "$iface" parent 1:1 handle 10: fq \
    limit "$limit_packets" \
    flow_limit "$flow_limit_packets" \
    maxrate "$maxrate"
}

apply_tcp_shared_bottleneck() {
  local subnet_prefix="$1"
  local rate="$2"
  local iface limit_packets

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  limit_packets="${MPTUNNEL_LAB_NETEM_LIMIT_PACKETS:-$(
    netem_limit_packets "$rate" "$fat_delay" "0ms"
  )}"
  if [[ ! "$limit_packets" =~ ^[1-9][0-9]*$ ]]; then
    echo "MPTUNNEL_LAB_NETEM_LIMIT_PACKETS must be a positive integer" >&2
    exit 2
  fi

  # Remove any per-flow child from an earlier profile before installing the
  # single aggregate bottleneck.
  tc qdisc del dev "$iface" root 2>/dev/null || true
  tc qdisc add dev "$iface" root handle 1: netem \
    limit "$limit_packets" \
    rate "$rate" \
    delay "$fat_delay"
}

blackhole_profile() {
  local subnet_prefix="$1"
  local iface

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  tc qdisc replace dev "$iface" root netem loss "$blackhole_loss"
}

clear_profile() {
  local subnet_prefix="$1"
  local iface

  iface="$(interface_for_subnet "$subnet_prefix")"
  if [[ -z "$iface" ]]; then
    return 0
  fi

  tc qdisc del dev "$iface" root >/dev/null 2>&1 || true
}

show_profile() {
  ip -o -4 addr show scope global | while read -r _ iface _ addr _; do
    case "$addr" in
      172.31.10.*|172.31.15.*|172.31.16.*|172.31.20.*|172.31.30.*|\
      172.31.4[1-5].*|172.31.5[1-9].*|172.31.60.*)
        echo "$iface $addr"
        tc -s -d qdisc show dev "$iface"
        tc -s -d class show dev "$iface"
        ;;
    esac
  done
}

run_bulk_interactive_loss_schedule() {
  # Keep deadline waiting and qdisc application in one endpoint-local process.
  # Starting a fresh container-control command at every epoch made its launch
  # latency part of the measured transition, rather than part of lab setup.
  exec python3 - <<'PY'
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time


def required(name):
    value = os.environ.get(name, "")
    if not value:
        raise SystemExit(f"{name} is required")
    return value


def atomic_write(path, value):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{target.name}.tmp-{os.getpid()}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, target)


def cancelled(cancel_file):
    return cancel_file.exists()


def wait_until(
    deadline_ms,
    cancel_file,
    finished_file,
    earliest_allowed_finish_ms=None,
):
    while True:
        if termination_requested or cancelled(cancel_file):
            return False
        if finished_file.exists():
            try:
                finished_ms = int(finished_file.read_text(encoding="utf-8").strip())
            except (OSError, ValueError):
                return False
            if (
                earliest_allowed_finish_ms is None
                or finished_ms < earliest_allowed_finish_ms
            ):
                return False
        remaining = deadline_ms - time.monotonic_ns() // 1_000_000
        if remaining <= 0:
            return True
        time.sleep(min(remaining / 1000.0, 0.01))


active_process = None
termination_requested = False


def request_termination(_signum, _frame):
    global termination_requested
    termination_requested = True
    if active_process is not None and active_process.poll() is None:
        try:
            os.killpg(active_process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def terminate_process_group(process):
    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        output, _ = process.communicate(timeout=0.25)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate(timeout=0.25)
    return output or ""


role = required("MPTUNNEL_LAB_SCHEDULE_ROLE")
service = required("MPTUNNEL_LAB_SCHEDULE_SERVICE")
started_file = Path(required("MPTUNNEL_LAB_SCHEDULE_STARTED_FILE"))
finished_file = Path(required("MPTUNNEL_LAB_SCHEDULE_FINISHED_FILE"))
cancel_file = Path(required("MPTUNNEL_LAB_SCHEDULE_CANCEL_FILE"))
ready_file = required("MPTUNNEL_LAB_SCHEDULE_READY_FILE")
status_file = required("MPTUNNEL_LAB_SCHEDULE_STATUS_FILE")
pid_file = required("MPTUNNEL_LAB_SCHEDULE_PID_FILE")
result_prefix = required("MPTUNNEL_LAB_SCHEDULE_RESULT_PREFIX")
epoch_ms = int(required("MPTUNNEL_LAB_SCHEDULE_EPOCH_MS"))
duration_ms = int(required("MPTUNNEL_LAB_SCHEDULE_DURATION_MS"))
lateness_ms = int(required("MPTUNNEL_LAB_SCHEDULE_LATENESS_MS"))
command_timeout_s = float(required("MPTUNNEL_LAB_SCHEDULE_COMMAND_TIMEOUT_S"))
losses = [int(value) for value in required("MPTUNNEL_LAB_SCHEDULE_LOSSES").split(",")]
netem_script = os.environ.get(
    "MPTUNNEL_LAB_SCHEDULE_NETEM_SCRIPT",
    "/workspace/lab/configure-netem.sh",
)
if epoch_ms <= 0 or duration_ms != epoch_ms * len(losses) or lateness_ms < 0:
    raise SystemExit("invalid bulk-interactive schedule dimensions")

atomic_write(pid_file, f"{os.getpid()}\n")
signal.signal(signal.SIGTERM, request_termination)
signal.signal(signal.SIGINT, request_termination)
atomic_write(ready_file, json.dumps({"pid": os.getpid()}, separators=(",", ":")) + "\n")

exit_code = 0
completed_offset_ms = None
try:
    while not started_file.exists():
        if termination_requested or cancelled(cancel_file) or finished_file.exists():
            exit_code = 124
            break
        time.sleep(0.005)
    if exit_code == 0:
        anchor_lines = started_file.read_text(encoding="utf-8").splitlines()
        if len(anchor_lines) != 3:
            exit_code = 2
        else:
            origin_ms = int(anchor_lines[1])
            for index, loss in enumerate(losses):
                planned_offset_ms = index * epoch_ms
                if not wait_until(
                    origin_ms + planned_offset_ms,
                    cancel_file,
                    finished_file,
                ):
                    exit_code = 124
                    break
                if termination_requested or cancelled(cancel_file):
                    exit_code = 124
                    break
                start_offset_ms = time.monotonic_ns() // 1_000_000 - origin_ms
                env = os.environ.copy()
                env["MPTUNNEL_LAB_BALANCED_LOSS"] = f"{loss}%"
                command_exit = 0
                output = ""
                try:
                    active_process = subprocess.Popen(
                        [netem_script, "change-balanced-observed"],
                        env=env,
                        text=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        start_new_session=True,
                    )
                    try:
                        output, _ = active_process.communicate(
                            timeout=command_timeout_s
                        )
                    except subprocess.TimeoutExpired:
                        output = terminate_process_group(active_process)
                        command_exit = 124
                    else:
                        command_exit = active_process.returncode
                    finally:
                        active_process = None
                except OSError:
                    command_exit = 124
                    output = ""
                end_offset_ms = time.monotonic_ns() // 1_000_000 - origin_ms
                apply_exit = 125
                readback_exit = 125
                readback_base64 = "-"
                if output:
                    match = re.fullmatch(
                        r"([0-9]+)\t([0-9]+)\t([A-Za-z0-9+/=]+|-)",
                        output.rstrip().splitlines()[-1],
                    )
                    if match:
                        apply_exit = int(match.group(1))
                        readback_exit = int(match.group(2))
                        readback_base64 = match.group(3)
                event = {
                    "role": role,
                    "service": service,
                    "start_offset_ms": start_offset_ms,
                    "end_offset_ms": end_offset_ms,
                    "command_exit_code": command_exit,
                    "apply_exit_code": apply_exit,
                    "readback_exit_code": readback_exit,
                    "readback_base64": readback_base64,
                }
                atomic_write(
                    f"{result_prefix}-epoch-{index}.json",
                    json.dumps(event, separators=(",", ":")) + "\n",
                )
                if not (
                    command_exit == apply_exit == readback_exit == 0
                    and readback_base64 != "-"
                    and planned_offset_ms <= start_offset_ms <= end_offset_ms
                    <= planned_offset_ms + lateness_ms
                ):
                    exit_code = 1
                if termination_requested or cancelled(cancel_file):
                    exit_code = 124
                    break
            if exit_code != 124 and wait_until(
                origin_ms + duration_ms,
                cancel_file,
                finished_file,
                earliest_allowed_finish_ms=origin_ms + duration_ms,
            ):
                completed_offset_ms = time.monotonic_ns() // 1_000_000 - origin_ms
            elif exit_code == 0:
                exit_code = 124
except (OSError, ValueError):
    exit_code = 2
finally:
    status = {
        "exit_code": exit_code,
        "completed_offset_ms": completed_offset_ms,
    }
    atomic_write(status_file, json.dumps(status, separators=(",", ":")) + "\n")

raise SystemExit(exit_code)
PY
}

case "$mode" in
  apply)
    # Short regional hop: low RTT, modest bandwidth, nearly clean.
    apply_profile "172.31.10" "$lowlat_rate" "$lowlat_delay" "$lowlat_jitter" "$lowlat_loss"
    # Balanced daily-use path: moderate RTT and useful bandwidth.
    apply_profile "172.31.15" "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$balanced_loss"
    # Lower-loss companion to the balanced link: half bandwidth, double base
    # latency, and the same jitter unless explicitly overridden.
    apply_profile "172.31.16" "$mildloss_rate" "$mildloss_delay" "$mildloss_jitter" "$mildloss_loss"
    # Cross-continent fiber: high RTT, high throughput, small random loss.
    apply_profile "172.31.20" "$fat_rate" "$fat_delay" "$fat_jitter" "$fat_loss"
    # Poor Internet: very high RTT, low throughput, heavy jitter/loss.
    apply_profile "172.31.30" "$poor_rate" "$poor_delay" "$poor_jitter" "$poor_loss"
    ;;
  apply-lowlat)
    apply_profile "172.31.10" "$lowlat_rate" "$lowlat_delay" "$lowlat_jitter" "$lowlat_loss"
    ;;
  apply-balanced)
    apply_profile "172.31.15" "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$balanced_loss"
    ;;
  change-balanced)
    # Dynamic loss/latency experiments must preserve the live qdisc and its
    # queued packets. Initial setup remains the responsibility of `apply`.
    apply_scale_profile \
      change "172.31.15" \
      "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$balanced_loss"
    ;;
  change-balanced-observed)
    # Apply and report the exact root-qdisc state used by a scheduled epoch.
    change_balanced_observed
    ;;
  bulk-interactive-loss-schedule)
    run_bulk_interactive_loss_schedule
    ;;
  apply-mildloss)
    apply_profile "172.31.16" "$mildloss_rate" "$mildloss_delay" "$mildloss_jitter" "$mildloss_loss"
    ;;
  apply-fat)
    apply_profile "172.31.20" "$fat_rate" "$fat_delay" "$fat_jitter" "$fat_loss"
    ;;
  apply-poor)
    apply_profile "172.31.30" "$poor_rate" "$poor_delay" "$poor_jitter" "$poor_loss"
    ;;
  ideal-lowlat)
    apply_profile "172.31.10" "$lowlat_rate" "$lowlat_delay" "$lowlat_jitter" "$ideal_loss"
    ;;
  ideal-balanced)
    apply_profile "172.31.15" "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$ideal_loss"
    ;;
  ideal-mildloss)
    apply_profile "172.31.16" "$mildloss_rate" "$mildloss_delay" "$mildloss_jitter" "$ideal_loss"
    ;;
  ideal-fat)
    apply_profile "172.31.20" "$fat_rate" "$fat_delay" "$fat_jitter" "$ideal_loss"
    ;;
  ideal-poor)
    apply_profile "172.31.30" "$poor_rate" "$poor_delay" "$poor_jitter" "$ideal_loss"
    ;;
  ideal-all-lowlat)
    apply_profile_all "$lowlat_rate" "$lowlat_delay" "$lowlat_jitter" "$ideal_loss"
    ;;
  ideal-all-balanced)
    apply_profile_all "$balanced_rate" "$balanced_delay" "$balanced_jitter" "$ideal_loss"
    ;;
  ideal-all-fat)
    apply_profile_all "$fat_rate" "$fat_delay" "$fat_jitter" "$ideal_loss"
    ;;
  ideal-all-poor)
    apply_profile_all "$poor_rate" "$poor_delay" "$poor_jitter" "$ideal_loss"
    ;;
  tcp-per-flow-qos)
    apply_tcp_per_flow_qos "172.31.20" "$tcp_per_flow_qos_rate"
    ;;
  tcp-shared-bottleneck)
    apply_tcp_shared_bottleneck "172.31.20" "$tcp_shared_bottleneck_rate"
    ;;
  asymmetric-client)
    # Upload egress: the balanced link is high-capacity and the low-latency
    # link is constrained. The server mode reverses these capacities.
    apply_profile "172.31.10" "20mbit" "40ms" "0ms" "0%"
    apply_profile "172.31.15" "200mbit" "40ms" "0ms" "0%"
    ;;
  asymmetric-server)
    # Download egress: the low-latency link is high-capacity and the balanced
    # link is constrained.
    apply_profile "172.31.10" "200mbit" "40ms" "0ms" "0%"
    apply_profile "172.31.15" "20mbit" "40ms" "0ms" "0%"
    ;;
  scale-*-epoch-*-client|scale-*-epoch-*-server)
    if [[ "$mode" =~ ^scale-(access|gigabit|multi-gigabit)-epoch-([0-9]+)-(client|server)$ ]]; then
      apply_scale_epoch "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}" "${BASH_REMATCH[1]}"
    else
      echo "invalid scale epoch mode: $mode" >&2
      exit 2
    fi
    ;;
  internet-five-path-epoch-*-client|internet-five-path-epoch-*-server)
    if [[ "$mode" =~ ^internet-five-path-epoch-([0-9]+)-(client|server)$ ]]; then
      apply_internet_five_path_epoch "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}"
    else
      echo "invalid Internet-condition epoch mode: $mode" >&2
      exit 2
    fi
    ;;
  internet-five-path-load-coupled-epoch-*-client|internet-five-path-load-coupled-epoch-*-server)
    if [[ "$mode" =~ ^internet-five-path-load-coupled-epoch-([0-9]+)-(client|server)$ ]]; then
      apply_internet_five_path_epoch \
        "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" load-coupled
    else
      echo "invalid load-coupled Internet-condition epoch mode: $mode" >&2
      exit 2
    fi
    ;;
  matrix-b*)
    bits="${mode#matrix-}"
    if [[ "$bits" != b[01][01][01] ]]; then
      echo "invalid matrix mode: $mode" >&2
      exit 2
    fi
    bandwidth_bit="${bits:1:1}"
    latency_bit="${bits:2:1}"
    loss_bit="${bits:3:1}"
    if [[ "$bandwidth_bit" == "1" ]]; then
      matrix_rate="$matrix_good_rate"
    else
      matrix_rate="$matrix_poor_rate"
    fi
    if [[ "$latency_bit" == "1" ]]; then
      matrix_delay="$matrix_good_delay"
      matrix_jitter="$matrix_good_jitter"
    else
      matrix_delay="$matrix_poor_delay"
      matrix_jitter="$matrix_poor_jitter"
    fi
    if [[ "$loss_bit" == "1" ]]; then
      matrix_loss="$matrix_good_loss"
    else
      matrix_loss="$matrix_poor_loss"
    fi
    apply_profile "172.31.10" "$matrix_rate" "$matrix_delay" "$matrix_jitter" "$matrix_loss"
    ;;
  blackhole-fat)
    blackhole_profile "172.31.20"
    ;;
  blackhole-lowlat)
    blackhole_profile "172.31.10"
    ;;
  blackhole-balanced)
    blackhole_profile "172.31.15"
    ;;
  blackhole-poor)
    blackhole_profile "172.31.30"
    ;;
  spike-fat)
    apply_profile "172.31.20" "$spike_fat_rate" "$spike_fat_delay" "$spike_fat_jitter" "$spike_fat_loss"
    ;;
  spike-lowlat)
    apply_profile "172.31.10" "$spike_lowlat_rate" "$spike_lowlat_delay" "$spike_lowlat_jitter" "$spike_lowlat_loss"
    ;;
  spike-balanced)
    apply_profile "172.31.15" "$spike_balanced_rate" "$spike_balanced_delay" "$spike_balanced_jitter" "$spike_balanced_loss"
    ;;
  spike-poor)
    apply_profile "172.31.30" "$spike_poor_rate" "$spike_poor_delay" "$spike_poor_jitter" "$spike_poor_loss"
    ;;
  unconstrained|unconstrained-all|clear)
    for subnet_prefix in "${scale_subnet_prefixes[@]}"; do
      clear_profile "$subnet_prefix"
    done
    ;;
  show)
    show_profile
    ;;
  *)
    echo "usage: $0 [apply|apply-lowlat|apply-balanced|change-balanced|change-balanced-observed|bulk-interactive-loss-schedule|apply-mildloss|apply-fat|apply-poor|ideal-lowlat|ideal-balanced|ideal-mildloss|ideal-fat|ideal-poor|ideal-all-lowlat|ideal-all-balanced|ideal-all-fat|ideal-all-poor|tcp-per-flow-qos|tcp-shared-bottleneck|asymmetric-client|asymmetric-server|scale-{access,gigabit,multi-gigabit}-epoch-N-{client,server}|internet-five-path-epoch-N-{client,server}|internet-five-path-load-coupled-epoch-N-{client,server}|unconstrained|unconstrained-all|matrix-b000..matrix-b111|blackhole-fat|blackhole-lowlat|blackhole-balanced|blackhole-poor|spike-fat|spike-lowlat|spike-balanced|spike-poor|clear|show]" >&2
    exit 2
    ;;
esac

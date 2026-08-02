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
  python3 - "$1" "$2" "$3" <<'PY'
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
# Netem's packet limit must hold the emulated delay-rate product. Include a
# three-sigma jitter horizon and two BDPs of headroom so queue overflow does
# not become an undocumented loss model.
horizon_seconds = delay_seconds + 3.0 * jitter_seconds
bdp_packets = math.ceil(rate_bps * horizon_seconds / 8.0 / 1500.0)
print(max(1000, 2 * bdp_packets + 256))
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
        ;;
    esac
  done
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
    echo "usage: $0 [apply|apply-lowlat|apply-balanced|apply-mildloss|apply-fat|apply-poor|ideal-lowlat|ideal-balanced|ideal-mildloss|ideal-fat|ideal-poor|ideal-all-lowlat|ideal-all-balanced|ideal-all-fat|ideal-all-poor|tcp-per-flow-qos|tcp-shared-bottleneck|asymmetric-client|asymmetric-server|scale-{access,gigabit,multi-gigabit}-epoch-N-{client,server}|unconstrained|unconstrained-all|matrix-b000..matrix-b111|blackhole-fat|blackhole-lowlat|blackhole-balanced|blackhole-poor|spike-fat|spike-lowlat|spike-balanced|spike-poor|clear|show]" >&2
    exit 2
    ;;
esac

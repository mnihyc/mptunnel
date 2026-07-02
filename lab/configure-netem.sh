#!/usr/bin/env bash
set -euo pipefail

mode="${1:-apply}"

lowlat_rate="${MPTUNNEL_LAB_LOWLAT_RATE:-80mbit}"
lowlat_delay="${MPTUNNEL_LAB_LOWLAT_DELAY:-20ms}"
lowlat_jitter="${MPTUNNEL_LAB_LOWLAT_JITTER:-2ms}"
lowlat_loss="${MPTUNNEL_LAB_LOWLAT_LOSS:-1.00%}"

balanced_rate="${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}"
balanced_delay="${MPTUNNEL_LAB_BALANCED_DELAY:-80ms}"
balanced_jitter="${MPTUNNEL_LAB_BALANCED_JITTER:-10ms}"
balanced_loss="${MPTUNNEL_LAB_BALANCED_LOSS:-1.00%}"

fat_rate="${MPTUNNEL_LAB_FAT_RATE:-500mbit}"
fat_delay="${MPTUNNEL_LAB_FAT_DELAY:-180ms}"
fat_jitter="${MPTUNNEL_LAB_FAT_JITTER:-20ms}"
fat_loss="${MPTUNNEL_LAB_FAT_LOSS:-1.00%}"

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

  tc qdisc replace dev "$iface" root netem \
    rate "$rate" \
    delay "$delay" "$jitter" distribution normal \
    loss "$loss"
}

apply_profile_all() {
  local rate="$1"
  local delay="$2"
  local jitter="$3"
  local loss="$4"

  apply_profile "172.31.10" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.15" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.20" "$rate" "$delay" "$jitter" "$loss"
  apply_profile "172.31.30" "$rate" "$delay" "$jitter" "$loss"
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
      172.31.10.*|172.31.15.*|172.31.20.*|172.31.30.*)
        echo "$iface $addr"
        tc qdisc show dev "$iface"
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
    clear_profile "172.31.10"
    clear_profile "172.31.15"
    clear_profile "172.31.20"
    clear_profile "172.31.30"
    ;;
  show)
    show_profile
    ;;
  *)
    echo "usage: $0 [apply|apply-lowlat|apply-balanced|apply-fat|apply-poor|ideal-lowlat|ideal-balanced|ideal-fat|ideal-poor|ideal-all-lowlat|ideal-all-balanced|ideal-all-fat|ideal-all-poor|unconstrained|unconstrained-all|matrix-b000..matrix-b111|blackhole-fat|blackhole-lowlat|blackhole-balanced|blackhole-poor|spike-fat|spike-lowlat|spike-balanced|spike-poor|clear|show]" >&2
    exit 2
    ;;
esac

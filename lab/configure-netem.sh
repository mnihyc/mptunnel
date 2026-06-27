#!/usr/bin/env bash
set -euo pipefail

mode="${1:-apply}"

lowlat_rate="${MPTUNNEL_LAB_LOWLAT_RATE:-30mbit}"
lowlat_delay="${MPTUNNEL_LAB_LOWLAT_DELAY:-20ms}"
lowlat_jitter="${MPTUNNEL_LAB_LOWLAT_JITTER:-2ms}"
lowlat_loss="${MPTUNNEL_LAB_LOWLAT_LOSS:-0.01%}"

fat_rate="${MPTUNNEL_LAB_FAT_RATE:-300mbit}"
fat_delay="${MPTUNNEL_LAB_FAT_DELAY:-180ms}"
fat_jitter="${MPTUNNEL_LAB_FAT_JITTER:-20ms}"
fat_loss="${MPTUNNEL_LAB_FAT_LOSS:-0.10%}"

poor_rate="${MPTUNNEL_LAB_POOR_RATE:-8mbit}"
poor_delay="${MPTUNNEL_LAB_POOR_DELAY:-420ms}"
poor_jitter="${MPTUNNEL_LAB_POOR_JITTER:-120ms}"
poor_loss="${MPTUNNEL_LAB_POOR_LOSS:-3.00%}"

blackhole_loss="${MPTUNNEL_LAB_BLACKHOLE_LOSS:-100%}"
spike_fat_rate="${MPTUNNEL_LAB_SPIKE_FAT_RATE:-20mbit}"
spike_fat_delay="${MPTUNNEL_LAB_SPIKE_FAT_DELAY:-900ms}"
spike_fat_jitter="${MPTUNNEL_LAB_SPIKE_FAT_JITTER:-250ms}"
spike_fat_loss="${MPTUNNEL_LAB_SPIKE_FAT_LOSS:-0.50%}"
spike_lowlat_rate="${MPTUNNEL_LAB_SPIKE_LOWLAT_RATE:-10mbit}"
spike_lowlat_delay="${MPTUNNEL_LAB_SPIKE_LOWLAT_DELAY:-650ms}"
spike_lowlat_jitter="${MPTUNNEL_LAB_SPIKE_LOWLAT_JITTER:-180ms}"
spike_lowlat_loss="${MPTUNNEL_LAB_SPIKE_LOWLAT_LOSS:-0.30%}"

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
      172.31.10.*|172.31.20.*|172.31.30.*)
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
    # Cross-continent fiber: high RTT, high throughput, small random loss.
    apply_profile "172.31.20" "$fat_rate" "$fat_delay" "$fat_jitter" "$fat_loss"
    # Poor Internet: very high RTT, low throughput, heavy jitter/loss.
    apply_profile "172.31.30" "$poor_rate" "$poor_delay" "$poor_jitter" "$poor_loss"
    ;;
  blackhole-fat)
    blackhole_profile "172.31.20"
    ;;
  blackhole-lowlat)
    blackhole_profile "172.31.10"
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
  clear)
    clear_profile "172.31.10"
    clear_profile "172.31.20"
    clear_profile "172.31.30"
    ;;
  show)
    show_profile
    ;;
  *)
    echo "usage: $0 [apply|blackhole-fat|blackhole-lowlat|blackhole-poor|spike-fat|spike-lowlat|clear|show]" >&2
    exit 2
    ;;
esac

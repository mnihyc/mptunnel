#!/usr/bin/env bash
set -euo pipefail

mode="${1:-apply}"

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

  tc qdisc replace dev "$iface" root netem loss 100%
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
    apply_profile "172.31.10" "30mbit" "20ms" "2ms" "0.01%"
    # Cross-continent fiber: high RTT, high throughput, small random loss.
    apply_profile "172.31.20" "300mbit" "180ms" "20ms" "0.10%"
    # Poor Internet: very high RTT, low throughput, heavy jitter/loss.
    apply_profile "172.31.30" "8mbit" "420ms" "120ms" "3.00%"
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
  clear)
    clear_profile "172.31.10"
    clear_profile "172.31.20"
    clear_profile "172.31.30"
    ;;
  show)
    show_profile
    ;;
  *)
    echo "usage: $0 [apply|blackhole-fat|blackhole-lowlat|blackhole-poor|clear|show]" >&2
    exit 2
    ;;
esac

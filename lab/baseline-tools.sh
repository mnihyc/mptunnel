#!/usr/bin/env bash
set -euo pipefail

base_dir="${MPTUNNEL_LAB_BASELINE_DIR:-/tmp/mptunnel-baselines}"
mkdir -p "$base_dir"

arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) return 1 ;;
  esac
}

xray_asset() {
  case "$(arch)" in
    amd64) echo Xray-linux-64.zip ;;
    arm64) echo Xray-linux-arm64-v8a.zip ;;
  esac
}

hysteria_asset() {
  case "$(arch)" in
    amd64) echo hysteria-linux-amd64 ;;
    arm64) echo hysteria-linux-arm64 ;;
  esac
}

ensure_xray() {
  if [[ -x "$base_dir/xray" ]]; then
    return 0
  fi
  local asset zip_path
  asset="$(xray_asset)"
  zip_path="$base_dir/xray.zip"
  curl -fsSL -o "$zip_path" "https://github.com/XTLS/Xray-core/releases/latest/download/${asset}"
  python3 - "$zip_path" "$base_dir" <<'PY'
import os
import stat
import sys
import zipfile

zip_path, out_dir = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(zip_path) as archive:
    archive.extract("xray", out_dir)
path = os.path.join(out_dir, "xray")
os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
PY
  "$base_dir/xray" version >/dev/null
}

ensure_hysteria2() {
  if [[ -x "$base_dir/hysteria" ]]; then
    return 0
  fi
  local asset
  asset="$(hysteria_asset)"
  curl -fsSL -o "$base_dir/hysteria" "https://github.com/apernet/hysteria/releases/latest/download/${asset}"
  chmod +x "$base_dir/hysteria"
  "$base_dir/hysteria" version >/dev/null
}

ensure_hysteria_cert() {
  if [[ -f "$base_dir/hysteria.crt" && -f "$base_dir/hysteria.key" ]]; then
    return 0
  fi
  command -v openssl >/dev/null
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -keyout "$base_dir/hysteria.key" \
    -out "$base_dir/hysteria.crt" \
    -subj "/CN=mptunnel.lab" \
    -addext "subjectAltName=DNS:mptunnel.lab" >/dev/null 2>&1
}

write_xray_server() {
  local uuid="$1"
  local listen="$2"
  local port="$3"
  cat > "$base_dir/xray-server.json" <<JSON
{
  "log": {"loglevel": "warning"},
  "inbounds": [{
    "listen": "${listen}",
    "port": ${port},
    "protocol": "vmess",
    "settings": {"clients": [{"id": "${uuid}", "alterId": 0}]},
    "streamSettings": {"network": "tcp"}
  }],
  "outbounds": [{"protocol": "freedom", "settings": {}}]
}
JSON
}

write_xray_client() {
  local uuid="$1"
  local server="$2"
  local server_port="$3"
  local listen="$4"
  local listen_port="$5"
  cat > "$base_dir/xray-client.json" <<JSON
{
  "log": {"loglevel": "warning"},
  "inbounds": [{
    "listen": "${listen}",
    "port": ${listen_port},
    "protocol": "socks",
    "settings": {"udp": true}
  }],
  "outbounds": [{
    "protocol": "vmess",
    "settings": {
      "vnext": [{
        "address": "${server}",
        "port": ${server_port},
        "users": [{"id": "${uuid}", "alterId": 0, "security": "auto"}]
      }]
    },
    "streamSettings": {"network": "tcp"}
  }]
}
JSON
}

write_hysteria_server() {
  local password="$1"
  local listen="$2"
  local port="$3"
  ensure_hysteria_cert
  cat > "$base_dir/hysteria-server.yaml" <<YAML
listen: "${listen}:${port}"
tls:
  cert: "${base_dir}/hysteria.crt"
  key: "${base_dir}/hysteria.key"
  sniGuard: disable
auth:
  type: password
  password: "${password}"
YAML
}

write_hysteria_client() {
  local password="$1"
  local server="$2"
  local server_port="$3"
  local listen="$4"
  local listen_port="$5"
  cat > "$base_dir/hysteria-client.yaml" <<YAML
server: "${server}:${server_port}"
auth: "${password}"
tls:
  sni: mptunnel.lab
  insecure: true
socks5:
  listen: "${listen}:${listen_port}"
  disableUDP: false
YAML
}

case "${1:-}" in
  ensure-xray) ensure_xray ;;
  ensure-hysteria2) ensure_hysteria2 ;;
  write-xray-server) shift; write_xray_server "$@" ;;
  write-xray-client) shift; write_xray_client "$@" ;;
  write-hysteria-server) shift; write_hysteria_server "$@" ;;
  write-hysteria-client) shift; write_hysteria_client "$@" ;;
  run-xray-server) exec "$base_dir/xray" run -config "$base_dir/xray-server.json" ;;
  run-xray-client) exec "$base_dir/xray" run -config "$base_dir/xray-client.json" ;;
  run-hysteria-server) exec "$base_dir/hysteria" server -c "$base_dir/hysteria-server.yaml" ;;
  run-hysteria-client) exec "$base_dir/hysteria" client -c "$base_dir/hysteria-client.yaml" ;;
  *)
    echo "usage: $0 ensure-xray|ensure-hysteria2|write-xray-server|write-xray-client|write-hysteria-server|write-hysteria-client|run-xray-server|run-xray-client|run-hysteria-server|run-hysteria-client" >&2
    exit 2
    ;;
esac

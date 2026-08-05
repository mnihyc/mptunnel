#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
base_dir="${MPTUNNEL_LAB_BASELINE_DIR:-$repo_root/.tmp/lab/baselines}"
lock_file="$script_dir/baseline-lock.json"
mkdir -p "$base_dir"

verify_lock() {
  local expected="${MPTUNNEL_LAB_BASELINE_LOCK_SHA256:-}"
  if [[ -z "$expected" ]]; then
    return 0
  fi
  if [[ ! "$expected" =~ ^[0-9a-f]{64}$ ]]; then
    echo "invalid MPTUNNEL_LAB_BASELINE_LOCK_SHA256" >&2
    return 2
  fi
  verify_asset "$lock_file" "$expected"
}

arch() {
  case "$(uname -m)" in
    x86_64 | amd64) echo amd64 ;;
    aarch64 | arm64) echo arm64 ;;
    *) return 1 ;;
  esac
}

locked_asset() {
  local tool="$1"
  python3 - "$lock_file" "$tool" "$(arch)" <<'PY'
import os
from pathlib import Path
import sys

path, tool_name, architecture = sys.argv[1:]
sys.path.insert(0, str(Path(path).resolve().parent))
from result_enrichment import load_baseline_lock

lock = load_baseline_lock(
    path, os.environ.get("MPTUNNEL_LAB_BASELINE_LOCK_SHA256") or None
)
tool = lock["tools"][tool_name]
asset = tool["assets"][architecture]
values = (asset["name"], asset["url"], asset["sha256"])
print("\t".join(values))
PY
}

verify_asset() {
  local path="$1"
  local expected_sha256="$2"
  local actual_sha256
  actual_sha256="$(sha256sum "$path" | awk '{print $1}')"
  if [[ "$actual_sha256" != "$expected_sha256" ]]; then
    echo "baseline checksum mismatch for $path: expected $expected_sha256, got $actual_sha256" >&2
    return 1
  fi
}

download_locked_asset() {
  local url="$1"
  local destination="$2"
  local expected_sha256="$3"
  local temporary="${destination}.tmp"
  rm -f "$temporary"
  curl -fsSL -o "$temporary" "$url"
  verify_asset "$temporary" "$expected_sha256"
  mv "$temporary" "$destination"
}

ensure_xray() {
  local asset url expected_sha256 zip_path identity
  identity="$(locked_asset xray)"
  IFS=$'\t' read -r asset url expected_sha256 <<< "$identity"
  zip_path="$base_dir/$asset"
  if [[ ! -f "$zip_path" ]] || ! verify_asset "$zip_path" "$expected_sha256"; then
    rm -f "$zip_path"
    download_locked_asset "$url" "$zip_path" "$expected_sha256"
  fi

  # Rebuild the executable from the verified archive before every launch. A
  # separately cached extraction must never stand in for the locked artifact.
  python3 - "$zip_path" "$base_dir/xray" <<'PY'
import os
import stat
import sys
import tempfile
import zipfile

zip_path, destination = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(zip_path) as archive:
    member = archive.getinfo("xray")
    if member.is_dir():
        raise ValueError("xray archive member is not a file")
    payload = archive.read(member)
directory = os.path.dirname(destination)
descriptor, temporary = tempfile.mkstemp(prefix=".xray-", dir=directory)
try:
    with os.fdopen(descriptor, "wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    os.chmod(
        temporary,
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        | stat.S_IRGRP | stat.S_IXGRP
        | stat.S_IROTH | stat.S_IXOTH,
    )
    os.replace(temporary, destination)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PY
  "$base_dir/xray" version >/dev/null
}

ensure_hysteria2() {
  local asset url expected_sha256 identity
  identity="$(locked_asset hysteria2)"
  IFS=$'\t' read -r asset url expected_sha256 <<< "$identity"
  if [[ -x "$base_dir/hysteria" ]] \
    && verify_asset "$base_dir/hysteria" "$expected_sha256"; then
    "$base_dir/hysteria" version >/dev/null
    return 0
  fi
  rm -f "$base_dir/hysteria"
  download_locked_asset "$url" "$base_dir/hysteria" "$expected_sha256"
  chmod +x "$base_dir/hysteria"
  "$base_dir/hysteria" version >/dev/null
}

baseline_identity() {
  local tool="$1"
  python3 - "$lock_file" "$tool" "$(arch)" "$base_dir" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile

lock_path, tool_name, architecture, base_dir = sys.argv[1:]
sys.path.insert(0, str(Path(lock_path).resolve().parent))
from result_enrichment import load_baseline_lock

lock = load_baseline_lock(
    lock_path, os.environ.get("MPTUNNEL_LAB_BASELINE_LOCK_SHA256") or None
)
tool = lock["tools"][tool_name]
asset = tool["assets"][architecture]
executable = Path(base_dir) / ("xray" if tool_name == "xray" else "hysteria")
asset_path = Path(base_dir) / asset["name"] if tool_name == "xray" else executable

def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

asset_sha256 = sha256(asset_path)
if asset_sha256 != asset["sha256"]:
    raise ValueError(f"{tool_name} locked asset changed before identity capture")
executable_sha256 = sha256(executable)
if tool_name == "xray":
    with zipfile.ZipFile(asset_path) as archive:
        member_sha256 = hashlib.sha256(archive.read("xray")).hexdigest()
    if executable_sha256 != member_sha256:
        raise ValueError("xray executable does not match the locked archive member")
    provenance = "locked_archive_member"
else:
    member_sha256 = None
    if executable_sha256 != asset_sha256:
        raise ValueError("hysteria executable does not match the locked asset")
    provenance = "locked_executable_asset"

completed = subprocess.run(
    [str(executable), "version"],
    check=True,
    capture_output=True,
    text=True,
)
version_output = "\n".join(
    line.rstrip()
    for line in (completed.stdout + completed.stderr).splitlines()
    if line.strip()
)
identity = {
    "tool": tool_name,
    "release": tool["release"],
    "architecture": architecture,
    "asset_name": asset["name"],
    "asset_sha256": asset_sha256,
    "executable_name": executable.name,
    "executable_sha256": executable_sha256,
    "executable_provenance": provenance,
    "version_output": version_output[:2000],
    "verified": True,
}
if member_sha256 is not None:
    identity["archive_member_sha256"] = member_sha256
print(json.dumps(identity, separators=(",", ":"), sort_keys=True))
PY
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
  local bandwidth_up="${6:-}"
  local bandwidth_down="${7:-}"
  if [[ -n "$bandwidth_up" && -z "$bandwidth_down" ]] || \
     [[ -z "$bandwidth_up" && -n "$bandwidth_down" ]]; then
    echo "Hysteria2 Brutal requires both upload and download bandwidth" >&2
    return 2
  fi
  cat > "$base_dir/hysteria-client.yaml" <<YAML
server: "${server}:${server_port}"
auth: "${password}"
tls:
  sni: mptunnel.lab
  insecure: true
$(if [[ -n "$bandwidth_up" ]]; then
  cat <<BANDWIDTH
bandwidth:
  up: "${bandwidth_up}"
  down: "${bandwidth_down}"
  disableLossCompensation: false
BANDWIDTH
fi)
socks5:
  listen: "${listen}:${listen_port}"
  disableUDP: false
YAML
}

verify_lock

case "${1:-}" in
  ensure-xray) ensure_xray ;;
  ensure-hysteria2) ensure_hysteria2 ;;
  identity-xray) baseline_identity xray ;;
  identity-hysteria2) baseline_identity hysteria2 ;;
  write-xray-server) shift; write_xray_server "$@" ;;
  write-xray-client) shift; write_xray_client "$@" ;;
  write-hysteria-server) shift; write_hysteria_server "$@" ;;
  write-hysteria-client) shift; write_hysteria_client "$@" ;;
  run-xray-server) ensure_xray; exec "$base_dir/xray" run -config "$base_dir/xray-server.json" ;;
  run-xray-client) ensure_xray; exec "$base_dir/xray" run -config "$base_dir/xray-client.json" ;;
  run-hysteria-server) ensure_hysteria2; exec "$base_dir/hysteria" server -c "$base_dir/hysteria-server.yaml" ;;
  run-hysteria-client) ensure_hysteria2; exec "$base_dir/hysteria" client -c "$base_dir/hysteria-client.yaml" ;;
  *)
    echo "usage: $0 ensure-xray|ensure-hysteria2|identity-xray|identity-hysteria2|write-xray-server|write-xray-client|write-hysteria-server|write-hysteria-client|run-xray-server|run-xray-client|run-hysteria-server|run-hysteria-client" >&2
    exit 2
    ;;
esac

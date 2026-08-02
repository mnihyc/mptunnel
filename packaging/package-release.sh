#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "$repo_root"
umask 022
mkdir -p .tmp/python-cache .tmp/release/dependencies .tmp/system
export PYTHONPYCACHEPREFIX="$repo_root/.tmp/python-cache"
export TMPDIR="$repo_root/.tmp/system"

target=""
profile="release"
build=1
wintun_version="0.14.1"
wintun_sha256="07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51"
wintun_url="https://www.wintun.net/builds/wintun-${wintun_version}.zip"

usage() {
  cat <<'USAGE'
Usage: packaging/package-release.sh [OPTIONS]

Build one normalized MPTUNNEL release archive. The seven supported Rust
targets map to stable product-OS-architecture asset names. Windows packages
also contain Wintun.

Options:
  --target TRIPLE    Required normalized release target triple
  --profile PROFILE  Cargo profile (defaults to release)
  --no-build         Package an existing target artifact
  -h, --help         Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      target="${2:?missing target triple}"
      shift 2
      ;;
    --profile)
      profile="${2:?missing profile}"
      shift 2
      ;;
    --no-build)
      build=0
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$target" ]]; then
  echo "missing required --target release triple" >&2
  usage >&2
  exit 2
fi
if [[ ! "$profile" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]]; then
  echo "invalid Cargo profile: $profile" >&2
  exit 2
fi

for command_name in cargo python3; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required command not found: $command_name" >&2
    exit 1
  fi
done

contract="$(
  python3 -B packaging/tools/release_contract.py target \
    --target "$target" --format tsv
)"
IFS=$'\t' read -r package archive_name _archive_format binary target_os \
  <<< "$contract"

wintun_arch=""
if [[ "$target_os" == "windows" ]]; then
  case "$target" in
    x86_64-pc-windows-msvc)
      wintun_arch="amd64"
      ;;
    aarch64-pc-windows-msvc)
      wintun_arch="arm64"
      ;;
    *)
      echo "unsupported Windows target architecture: $target" >&2
      exit 2
      ;;
  esac
  for command_name in curl unzip; do
    if ! command -v "$command_name" >/dev/null 2>&1; then
      echo "required packaging command not found: $command_name" >&2
      exit 1
    fi
  done
fi

file_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    echo "required checksum command not found: sha256sum or shasum" >&2
    return 1
  fi
}

prepare_wintun_archive() {
  local cache_dir=".tmp/release/dependencies"
  local archive="${cache_dir}/wintun-${wintun_version}.zip"
  local download="${archive}.download.$$"

  mkdir -p "$cache_dir"
  if [[ -f "$archive" ]] && [[ "$(file_sha256 "$archive")" == "$wintun_sha256" ]]; then
    printf '%s\n' "$archive"
    return
  fi

  rm -f "$download"
  curl --fail --location --proto '=https' --tlsv1.2 \
    --output "$download" "$wintun_url"
  if [[ "$(file_sha256 "$download")" != "$wintun_sha256" ]]; then
    rm -f "$download"
    echo "Wintun ${wintun_version} checksum verification failed" >&2
    exit 1
  fi
  mv -f "$download" "$archive"
  printf '%s\n' "$archive"
}

if [[ "$build" -eq 1 ]]; then
  cargo build --locked --profile "$profile" --target "$target" --bin mptunnel
fi

cargo_target_dir="$(
  cargo metadata --locked --no-deps --format-version 1 | python3 -B -c '
import json
import sys

metadata = json.load(sys.stdin)
packages = [package for package in metadata["packages"] if package["name"] == "mptunnel"]
if len(packages) != 1:
    raise SystemExit("cargo metadata did not contain exactly one mptunnel package")
print(metadata["target_directory"])
'
)"

profile_dir="$profile"
if [[ "$profile" == "dev" ]]; then
  profile_dir="debug"
fi
target_dir="${cargo_target_dir}/${target}/${profile_dir}"
binary_path="${target_dir}/${binary}"
if [[ ! -s "$binary_path" ]]; then
  echo "built binary is missing or empty: $binary_path" >&2
  exit 1
fi

dist_dir=".tmp/release/dist"
stage="${dist_dir}/${package}"

release_files=(packaging/README.md)
release_examples=(examples/client.toml examples/server.toml)
for release_file in "${release_files[@]}" "${release_examples[@]}"; do
  if [[ ! -f "$release_file" ]]; then
    echo "required release file is missing: $release_file" >&2
    exit 1
  fi
done

rm -rf "$stage"
mkdir -p "$stage/examples"
cp "$binary_path" "$stage/"
cp packaging/README.md "$stage/README.md"
cp "${release_examples[@]}" "$stage/examples/"

case "$target_os" in
  linux)
    mkdir -p "$stage/service/systemd"
    cp packaging/service/systemd/mptunnel.service "$stage/service/systemd/"
    ;;
  windows)
    wintun_archive="$(prepare_wintun_archive)"
    unzip -p "$wintun_archive" "wintun/bin/${wintun_arch}/wintun.dll" \
      > "${stage}/wintun.dll"
    unzip -p "$wintun_archive" "wintun/LICENSE.txt" \
      > "${stage}/WINTUN-LICENSE.txt"
    ;;
  macos|android)
    :
    ;;
  *)
    echo "unsupported release OS: $target_os" >&2
    exit 2
    ;;
esac

mkdir -p "$dist_dir"
archive="${dist_dir}/${archive_name}"
rm -f "$archive"
python3 -B packaging/tools/build_release_archive.py \
  --stage "$stage" \
  --archive "$archive" \
  --target "$target" >/dev/null
rm -rf "$stage"
echo "$archive"

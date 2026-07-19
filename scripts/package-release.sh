#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "$repo_root"
umask 022

target=""
profile="release"
build=1
wintun_version="0.14.1"
wintun_sha256="07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51"
wintun_url="https://www.wintun.net/builds/wintun-${wintun_version}.zip"

usage() {
  cat <<'USAGE'
Usage: scripts/package-release.sh [OPTIONS]

Build and package mptunnel for a Rust target triple. Linux, macOS, Android,
and Windows targets are supported; Windows packages also contain Wintun.

Options:
  --target TRIPLE    Rust target triple (defaults to the rustc host)
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
  target="$(rustc -vV | awk '/host:/ {print $2}')"
fi
if [[ ! "$target" =~ ^[A-Za-z0-9][A-Za-z0-9_.+-]*$ ]]; then
  echo "invalid target triple: $target" >&2
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

binary="mptunnel"
wintun_arch=""
if [[ "$target" == *windows* ]]; then
  binary="mptunnel.exe"
  case "$target" in
    x86_64-*-windows-*)
      wintun_arch="amd64"
      ;;
    aarch64-*-windows-*)
      wintun_arch="arm64"
      ;;
    *)
      echo "unsupported Windows target architecture: $target" >&2
      exit 2
      ;;
  esac
  required_packaging_commands=(curl unzip zip)
else
  required_packaging_commands=(tar)
fi
for command_name in "${required_packaging_commands[@]}"; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "required packaging command not found: $command_name" >&2
    exit 1
  fi
done

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
  local cache_dir="target/release-dependencies"
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

metadata_fields="$(
  cargo metadata --locked --no-deps --format-version 1 | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)
packages = [package for package in metadata["packages"] if package["name"] == "mptunnel"]
if len(packages) != 1:
    raise SystemExit("cargo metadata did not contain exactly one mptunnel package")
print(packages[0]["version"], metadata["target_directory"], sep="\t")
'
)"
IFS=$'\t' read -r version cargo_target_dir <<< "$metadata_fields"
if [[ ! "$version" =~ ^[A-Za-z0-9][A-Za-z0-9.+-]*$ ]]; then
  echo "Cargo returned an unsafe package version: $version" >&2
  exit 1
fi

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

package="mptunnel-${version}-${target}"
dist_dir="dist"
stage="${dist_dir}/${package}"

release_files=(README.md RFC.md LICENSE THIRD_PARTY_LICENSES.html SECURITY.md CONTRIBUTING.md config.toml)
release_docs=(docs/ARCHITECTURE.md docs/OPERATIONS.md docs/PERFORMANCE.md)
release_examples=(examples/client.toml examples/server.toml)
release_assets=(docs/assets/dashboard.png)
for release_file in "${release_files[@]}" "${release_docs[@]}" "${release_examples[@]}" "${release_assets[@]}"; do
  if [[ ! -f "$release_file" ]]; then
    echo "required release file is missing: $release_file" >&2
    exit 1
  fi
done

rm -rf "$stage"
mkdir -p "$stage/docs/assets" "$stage/examples"
cp "$binary_path" "$stage/"
cp "${release_files[@]}" "$stage/"
cp "${release_docs[@]}" "$stage/docs/"
cp "${release_examples[@]}" "$stage/examples/"
cp "${release_assets[@]}" "$stage/docs/assets/"

if [[ -n "$wintun_arch" ]]; then
  wintun_archive="$(prepare_wintun_archive)"
  unzip -p "$wintun_archive" "wintun/bin/${wintun_arch}/wintun.dll" \
    > "${stage}/wintun.dll"
  unzip -p "$wintun_archive" "wintun/LICENSE.txt" \
    > "${stage}/WINTUN-LICENSE.txt"
fi

mkdir -p "$dist_dir"
if [[ "$target" == *windows* ]]; then
  archive="${dist_dir}/${package}.zip"
  rm -f "$archive" "${archive}.sha256"
  (cd "$dist_dir" && zip -qr "${package}.zip" "$package")
else
  archive="${dist_dir}/${package}.tar.gz"
  rm -f "$archive" "${archive}.sha256"
  tar -C "$dist_dir" -czf "$archive" "$package"
fi

archive_name="$(basename "$archive")"
printf '%s  %s\n' "$(file_sha256 "$archive")" "$archive_name" > "${archive}.sha256"
echo "$archive"

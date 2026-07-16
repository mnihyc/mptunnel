#!/usr/bin/env bash
set -euo pipefail

target=""
profile="release"
build=1
wintun_version="0.14.1"
wintun_sha256="07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51"
wintun_url="https://www.wintun.net/builds/wintun-${wintun_version}.zip"

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
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$target" ]]; then
  target="$(rustc -vV | awk '/host:/ {print $2}')"
fi

binary="mptunnel"
target_dir="target/${target}/${profile}"
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
fi

file_sha256() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
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
  cargo build --profile "$profile" --target "$target" --bin mptunnel
fi

version="$(cargo metadata --no-deps --format-version 1 | sed -n 's/.*"version":"\([^"]*\)".*/\1/p' | head -n1)"
package="mptunnel-${version}-${target}"
dist_dir="dist"
stage="${dist_dir}/${package}"

rm -rf "$stage"
mkdir -p "$stage"
cp "${target_dir}/${binary}" "$stage/"
cp README.md RFC.md LICENSE "$stage/"
cp -R docs "$stage/"

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
  rm -f "$archive"
  (cd "$dist_dir" && zip -qr "${package}.zip" "$package")
else
  archive="${dist_dir}/${package}.tar.gz"
  rm -f "$archive"
  tar -C "$dist_dir" -czf "$archive" "$package"
fi

if command -v sha256sum >/dev/null 2>&1; then
  sha256sum "$archive" > "${archive}.sha256"
else
  shasum -a 256 "$archive" > "${archive}.sha256"
fi

echo "$archive"

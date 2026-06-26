#!/usr/bin/env bash
set -euo pipefail

target=""
profile="release"
build=1

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
if [[ "$target" == *windows* ]]; then
  binary="mptunnel.exe"
fi

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
cp README.md LICENSE "$stage/"
cp -R docs "$stage/"

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

#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$project_root"

jni_root="${1:-${project_root}/.tmp/android/jniLibs}"
if [[ ! -d "$jni_root" ]]; then
  echo "Android JNI input directory does not exist: $jni_root" >&2
  exit 2
fi

contract="$(
  python3 -B packaging/tools/release_contract.py android-jni --format tsv
)"
IFS=$'\t' read -r package archive_name _archive_format <<<"$contract"

expected_entries=$'arm64-v8a\narm64-v8a/libmptunnel.so\nx86_64\nx86_64/libmptunnel.so'
actual_entries="$(
  cd "$jni_root"
  find . -mindepth 1 -printf '%P\n' | LC_ALL=C sort
)"
if [[ "$actual_entries" != "$expected_entries" ]]; then
  echo "Android JNI input must contain exactly arm64-v8a and x86_64 libraries" >&2
  diff -u <(printf '%s\n' "$expected_entries") <(printf '%s\n' "$actual_entries") >&2 || true
  exit 1
fi

for abi in arm64-v8a x86_64; do
  library="${jni_root}/${abi}/libmptunnel.so"
  if [[ ! -f "$library" || -L "$library" || ! -s "$library" ]]; then
    echo "Android JNI library is missing, empty, or a symlink: $library" >&2
    exit 1
  fi
done

dist_dir=".tmp/release/dist"
stage="${dist_dir}/${package}"
archive="${dist_dir}/${archive_name}"
rm -rf "$stage"
mkdir -p "$stage/arm64-v8a" "$stage/x86_64"
install -m 0644 LICENSE "$stage/LICENSE"
install -m 0644 packaging/android/README.md "$stage/README.md"
install -m 0644 \
  "$jni_root/arm64-v8a/libmptunnel.so" \
  "$stage/arm64-v8a/libmptunnel.so"
install -m 0644 \
  "$jni_root/x86_64/libmptunnel.so" \
  "$stage/x86_64/libmptunnel.so"

mkdir -p "$dist_dir"
rm -f "$archive"
python3 -B packaging/tools/build_release_archive.py \
  --stage "$stage" \
  --archive "$archive" \
  --bundle android-jni >/dev/null
python3 -B packaging/tools/verify_release_archive.py \
  --archive "$archive" \
  --bundle android-jni >/dev/null
rm -rf "$stage"
printf '%s\n' "$archive"

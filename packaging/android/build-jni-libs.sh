#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
output_root="${1:-${project_root}/.tmp/android/jniLibs}"
android_api="${MPTUNNEL_ANDROID_API:-24}"

if [[ ! "$android_api" =~ ^[0-9]+$ ]] || ((android_api < 24)); then
  echo "MPTUNNEL_ANDROID_API must be an integer >= 24" >&2
  exit 2
fi

ndk_root="${ANDROID_NDK_HOME:-${ANDROID_NDK_ROOT:-}}"
if [[ -z "$ndk_root" ]]; then
  if [[ -z "${ANDROID_HOME:-}" ]]; then
    echo "set ANDROID_NDK_HOME (or ANDROID_HOME with an installed NDK)" >&2
    exit 2
  fi
  mapfile -t installed_ndks < <(find "${ANDROID_HOME}/ndk" -mindepth 1 -maxdepth 1 -type d -print 2>/dev/null | sort -V)
  if ((${#installed_ndks[@]} == 0)); then
    echo "no Android NDK found under ANDROID_HOME/ndk" >&2
    exit 2
  fi
  ndk_root="${installed_ndks[-1]}"
fi

case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) host_tag="linux-x86_64" ;;
  Linux-aarch64) host_tag="linux-aarch64" ;;
  Darwin-x86_64) host_tag="darwin-x86_64" ;;
  Darwin-arm64) host_tag="darwin-x86_64" ;;
  *)
    echo "unsupported Android NDK host: $(uname -s)-$(uname -m)" >&2
    exit 2
    ;;
esac

toolchain="${ndk_root}/toolchains/llvm/prebuilt/${host_tag}/bin"
test -x "${toolchain}/llvm-ar"
test -x "${toolchain}/llvm-nm"
test -x "${toolchain}/llvm-readelf"
test -x "${toolchain}/llvm-strip"

targets=(
  "arm64-v8a:aarch64-linux-android:aarch64-linux-android"
  "x86_64:x86_64-linux-android:x86_64-linux-android"
)

jni_exports=(
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeDeleteProfile
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeIsRunning
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeStart
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeState
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeStatsJson
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeStop
  Java_com_v2ray_ang_mpp_MptunnelNative_nativeVersion
)

installed_targets="$(rustup target list --installed)"
for entry in "${targets[@]}"; do
  IFS=: read -r _ target _ <<<"$entry"
  if ! grep -Fxq "$target" <<<"$installed_targets"; then
    echo "missing Rust target $target; run: rustup target add $target" >&2
    exit 2
  fi
done

mkdir -p "$output_root"
android_rustflags="${RUSTFLAGS:-} -C link-arg=-Wl,-z,max-page-size=16384 -C link-arg=-Wl,-z,common-page-size=16384"

for entry in "${targets[@]}"; do
  IFS=: read -r abi target clang_prefix <<<"$entry"
  clang="${toolchain}/${clang_prefix}${android_api}-clang"
  test -x "$clang"

  target_env="${target^^}"
  target_env="${target_env//-/_}"
  cc_env="${target//-/_}"
  echo "Building MPTUNNEL JNI for ${abi} (${target}, API ${android_api})"
  env \
    "CARGO_TARGET_${target_env}_LINKER=${clang}" \
    "CC_${cc_env}=${clang}" \
    "AR_${cc_env}=${toolchain}/llvm-ar" \
    RUSTFLAGS="$android_rustflags" \
    cargo build \
      --manifest-path "${project_root}/Cargo.toml" \
      --locked \
      --release \
      --target "$target" \
      --lib

  source_library="${project_root}/target/${target}/release/libmptunnel.so"
  destination_directory="${output_root}/${abi}"
  mkdir -p "$destination_directory"
  install -m 0644 "$source_library" "${destination_directory}/libmptunnel.so"
  "${toolchain}/llvm-strip" --strip-unneeded "${destination_directory}/libmptunnel.so"

  header="$("${toolchain}/llvm-readelf" -h "${destination_directory}/libmptunnel.so")"
  printf '%s\n' "$header"
  grep -Fq 'Class:                             ELF64' <<<"$header"
  case "$abi" in
    arm64-v8a) grep -Fq 'Machine:                           AArch64' <<<"$header" ;;
    x86_64) grep -Fq 'Machine:                           Advanced Micro Devices X86-64' <<<"$header" ;;
    *) echo "unsupported Android ABI: $abi" >&2; exit 1 ;;
  esac

  dynamic_symbols="$(
    "${toolchain}/llvm-nm" -D --defined-only --format=posix \
      "${destination_directory}/libmptunnel.so" | awk '{print $1}'
  )"
  for symbol in "${jni_exports[@]}"; do
    if [[ "$(grep -Fxc "$symbol" <<<"$dynamic_symbols")" -ne 1 ]]; then
      echo "${abi}/libmptunnel.so does not export exactly one ${symbol}" >&2
      exit 1
    fi
  done

  mapfile -t load_alignments < <(
    "${toolchain}/llvm-readelf" -W -l "${destination_directory}/libmptunnel.so" |
      awk '$1 == "LOAD" { print $NF }'
  )
  aligned=true
  if ((${#load_alignments[@]} == 0)); then
    aligned=false
  fi
  for alignment in "${load_alignments[@]}"; do
    if ((alignment < 0x4000)); then
      aligned=false
    fi
  done
  if [[ "$aligned" != true ]]; then
    echo "${abi}/libmptunnel.so is not 16 KiB page aligned" >&2
    exit 1
  fi
done

echo "JNI libraries written to ${output_root}"

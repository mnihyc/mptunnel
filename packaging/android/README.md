# Android JNI package

`build-jni-libs.sh` builds `libmptunnel.so` for the two Android ABIs shipped by
MPTUNNEL and v2rayNG-MPP:

| Android ABI | Rust target |
| --- | --- |
| `arm64-v8a` | `aarch64-linux-android` |
| `x86_64` | `x86_64-linux-android` |

The minimum API defaults to 24 to match v2rayNG. The linker is explicitly
configured for 16 KiB ELF page alignment, and staged libraries are stripped
with the NDK toolchain (the unstripped Cargo outputs remain under `target/`).
Install the four Rust targets, set `ANDROID_NDK_HOME` (or `ANDROID_HOME`), then
run:

```sh
rustup target add aarch64-linux-android x86_64-linux-android
./packaging/android/build-jni-libs.sh ../v2rayNG-MPP/V2rayNG/app/libs
```

With no argument, output goes to `.tmp/android/jniLibs`. The optional
`MPTUNNEL_ANDROID_API` environment variable may select a newer API.

Tagged releases also contain `mptunnel-<version>-android-jni.tar.gz`. It has
this guide, the Apache-2.0 license, and exactly
`arm64-v8a/libmptunnel.so` and `x86_64/libmptunnel.so`. GitHub Actions alone
builds and publishes that release archive.

## Kotlin/JNI contract

The shared library exports static methods for
`com.v2ray.ang.mpp.MptunnelNative`:

- `nativeStart(String noBackupRoot, String profileId, String configTemplate,
  byte[][] materials, SocketProtector protector, long readyTimeoutMs): boolean`
- `nativeStop(long timeoutMs): boolean`
- `nativeIsRunning(): boolean`
- `nativeState(): String`
- `nativeVersion(): String`
- `nativeStatsJson(): String`
- `nativeDeleteProfile(String noBackupRoot, String profileId): boolean`

`SocketProtector.protect(int fd)` must synchronously call
`VpnService.protect(fd)`. Rejection or a JNI exception fails socket creation
closed.

`materials` has fixed order: MPP credential bytes, pinned-certificate PEM,
optional 32-byte transport secret, optional local-proxy password. The last two
entries are empty byte arrays when absent. The TOML must use these exact
semantic placeholders as string values in the existing file-reference fields:

- `@mptunnel-profile-credential@`
- `@mptunnel-profile-certificate@`
- `@mptunnel-profile-transport-secret@`
- `@mptunnel-local-proxy-password@`

The bridge requires the first two once, and requires each optional token once
exactly when its bytes are present. It substitutes fixed relative basenames,
rejects every other file reference or unresolved MPTUNNEL token, writes with
private permissions beneath `noBackupFilesDir/mptunnel/<profile-id>`, loads the
configuration and materials eagerly, then removes the plaintext files before
starting the runtime thread. Neither TOML nor material contents are logged.

`nativeStart` waits for MPTUNNEL's listener-readiness barrier, not server
reachability. Configurations for an Android catch-all VPN must avoid system DNS,
because those resolver sockets cannot pass through `VpnService.protect`.

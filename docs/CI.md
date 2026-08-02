# Build and verification procedure

This procedure is the supported source-to-binary path. Do not substitute a
local cross-compiler image for a native GitHub runner, and do not publish a
GitHub Actions artifact ZIP as a GitHub Release download.

## 1. Local Linux development gate

Use the repository-pinned Rust toolchain from `rust-toolchain.toml`:

```bash
mkdir -p .tmp/python-cache .tmp/system
export PYTHONPYCACHEPREFIX="$PWD/.tmp/python-cache"
export TMPDIR="$PWD/.tmp/system"
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --manifest-path crates/quinn-proto/Cargo.toml
python3 lab/validate_performance_declaration.py --check-registry
python3 -m unittest discover --start-directory lab --pattern 'test_*.py'
cargo test --locked --manifest-path lab/benchmarks/Cargo.toml
python3 -m unittest discover \
  --start-directory packaging/tools --pattern 'test_*.py'
find lab packaging -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
python3 packaging/tools/check_release_version_gate.py --self-test
```

Linux runtime, integration, and performance work runs locally. Privileged TUN,
route, DNS, and network-shaping tests run only in an isolated Linux namespace
or the existing lab containers; normal source checks do not alter host
networking.

For a local Linux release-package check:

```bash
packaging/package-release.sh --target x86_64-unknown-linux-musl
version="$(cargo metadata --locked --no-deps --format-version 1 | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])')"
python3 packaging/tools/verify_release_archive.py \
  --archive ".tmp/release/dist/mptunnel-${version}-linux-amd64.tar.gz" \
  --target x86_64-unknown-linux-musl
```

Cargo uses its standard ignored `target/` directory; standalone benchmark and
Quinn invocations may likewise create their standard ignored local `target/`.
All non-Cargo scratch—including lab evidence, Python caches, CI evidence, and
release staging—remains below `.tmp/`.

## 2. Authoritative cross-platform build

Push a normal source branch. `.github/workflows/ci.yml` then runs:

- the complete Linux quality gate;
- Linux musl builds for amd64 and arm64;
- native MSVC builds and tests on Windows amd64 and arm64 runners;
- native builds and tests on macOS amd64 and arm64 runners; and
- an Android arm64 build with NDK `27.3.13750724` and API 21.

The Rust compiler is pinned by `rust-toolchain.toml`. Windows and macOS results
are authoritative only on their native GitHub runners. Android is
authoritative only in the pinned GitHub Android lane. Local Linux builds do not
stand in for any of those three platforms.

These lanes prove native compilation, portable/native unit tests where the
runner can execute them, binary format, and package shape. They do not claim a
privileged clean-machine Wintun session, a signed Network Extension lifecycle,
or an Android application/VpnService lifecycle; those remain native product
evidence rather than cross-build evidence.

The `CI` workflow uploads no release bundle. Its result is build/test evidence
for the source commit.

## 3. Package validation without publication

Run the manually dispatched `Release Check` workflow for a proposed version.
It uses the same native/NDK target matrix, builds the normalized archives,
verifies their binary format and exact contents, and merges them only as
short-lived Actions evidence excluded from release assets. It never creates or edits a GitHub
Release.

## 4. Release publication

Only an intentional stable `v*` tag starts `.github/workflows/release.yml`.
That workflow:

1. repeats the quality and native/NDK package matrix;
2. transfers normalized archives through one-day Actions staging artifacts;
3. creates exactly seven versioned platform archives and one `version.json`;
4. records the release identity and each bundle's name and tag-specific GitHub
   URL in `version.json`, while GitHub supplies asset digests;
5. creates a draft using only the eight allowlisted public files;
6. downloads and verifies the draft from scratch; and
7. publishes only after the exact inventory and version index pass, at which
   point GitHub release immutability freezes the tag, title, notes, and assets.

Never upload the automatic Actions artifact ZIP, a target directory, raw build
logs, a separate checksum manifest, or provenance sidecars as user-facing
release assets. Never replace a published release; corrections require a new
version.

## Sources of truth

- Toolchain: `rust-toolchain.toml`
- Push/PR matrix: `.github/workflows/ci.yml`
- Non-publishing package check: `.github/workflows/release-check.yml`
- Publishing gate: `.github/workflows/release.yml`
- Patched Quinn source and lockfile: `crates/quinn-proto/`
- Release version gate: `packaging/tools/check_release_version_gate.py`
- Target and public-asset contract: `packaging/tools/release_contract.py`
- Deterministic archive builder: `packaging/tools/build_release_archive.py`
- Archive verifier: `packaging/tools/verify_release_archive.py`
- Release archive contract tests: `packaging/tools/test_release_archives.py`

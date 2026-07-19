#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "${script_dir}/.." && pwd)"
cd "$repo_root"

required_version="cargo-about 0.9.1"
if ! command -v cargo-about >/dev/null 2>&1; then
  echo "cargo-about 0.9.1 is required; install it with:" >&2
  echo "  cargo install --locked cargo-about --version 0.9.1 --features cli" >&2
  exit 1
fi
if [[ "$(cargo about --version)" != "$required_version" ]]; then
  echo "expected ${required_version}, got $(cargo about --version)" >&2
  exit 1
fi

cargo about generate --locked --all-features --fail \
  --output-file THIRD_PARTY_LICENSES.html about.hbs

# Dependency license files use mixed line endings and sometimes carry trailing
# spaces. Normalize generated markup so the committed notice is reproducible.
LC_ALL=C sed -i 's/[[:space:]]*$//' THIRD_PARTY_LICENSES.html

#!/usr/bin/env bash
set -euo pipefail

limit="${LINE_COUNT_WARN_LIMIT:-2000}"

if ! [[ "$limit" =~ ^[0-9]+$ ]] || (( limit == 0 )); then
  echo "LINE_COUNT_WARN_LIMIT must be a positive integer" >&2
  exit 2
fi

status=0
while IFS= read -r file; do
  lines="$(wc -l < "$file")"
  if (( lines > limit )); then
    printf 'warning: %s has %s lines, above %s-line maintainability warning threshold\n' \
      "$file" "$lines" "$limit" >&2
    status=1
  fi
done < <(
  git ls-files --cached --others --exclude-standard \
    '*.rs' '*.md' '*.toml' '*.yml' '*.yaml' '*.sh' '*.ps1' \
    ':!:lab/results/**' \
    ':!:target/**' \
    ':!:lab/benchmarks/target/**'
)

if (( status != 0 )); then
  echo "warning: split oversized files by cohesive ownership before expanding them further" >&2
fi

exit 0

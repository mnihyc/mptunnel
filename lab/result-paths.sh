#!/usr/bin/env bash

# Lab labels are repository-local temporary evidence. Every destination stays
# below the one ignored scratch owner instead of creating root or source-tree
# output directories.
normalize_lab_result_path() {
  local path="${1#./}"

  case "$path" in
    .tmp/lab/results|.tmp/lab/results/*)
      printf '%s\n' "$path"
      ;;
    /*)
      echo "lab result paths must stay below .tmp/lab/results: $path" >&2
      return 2
      ;;
    *)
      printf '.tmp/lab/results/%s\n' "$path"
      ;;
  esac
}

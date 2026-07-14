#!/usr/bin/env bash

# Lab labels are repository-local evidence, so relative destinations always
# belong below lab/results. Absolute paths remain available for external disks.
normalize_lab_result_path() {
  local path="${1#./}"

  case "$path" in
    /*|lab/results|lab/results/*)
      printf '%s\n' "$path"
      ;;
    *)
      printf 'lab/results/%s\n' "$path"
      ;;
  esac
}

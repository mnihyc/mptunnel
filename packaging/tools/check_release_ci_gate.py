#!/usr/bin/env python3
"""Require completed same-commit branch CI before publishing a new release."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def validate(pages: object, commit: str, repository: str) -> list[dict]:
    """Check every current run attempt; a separate green run cannot hide failure."""
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("release commit must be an exact 40-character commit ID")
    if not isinstance(pages, list) or not pages:
        raise ValueError("missing CI API pages")

    total = None
    runs = {}
    for page in pages:
        if not isinstance(page, dict):
            raise ValueError("malformed CI API page")
        count = page.get("total_count")
        if type(count) is not int or count < 0:
            raise ValueError("missing or invalid CI total_count")
        if total is not None and count != total:
            raise ValueError("CI run inventory changed during pagination")
        total = count
        if not isinstance(page.get("workflow_runs"), list):
            raise ValueError("missing CI workflow_runs")
        for run in page["workflow_runs"]:
            if not isinstance(run, dict):
                raise ValueError("malformed CI run")
            run_id = run.get("id")
            attempt = run.get("run_attempt")
            if type(run_id) is not int or run_id <= 0:
                raise ValueError("missing or invalid CI run ID")
            if type(attempt) is not int or attempt <= 0:
                raise ValueError(f"CI run {run_id} has no current attempt")
            if run_id in runs:
                raise ValueError(f"CI run {run_id} repeated during pagination")
            path = run.get("path")
            branch = run.get("head_branch")
            if (
                run.get("head_sha") != commit
                or run.get("event") != "push"
                or not isinstance(path, str)
                or path.split("@", 1)[0] != ".github/workflows/ci.yml"
                or not isinstance(branch, str)
                or not branch.strip()
            ):
                raise ValueError(f"CI run {run_id} does not match required branch CI")
            for field in ("repository", "head_repository"):
                identity = run.get(field)
                name = identity.get("full_name") if isinstance(identity, dict) else None
                if not isinstance(name, str) or name.casefold() != repository.casefold():
                    raise ValueError(f"CI run {run_id} has a different {field}")
            runs[run_id] = {
                "id": run_id,
                "run_attempt": attempt,
                "head_branch": branch,
                "status": run.get("status"),
                "conclusion": run.get("conclusion"),
            }

    if total == 0:
        raise ValueError(f"no branch-push CI exists for release commit {commit}")
    if len(runs) != total:
        raise ValueError(f"incomplete CI inventory: received {len(runs)} of {total} runs")
    blocked = [
        f"{run['id']} attempt {run['run_attempt']}: "
        f"{run['status']}/{run['conclusion']}"
        for run in runs.values()
        if run["status"] != "completed" or run["conclusion"] != "success"
    ]
    if blocked:
        raise ValueError("required branch CI is not successful: " + "; ".join(blocked))
    return sorted(runs.values(), key=lambda run: run["id"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs-file", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--repository", required=True)
    args = parser.parse_args()
    try:
        runs = validate(
            json.loads(args.runs_file.read_text(encoding="utf-8")),
            args.commit,
            args.repository,
        )
    except (OSError, ValueError) as error:
        print(f"Release publication refused: {error}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "commit": args.commit,
                "repository": args.repository,
                "workflow": ".github/workflows/ci.yml",
                "runs": runs,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

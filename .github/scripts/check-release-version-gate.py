#!/usr/bin/env python3
"""Validate a stable release tag against Cargo metadata and known release tags."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")


def parse_version(value: str) -> tuple[int, int, int]:
    match = VERSION_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"{value!r} is not a stable semantic version")
    return tuple(int(part) for part in match.groups())


def parse_tag(tag: str) -> tuple[int, int, int]:
    value = tag.strip()
    if not value.startswith("v"):
        raise ValueError(f"{tag!r} must start with 'v'")
    return parse_version(value[1:])


def validate(
    candidate_tag: str,
    package_version: str,
    known_tags: list[str],
) -> None:
    candidate = parse_tag(candidate_tag)
    package = parse_version(package_version)
    expected_tag = f"v{package_version.strip()}"
    if candidate != package or candidate_tag.strip() != expected_tag:
        raise ValueError(
            f"release tag {candidate_tag!r} must exactly match Cargo version {expected_tag!r}"
        )

    known: list[tuple[tuple[int, int, int], str]] = []
    for raw_tag in known_tags:
        tag = raw_tag.strip()
        if not tag:
            continue
        try:
            known.append((parse_tag(tag), tag))
        except ValueError:
            print(f"Ignoring non-stable release tag: {tag}", file=sys.stderr)

    if not known:
        print(f"No earlier stable release tag exists; allowing {candidate_tag}.")
        return

    newest_version, newest_tag = max(known)
    if candidate <= newest_version:
        relation = "matches" if candidate == newest_version else "is older than"
        raise ValueError(
            f"release tag {candidate_tag} {relation} existing release tag {newest_tag}; "
            "release versions are immutable and must increase"
        )
    print(f"Release tag {candidate_tag} is newer than existing release tag {newest_tag}.")


def self_test() -> None:
    assert parse_version("0.1.0") == (0, 1, 0)
    assert parse_tag("v12.3.4") == (12, 3, 4)
    validate("v0.1.0", "0.1.0", [])
    validate("v1.2.4", "1.2.4", ["v1.2.3", "notes"])

    rejected = [
        ("0.1.0", "0.1.0", []),
        ("v0.1.1", "0.1.0", []),
        ("v1.2.3", "1.2.3", ["v1.2.3"]),
        ("v1.2.3", "1.2.3", ["v1.2.4"]),
        ("v1.2.3-rc.1", "1.2.3-rc.1", []),
    ]
    for candidate, package, published in rejected:
        try:
            validate(candidate, package, published)
        except ValueError:
            continue
        raise AssertionError(f"version gate accepted invalid release {candidate}")
    print("release version gate self-test passed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-tag")
    parser.add_argument("--package-version")
    parser.add_argument("--known-tags-file", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return 0
    if not args.candidate_tag or not args.package_version:
        parser.error("--candidate-tag and --package-version are required")

    known_tags: list[str] = []
    if args.known_tags_file is not None:
        known_tags = args.known_tags_file.read_text(encoding="utf-8").splitlines()
    try:
        validate(args.candidate_tag, args.package_version, known_tags)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

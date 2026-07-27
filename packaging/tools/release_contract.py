#!/usr/bin/env python3
"""Canonical user-facing release asset and archive-content contract."""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import pathlib
import re
from collections.abc import Iterable


PRODUCT = "mptunnel"
CHECKSUM_MANIFEST = "SHA256SUMS"
COMMON_ARCHIVE_FILES = frozenset(
    {
        "LICENSE",
        "README.md",
        "examples/client.toml",
        "examples/server.toml",
    }
)


@dataclasses.dataclass(frozen=True)
class ReleaseTarget:
    rust_target: str
    os: str
    arch: str
    archive_format: str
    variant: str | None = None

    @property
    def slug(self) -> str:
        parts = [PRODUCT, self.os, self.arch]
        if self.variant is not None:
            parts.append(self.variant)
        return "-".join(parts)

    @property
    def package(self) -> str:
        return self.slug

    @property
    def archive_name(self) -> str:
        return f"{self.slug}.{self.archive_format}"

    @property
    def binary_name(self) -> str:
        return "mptunnel.exe" if self.os == "windows" else "mptunnel"

    @property
    def platform_files(self) -> frozenset[str]:
        if self.os == "linux":
            return frozenset({"service/systemd/mptunnel.service"})
        if self.os == "windows":
            return frozenset({"WINTUN-LICENSE.txt", "wintun.dll"})
        if self.os in {"macos", "android"}:
            return frozenset()
        raise AssertionError(f"unhandled release OS: {self.os}")

    @property
    def expected_files(self) -> frozenset[str]:
        return COMMON_ARCHIVE_FILES | self.platform_files | {self.binary_name}

    def as_dict(self) -> dict[str, str | None]:
        return {
            "target": self.rust_target,
            "os": self.os,
            "arch": self.arch,
            "variant": self.variant,
            "package": self.package,
            "archive_name": self.archive_name,
            "archive_format": self.archive_format,
            "binary_name": self.binary_name,
        }


RELEASE_TARGETS = (
    ReleaseTarget("x86_64-unknown-linux-musl", "linux", "amd64", "tar.gz"),
    ReleaseTarget("aarch64-unknown-linux-musl", "linux", "arm64", "tar.gz"),
    ReleaseTarget("x86_64-pc-windows-msvc", "windows", "amd64", "zip"),
    ReleaseTarget("aarch64-pc-windows-msvc", "windows", "arm64", "zip"),
    ReleaseTarget("x86_64-apple-darwin", "macos", "amd64", "zip"),
    ReleaseTarget("aarch64-apple-darwin", "macos", "arm64", "zip"),
    ReleaseTarget("aarch64-linux-android", "android", "arm64", "tar.gz"),
)
TARGET_BY_RUST_TRIPLE = {target.rust_target: target for target in RELEASE_TARGETS}
if len(TARGET_BY_RUST_TRIPLE) != len(RELEASE_TARGETS):
    raise AssertionError("release target triples must be unique")


class ReleaseContractError(ValueError):
    """A release target, asset set, or checksum manifest violated the contract."""


def target_for_rust_triple(rust_target: str) -> ReleaseTarget:
    try:
        return TARGET_BY_RUST_TRIPLE[rust_target]
    except KeyError as error:
        supported = ", ".join(target.rust_target for target in RELEASE_TARGETS)
        raise ReleaseContractError(
            f"unsupported release target {rust_target!r}; expected one of: {supported}"
        ) from error


def archive_asset_names() -> tuple[str, ...]:
    return tuple(sorted(target.archive_name for target in RELEASE_TARGETS))


def public_asset_names() -> tuple[str, ...]:
    return tuple(sorted((*archive_asset_names(), CHECKSUM_MANIFEST)))


def expected_directories(files: Iterable[str]) -> frozenset[str]:
    directories = {""}
    for file_name in files:
        parent = pathlib.PurePosixPath(file_name).parent
        while parent.as_posix() != ".":
            directories.add(parent.as_posix())
            parent = parent.parent
    return frozenset(directories)


def file_sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _inventory(directory: pathlib.Path) -> set[str]:
    if not directory.is_dir():
        raise ReleaseContractError(
            f"release asset directory does not exist: {directory}"
        )
    entries: set[str] = set()
    for path in directory.iterdir():
        if not path.is_file():
            raise ReleaseContractError(
                f"release asset directory contains a non-file entry: {path.name}"
            )
        entries.add(path.name)
    return entries


def _require_exact_inventory(directory: pathlib.Path, expected: set[str]) -> None:
    actual = _inventory(directory)
    missing = expected - actual
    extra = actual - expected
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if extra:
            details.append("unexpected: " + ", ".join(sorted(extra)))
        raise ReleaseContractError(
            "release asset inventory mismatch; " + "; ".join(details)
        )


CHECKSUM_LINE = re.compile(
    r"^(?P<digest>[0-9a-f]{64})  (?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)$"
)


def read_checksum_manifest(path: pathlib.Path) -> dict[str, str]:
    try:
        lines = path.read_text(encoding="ascii").splitlines()
    except (OSError, UnicodeError) as error:
        raise ReleaseContractError(
            f"cannot read checksum manifest {path}: {error}"
        ) from error
    if not lines:
        raise ReleaseContractError("checksum manifest is empty")
    checksums: dict[str, str] = {}
    for line in lines:
        match = CHECKSUM_LINE.fullmatch(line)
        if match is None:
            raise ReleaseContractError(f"invalid checksum manifest line: {line!r}")
        name = match.group("name")
        if name in checksums:
            raise ReleaseContractError(f"duplicate checksum entry: {name}")
        checksums[name] = match.group("digest")
    if list(checksums) != sorted(checksums):
        raise ReleaseContractError("checksum manifest entries are not sorted")
    expected = set(archive_asset_names())
    actual = set(checksums)
    if actual != expected:
        missing = expected - actual
        extra = actual - expected
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if extra:
            details.append("unexpected: " + ", ".join(sorted(extra)))
        raise ReleaseContractError(
            "checksum manifest inventory mismatch; " + "; ".join(details)
        )
    return checksums


def write_checksum_manifest(directory: pathlib.Path) -> pathlib.Path:
    manifest = directory / CHECKSUM_MANIFEST
    if manifest.exists():
        manifest.unlink()
    _require_exact_inventory(directory, set(archive_asset_names()))
    lines = [
        f"{file_sha256(directory / name)}  {name}" for name in archive_asset_names()
    ]
    manifest.write_text("\n".join(lines) + "\n", encoding="ascii", newline="\n")
    return manifest


def verify_public_assets(directory: pathlib.Path, *, with_checksums: bool) -> None:
    expected = (
        set(public_asset_names()) if with_checksums else set(archive_asset_names())
    )
    _require_exact_inventory(directory, expected)
    if not with_checksums:
        return
    checksums = read_checksum_manifest(directory / CHECKSUM_MANIFEST)
    for name, expected_digest in checksums.items():
        actual_digest = file_sha256(directory / name)
        if actual_digest != expected_digest:
            raise ReleaseContractError(
                f"checksum mismatch for {name}: expected {expected_digest}, got {actual_digest}"
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    target_parser = subparsers.add_parser("target", help="describe one Rust target")
    target_parser.add_argument("--target", required=True)
    target_parser.add_argument(
        "--format",
        choices=("json", "tsv"),
        default="json",
    )

    subparsers.add_parser("targets", help="list supported Rust target triples")

    assets_parser = subparsers.add_parser(
        "assets", help="list normalized public release assets"
    )
    assets_parser.add_argument("--without-checksums", action="store_true")

    write_parser = subparsers.add_parser(
        "write-checksums", help="write the one public checksum manifest"
    )
    write_parser.add_argument("--directory", type=pathlib.Path, required=True)

    verify_parser = subparsers.add_parser(
        "verify-assets", help="verify an exact normalized release asset directory"
    )
    verify_parser.add_argument("--directory", type=pathlib.Path, required=True)
    verify_parser.add_argument("--with-checksums", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        if args.command == "target":
            target = target_for_rust_triple(args.target)
            if args.format == "json":
                print(json.dumps(target.as_dict(), sort_keys=True))
            else:
                print(
                    "\t".join(
                        (
                            target.package,
                            target.archive_name,
                            target.archive_format,
                            target.binary_name,
                            target.os,
                        )
                    )
                )
        elif args.command == "targets":
            for target in RELEASE_TARGETS:
                print(target.rust_target)
        elif args.command == "assets":
            names = (
                archive_asset_names()
                if args.without_checksums
                else public_asset_names()
            )
            print("\n".join(names))
        elif args.command == "write-checksums":
            print(write_checksum_manifest(args.directory))
        elif args.command == "verify-assets":
            verify_public_assets(
                args.directory,
                with_checksums=args.with_checksums,
            )
            print(f"verified release assets in {args.directory}")
        else:
            raise AssertionError(f"unhandled command: {args.command}")
    except ReleaseContractError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()

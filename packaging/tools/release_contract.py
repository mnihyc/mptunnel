#!/usr/bin/env python3
"""Canonical user-facing release asset and archive-content contract."""

from __future__ import annotations

import argparse
import dataclasses
import json
import pathlib
import re
import tomllib
from collections.abc import Iterable


PRODUCT = "mptunnel"
VERSION_ASSET = "version.json"
REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[2]
VERSION_RE = re.compile(r"^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY_RE = re.compile(
    r"^[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?/"
    r"[A-Za-z0-9](?:[A-Za-z0-9_.-]*[A-Za-z0-9])?$"
)
COMMON_ARCHIVE_FILES = frozenset(
    {
        "README.md",
        "examples/client.toml",
        "examples/server.toml",
    }
)
class ReleaseContractError(ValueError):
    """A release identity, target, asset set, or archive violated the contract."""


def repository_package_version() -> str:
    try:
        manifest = tomllib.loads(
            (REPOSITORY_ROOT / "Cargo.toml").read_text(encoding="utf-8")
        )
        version = manifest["package"]["version"]
    except (OSError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
        raise ReleaseContractError(
            f"cannot read the MPTUNNEL package version: {error}"
        ) from error
    if not isinstance(version, str) or VERSION_RE.fullmatch(version) is None:
        raise ReleaseContractError(f"invalid MPTUNNEL package version: {version!r}")
    return version


PACKAGE_VERSION = repository_package_version()


@dataclasses.dataclass(frozen=True)
class ReleaseTarget:
    rust_target: str
    os: str
    arch: str
    archive_format: str
    variant: str | None = None

    @property
    def slug(self) -> str:
        parts = [PRODUCT, PACKAGE_VERSION, self.os, self.arch]
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
        if self.os == "macos":
            return frozenset()
        if self.os == "android":
            abi = {
                "aarch64-linux-android": "arm64-v8a",
                "x86_64-linux-android": "x86_64",
            }.get(self.rust_target)
            if abi is None:
                raise AssertionError(f"unhandled Android target: {self.rust_target}")
            return frozenset({f"{abi}/libmptunnel.so"})
        raise AssertionError(f"unhandled release OS: {self.os}")

    @property
    def expected_files(self) -> frozenset[str]:
        return COMMON_ARCHIVE_FILES | self.platform_files | {self.binary_name}

    def as_dict(self) -> dict[str, str | None]:
        return {
            "target": self.rust_target,
            "version": PACKAGE_VERSION,
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
    ReleaseTarget("x86_64-linux-android", "android", "x86_64", "tar.gz"),
)
TARGET_BY_RUST_TRIPLE = {target.rust_target: target for target in RELEASE_TARGETS}
if len(TARGET_BY_RUST_TRIPLE) != len(RELEASE_TARGETS):
    raise AssertionError("release target triples must be unique")


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
    return tuple(sorted((*archive_asset_names(), VERSION_ASSET)))


def expected_directories(files: Iterable[str]) -> frozenset[str]:
    directories = {""}
    for file_name in files:
        parent = pathlib.PurePosixPath(file_name).parent
        while parent.as_posix() != ".":
            directories.add(parent.as_posix())
            parent = parent.parent
    return frozenset(directories)


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


def version_metadata(*, tag: str, commit: str, repository: str) -> dict[str, object]:
    expected_tag = f"v{PACKAGE_VERSION}"
    if tag != expected_tag:
        raise ReleaseContractError(f"release tag must be {expected_tag!r}, got {tag!r}")
    if COMMIT_RE.fullmatch(commit) is None:
        raise ReleaseContractError(f"invalid release commit: {commit!r}")
    if REPOSITORY_RE.fullmatch(repository) is None:
        raise ReleaseContractError(f"invalid GitHub repository: {repository!r}")
    assets = [
        {
            "name": name,
            "download_url": (
                f"https://github.com/{repository}/releases/download/{tag}/{name}"
            ),
        }
        for name in archive_asset_names()
    ]
    return {
        "schema_version": 2,
        "product": PRODUCT,
        "version": PACKAGE_VERSION,
        "tag": tag,
        "commit": commit,
        "assets": assets,
    }


def write_version_asset(
    directory: pathlib.Path,
    *,
    tag: str,
    commit: str,
    repository: str,
) -> pathlib.Path:
    _require_exact_inventory(directory, set(archive_asset_names()))
    path = directory / VERSION_ASSET
    path.write_text(
        json.dumps(
            version_metadata(tag=tag, commit=commit, repository=repository),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return path


def verify_version_asset(path: pathlib.Path) -> None:
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ReleaseContractError(
            f"cannot read release version metadata {path}: {error}"
        ) from error
    expected_keys = {
        "schema_version",
        "product",
        "version",
        "tag",
        "commit",
        "assets",
    }
    if not isinstance(metadata, dict) or set(metadata) != expected_keys:
        raise ReleaseContractError(
            "release version metadata must contain exactly: "
            + ", ".join(sorted(expected_keys))
        )
    expected_values = {
        "schema_version": 2,
        "product": PRODUCT,
        "version": PACKAGE_VERSION,
        "tag": f"v{PACKAGE_VERSION}",
    }
    for key, expected in expected_values.items():
        if metadata[key] != expected:
            raise ReleaseContractError(
                f"release version metadata {key!r} must be {expected!r}, "
                f"got {metadata[key]!r}"
            )
    if (
        not isinstance(metadata["commit"], str)
        or COMMIT_RE.fullmatch(metadata["commit"]) is None
    ):
        raise ReleaseContractError("release version metadata has an invalid commit")
    assets = metadata["assets"]
    if not isinstance(assets, list) or len(assets) != len(archive_asset_names()):
        raise ReleaseContractError(
            "release version metadata must index exactly eight bundles"
        )
    expected_names = archive_asset_names()
    actual_names: list[str] = []
    repository: str | None = None
    for asset in assets:
        if not isinstance(asset, dict) or set(asset) != {"name", "download_url"}:
            raise ReleaseContractError(
                "each release bundle entry must contain only name and download_url"
            )
        name = asset["name"]
        download_url = asset["download_url"]
        if not isinstance(name, str) or not isinstance(download_url, str):
            raise ReleaseContractError("release bundle name and URL must be strings")
        suffix = f"/releases/download/v{PACKAGE_VERSION}/{name}"
        github_prefix = "https://github.com/"
        if not download_url.startswith(github_prefix) or not download_url.endswith(
            suffix
        ):
            raise ReleaseContractError(
                f"release bundle has an invalid tag-specific GitHub URL: {name}"
            )
        current_repository = download_url[len(github_prefix) : -len(suffix)]
        if REPOSITORY_RE.fullmatch(current_repository) is None:
            raise ReleaseContractError(
                f"release bundle has an invalid GitHub repository URL: {name}"
            )
        if repository is None:
            repository = current_repository
        elif current_repository != repository:
            raise ReleaseContractError(
                "release bundle URLs do not use one GitHub repository"
            )
        actual_names.append(name)
    if tuple(actual_names) != expected_names:
        raise ReleaseContractError(
            "release version metadata bundle inventory or order is invalid"
        )


def verify_public_assets(directory: pathlib.Path) -> None:
    _require_exact_inventory(directory, set(public_asset_names()))
    verify_version_asset(directory / VERSION_ASSET)


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

    subparsers.add_parser("assets", help="list normalized public release assets")

    version_parser = subparsers.add_parser(
        "write-version", help="write the public release version index"
    )
    version_parser.add_argument("--directory", type=pathlib.Path, required=True)
    version_parser.add_argument("--tag", required=True)
    version_parser.add_argument("--commit", required=True)
    version_parser.add_argument("--repository", required=True)

    verify_parser = subparsers.add_parser(
        "verify-assets", help="verify an exact normalized release asset directory"
    )
    verify_parser.add_argument("--directory", type=pathlib.Path, required=True)
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
            print("\n".join(public_asset_names()))
        elif args.command == "write-version":
            print(
                write_version_asset(
                    args.directory,
                    tag=args.tag,
                    commit=args.commit,
                    repository=args.repository,
                )
            )
        elif args.command == "verify-assets":
            verify_public_assets(args.directory)
            print(f"verified release assets in {args.directory}")
        else:
            raise AssertionError(f"unhandled command: {args.command}")
    except ReleaseContractError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Verify the exact, portable file manifest of one release archive."""

from __future__ import annotations

import argparse
import pathlib
import tarfile
import zipfile


COMMON_FILES = {
    "CONTRIBUTING.md",
    "LICENSE",
    "README.md",
    "RFC.md",
    "SECURITY.md",
    "config.toml",
    "docs/ARCHITECTURE.md",
    "docs/OPERATIONS.md",
    "docs/PERFORMANCE.md",
    "docs/assets/dashboard.png",
    "examples/client.toml",
    "examples/server.toml",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--version", required=True)
    return parser.parse_args()


def validate_member_name(raw_name: str, package: str) -> tuple[str, str]:
    if not raw_name or "\\" in raw_name:
        raise ValueError(f"archive member has a non-portable name: {raw_name!r}")
    name = raw_name.rstrip("/")
    path = pathlib.PurePosixPath(name)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"archive member has an unsafe name: {raw_name!r}")
    if not path.parts or path.parts[0] != package:
        raise ValueError(f"archive member is outside {package!r}: {raw_name!r}")
    relative = (
        pathlib.PurePosixPath(*path.parts[1:]).as_posix()
        if len(path.parts) > 1
        else ""
    )
    return name, relative


def read_tar(archive: pathlib.Path, package: str) -> tuple[set[str], set[str], int | None]:
    files: set[str] = set()
    directories: set[str] = set()
    binary_mode: int | None = None
    seen: set[str] = set()
    with tarfile.open(archive, "r:*") as bundle:
        for member in bundle.getmembers():
            name, relative = validate_member_name(member.name, package)
            if name in seen:
                raise ValueError(f"duplicate archive member: {name}")
            seen.add(name)
            if member.isdir():
                directories.add(relative)
            elif member.isfile():
                files.add(relative)
                if relative == "mptunnel":
                    binary_mode = member.mode
            else:
                raise ValueError(f"archive member is not a regular file or directory: {name}")
    return files, directories, binary_mode


def read_zip(archive: pathlib.Path, package: str) -> tuple[set[str], set[str], int | None]:
    files: set[str] = set()
    directories: set[str] = set()
    seen: set[str] = set()
    with zipfile.ZipFile(archive) as bundle:
        for member in bundle.infolist():
            name, relative = validate_member_name(member.filename, package)
            if name in seen:
                raise ValueError(f"duplicate archive member: {name}")
            seen.add(name)
            if member.is_dir():
                directories.add(relative)
            else:
                files.add(relative)
    return files, directories, None


def main() -> None:
    args = parse_args()
    if not args.archive.is_file():
        raise SystemExit(f"release archive does not exist: {args.archive}")

    windows = "windows" in args.target
    expected_suffix = ".zip" if windows else ".tar.gz"
    package = f"mptunnel-{args.version}-{args.target}"
    expected_name = f"{package}{expected_suffix}"
    if args.archive.name != expected_name:
        raise SystemExit(
            f"release archive name mismatch: expected {expected_name}, got {args.archive.name}"
        )

    try:
        if windows:
            files, directories, binary_mode = read_zip(args.archive, package)
            expected_files = COMMON_FILES | {
                "WINTUN-LICENSE.txt",
                "mptunnel.exe",
                "wintun.dll",
            }
        else:
            files, directories, binary_mode = read_tar(args.archive, package)
            expected_files = COMMON_FILES | {"mptunnel"}
    except (OSError, tarfile.TarError, zipfile.BadZipFile, ValueError) as error:
        raise SystemExit(str(error)) from error

    allowed_directories = {"", "docs", "docs/assets", "examples"}
    unexpected_directories = directories - allowed_directories
    if unexpected_directories:
        raise SystemExit(
            "release archive has unexpected directories: "
            + ", ".join(sorted(unexpected_directories))
        )
    missing = expected_files - files
    extra = files - expected_files
    if missing or extra:
        details = []
        if missing:
            details.append("missing: " + ", ".join(sorted(missing)))
        if extra:
            details.append("unexpected: " + ", ".join(sorted(extra)))
        raise SystemExit("release archive manifest mismatch; " + "; ".join(details))
    if not windows and (binary_mode is None or binary_mode & 0o111 == 0):
        raise SystemExit("Unix release binary is not executable in the archive")

    print(f"verified {args.archive}: {len(files)} files under {package}/")


if __name__ == "__main__":
    main()

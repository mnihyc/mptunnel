#!/usr/bin/env python3
"""Verify one normalized release archive, including deterministic metadata."""

from __future__ import annotations

import argparse
import dataclasses
import pathlib
import stat
import tarfile
import zipfile

from release_contract import (
    ReleaseContractError,
    ReleaseBundle,
    bundle_for_name,
    expected_directories,
    target_for_rust_triple,
)


@dataclasses.dataclass(frozen=True)
class ArchiveMember:
    relative: str
    mode: int
    size: int
    timestamp: int | tuple[int, int, int, int, int, int]


@dataclasses.dataclass(frozen=True)
class ArchiveInventory:
    files: dict[str, ArchiveMember]
    directories: dict[str, ArchiveMember]
    order: tuple[str, ...]


class ReleaseArchiveError(ReleaseContractError):
    """A release archive violated its filename, contents, or metadata contract."""


def validate_member_name(raw_name: str, package: str) -> tuple[str, str]:
    if not raw_name or "\\" in raw_name:
        raise ReleaseArchiveError(
            f"archive member has a non-portable name: {raw_name!r}"
        )
    name = raw_name.rstrip("/")
    path = pathlib.PurePosixPath(name)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ReleaseArchiveError(f"archive member has an unsafe name: {raw_name!r}")
    if not path.parts or path.parts[0] != package:
        raise ReleaseArchiveError(
            f"archive member is outside {package!r}: {raw_name!r}"
        )
    relative = (
        pathlib.PurePosixPath(*path.parts[1:]).as_posix() if len(path.parts) > 1 else ""
    )
    return name, relative


def read_tar(archive: pathlib.Path, package: str) -> ArchiveInventory:
    files: dict[str, ArchiveMember] = {}
    directories: dict[str, ArchiveMember] = {}
    order: list[str] = []
    seen: set[str] = set()
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            name, relative = validate_member_name(member.name, package)
            if name in seen:
                raise ReleaseArchiveError(f"duplicate archive member: {name}")
            seen.add(name)
            order.append(relative)
            value = ArchiveMember(relative, member.mode, member.size, member.mtime)
            if member.isdir():
                directories[relative] = value
            elif member.isfile():
                files[relative] = value
            else:
                raise ReleaseArchiveError(
                    f"archive member is not a regular file or directory: {name}"
                )
            if member.uid != 0 or member.gid != 0:
                raise ReleaseArchiveError(f"archive member has non-root IDs: {name}")
            if member.uname != "root" or member.gname != "root":
                raise ReleaseArchiveError(
                    f"archive member has unstable ownership: {name}"
                )
    return ArchiveInventory(files, directories, tuple(order))


def read_zip(archive: pathlib.Path, package: str) -> ArchiveInventory:
    files: dict[str, ArchiveMember] = {}
    directories: dict[str, ArchiveMember] = {}
    order: list[str] = []
    seen: set[str] = set()
    with zipfile.ZipFile(archive) as bundle:
        if bundle.comment:
            raise ReleaseArchiveError("release ZIP has an unexpected archive comment")
        for member in bundle.infolist():
            name, relative = validate_member_name(member.filename, package)
            if name in seen:
                raise ReleaseArchiveError(f"duplicate archive member: {name}")
            seen.add(name)
            order.append(relative)
            raw_mode = member.external_attr >> 16
            file_type = stat.S_IFMT(raw_mode)
            mode = stat.S_IMODE(raw_mode)
            value = ArchiveMember(relative, mode, member.file_size, member.date_time)
            if member.is_dir():
                if file_type not in {0, stat.S_IFDIR}:
                    raise ReleaseArchiveError(
                        f"ZIP directory has a non-directory file type: {name}"
                    )
                directories[relative] = value
            else:
                if file_type not in {0, stat.S_IFREG}:
                    raise ReleaseArchiveError(
                        f"ZIP member is not a regular file: {name}"
                    )
                files[relative] = value
            if member.create_system != 3:
                raise ReleaseArchiveError(
                    f"ZIP member lacks portable Unix metadata: {name}"
                )
            if member.extra or member.comment:
                raise ReleaseArchiveError(
                    f"ZIP member has unstable extra metadata: {name}"
                )
    return ArchiveInventory(files, directories, tuple(order))


def verify_inventory(inventory: ArchiveInventory, target: ReleaseBundle) -> None:
    expected_files = set(target.expected_files)
    expected_dirs = set(expected_directories(expected_files))
    actual_files = set(inventory.files)
    actual_dirs = set(inventory.directories)
    missing_files = expected_files - actual_files
    extra_files = actual_files - expected_files
    missing_dirs = expected_dirs - actual_dirs
    extra_dirs = actual_dirs - expected_dirs
    if missing_files or extra_files or missing_dirs or extra_dirs:
        details = []
        if missing_files:
            details.append("missing files: " + ", ".join(sorted(missing_files)))
        if extra_files:
            details.append("unexpected files: " + ", ".join(sorted(extra_files)))
        if missing_dirs:
            details.append("missing directories: " + ", ".join(sorted(missing_dirs)))
        if extra_dirs:
            details.append("unexpected directories: " + ", ".join(sorted(extra_dirs)))
        raise ReleaseArchiveError(
            "release archive manifest mismatch; " + "; ".join(details)
        )

    expected_order = ("", *sorted((expected_files | expected_dirs) - {""}))
    if inventory.order != expected_order:
        raise ReleaseArchiveError("release archive members are not in canonical order")

    timestamps = {
        member.timestamp
        for member in (*inventory.files.values(), *inventory.directories.values())
    }
    if len(timestamps) != 1:
        raise ReleaseArchiveError(
            "release archive member timestamps are not normalized"
        )

    for relative, member in inventory.directories.items():
        if member.mode != 0o755:
            raise ReleaseArchiveError(
                f"release archive directory mode is not 0755: {relative or target.package}"
            )
    for relative, member in inventory.files.items():
        expected_mode = (
            0o755
            if relative == target.binary_name and target.os != "windows"
            else 0o644
        )
        if member.mode != expected_mode:
            raise ReleaseArchiveError(
                f"release archive file mode is not {expected_mode:04o}: {relative}"
            )
        if member.size <= 0:
            raise ReleaseArchiveError(f"release archive file is empty: {relative}")


def verify_archive(archive: pathlib.Path, target: ReleaseBundle) -> None:
    if not archive.is_file():
        raise ReleaseArchiveError(f"release archive does not exist: {archive}")
    if archive.name != target.archive_name:
        raise ReleaseArchiveError(
            f"release archive name mismatch: expected {target.archive_name}, "
            f"got {archive.name}"
        )
    try:
        if target.archive_format == "zip":
            inventory = read_zip(archive, target.package)
        elif target.archive_format == "tar.gz":
            inventory = read_tar(archive, target.package)
        else:
            raise AssertionError(f"unhandled archive format: {target.archive_format}")
        verify_inventory(inventory, target)
    except (OSError, tarfile.TarError, zipfile.BadZipFile) as error:
        raise ReleaseArchiveError(str(error)) from error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--target")
    selector.add_argument("--bundle", choices=("android-jni",))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        target = (
            target_for_rust_triple(args.target)
            if args.target is not None
            else bundle_for_name(args.bundle)
        )
        verify_archive(args.archive, target)
    except ReleaseContractError as error:
        raise SystemExit(str(error)) from error
    print(
        f"verified {args.archive}: {len(target.expected_files)} files "
        f"under {target.package}/"
    )


if __name__ == "__main__":
    main()

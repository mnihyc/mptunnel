#!/usr/bin/env python3
"""Build one byte-reproducible release archive from an exact staging tree."""

from __future__ import annotations

import argparse
import gzip
import os
import pathlib
import stat
import tarfile
import time
import zipfile

from release_contract import (
    ReleaseContractError,
    ReleaseBundle,
    bundle_for_name,
    expected_directories,
    target_for_rust_triple,
)


DEFAULT_SOURCE_DATE_EPOCH = 315_532_800  # 1980-01-01, ZIP's minimum timestamp.
MAX_ZIP_EPOCH = 4_354_819_198  # 2107-12-31 23:59:58 UTC.


def archive_mode(target: ReleaseBundle, relative: str, *, directory: bool) -> int:
    if directory:
        return 0o755
    if relative == target.binary_name and target.os != "windows":
        return 0o755
    return 0o644


def validate_stage(stage: pathlib.Path, target: ReleaseBundle) -> None:
    if not stage.is_dir():
        raise ReleaseContractError(f"release staging directory does not exist: {stage}")
    if stage.name != target.package:
        raise ReleaseContractError(
            f"release staging directory must be named {target.package!r}, "
            f"got {stage.name!r}"
        )
    files: set[str] = set()
    directories = {""}
    for path in stage.rglob("*"):
        if path.is_symlink():
            raise ReleaseContractError(
                f"release staging tree contains a symlink: {path}"
            )
        relative = path.relative_to(stage).as_posix()
        if path.is_dir():
            directories.add(relative)
        elif path.is_file():
            if not path.stat().st_size:
                raise ReleaseContractError(f"release staging file is empty: {relative}")
            files.add(relative)
        else:
            raise ReleaseContractError(
                f"release staging tree contains a non-file entry: {path}"
            )
    expected_files = set(target.expected_files)
    expected_dirs = set(expected_directories(expected_files))
    missing_files = expected_files - files
    extra_files = files - expected_files
    missing_dirs = expected_dirs - directories
    extra_dirs = directories - expected_dirs
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
        raise ReleaseContractError(
            "release staging manifest mismatch; " + "; ".join(details)
        )


def stage_entries(stage: pathlib.Path) -> list[tuple[pathlib.Path, str, bool]]:
    entries = [(stage, stage.name, True)]
    for path in sorted(
        stage.rglob("*"), key=lambda item: item.relative_to(stage).as_posix()
    ):
        archive_name = f"{stage.name}/{path.relative_to(stage).as_posix()}"
        entries.append((path, archive_name, path.is_dir()))
    return entries


def build_tar_gz(
    stage: pathlib.Path,
    archive: pathlib.Path,
    target: ReleaseBundle,
    epoch: int,
) -> None:
    with archive.open("wb") as output:
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=9,
            fileobj=output,
            mtime=epoch,
        ) as compressed:
            with tarfile.open(
                fileobj=compressed,
                mode="w",
                format=tarfile.USTAR_FORMAT,
            ) as bundle:
                for path, archive_name, directory in stage_entries(stage):
                    relative = (
                        ""
                        if archive_name == stage.name
                        else archive_name.removeprefix(f"{stage.name}/")
                    )
                    info = tarfile.TarInfo(
                        f"{archive_name}/" if directory else archive_name
                    )
                    info.uid = 0
                    info.gid = 0
                    info.uname = "root"
                    info.gname = "root"
                    info.mtime = epoch
                    info.mode = archive_mode(target, relative, directory=directory)
                    if directory:
                        info.type = tarfile.DIRTYPE
                        bundle.addfile(info)
                    else:
                        info.size = path.stat().st_size
                        with path.open("rb") as source:
                            bundle.addfile(info, source)


def build_zip(
    stage: pathlib.Path,
    archive: pathlib.Path,
    target: ReleaseBundle,
    epoch: int,
) -> None:
    zip_epoch = min(max(epoch, DEFAULT_SOURCE_DATE_EPOCH), MAX_ZIP_EPOCH)
    timestamp = time.gmtime(zip_epoch)[:6]
    with zipfile.ZipFile(
        archive,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
        strict_timestamps=True,
    ) as bundle:
        for path, archive_name, directory in stage_entries(stage):
            relative = (
                ""
                if archive_name == stage.name
                else archive_name.removeprefix(f"{stage.name}/")
            )
            member_name = f"{archive_name}/" if directory else archive_name
            info = zipfile.ZipInfo(member_name, date_time=timestamp)
            info.create_system = 3
            mode = archive_mode(target, relative, directory=directory)
            file_type = stat.S_IFDIR if directory else stat.S_IFREG
            info.external_attr = (file_type | mode) << 16
            info.compress_type = (
                zipfile.ZIP_STORED if directory else zipfile.ZIP_DEFLATED
            )
            if directory:
                bundle.writestr(info, b"")
            else:
                bundle.writestr(
                    info,
                    path.read_bytes(),
                    compress_type=zipfile.ZIP_DEFLATED,
                    compresslevel=9,
                )


def build_archive(
    stage: pathlib.Path,
    archive: pathlib.Path,
    target: ReleaseBundle,
    epoch: int,
) -> None:
    validate_stage(stage, target)
    if archive.name != target.archive_name:
        raise ReleaseContractError(
            f"release archive must be named {target.archive_name!r}, "
            f"got {archive.name!r}"
        )
    if epoch < 0:
        raise ReleaseContractError("SOURCE_DATE_EPOCH must not be negative")
    archive.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive.with_name(f".{archive.name}.tmp-{os.getpid()}")
    temporary.unlink(missing_ok=True)
    try:
        if target.archive_format == "tar.gz":
            build_tar_gz(stage, temporary, target, epoch)
        elif target.archive_format == "zip":
            build_zip(stage, temporary, target, epoch)
        else:
            raise AssertionError(f"unhandled archive format: {target.archive_format}")
        os.replace(temporary, archive)
    finally:
        temporary.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=pathlib.Path, required=True)
    parser.add_argument("--archive", type=pathlib.Path, required=True)
    selector = parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--target")
    selector.add_argument("--bundle", choices=("android-jni",))
    parser.add_argument(
        "--source-date-epoch",
        type=int,
        default=int(os.environ.get("SOURCE_DATE_EPOCH", DEFAULT_SOURCE_DATE_EPOCH)),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        target = (
            target_for_rust_triple(args.target)
            if args.target is not None
            else bundle_for_name(args.bundle)
        )
        build_archive(
            args.stage,
            args.archive,
            target,
            args.source_date_epoch,
        )
    except ReleaseContractError as error:
        raise SystemExit(str(error)) from error
    print(args.archive)


if __name__ == "__main__":
    main()

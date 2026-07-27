#!/usr/bin/env python3
"""Seal or verify one immutable-by-digest performance evidence directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any, Sequence


SCHEMA_VERSION = 1
KIND = "mptunnel.performance-evidence-bundle"
DEFAULT_MANIFEST = "EVIDENCE-MANIFEST.json"


class EvidenceBundleError(ValueError):
    """Evidence cannot be sealed or does not match its seal."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _atomic_write(path: Path, document: dict[str, Any]) -> None:
    payload = (
        json.dumps(
            document,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            "w",
            dir=path.parent,
            encoding="utf-8",
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary:
            temporary_name = temporary.name
            temporary.write(payload)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.replace(temporary_name, path)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)


def _artifact_rows(root: Path, manifest_name: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if relative.as_posix() == manifest_name:
            continue
        if path.is_symlink():
            raise EvidenceBundleError(
                f"symlinks are forbidden in evidence bundles: {relative.as_posix()}"
            )
        if not path.is_file():
            continue
        rows.append(
            {
                "path": relative.as_posix(),
                "size_bytes": path.stat().st_size,
                "sha256": _sha256_file(path),
            }
        )
    if not rows:
        raise EvidenceBundleError("evidence directory contains no artifacts")
    return rows


def _bundle_digest(artifacts: list[dict[str, Any]]) -> str:
    return hashlib.sha256(_canonical_bytes(artifacts)).hexdigest()


def seal(root_value: str | Path, manifest_name: str = DEFAULT_MANIFEST) -> dict[str, Any]:
    root = Path(root_value).resolve()
    if not root.is_dir():
        raise EvidenceBundleError(f"evidence root is not a directory: {root}")
    if Path(manifest_name).name != manifest_name:
        raise EvidenceBundleError("manifest name must be one filename")
    manifest_path = root / manifest_name
    if manifest_path.exists():
        raise EvidenceBundleError(
            f"refusing to overwrite existing evidence seal: {manifest_path}"
        )
    artifacts = _artifact_rows(root, manifest_name)
    document = {
        "schema_version": SCHEMA_VERSION,
        "kind": KIND,
        "artifact_count": len(artifacts),
        "total_bytes": sum(row["size_bytes"] for row in artifacts),
        "artifacts": artifacts,
        "bundle_sha256": _bundle_digest(artifacts),
    }
    _atomic_write(manifest_path, document)
    return document


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise EvidenceBundleError(f"cannot load evidence manifest {path}: {error}") from error
    if not isinstance(value, dict):
        raise EvidenceBundleError("evidence manifest must be one JSON object")
    expected = {
        "schema_version",
        "kind",
        "artifact_count",
        "total_bytes",
        "artifacts",
        "bundle_sha256",
    }
    if set(value) != expected:
        raise EvidenceBundleError("evidence manifest fields do not match schema")
    if value["schema_version"] != SCHEMA_VERSION or value["kind"] != KIND:
        raise EvidenceBundleError("unsupported evidence manifest identity")
    return value


def verify(
    root_value: str | Path, manifest_name: str = DEFAULT_MANIFEST
) -> dict[str, Any]:
    root = Path(root_value).resolve()
    manifest_path = root / manifest_name
    document = _load_manifest(manifest_path)
    actual = _artifact_rows(root, manifest_name)
    if document["artifacts"] != actual:
        raise EvidenceBundleError(
            "evidence artifacts differ from the sealed paths, sizes, or hashes"
        )
    if document["artifact_count"] != len(actual):
        raise EvidenceBundleError("evidence artifact_count does not match")
    if document["total_bytes"] != sum(row["size_bytes"] for row in actual):
        raise EvidenceBundleError("evidence total_bytes does not match")
    digest = _bundle_digest(actual)
    if document["bundle_sha256"] != digest:
        raise EvidenceBundleError("evidence bundle_sha256 does not match")
    return document


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("seal", "verify"):
        command_parser = subparsers.add_parser(command)
        command_parser.add_argument("root")
        command_parser.add_argument("--manifest-name", default=DEFAULT_MANIFEST)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "seal":
            document = seal(args.root, args.manifest_name)
        else:
            document = verify(args.root, args.manifest_name)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "kind": document["kind"],
                    "bundle_sha256": document["bundle_sha256"],
                    "artifact_count": document["artifact_count"],
                },
                sort_keys=True,
            )
        )
        return 0
    except EvidenceBundleError as error:
        print(f"evidence bundle error: {error}", file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

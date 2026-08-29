#!/usr/bin/env python3
"""Capture an anonymized, versioned lab host and source snapshot.

The performance lab needs enough information to reject noisy or irreproducible
runs without retaining hostnames, usernames, environment variables, container
names/IDs, absolute source paths, or source contents.  This module therefore
stores machine characteristics and cryptographic identities only.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterable, Mapping, Sequence


HOST_SNAPSHOT_SCHEMA_VERSION = 1
HOST_VALIDITY_RULES_VERSION = 2
HOST_SNAPSHOT_KIND = "mptunnel.lab.host-snapshot"
SOURCE_SNAPSHOT_ALGORITHM = "git-visible-tree-sha256-v1"

MAX_LOAD1_PER_AFFINITY_CPU = 0.5
MAX_RUNNABLE_PER_AFFINITY_CPU = 1.0
MIN_MEMORY_AVAILABLE_RATIO = 0.15
MAX_THERMAL_MILLICELSIUS = 85_000

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{12,64}")
_TRUTHY = {"1", "true", "yes"}
_FALSY = {"", "0", "false", "no"}

CommandRunner = Callable[[Sequence[str], Path | None], bytes | str]


class SnapshotError(RuntimeError):
    """Raised when a required, anonymized snapshot field cannot be captured."""


def _command_stdout(arguments: Sequence[str], cwd: Path | None = None) -> bytes:
    try:
        completed = subprocess.run(
            list(arguments),
            cwd=cwd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        command = Path(arguments[0]).name if arguments else "command"
        raise SnapshotError(f"{command} identity capture failed") from exc
    return completed.stdout


def _run_bytes(
    runner: CommandRunner,
    arguments: Sequence[str],
    cwd: Path | None = None,
) -> bytes:
    try:
        value = runner(arguments, cwd)
    except SnapshotError:
        raise
    except Exception as exc:
        command = Path(arguments[0]).name if arguments else "command"
        raise SnapshotError(f"{command} identity capture failed") from exc
    return value.encode("utf-8") if isinstance(value, str) else bytes(value)


def _decode_command_output(value: bytes, field: str) -> str:
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SnapshotError(f"{field} is not UTF-8") from exc
    if "\x00" in decoded:
        raise SnapshotError(f"{field} contains a NUL byte")
    return decoded.strip()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    try:
        with Path(path).open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(chunk)
    except OSError as exc:
        raise SnapshotError("required identity file cannot be read") from exc
    return digest.hexdigest()


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError) as exc:
        raise SnapshotError("required host state cannot be read") from exc


def _optional_text(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None


def _optional_integer(path: Path) -> int | None:
    value = _optional_text(path)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def compact_cpu_set(cpu_ids: Iterable[int]) -> str:
    """Return a deterministic Linux-style compact CPU set."""

    values = sorted(set(cpu_ids))
    if not values:
        return ""
    ranges: list[str] = []
    start = previous = values[0]
    for value in values[1:]:
        if value == previous + 1:
            previous = value
            continue
        ranges.append(str(start) if start == previous else f"{start}-{previous}")
        start = previous = value
    ranges.append(str(start) if start == previous else f"{start}-{previous}")
    return ",".join(ranges)


def _capture_cpu_models(cpuinfo: str) -> list[str]:
    fields = ("model name", "hardware", "processor")
    found: list[str] = []
    for line in cpuinfo.splitlines():
        name, separator, value = line.partition(":")
        if separator and name.strip().lower() in fields and value.strip():
            normalized = " ".join(value.strip().split())
            if not normalized.isdecimal():
                found.append(normalized[:256])
    return sorted(set(found))


def _capture_load(proc_root: Path, affinity_count: int) -> dict[str, Any]:
    fields = _read_text(proc_root / "loadavg").split()
    if len(fields) < 4:
        raise SnapshotError("host load state is incomplete")
    try:
        load1, load5, load15 = (float(fields[index]) for index in range(3))
        runnable_text, process_text = fields[3].split("/", 1)
        runnable = int(runnable_text)
        processes = int(process_text)
    except (ValueError, ZeroDivisionError) as exc:
        raise SnapshotError("host load state is invalid") from exc
    if min(load1, load5, load15, runnable, processes) < 0 or affinity_count < 1:
        raise SnapshotError("host load state is invalid")
    return {
        "load1": load1,
        "load5": load5,
        "load15": load15,
        "load1_per_affinity_cpu": round(load1 / affinity_count, 6),
        "runnable": runnable,
        "processes": processes,
        "runnable_per_affinity_cpu": round(runnable / affinity_count, 6),
    }


def _capture_memory(proc_root: Path) -> dict[str, Any]:
    values: dict[str, int] = {}
    for line in _read_text(proc_root / "meminfo").splitlines():
        name, separator, raw_value = line.partition(":")
        if not separator:
            continue
        parts = raw_value.split()
        if not parts:
            continue
        try:
            value = int(parts[0])
        except ValueError:
            continue
        if len(parts) > 1 and parts[1] == "kB":
            value *= 1024
        values[name] = value
    required = ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree")
    if any(name not in values for name in required):
        raise SnapshotError("host memory state is incomplete")
    total = values["MemTotal"]
    available = values["MemAvailable"]
    if total <= 0 or available < 0 or available > total:
        raise SnapshotError("host memory state is invalid")
    return {
        "total_bytes": total,
        "available_bytes": available,
        "available_ratio": round(available / total, 6),
        "swap_total_bytes": values["SwapTotal"],
        "swap_free_bytes": values["SwapFree"],
    }


def _capture_frequency(
    sys_root: Path, affinity: Sequence[int]
) -> dict[str, Any]:
    policies: list[dict[str, Any]] = []
    for cpu_id in affinity:
        base = (
            sys_root
            / "devices"
            / "system"
            / "cpu"
            / f"cpu{cpu_id}"
            / "cpufreq"
        )
        if not base.is_dir():
            continue
        policy: dict[str, Any] = {"cpus": str(cpu_id)}
        governor = _optional_text(base / "scaling_governor")
        if governor:
            policy["governor"] = governor[:64]
        for field, filename in (
            ("current_khz", "scaling_cur_freq"),
            ("minimum_khz", "scaling_min_freq"),
            ("maximum_khz", "scaling_max_freq"),
        ):
            value = _optional_integer(base / filename)
            if value is not None and value >= 0:
                policy[field] = value
        if len(policy) > 1:
            policies.append(policy)
    return {
        "exposed": bool(policies),
        "governors": sorted(
            {
                str(policy["governor"])
                for policy in policies
                if "governor" in policy
            }
        ),
        "policies": policies,
    }


def _capture_thermal(sys_root: Path) -> dict[str, Any]:
    zones: list[dict[str, Any]] = []
    thermal_root = sys_root / "class" / "thermal"
    for zone_path in sorted(thermal_root.glob("thermal_zone*")):
        temperature = _optional_integer(zone_path / "temp")
        if temperature is None:
            continue
        zone: dict[str, Any] = {
            "zone": zone_path.name[:64],
            "temp_millicelsius": temperature,
        }
        zone_type = _optional_text(zone_path / "type")
        if zone_type:
            zone["type"] = " ".join(zone_type.split())[:128]
        zones.append(zone)
    maximum = max(
        (zone["temp_millicelsius"] for zone in zones),
        default=None,
    )
    return {
        "exposed": bool(zones),
        "max_temp_millicelsius": maximum,
        "zones": zones,
    }


def _capture_containers(
    runner: CommandRunner, excluded_container_ids: Sequence[str]
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    warnings: list[dict[str, Any]] = []
    try:
        output = _decode_command_output(
            _run_bytes(runner, ("docker", "ps", "-q", "--no-trunc")),
            "Docker container inventory",
        )
    except SnapshotError:
        warnings.append({"code": "container_inventory_unavailable"})
        return {
            "inventory_available": False,
            "running_total": None,
            "excluded_lab_running": None,
            "external_running": None,
        }, warnings

    running_ids = [line.strip() for line in output.splitlines() if line.strip()]
    if any(_CONTAINER_ID_RE.fullmatch(value) is None for value in running_ids):
        warnings.append({"code": "container_inventory_invalid"})
        return {
            "inventory_available": False,
            "running_total": None,
            "excluded_lab_running": None,
            "external_running": None,
        }, warnings
    excluded = [
        value
        for value in excluded_container_ids
        if _CONTAINER_ID_RE.fullmatch(value) is not None
    ]

    def belongs_to_lab(container_id: str) -> bool:
        return any(
            container_id.startswith(lab_id) or lab_id.startswith(container_id)
            for lab_id in excluded
        )

    excluded_count = sum(belongs_to_lab(value) for value in running_ids)
    return {
        "inventory_available": True,
        "running_total": len(running_ids),
        "excluded_lab_running": excluded_count,
        "external_running": len(running_ids) - excluded_count,
    }, warnings


def _safe_source_path(repo_root: Path, raw_path: bytes) -> tuple[Path, bytes]:
    if not raw_path or raw_path.startswith(b"/"):
        raise SnapshotError("Git returned an unsafe source path")
    decoded = os.fsdecode(raw_path)
    pure_path = PurePosixPath(decoded)
    if any(part in {"", ".", ".."} for part in pure_path.parts):
        raise SnapshotError("Git returned an unsafe source path")
    path = repo_root.joinpath(*pure_path.parts)
    try:
        # A dirty tree may contain tracked deletions whose final parent
        # directory no longer exists. Non-strict resolution still resolves
        # every existing symlink component, so an escape is rejected without
        # making deleted entries impossible to hash as missing.
        parent = path.parent.resolve(strict=False)
        parent.relative_to(repo_root)
    except (OSError, ValueError) as exc:
        raise SnapshotError("Git source path leaves the repository") from exc
    return path, raw_path


def _hash_source_entries(repo_root: Path, raw_paths: bytes) -> str:
    digest = hashlib.sha256()
    paths = sorted(set(raw_paths.split(b"\0")) - {b""})
    for raw_path in paths:
        path, identity_path = _safe_source_path(repo_root, raw_path)
        digest.update(len(identity_path).to_bytes(8, "big"))
        digest.update(identity_path)
        try:
            entry_stat = path.lstat()
        except FileNotFoundError:
            digest.update(b"\0missing\0")
            continue
        except OSError as exc:
            raise SnapshotError("Git-visible source entry cannot be inspected") from exc
        executable = bool(entry_stat.st_mode & stat.S_IXUSR)
        if stat.S_ISLNK(entry_stat.st_mode):
            try:
                payload = os.fsencode(os.readlink(path))
            except OSError as exc:
                raise SnapshotError("Git-visible symlink cannot be read") from exc
            entry_type = b"symlink"
        elif stat.S_ISREG(entry_stat.st_mode):
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise SnapshotError("Git-visible source file cannot be read") from exc
            entry_type = b"file"
        elif stat.S_ISDIR(entry_stat.st_mode):
            payload = b""
            entry_type = b"directory"
        else:
            payload = b""
            entry_type = b"other"
        digest.update(b"\0" + entry_type + b"\0")
        digest.update(b"1" if executable else b"0")
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
    return digest.hexdigest()


def _capture_source(repo_root: Path, runner: CommandRunner) -> dict[str, Any]:
    commit_start = _decode_command_output(
        _run_bytes(
            runner,
            ("git", "rev-parse", "--verify", "HEAD"),
            repo_root,
        ),
        "Git source commit",
    )
    status_start = _run_bytes(
        runner,
        (
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=normal",
            "--",
            ".",
        ),
        repo_root,
    )
    paths_start = _run_bytes(
        runner,
        (
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ),
        repo_root,
    )
    snapshot_start = _hash_source_entries(repo_root, paths_start)

    paths_end = _run_bytes(
        runner,
        (
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ),
        repo_root,
    )
    snapshot_end = _hash_source_entries(repo_root, paths_end)
    status_end = _run_bytes(
        runner,
        (
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=normal",
            "--",
            ".",
        ),
        repo_root,
    )
    commit_end = _decode_command_output(
        _run_bytes(
            runner,
            ("git", "rev-parse", "--verify", "HEAD"),
            repo_root,
        ),
        "Git source commit",
    )
    patch = _run_bytes(
        runner,
        ("git", "diff", "--binary", "--no-ext-diff", "HEAD", "--", "."),
        repo_root,
    )
    cargo_lock = repo_root / "Cargo.lock"
    stable = (
        commit_start == commit_end
        and status_start == status_end
        and paths_start == paths_end
        and snapshot_start == snapshot_end
    )
    if re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", commit_end) is None:
        raise SnapshotError("Git source commit is invalid")
    return {
        "commit": commit_end,
        "tree_dirty": bool(status_end.strip()),
        "capture_stable": stable,
        "snapshot_algorithm": SOURCE_SNAPSHOT_ALGORITHM,
        "snapshot_sha256": snapshot_end,
        "tracked_patch_sha256": _sha256_bytes(patch),
        "cargo_lock_sha256": sha256_file(cargo_lock),
    }


def _capture_tool(
    name: str,
    version_arguments: Sequence[str],
    runner: CommandRunner,
    executable_path: Path | None,
) -> dict[str, Any]:
    raw_version = _run_bytes(runner, version_arguments)
    version_output = _decode_command_output(raw_version, f"{name} version")
    if not version_output:
        raise SnapshotError(f"{name} version is empty")
    if len(version_output.encode("utf-8")) > 16_384:
        raise SnapshotError(f"{name} version is unexpectedly large")
    if executable_path is None:
        resolved = shutil.which(name)
        if resolved is None:
            raise SnapshotError(f"{name} executable cannot be resolved")
        executable_path = Path(resolved)
    return {
        "version": version_output.splitlines()[0],
        "version_verbose": version_output,
        "version_verbose_sha256": _sha256_bytes(version_output.encode("utf-8")),
        "executable_sha256": sha256_file(executable_path),
    }


def _reason(code: str, observed: Any = None, limit: Any = None) -> dict[str, Any]:
    value: dict[str, Any] = {"code": code}
    if observed is not None:
        value["observed"] = observed
    if limit is not None:
        value["limit"] = limit
    return value


def _evaluate_validity(
    host: Mapping[str, Any],
    source: Mapping[str, Any],
    collection_warnings: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    reasons: list[dict[str, Any]] = []
    warnings = list(collection_warnings)
    cpu = host["cpu"]
    load = host["load"]
    memory = host["memory"]
    frequency = host["frequency"]
    thermal = host["thermal"]
    containers = host["containers"]

    if not cpu["models"]:
        reasons.append(_reason("cpu_model_unavailable"))
    if cpu["affinity_count"] < 1:
        reasons.append(_reason("cpu_affinity_empty"))
    if load["load1_per_affinity_cpu"] > MAX_LOAD1_PER_AFFINITY_CPU:
        reasons.append(
            _reason(
                "host_load_high",
                load["load1_per_affinity_cpu"],
                MAX_LOAD1_PER_AFFINITY_CPU,
            )
        )
    if load["runnable_per_affinity_cpu"] > MAX_RUNNABLE_PER_AFFINITY_CPU:
        reasons.append(
            _reason(
                "host_runnable_high",
                load["runnable_per_affinity_cpu"],
                MAX_RUNNABLE_PER_AFFINITY_CPU,
            )
        )
    if memory["available_ratio"] < MIN_MEMORY_AVAILABLE_RATIO:
        reasons.append(
            _reason(
                "host_memory_pressure",
                memory["available_ratio"],
                MIN_MEMORY_AVAILABLE_RATIO,
            )
        )
    if frequency["exposed"]:
        governors = frequency["governors"]
        if not governors or any(value != "performance" for value in governors):
            reasons.append(
                _reason("cpu_governor_not_performance", governors, ["performance"])
            )
    else:
        warnings.append({"code": "cpu_frequency_unavailable"})
    if thermal["exposed"]:
        maximum_temperature = thermal["max_temp_millicelsius"]
        if (
            maximum_temperature is not None
            and maximum_temperature > MAX_THERMAL_MILLICELSIUS
        ):
            reasons.append(
                _reason(
                    "host_thermal_limit_exceeded",
                    maximum_temperature,
                    MAX_THERMAL_MILLICELSIUS,
                )
            )
    else:
        warnings.append({"code": "thermal_state_unavailable"})
    # A container's existence is inventory, not evidence of pressure. Actual
    # CPU, runnable-work, memory, and thermal signals remain validity gates.
    if (
        containers["inventory_available"]
        and containers["external_running"] > 0
    ):
        warnings.append(
            {
                "code": "external_containers_observed",
                "observed": containers["external_running"],
            }
        )
    if source["tree_dirty"]:
        reasons.append(_reason("source_tree_dirty"))
    if not source["capture_stable"]:
        reasons.append(_reason("source_snapshot_unstable"))

    unique_warnings = {
        json.dumps(warning, sort_keys=True): warning for warning in warnings
    }
    return {
        "rules_version": HOST_VALIDITY_RULES_VERSION,
        "valid": not reasons,
        "invalid_reasons": reasons,
        "warnings": [unique_warnings[key] for key in sorted(unique_warnings)],
        "thresholds": {
            "max_load1_per_affinity_cpu": MAX_LOAD1_PER_AFFINITY_CPU,
            "max_runnable_per_affinity_cpu": MAX_RUNNABLE_PER_AFFINITY_CPU,
            "min_memory_available_ratio": MIN_MEMORY_AVAILABLE_RATIO,
            "max_thermal_millicelsius": MAX_THERMAL_MILLICELSIUS,
            "required_governor_when_exposed": "performance",
            "source_tree_must_be_clean": True,
            "source_capture_must_be_stable": True,
        },
    }


def capture_snapshot(
    repo_root: str | Path,
    *,
    excluded_container_ids: Sequence[str] = (),
    proc_root: str | Path = "/proc",
    sys_root: str | Path = "/sys",
    runner: CommandRunner = _command_stdout,
    affinity: Iterable[int] | None = None,
    logical_cpu_count: int | None = None,
    tool_paths: Mapping[str, Path] | None = None,
    captured_utc: str | None = None,
) -> dict[str, Any]:
    """Capture one complete start-of-run lab identity and validity decision."""

    root = Path(repo_root).resolve(strict=True)
    proc = Path(proc_root)
    sysfs = Path(sys_root)
    if affinity is None:
        try:
            affinity_values = sorted(os.sched_getaffinity(0))
        except (AttributeError, OSError):
            affinity_values = list(range(os.cpu_count() or 0))
    else:
        affinity_values = sorted(set(int(value) for value in affinity))
    if logical_cpu_count is None:
        logical_cpu_count = os.cpu_count() or len(affinity_values)
    if logical_cpu_count < 1 or not affinity_values:
        raise SnapshotError("CPU count or affinity is unavailable")

    cpuinfo = _read_text(proc / "cpuinfo")
    load = _capture_load(proc, len(affinity_values))
    memory = _capture_memory(proc)
    frequency = _capture_frequency(sysfs, affinity_values)
    thermal = _capture_thermal(sysfs)
    containers, collection_warnings = _capture_containers(
        runner, excluded_container_ids
    )
    source = _capture_source(root, runner)
    paths = dict(tool_paths or {})
    rustc = _capture_tool(
        "rustc",
        ("rustc", "-vV"),
        runner,
        paths.get("rustc"),
    )
    cargo = _capture_tool(
        "cargo",
        ("cargo", "-Vv"),
        runner,
        paths.get("cargo"),
    )
    host = {
        "kernel": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
        },
        "cpu": {
            "models": _capture_cpu_models(cpuinfo),
            "logical_count": logical_cpu_count,
            "affinity": compact_cpu_set(affinity_values),
            "affinity_count": len(affinity_values),
        },
        "load": load,
        "memory": memory,
        "frequency": frequency,
        "thermal": thermal,
        "containers": containers,
    }
    snapshot = {
        "schema_version": HOST_SNAPSHOT_SCHEMA_VERSION,
        "kind": HOST_SNAPSHOT_KIND,
        "captured_utc": captured_utc
        or datetime.now(timezone.utc).isoformat(),
        "host": host,
        "toolchain": {
            "rustc": rustc,
            "cargo": cargo,
        },
        "source": source,
        "validity": _evaluate_validity(host, source, collection_warnings),
    }
    validate_snapshot(snapshot)
    return snapshot


def validate_snapshot(snapshot: Any) -> dict[str, Any]:
    """Fail closed on incompatible or incomplete host-snapshot documents."""

    if not isinstance(snapshot, dict):
        raise SnapshotError("host snapshot must be an object")
    required_top_level = {
        "schema_version",
        "kind",
        "captured_utc",
        "host",
        "toolchain",
        "source",
        "validity",
    }
    if set(snapshot) != required_top_level:
        raise SnapshotError("host snapshot fields do not match schema version 1")
    if snapshot["schema_version"] != HOST_SNAPSHOT_SCHEMA_VERSION:
        raise SnapshotError("host snapshot schema_version must be 1")
    if snapshot["kind"] != HOST_SNAPSHOT_KIND:
        raise SnapshotError("host snapshot kind is invalid")
    if not isinstance(snapshot["captured_utc"], str) or not snapshot["captured_utc"]:
        raise SnapshotError("host snapshot timestamp is invalid")
    for field in ("host", "toolchain", "source", "validity"):
        if not isinstance(snapshot[field], dict):
            raise SnapshotError(f"host snapshot {field} must be an object")

    source = snapshot["source"]
    for field in (
        "snapshot_sha256",
        "tracked_patch_sha256",
        "cargo_lock_sha256",
    ):
        if _SHA256_RE.fullmatch(str(source.get(field, ""))) is None:
            raise SnapshotError(f"host snapshot source {field} is invalid")
    if source.get("snapshot_algorithm") != SOURCE_SNAPSHOT_ALGORITHM:
        raise SnapshotError("host snapshot source algorithm is invalid")
    if not isinstance(source.get("tree_dirty"), bool) or not isinstance(
        source.get("capture_stable"), bool
    ):
        raise SnapshotError("host snapshot source state is invalid")
    if (
        re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", str(source.get("commit", "")))
        is None
    ):
        raise SnapshotError("host snapshot source commit is invalid")

    toolchain = snapshot["toolchain"]
    if set(toolchain) != {"rustc", "cargo"}:
        raise SnapshotError("host snapshot toolchain is incomplete")
    for name in ("rustc", "cargo"):
        tool = toolchain[name]
        if not isinstance(tool, dict):
            raise SnapshotError(f"host snapshot {name} identity is invalid")
        for field in (
            "version",
            "version_verbose",
            "version_verbose_sha256",
            "executable_sha256",
        ):
            if field not in tool:
                raise SnapshotError(f"host snapshot {name} identity is incomplete")
        if not isinstance(tool["version"], str) or not tool["version"]:
            raise SnapshotError(f"host snapshot {name} version is invalid")
        if not isinstance(tool["version_verbose"], str) or not tool["version_verbose"]:
            raise SnapshotError(f"host snapshot {name} version is invalid")
        if (
            _sha256_bytes(tool["version_verbose"].encode("utf-8"))
            != tool["version_verbose_sha256"]
        ):
            raise SnapshotError(f"host snapshot {name} version digest is invalid")
        if _SHA256_RE.fullmatch(str(tool["executable_sha256"])) is None:
            raise SnapshotError(f"host snapshot {name} executable digest is invalid")

    validity = snapshot["validity"]
    if validity.get("rules_version") != HOST_VALIDITY_RULES_VERSION:
        raise SnapshotError(
            f"host validity rules_version must be {HOST_VALIDITY_RULES_VERSION}"
        )
    if not isinstance(validity.get("valid"), bool):
        raise SnapshotError("host validity decision is invalid")
    reasons = validity.get("invalid_reasons")
    warnings = validity.get("warnings")
    thresholds = validity.get("thresholds")
    if not isinstance(reasons, list) or not all(
        isinstance(value, dict) and isinstance(value.get("code"), str)
        for value in reasons
    ):
        raise SnapshotError("host validity reasons are invalid")
    if not isinstance(warnings, list) or not all(
        isinstance(value, dict) and isinstance(value.get("code"), str)
        for value in warnings
    ):
        raise SnapshotError("host validity warnings are invalid")
    if not isinstance(thresholds, dict):
        raise SnapshotError("host validity thresholds are invalid")
    if validity["valid"] != (len(reasons) == 0):
        raise SnapshotError("host validity decision disagrees with its reasons")
    return snapshot


def write_snapshot(path: str | Path, snapshot: Mapping[str, Any]) -> None:
    """Atomically write canonical JSON so its external SHA-256 is stable."""

    validate_snapshot(dict(snapshot))
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(snapshot, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
    ).encode("utf-8")
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=destination.parent,
            prefix=f".{destination.name}.",
            delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, destination)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def load_snapshot(
    path: str | Path, expected_sha256: str | None = None
) -> dict[str, Any]:
    payload = Path(path).read_bytes()
    if (
        expected_sha256 is not None
        and _sha256_bytes(payload) != expected_sha256
    ):
        raise SnapshotError("host snapshot SHA-256 does not match")
    try:
        snapshot = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise SnapshotError("host snapshot is not valid JSON") from exc
    return validate_snapshot(snapshot)


def require_valid_snapshot(snapshot: Mapping[str, Any], required: bool) -> None:
    validate_snapshot(dict(snapshot))
    if required and not snapshot["validity"]["valid"]:
        codes = ",".join(
            reason["code"] for reason in snapshot["validity"]["invalid_reasons"]
        )
        raise SnapshotError(f"lab host is invalid: {codes}")


def _environment_flag(environment: Mapping[str, str], name: str) -> bool:
    value = environment.get(name, "").strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    raise SnapshotError(f"{name} must be 0/1, false/true, or no/yes")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Capture an anonymized MPTunnel lab host snapshot"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    capture = subparsers.add_parser("capture")
    capture.add_argument("--repo-root", required=True)
    capture.add_argument("--output", required=True)
    capture.add_argument(
        "--exclude-container-id",
        action="append",
        default=[],
        help="running lab container ID to exclude from the external count",
    )
    capture.add_argument(
        "--require-valid",
        action="store_true",
        help="fail after retaining the snapshot when validity rules reject it",
    )
    return parser


def main(
    argv: Sequence[str] | None = None,
    environment: Mapping[str, str] | None = None,
) -> int:
    arguments = build_parser().parse_args(argv)
    env = os.environ if environment is None else environment
    try:
        required = arguments.require_valid or _environment_flag(
            env, "MPTUNNEL_LAB_REQUIRE_VALID_HOST"
        )
        snapshot = capture_snapshot(
            arguments.repo_root,
            excluded_container_ids=arguments.exclude_container_id,
        )
        write_snapshot(arguments.output, snapshot)
        require_valid_snapshot(snapshot, required)
    except (OSError, SnapshotError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

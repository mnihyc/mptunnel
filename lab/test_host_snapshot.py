import copy
import json
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from host_snapshot import (
    HOST_SNAPSHOT_SCHEMA_VERSION,
    SnapshotError,
    capture_snapshot,
    compact_cpu_set,
    load_snapshot,
    require_valid_snapshot,
    sha256_file,
    validate_snapshot,
    write_snapshot,
)


class FakeCommands:
    def __init__(self, source_paths: bytes):
        self.source_paths = source_paths
        self.status = b""
        self.containers = b""
        self.container_inventory_available = True

    def __call__(self, arguments, cwd=None):
        command = tuple(arguments)
        if command == ("git", "rev-parse", "--verify", "HEAD"):
            return b"a" * 40 + b"\n"
        if command == (
            "git",
            "status",
            "--porcelain=v1",
            "--untracked-files=normal",
            "--",
            ".",
        ):
            return self.status
        if command == (
            "git",
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ):
            return self.source_paths
        if command == (
            "git",
            "diff",
            "--binary",
            "--no-ext-diff",
            "HEAD",
            "--",
            ".",
        ):
            return b""
        if command == ("docker", "ps", "-q", "--no-trunc"):
            if not self.container_inventory_available:
                raise SnapshotError("container inventory unavailable")
            return self.containers
        if command == ("rustc", "-vV"):
            return (
                b"rustc 1.96.0 (abc 2026-01-01)\n"
                b"host: x86_64-unknown-linux-gnu\n"
            )
        if command == ("cargo", "-Vv"):
            return (
                b"cargo 1.96.0 (def 2026-01-01)\n"
                b"host: x86_64-unknown-linux-gnu\n"
            )
        raise AssertionError(f"unexpected command: {command}")


class HostSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        root = Path(self.directory.name)
        self.repo = root / "project-with-private-name"
        self.proc = root / "proc"
        self.sys = root / "sys"
        self.repo.mkdir()
        self.proc.mkdir()
        self.sys.mkdir()
        (self.repo / "Cargo.lock").write_text("locked\n", encoding="utf-8")
        (self.repo / "src").mkdir()
        (self.repo / "src" / "main.rs").write_text(
            "fn main() {}\n", encoding="utf-8"
        )
        (self.proc / "cpuinfo").write_text(
            "processor: 0\nmodel name: Test CPU\n"
            "processor: 1\nmodel name: Test CPU\n",
            encoding="utf-8",
        )
        (self.proc / "loadavg").write_text(
            "0.20 0.10 0.05 1/100 42\n", encoding="utf-8"
        )
        (self.proc / "meminfo").write_text(
            "MemTotal: 8000000 kB\n"
            "MemAvailable: 6000000 kB\n"
            "SwapTotal: 1000000 kB\n"
            "SwapFree: 900000 kB\n",
            encoding="utf-8",
        )
        for cpu in (0, 1):
            cpufreq = (
                self.sys
                / "devices"
                / "system"
                / "cpu"
                / f"cpu{cpu}"
                / "cpufreq"
            )
            cpufreq.mkdir(parents=True)
            (cpufreq / "scaling_governor").write_text(
                "performance\n", encoding="utf-8"
            )
            (cpufreq / "scaling_cur_freq").write_text(
                "3200000\n", encoding="utf-8"
            )
            (cpufreq / "scaling_min_freq").write_text(
                "2200000\n", encoding="utf-8"
            )
            (cpufreq / "scaling_max_freq").write_text(
                "4200000\n", encoding="utf-8"
            )
        thermal = self.sys / "class" / "thermal" / "thermal_zone0"
        thermal.mkdir(parents=True)
        (thermal / "type").write_text("package\n", encoding="utf-8")
        (thermal / "temp").write_text("50000\n", encoding="utf-8")
        self.rustc = root / "rustc"
        self.cargo = root / "cargo"
        self.rustc.write_bytes(b"fake rustc")
        self.cargo.write_bytes(b"fake cargo")
        self.commands = FakeCommands(b"Cargo.lock\0src/main.rs\0")

    def tearDown(self):
        self.directory.cleanup()

    def capture(self):
        return capture_snapshot(
            self.repo,
            proc_root=self.proc,
            sys_root=self.sys,
            runner=self.commands,
            affinity={0, 1},
            logical_cpu_count=2,
            tool_paths={"rustc": self.rustc, "cargo": self.cargo},
            captured_utc="2026-07-26T00:00:00+00:00",
        )

    def test_valid_snapshot_captures_host_toolchain_and_source_identity(self):
        snapshot = self.capture()

        self.assertEqual(snapshot["schema_version"], HOST_SNAPSHOT_SCHEMA_VERSION)
        self.assertEqual(snapshot["host"]["cpu"]["models"], ["Test CPU"])
        self.assertEqual(snapshot["host"]["cpu"]["logical_count"], 2)
        self.assertEqual(snapshot["host"]["cpu"]["affinity"], "0-1")
        self.assertEqual(snapshot["host"]["load"]["runnable"], 1)
        self.assertEqual(
            snapshot["host"]["memory"]["available_bytes"], 6_000_000 * 1024
        )
        self.assertEqual(
            snapshot["host"]["frequency"]["governors"], ["performance"]
        )
        self.assertEqual(
            snapshot["host"]["thermal"]["max_temp_millicelsius"], 50_000
        )
        self.assertEqual(snapshot["host"]["containers"]["external_running"], 0)
        self.assertEqual(
            snapshot["toolchain"]["rustc"]["executable_sha256"],
            sha256_file(self.rustc),
        )
        self.assertEqual(
            snapshot["source"]["cargo_lock_sha256"],
            sha256_file(self.repo / "Cargo.lock"),
        )
        self.assertTrue(snapshot["source"]["capture_stable"])
        self.assertFalse(snapshot["source"]["tree_dirty"])
        self.assertTrue(snapshot["validity"]["valid"])

    def test_invalid_conditions_are_all_reported_and_strict_mode_fails(self):
        self.commands.status = b" M src/main.rs\n"
        self.commands.containers = (b"1" * 64) + b"\n"
        (self.proc / "loadavg").write_text(
            "3.00 2.00 1.00 5/100 42\n", encoding="utf-8"
        )
        (self.proc / "meminfo").write_text(
            "MemTotal: 8000000 kB\n"
            "MemAvailable: 100000 kB\n"
            "SwapTotal: 1000000 kB\n"
            "SwapFree: 900000 kB\n",
            encoding="utf-8",
        )
        frequency = (
            self.sys
            / "devices"
            / "system"
            / "cpu"
            / "cpu0"
            / "cpufreq"
            / "scaling_governor"
        )
        frequency.write_text("powersave\n", encoding="utf-8")
        (
            self.sys / "class" / "thermal" / "thermal_zone0" / "temp"
        ).write_text("90000\n", encoding="utf-8")

        snapshot = self.capture()
        codes = {
            reason["code"] for reason in snapshot["validity"]["invalid_reasons"]
        }

        self.assertFalse(snapshot["validity"]["valid"])
        self.assertTrue(
            {
                "host_load_high",
                "host_runnable_high",
                "host_memory_pressure",
                "cpu_governor_not_performance",
                "host_thermal_limit_exceeded",
                "source_tree_dirty",
            }.issubset(codes)
        )
        require_valid_snapshot(snapshot, False)
        with self.assertRaisesRegex(SnapshotError, "lab host is invalid"):
            require_valid_snapshot(snapshot, True)

    def test_lab_container_ids_are_excluded_but_never_retained(self):
        lab_id = "1" * 64
        external_id = "2" * 64
        self.commands.containers = f"{lab_id}\n{external_id}\n".encode()

        snapshot = capture_snapshot(
            self.repo,
            excluded_container_ids=[lab_id],
            proc_root=self.proc,
            sys_root=self.sys,
            runner=self.commands,
            affinity={0, 1},
            logical_cpu_count=2,
            tool_paths={"rustc": self.rustc, "cargo": self.cargo},
            captured_utc="2026-07-26T00:00:00+00:00",
        )
        serialized = json.dumps(snapshot, sort_keys=True)

        self.assertEqual(snapshot["host"]["containers"]["running_total"], 2)
        self.assertEqual(
            snapshot["host"]["containers"]["excluded_lab_running"], 1
        )
        self.assertEqual(snapshot["host"]["containers"]["external_running"], 1)
        self.assertNotIn(lab_id, serialized)
        self.assertNotIn(external_id, serialized)
        self.assertNotIn(str(self.repo), serialized)

    def test_external_container_is_inventory_not_host_pressure(self):
        self.commands.containers = (b"2" * 64) + b"\n"

        snapshot = self.capture()

        self.assertEqual(snapshot["host"]["containers"]["external_running"], 1)
        self.assertTrue(snapshot["validity"]["valid"])
        self.assertIn(
            {"code": "external_containers_observed", "observed": 1},
            snapshot["validity"]["warnings"],
        )

    def test_unavailable_container_inventory_is_not_host_pressure(self):
        self.commands.container_inventory_available = False

        snapshot = self.capture()

        self.assertFalse(snapshot["host"]["containers"]["inventory_available"])
        self.assertTrue(snapshot["validity"]["valid"])
        self.assertIn(
            {"code": "container_inventory_unavailable"},
            snapshot["validity"]["warnings"],
        )

    def test_source_snapshot_changes_with_git_visible_content(self):
        first = self.capture()
        (self.repo / "src" / "main.rs").write_text(
            "fn main() { println!(\"changed\"); }\n", encoding="utf-8"
        )
        second = self.capture()

        self.assertNotEqual(
            first["source"]["snapshot_sha256"],
            second["source"]["snapshot_sha256"],
        )

    def test_source_snapshot_handles_deletions_but_rejects_symlink_escape(self):
        self.commands.source_paths += b"removed/old.rs\0"
        deleted = self.capture()
        self.assertTrue(deleted["source"]["capture_stable"])

        outside = Path(self.directory.name) / "outside"
        outside.mkdir()
        (outside / "visible.rs").write_text("outside\n", encoding="utf-8")
        (self.repo / "escape").symlink_to(outside, target_is_directory=True)
        self.commands.source_paths += b"escape/visible.rs\0"
        with self.assertRaisesRegex(SnapshotError, "leaves the repository"):
            self.capture()

    def test_frequency_and_thermal_absence_are_explicit_warnings(self):
        frequency_root = self.sys / "devices" / "system" / "cpu"
        thermal_root = self.sys / "class" / "thermal"
        for path in sorted(frequency_root.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        for path in sorted(thermal_root.rglob("*"), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()

        snapshot = self.capture()
        warning_codes = {
            warning["code"] for warning in snapshot["validity"]["warnings"]
        }

        self.assertTrue(snapshot["validity"]["valid"])
        self.assertIn("cpu_frequency_unavailable", warning_codes)
        self.assertIn("thermal_state_unavailable", warning_codes)

    def test_write_and_load_are_canonical_and_digest_checked(self):
        snapshot = self.capture()
        output = Path(self.directory.name) / "results" / "host-snapshot.json"

        write_snapshot(output, snapshot)
        digest = sha256_file(output)

        self.assertEqual(load_snapshot(output, digest), snapshot)
        with self.assertRaisesRegex(SnapshotError, "SHA-256 does not match"):
            load_snapshot(output, "0" * 64)

    def test_validation_rejects_schema_or_decision_mismatch(self):
        snapshot = self.capture()
        incompatible = copy.deepcopy(snapshot)
        incompatible["schema_version"] = 2
        with self.assertRaisesRegex(SnapshotError, "schema_version"):
            validate_snapshot(incompatible)

        inconsistent = copy.deepcopy(snapshot)
        inconsistent["validity"]["valid"] = False
        with self.assertRaisesRegex(SnapshotError, "disagrees"):
            validate_snapshot(inconsistent)

    def test_compact_cpu_set(self):
        self.assertEqual(compact_cpu_set([5, 1, 2, 3, 8, 8]), "1-3,5,8")


if __name__ == "__main__":
    unittest.main()

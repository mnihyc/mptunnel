import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mixed_workload_probe import (
    attempt_has_response_budget,
    browser_full_load_response_timeout_seconds,
    build_record,
    small_http_response_budget_seconds,
    write_started_file,
)


class MixedWorkloadProbeTests(unittest.TestCase):
    def test_started_file_keeps_unix_time_and_adds_monotonic_anchor(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "started"
            write_started_file(path)
            unix_seconds, monotonic_ms, unix_ms = path.read_text(
                encoding="utf-8"
            ).splitlines()

        self.assertGreater(float(unix_seconds), 0)
        self.assertGreater(int(monotonic_ms), 0)
        self.assertAlmostEqual(float(unix_seconds) * 1000, int(unix_ms), delta=1)

    def test_attempt_requires_full_response_budget_before_workload_deadline(self):
        self.assertTrue(attempt_has_response_budget(7.0, 10.0, 2.5))
        self.assertTrue(attempt_has_response_budget(7.5, 10.0, 2.5))
        self.assertFalse(attempt_has_response_budget(7.6, 10.0, 2.5))

    def test_small_http_start_admission_uses_dedicated_response_budget(self):
        args = SimpleNamespace(timeout=120.0, small_response_budget_ms=2500)
        self.assertEqual(small_http_response_budget_seconds(args), 2.5)

        args = SimpleNamespace(timeout=1.0, small_response_budget_ms=2500)
        self.assertEqual(small_http_response_budget_seconds(args), 1.0)

    def test_full_load_completion_is_independent_from_periodic_batch_deadline(self):
        args = SimpleNamespace(timeout=120.0, small_response_budget_ms=3000)

        self.assertEqual(browser_full_load_response_timeout_seconds(args), 120.0)

    def test_required_concurrent_batch_deadline_is_an_acceptance_failure(self):
        args = SimpleNamespace(
            label="browser-batches",
            mode="socks5",
            http_target="target:80",
            udp_target="target:53",
            tcp_echo_target=None,
            failover_after=-1,
            load_duration=30.0,
            timeout=40.0,
            require_small_response_budget=True,
            small_batch_size=10,
        )
        bulk = {"bulk_status": "ok", "bulk_bytes": 1}
        small = {
            "small_count": 10,
            "small_ok": 10,
            "small_fail": 0,
            "small_batch_count": 1,
            "small_batch_sizes": [10],
            "small_batch_deadline_misses": 1,
        }
        udp = {"udp_count": 1, "udp_received": 1}

        record = build_record(args, bulk, small, {}, udp)

        self.assertEqual(record["status"], "fail")

    def test_browser_only_acceptance_is_independent_of_bulk_and_datagrams(self):
        args = SimpleNamespace(
            label="browser-batches",
            mode="socks5",
            http_target="target:80",
            udp_target=None,
            tcp_echo_target=None,
            failover_after=-1,
            load_duration=30.0,
            timeout=40.0,
            require_small_response_budget=True,
            small_batch_size=10,
            browser_only=True,
        )
        small = {
            "small_count": 10,
            "small_ok": 10,
            "small_fail": 0,
            "small_batch_count": 1,
            "small_batch_sizes": [10],
            "small_batch_deadline_misses": 0,
        }

        record = build_record(args, {}, small, {}, {})

        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["protocol"], "browser")

    def test_browser_full_load_requires_the_declared_peak_and_exact_completion(self):
        args = SimpleNamespace(
            label="browser-load",
            mode="socks5",
            http_target="target:80",
            udp_target=None,
            tcp_echo_target=None,
            failover_after=-1,
            load_duration=30.0,
            timeout=40.0,
            require_small_response_budget=False,
            small_batch_size=10,
            browser_only=True,
            browser_full_load=True,
        )
        full_load = {
            "small_fail": 0,
            "browser_connections_started": 100,
            "browser_connections_accepted": 100,
            "browser_connections_completed": 100,
            "browser_peak_concurrency": 10,
        }

        record = build_record(args, {}, full_load, {}, {})

        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["protocol"], "browser-load")

        full_load["browser_connections_completed"] = 99
        full_load["small_fail"] = 1
        self.assertEqual(build_record(args, {}, full_load, {}, {})["status"], "fail")


if __name__ == "__main__":
    unittest.main()

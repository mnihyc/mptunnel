import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from failover_download_probe import (
    read_failover_marker_elapsed,
    run_download_worker,
    watch_failover_marker,
)


class FailoverDownloadMarkerTests(unittest.TestCase):
    def test_marker_converts_shared_monotonic_timestamp_to_probe_elapsed(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "fault.marker"
            marker.write_text("100.375\n", encoding="ascii")
            self.assertEqual(read_failover_marker_elapsed(marker, 100.0), 0.375)

    def test_watcher_updates_fault_time_and_source(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "fault.marker"
            started = time.monotonic()
            state = {"failover_after_s": -1.0, "failover_trigger_source": "pending"}
            lock = threading.Lock()
            marker.write_text(f"{started + 0.25}\n", encoding="ascii")

            watch_failover_marker(
                marker,
                started,
                started + 1.0,
                state,
                lock,
                interval=0.001,
            )

            self.assertAlmostEqual(state["failover_after_s"], 0.25, places=6)
            self.assertEqual(state["failover_trigger_source"], "marker")

    def test_missing_or_invalid_marker_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "fault.marker"
            self.assertIsNone(read_failover_marker_elapsed(marker, 1.0))
            marker.write_text("invalid\n", encoding="ascii")
            self.assertIsNone(read_failover_marker_elapsed(marker, 1.0))


class FixedRequestLifecycleTests(unittest.TestCase):
    def state(self):
        return {
            "request_attempts_started": 0,
            "requests": 0,
            "complete_requests": 0,
            "partial_requests": 0,
            "failures": 0,
            "early_terminations": 0,
        }

    def test_fixed_lifecycle_never_replaces_an_early_request(self):
        calls = []

        def early_eof(*_args):
            calls.append(True)
            return False, 200, "eof"

        state = self.state()
        lock = threading.Lock()
        started = time.monotonic()
        run_download_worker(
            SimpleNamespace(request_lifecycle="fixed"),
            started,
            started + 60,
            state,
            lock,
            download_request=early_eof,
        )

        self.assertEqual(len(calls), 1)
        self.assertEqual(state["request_attempts_started"], 1)
        self.assertEqual(state["partial_requests"], 1)
        self.assertEqual(state["early_terminations"], 1)

    def test_fixed_lifecycle_accepts_a_request_held_until_deadline(self):
        def deadline(*_args):
            return False, 200, "deadline"

        state = self.state()
        lock = threading.Lock()
        started = time.monotonic()
        run_download_worker(
            SimpleNamespace(request_lifecycle="fixed"),
            started,
            started + 60,
            state,
            lock,
            download_request=deadline,
        )

        self.assertEqual(state["request_attempts_started"], 1)
        self.assertEqual(state["partial_requests"], 1)
        self.assertEqual(state["early_terminations"], 0)


if __name__ == "__main__":
    unittest.main()

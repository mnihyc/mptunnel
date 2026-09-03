import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from failover_download_probe import (
    SynchronizedStartAnchor,
    download_one_request,
    read_failover_marker_elapsed,
    run_download_worker,
    watch_failover_marker,
    write_started_file,
)


class FailoverDownloadMarkerTests(unittest.TestCase):
    def test_started_marker_retains_unix_time_and_adds_monotonic_anchor(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / "started"

            write_started_file(marker, timestamp=123.5, monotonic=45.25)

            self.assertEqual(marker.read_text(encoding="ascii").splitlines(), [
                "123.500000000",
                "45250",
                "123500",
            ])

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


class SynchronizedDownloadStartTests(unittest.TestCase):
    def test_all_connections_precede_payload_and_anchor_the_full_load_window(self):
        connected = 0
        connected_lock = threading.Lock()
        early_payload = []

        class FakeSocket:
            def __init__(self):
                self.response = bytearray(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\nx"
                )

            def __enter__(self):
                return self

            def __exit__(self, _exc_type, _exc, _traceback):
                return False

            def settimeout(self, _timeout):
                return None

            def sendall(self, _request):
                with connected_lock:
                    early_payload.append(connected < 2)

            def recv(self, size):
                chunk = bytes(self.response[:size])
                del self.response[:size]
                return chunk

        next_connection = 0

        def connect(_args):
            nonlocal connected, next_connection
            with connected_lock:
                connection_index = next_connection
                next_connection += 1
            if connection_index == 1:
                time.sleep(0.08)
            with connected_lock:
                connected += 1
            return FakeSocket(), "target.example", 80

        args = SimpleNamespace(path="/large.bin", timeout=1.0, chunk_bytes=4096)
        started = time.monotonic()
        deadline = started + 0.03
        setup_deadline = started + args.timeout
        state = {
            "bytes": 0,
            "first_body_at": None,
            "last_body_at": None,
            "max_read_gap_s": 0.0,
            "recovery_gap_s": 0.0,
            "failover_after_s": -1.0,
            "interval_seconds": 0.1,
            "interval_bytes": {},
        }
        lock = threading.Lock()
        anchor = SynchronizedStartAnchor(2)
        errors = []

        def worker():
            try:
                download_one_request(
                    args,
                    started,
                    deadline,
                    state,
                    lock,
                    start_anchor=anchor,
                    setup_deadline=setup_deadline,
                )
            except Exception as exc:
                errors.append(exc)

        with mock.patch("failover_download_probe.connect_http", side_effect=connect):
            threads = [threading.Thread(target=worker) for _ in range(2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=1.0)

        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])
        self.assertTrue(anchor.completed)
        self.assertGreater(anchor.monotonic - started, 0.03)
        self.assertEqual(early_payload, [False, False])
        self.assertEqual(state["bytes"], 2)


if __name__ == "__main__":
    unittest.main()

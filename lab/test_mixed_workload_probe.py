import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import mixed_workload_probe as probe
from mixed_workload_probe import (
    attempt_has_response_budget,
    browser_full_load_response_timeout_seconds,
    build_record,
    interactive_attempt_record,
    interval_metric_fields,
    small_http_response_budget_seconds,
    write_finished_file,
    write_started_file,
)


class MixedWorkloadProbeTests(unittest.TestCase):
    def test_interval_series_preserves_observed_trailing_zero_goodput(self):
        fields = interval_metric_fields(
            {0: 25_000, 1: 25_000, 20: 25_000},
            0.2,
            prefix="bulk",
            observation_seconds=4.0,
        )

        raw = fields["bulk_interval_goodput_raw_mbps"]
        self.assertEqual(len(raw), 20)
        self.assertEqual(raw[:2], [1.0, 1.0])
        self.assertEqual(raw[2:], [0.0] * 18)
        self.assertEqual(len(fields["bulk_interval_goodput_mbps"]), 14)
        self.assertEqual(fields["bulk_interval_goodput_mbps"][-1], 0.0)

    def test_interactive_attempt_series_uses_monotonic_offsets_and_null_failure_latency(self):
        success = interactive_attempt_record(3, 100.0, 101.25, 101.375, "success")
        failure = interactive_attempt_record(
            4, 100.0, 101.5, 101.75, "io_error"
        )

        self.assertEqual(
            success,
            {
                "index": 3,
                "start_offset_s": 1.25,
                "end_offset_s": 1.375,
                "latency_ms": 125.0,
                "outcome": "success",
            },
        )
        self.assertEqual(failure["start_offset_s"], 1.5)
        self.assertEqual(failure["end_offset_s"], 1.75)
        self.assertIsNone(failure["latency_ms"])
        self.assertEqual(failure["outcome"], "io_error")

    def test_persistent_echo_worker_retains_success_failure_and_unavailable_attempts(self):
        class Clock:
            now = 10.0

            def monotonic(self):
                return self.now

            def sleep(self, seconds):
                self.now += seconds

        class EchoSocket:
            def __init__(self, clock):
                self.clock = clock
                self.payload = b""
                self.recv_count = 0

            def settimeout(self, _timeout):
                pass

            def sendall(self, payload):
                self.payload = payload

            def recv(self, _length):
                self.recv_count += 1
                if self.recv_count == 2:
                    self.clock.now += 0.010
                    raise OSError("connection lost")
                self.clock.now += 0.005
                return self.payload

            def close(self):
                pass

        clock = Clock()
        sock = EchoSocket(clock)
        args = SimpleNamespace(
            mode="socks5",
            proxy="127.0.0.1:1080",
            tcp_echo_target="target:10022",
            tcp_echo_timeout_ms=100,
            tcp_echo_payload_bytes=64,
            tcp_echo_interval_ms=100,
            timeout=1.0,
            load_duration=0.25,
            failover_after=-1,
        )
        result = {}
        ready = threading.Event()

        with mock.patch.object(probe.time, "monotonic", clock.monotonic), mock.patch.object(
            probe.time, "sleep", clock.sleep
        ), mock.patch.object(
            probe,
            "connect_target",
            return_value=(sock, "target", 10022),
        ):
            probe.interactive_tcp_worker(args, 10.0, ready, result)

        series = result["interactive_attempt_series"]
        self.assertEqual([attempt["index"] for attempt in series], [0, 1, 2])
        self.assertEqual(len(series), result["interactive_count"])
        self.assertEqual(
            [attempt["outcome"] for attempt in series],
            ["success", "io_error", "unavailable_after_disconnect"],
        )
        self.assertGreater(series[0]["latency_ms"], 0)
        self.assertIsNone(series[1]["latency_ms"])
        self.assertIsNone(series[2]["latency_ms"])
        self.assertTrue(
            all(
                left["end_offset_s"] <= right["start_offset_s"]
                for left, right in zip(series, series[1:])
            )
        )

    def test_bulk_interactive_mode_does_not_require_small_http_or_udp(self):
        args = SimpleNamespace(
            label="bulk-interactive",
            workload_mode="bulk-interactive",
            mode="socks5",
            http_target="target:80",
            udp_target=None,
            tcp_echo_target="target:10022",
            failover_after=-1,
            load_duration=30.0,
            timeout=40.0,
            require_small_response_budget=False,
            small_batch_size=1,
            tcp_echo_interval_ms=500,
            tcp_echo_timeout_ms=5000,
            tcp_echo_payload_bytes=64,
        )
        bulk = {"bulk_status": "ok", "bulk_bytes": 1}
        interactive = {
            "interactive_count": 1,
            "interactive_ok": 1,
            "interactive_fail": 0,
        }

        record = build_record(args, bulk, {}, interactive, {})

        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["protocol"], "bulk-interactive")
        self.assertEqual(record["workload_mode"], "bulk-interactive")
        self.assertEqual(record["interactive_interval_ms"], 500)

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

    def test_started_file_returns_same_monotonic_sample_as_persisted_anchor(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "started"
            with mock.patch.object(
                probe.time, "monotonic_ns", return_value=1_999_999
            ):
                started_at = write_started_file(path)
            monotonic_ms = int(path.read_text(encoding="utf-8").splitlines()[1])

        self.assertEqual(started_at, 0.001999999)
        self.assertEqual(monotonic_ms, 1)

    def test_finished_file_records_monotonic_completion_atomically(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "finished"
            write_finished_file(path)
            finished_ms = int(path.read_text(encoding="utf-8"))
            leftovers = list(path.parent.glob("finished.tmp-*"))

        self.assertGreater(finished_ms, 0)
        self.assertEqual(leftovers, [])

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
        self.assertEqual(record["protocol"], "mixed")
        self.assertEqual(record["workload_mode"], "mixed")

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

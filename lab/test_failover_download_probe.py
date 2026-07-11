import tempfile
import threading
import time
import unittest
from pathlib import Path

from failover_download_probe import (
    read_failover_marker_elapsed,
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


if __name__ == "__main__":
    unittest.main()

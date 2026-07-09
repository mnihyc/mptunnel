import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mixed_workload_probe import (
    attempt_has_response_budget,
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


if __name__ == "__main__":
    unittest.main()

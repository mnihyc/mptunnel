import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from result_enrichment import (
    application_payload_bytes,
    enrich_instrumentation,
    enrich_instrumentation_for_scope,
)


class ResultEnrichmentTests(unittest.TestCase):
    def test_instrumentation_metadata_records_exact_filter(self):
        row = {}
        enrich_instrumentation(row, "yes", "0", " path_timeout,stream_open,path_timeout ")

        self.assertTrue(row["lab_diagnostics_enabled"])
        self.assertFalse(row["lab_perf_enabled"])
        self.assertEqual(row["lab_diagnostic_events"], ["path_timeout", "stream_open"])
        self.assertFalse(row["performance_comparable"])
        self.assertIn("causal analysis", row["performance_comparable_reason"])

    def test_instrumentation_metadata_treats_empty_filter_as_full(self):
        row = {}
        enrich_instrumentation(row, "1", "0", "")

        self.assertEqual(row["lab_diagnostic_events"], ["*"])

    def test_instrumentation_metadata_marks_clean_row_comparable(self):
        row = {"performance_comparable_reason": "stale"}
        enrich_instrumentation(row, "0", "0", "ignored")

        self.assertTrue(row["performance_comparable"])
        self.assertNotIn("lab_diagnostic_events", row)
        self.assertNotIn("performance_comparable_reason", row)

    def test_direct_scope_ignores_global_instrumentation_flags(self):
        row = {
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["stale"],
            "performance_comparable": False,
        }
        enabled = enrich_instrumentation_for_scope(
            row,
            "0",
            "1",
            "1",
            "stream_open",
        )

        self.assertEqual(enabled, (False, False))
        self.assertNotIn("lab_diagnostics_enabled", row)
        self.assertNotIn("lab_diagnostic_events", row)
        self.assertNotIn("performance_comparable", row)

    def test_mixed_payload_uses_explicit_all_lane_app_bytes(self):
        row = {
            "protocol": "mixed",
            "bulk_bytes": 1000,
            "mixed_app_payload_bytes": 1420,
        }

        self.assertEqual(
            application_payload_bytes(row),
            (1420, "mixed_app_payload_bytes"),
        )


if __name__ == "__main__":
    unittest.main()

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("summarize-results.py")
SPEC = importlib.util.spec_from_file_location("summarize_results", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
summarize_results = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(summarize_results)


class SummarizeTrafficAccountingTests(unittest.TestCase):
    def test_signed_endpoint_diagnostics_are_reported_separately(self):
        records = [
            {
                "case": "tcp_case",
                "protocol": "tcp",
                "status": "ok",
                "goodput_mbps": 100.0,
                "client_vs_probe_payload_excess_pct_approx": -2.5,
                "client_target_endpoint_balance_pct_approx": 1.25,
                "client_edge_traffic_bytes_approx": 2 * 1024 * 1024,
                "_source": "current.jsonl",
            }
        ]

        rows = summarize_results.tcp_rows(summarize_results.grouped(records))
        markdown = summarize_results.render_markdown(records)

        self.assertEqual(rows[0]["median_client_probe_gap_pct"], -2.5)
        self.assertEqual(rows[0]["median_client_target_balance_pct"], 1.25)
        self.assertEqual(rows[0]["median_client_edge_mib"], 2.0)
        self.assertIn("client/probe gap % approx", markdown)
        self.assertIn("client/target balance % approx", markdown)
        self.assertIn("client edge MiB approx", markdown)
        self.assertIn("| -2.500 | 1.250 | 2.000 |", markdown)
        self.assertNotIn("overhead % approx", markdown)
        self.assertNotIn("expansion lower-bound", markdown)
        self.assertNotIn("tunnel MiB", markdown)

    def test_legacy_overhead_is_not_used_as_a_new_metric(self):
        records = [
            {
                "case": "legacy_tcp_case",
                "protocol": "tcp",
                "status": "ok",
                "goodput_mbps": 80.0,
                "traffic_overhead_pct_approx": 19.0,
                "_source": "legacy.jsonl",
            }
        ]

        rows = summarize_results.tcp_rows(summarize_results.grouped(records))
        markdown = summarize_results.render_markdown(records)

        self.assertIsNone(rows[0]["median_client_probe_gap_pct"])
        self.assertIsNone(rows[0]["median_client_target_balance_pct"])
        self.assertIsNone(rows[0]["median_client_edge_mib"])
        tcp_line = next(line for line in markdown.splitlines() if "legacy_tcp_case" in line)
        self.assertIn("| - | - | - |", tcp_line)
        self.assertNotIn("19.000", tcp_line)

    def test_mixed_loss_row_remains_accepted_measurement(self):
        records = [
            {
                "case": "mixed_loss_case",
                "protocol": "mixed",
                "status": "loss",
                "bulk_goodput_mbps": 120.0,
                "bulk_recovery_gap_s": 0.25,
                "client_vs_probe_payload_excess_pct_approx": 3.0,
                "client_target_endpoint_balance_pct_approx": 0.5,
                "client_edge_traffic_bytes_approx": 1024 * 1024,
                "_source": "loss.jsonl",
            }
        ]

        rows = summarize_results.mixed_rows(summarize_results.grouped(records))

        self.assertEqual(rows[0]["ok"], 0)
        self.assertEqual(rows[0]["loss"], 1)
        self.assertEqual(rows[0]["fail"], 0)
        self.assertEqual(rows[0]["bulk_median_goodput"], 120.0)
        self.assertEqual(rows[0]["bulk_max_gap"], 0.25)
        self.assertEqual(rows[0]["median_client_probe_gap_pct"], 3.0)
        self.assertEqual(rows[0]["median_client_target_balance_pct"], 0.5)

    def test_mixed_schema_cohort_does_not_hide_missing_metric_rows(self):
        records = [
            {
                "case": "tcp_case",
                "protocol": "tcp",
                "status": "ok",
                "client_vs_probe_payload_excess_pct_approx": 2.0,
                "client_target_endpoint_balance_pct_approx": 1.0,
                "client_edge_traffic_bytes_approx": 1024,
            },
            {
                "case": "tcp_case",
                "protocol": "tcp",
                "status": "ok",
                "traffic_overhead_pct_approx": 20.0,
            },
        ]

        rows = summarize_results.tcp_rows(summarize_results.grouped(records))

        self.assertIsNone(rows[0]["median_client_probe_gap_pct"])
        self.assertIsNone(rows[0]["median_client_target_balance_pct"])
        self.assertIsNone(rows[0]["median_client_edge_mib"])

    def test_directory_input_ignores_container_telemetry_sidecars(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "results.jsonl").write_text(
                json.dumps({"case": "tcp_case", "protocol": "tcp", "status": "ok"})
                + "\n",
                encoding="utf-8",
            )
            (root / "container-stats-tcp_case.jsonl").write_text(
                json.dumps({"case": "tcp_case", "service": "client"}) + "\n",
                encoding="utf-8",
            )

            records = summarize_results.load_records(
                summarize_results.collect_files([str(root)])
            )

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["status"], "ok")


if __name__ == "__main__":
    unittest.main()

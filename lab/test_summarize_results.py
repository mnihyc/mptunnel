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
    def test_upload_summary_separates_exact_lower_bound_and_unverified_metrics(self):
        records = [
            {
                "case": "upload_case",
                "protocol": "tcp-upload",
                "status": "ok",
                "upload_goodput_mbps": 900.0,
                "time_s": 1.0,
                "_source": "legacy.jsonl",
            },
            {
                "case": "upload_case",
                "protocol": "tcp-upload",
                "status": "loss",
                "upload_metric_version": 2,
                "upload_accounting_source": "target_sink_ack",
                "upload_accounting_exact": False,
                "upload_accounting_lower_bound": True,
                "complete": False,
                "bytes": 1_000,
                "upload_goodput_mbps": 100.0,
                "time_s": 9.0,
                "recovery_gap_s": 2.0,
                "_source": "current.jsonl",
            },
            {
                "case": "upload_case",
                "protocol": "tcp-upload",
                "status": "ok",
                "upload_metric_version": 2,
                "upload_accounting_source": "target_sink_ack",
                "upload_accounting_exact": True,
                "upload_accounting_lower_bound": False,
                "complete": True,
                "failed_streams": 0,
                "bytes": 2_000,
                "upload_goodput_mbps": 200.0,
                "time_s": 3.0,
                "recovery_gap_s": 0.5,
                "_source": "current.jsonl",
            },
            {
                "case": "upload_case",
                "protocol": "tcp-upload",
                "status": "ok",
                "upload_metric_version": 3,
                "upload_accounting_source": "target_sink_observer",
                "upload_accounting_exact": True,
                "upload_accounting_lower_bound": False,
                "complete": True,
                "failed_streams": 0,
                "bytes": 8_000,
                "upload_goodput_mbps": 800.0,
                "time_s": 2.0,
                "_source": "current.jsonl",
            },
        ]

        rows = summarize_results.upload_rows(summarize_results.grouped(records))
        markdown = summarize_results.render_markdown(records)

        self.assertEqual(rows[0]["runs"], 4)
        self.assertEqual(rows[0]["proven"], 3)
        self.assertEqual(rows[0]["exact"], 1)
        self.assertEqual(rows[0]["lower_bound"], 1)
        self.assertEqual(rows[0]["receiver_other"], 1)
        self.assertEqual(rows[0]["unverified"], 1)
        self.assertEqual(rows[0]["ok"], 3)
        self.assertEqual(rows[0]["loss"], 1)
        self.assertEqual(rows[0]["median_goodput"], 200.0)
        self.assertEqual(rows[0]["best_goodput"], 200.0)
        self.assertEqual(rows[0]["lower_bound_median_goodput"], 100.0)
        self.assertEqual(rows[0]["lower_bound_best_goodput"], 100.0)
        self.assertEqual(rows[0]["median_time"], 3.0)
        self.assertEqual(rows[0]["lower_bound_median_time"], 9.0)
        self.assertEqual(rows[0]["median_recovery_gap"], 0.5)
        self.assertEqual(rows[0]["lower_bound_median_recovery_gap"], 2.0)
        upload_line = next(
            line for line in markdown.splitlines() if "upload_case" in line
        )
        self.assertIn(
            "| upload_case | 4 | 1 | 1 | 1 | 1 | 3 | 1 | 0 | 200.000 | 200.000 | 100.000 | 100.000 |",
            upload_line,
        )
        self.assertNotIn("900.000", upload_line)

    def test_legacy_only_upload_stays_visible_but_has_no_proven_median(self):
        records = [
            {
                "case": "legacy_upload_case",
                "protocol": "tcp-upload",
                "status": "ok",
                "upload_goodput_mbps": 900.0,
                "_source": "legacy.jsonl",
            }
        ]

        rows = summarize_results.upload_rows(summarize_results.grouped(records))
        markdown = summarize_results.render_markdown(records)

        self.assertEqual(rows[0]["proven"], 0)
        self.assertEqual(rows[0]["exact"], 0)
        self.assertEqual(rows[0]["lower_bound"], 0)
        self.assertEqual(rows[0]["receiver_other"], 0)
        self.assertEqual(rows[0]["unverified"], 1)
        self.assertIsNone(rows[0]["median_goodput"])
        self.assertIn("| legacy_upload_case | 1 | 0 | 0 | 0 | 1 |", markdown)

    def test_equal_upload_comparison_excludes_unverified_cohort(self):
        case_pattern = "mptunnel_{kind}_multipath_equal_balanced_upload"
        records = [
            {
                "case": case_pattern.format(kind="tcp"),
                "protocol": "tcp-upload",
                "status": "ok",
                "upload_goodput_mbps": 300.0,
                "_source": "equal.jsonl",
            },
            {
                "case": case_pattern.format(kind="udp_stream"),
                "protocol": "udp-upload",
                "status": "ok",
                "upload_metric_version": 2,
                "upload_accounting_source": "target_sink_ack",
                "upload_accounting_exact": False,
                "upload_accounting_lower_bound": True,
                "complete": False,
                "bytes": 200,
                "upload_goodput_mbps": 200.0,
                "_source": "equal.jsonl",
            },
            {
                "case": case_pattern.format(kind="reliable_mixed"),
                "protocol": "mixed-upload",
                "status": "ok",
                "upload_metric_version": 4,
                "upload_accounting_source": "target_sink_observer",
                "upload_accounting_exact": True,
                "upload_accounting_lower_bound": False,
                "complete": True,
                "failed_streams": 0,
                "bytes": 100,
                "upload_probe_errors": [],
                "upload_ack_accounting_valid": True,
                "probe_elapsed_s": 1.0,
                "observer_elapsed_s": 1.25,
                "time_s": 1.25,
                "target_observer_snapshot_version": 2,
                "target_observer_quiesced": True,
                "target_observer_finalized": True,
                "upload_goodput_mbps": 100.0,
                "_source": "equal.jsonl",
            },
        ]

        rows = summarize_results.equal_cohort_comparisons(records)
        row = next(
            row
            for row in rows
            if row["profile"] == "balanced" and row["workload"] == "upload"
        )

        self.assertIsNone(row["tcp"])
        self.assertIsNone(row["udp"])
        self.assertEqual(row["mixed"], 100.0)
        self.assertEqual(row["exact"], 1)
        self.assertEqual(row["lower_bound"], 1)
        self.assertEqual(row["receiver_other"], 0)
        self.assertEqual(row["unverified"], 1)
        self.assertIsNone(row["min_vs_best"])

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
        tcp_line = next(
            line for line in markdown.splitlines() if "legacy_tcp_case" in line
        )
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

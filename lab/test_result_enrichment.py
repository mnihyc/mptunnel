import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from result_enrichment import (
    application_payload_bytes,
    enrich_instrumentation,
    enrich_instrumentation_for_scope,
    enrich_traffic_overhead,
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

    def test_signed_endpoint_identity_preserves_target_probe_excess(self):
        row = {"bytes": 1_000}
        telemetry = {
            "services": {
                "client": {"delta_rx_bytes": 1_400, "delta_tx_bytes": 100},
                "target": {"delta_rx_bytes": 100, "delta_tx_bytes": 1_500},
            }
        }

        enrich_traffic_overhead(row, telemetry)

        self.assertEqual(row["traffic_overhead_bytes_approx"], 500)
        self.assertEqual(
            row["traffic_accounting_ratio_reference"], "probe_payload_bytes"
        )
        self.assertEqual(row["client_edge_traffic_bytes_approx"], 1_500)
        self.assertEqual(row["client_vs_probe_payload_excess_bytes_approx"], 500)
        self.assertEqual(row["client_vs_probe_payload_excess_pct_approx"], 50.0)
        self.assertEqual(row["target_vs_probe_payload_excess_bytes_approx"], 600)
        self.assertEqual(row["client_target_endpoint_balance_bytes_approx"], -100)
        self.assertEqual(row["client_target_endpoint_balance_pct_approx"], -10.0)
        self.assertEqual(row["traffic_accounting_identity_residual_bytes_approx"], 0)
        self.assertFalse(row["traffic_expansion_estimate_available"])
        self.assertFalse(row["traffic_expansion_exact_available"])
        self.assertNotIn("traffic_expansion_pct_lower_bound_approx", row)

    def test_positive_endpoint_balance_is_not_called_expansion(self):
        row = {"bytes": 1_000}
        telemetry = {
            "services": {
                "client": {"delta_rx_bytes": 1_200, "delta_tx_bytes": 100},
                "target": {"delta_rx_bytes": 100, "delta_tx_bytes": 1_000},
            }
        }

        enrich_traffic_overhead(row, telemetry)

        self.assertEqual(row["target_edge_traffic_bytes_approx"], 1_100)
        self.assertEqual(row["client_vs_probe_payload_excess_bytes_approx"], 300)
        self.assertEqual(row["target_vs_probe_payload_excess_bytes_approx"], 100)
        self.assertEqual(row["client_target_endpoint_balance_bytes_approx"], 200)
        self.assertEqual(row["client_target_endpoint_balance_pct_approx"], 20.0)
        self.assertEqual(row["traffic_accounting_identity_residual_bytes_approx"], 0)
        self.assertFalse(row["traffic_expansion_estimate_available"])
        self.assertNotIn("traffic_expansion_bytes_lower_bound_approx", row)

    def test_signed_client_app_gap_survives_missing_target_telemetry(self):
        row = {"bytes": 1_000}
        telemetry = {
            "services": {
                "client": {"delta_rx_bytes": 800, "delta_tx_bytes": 100},
            }
        }

        enrich_traffic_overhead(row, telemetry)

        self.assertEqual(row["traffic_metric_version"], 3)
        self.assertEqual(row["client_vs_probe_payload_excess_bytes_approx"], -100)
        self.assertEqual(row["client_vs_probe_payload_excess_pct_approx"], -10.0)
        self.assertEqual(row["traffic_overhead_bytes_approx"], 0)
        self.assertEqual(
            row["traffic_accounting_source"],
            "client_container_non_loopback_netdev_case_boundary_delta",
        )
        self.assertFalse(row["traffic_expansion_estimate_available"])
        self.assertFalse(row["traffic_expansion_exact_available"])
        self.assertNotIn("client_target_endpoint_balance_bytes_approx", row)

    def test_direct_control_uses_generic_endpoint_names(self):
        row = {"case": "direct_balanced", "bytes": 100}
        telemetry = {
            "services": {
                "client": {"delta_rx_bytes": 100, "delta_tx_bytes": 100},
                "target": {"delta_rx_bytes": 50, "delta_tx_bytes": 50},
            }
        }

        enrich_traffic_overhead(row, telemetry)

        self.assertEqual(row["client_edge_traffic_bytes_approx"], 200)
        self.assertEqual(row["client_target_endpoint_balance_bytes_approx"], 100)
        self.assertEqual(row["traffic_accounting_identity_residual_bytes_approx"], 0)
        self.assertNotIn("tunnel_app_endpoint_balance_bytes_approx", row)
        self.assertFalse(row["traffic_expansion_estimate_available"])


if __name__ == "__main__":
    unittest.main()

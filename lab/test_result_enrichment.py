import json
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from result_enrichment import (
    application_payload_bytes,
    enrich_upload_target_observer,
    enrich_instrumentation,
    enrich_instrumentation_for_scope,
    enrich_traffic_overhead,
    is_exact_upload_measurement,
    is_proven_upload_measurement,
)


class ResultEnrichmentTests(unittest.TestCase):
    def test_target_observer_exact_snapshot_becomes_primary(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 2,
            "time_s": 2.0,
            "bytes": 100,
            "target_confirmed_bytes": 100,
            "local_accepted_bytes": 300,
            "streams": 2,
            "complete_streams": 0,
            "failed_streams": 2,
            "status": "loss",
            "upload_probe_errors": [],
            "upload_ack_accounting_valid": True,
        }
        snapshot = {
            "version": 2,
            "quiesced": True,
            "finalized": True,
            "updated_wall_time_ns": 1_000,
            "merged_max_receive_gap_ns": 200_000_000,
            "merged_max_receive_gap_start_connection_id": 0,
            "merged_max_receive_gap_start_bytes": 40,
            "merged_max_receive_gap_end_connection_id": 1,
            "merged_max_receive_gap_end_bytes": 60,
            "connections": {
                "0": {
                    "bytes": 120,
                    "final": True,
                    "updated_wall_time_ns": 900,
                    "max_receive_gap_ns": 250_000_000,
                    "max_receive_gap_start_bytes": 40,
                    "max_receive_gap_end_bytes": 80,
                },
                "1": {
                    "bytes": 180,
                    "final": True,
                    "updated_wall_time_ns": 950,
                    "max_receive_gap_ns": 125_000_000,
                    "max_receive_gap_start_bytes": 60,
                    "max_receive_gap_end_bytes": 90,
                },
            },
        }

        enrich_upload_target_observer(row, json.dumps(snapshot), "2.5")

        self.assertEqual(row["in_band_target_confirmed_bytes"], 100)
        self.assertEqual(row["bytes"], 300)
        self.assertEqual(row["target_confirmed_bytes"], 300)
        self.assertEqual(row["target_observed_bytes"], 300)
        self.assertEqual(row["upload_metric_version"], 4)
        self.assertEqual(row["upload_accounting_source"], "target_sink_observer")
        self.assertEqual(row["upload_interval_accounting_source"], "target_sink_ack")
        self.assertEqual(row["probe_elapsed_s"], 2.0)
        self.assertEqual(row["observer_elapsed_s"], 2.5)
        self.assertEqual(row["time_s"], 2.5)
        self.assertEqual(row["goodput_mbps"], 0.001)
        self.assertEqual(row["upload_goodput_mbps"], 0.001)
        self.assertEqual(row["target_observer_connections"], 2)
        self.assertEqual(row["target_observer_final_connections"], 2)
        self.assertEqual(row["target_observer_max_receive_gap_s"], 0.25)
        self.assertEqual(row["target_observer_merged_max_receive_gap_s"], 0.2)
        self.assertEqual(
            row["target_observer_merged_max_receive_gap_end_connection_id"], 1
        )
        self.assertEqual(
            row["target_observer_connection_summaries"][0]["max_receive_gap_s"],
            0.25,
        )
        self.assertEqual(row["streams_with_delivery"], 2)
        self.assertEqual(row["complete_streams"], 2)
        self.assertEqual(row["failed_streams"], 0)
        self.assertTrue(row["upload_accounting_exact"])
        self.assertFalse(row["upload_accounting_lower_bound"])
        self.assertTrue(row["complete"])
        self.assertEqual(row["status"], "ok")
        self.assertEqual(row["exit_code"], 0)
        self.assertTrue(is_exact_upload_measurement(row))

    def test_target_observer_partial_snapshot_is_loss(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 2,
            "time_s": 1.0,
            "target_confirmed_bytes": 25,
            "local_accepted_bytes": 1_000,
            "upload_probe_errors": [],
            "upload_ack_accounting_valid": True,
        }
        snapshot = {
            "version": 2,
            "quiesced": True,
            "finalized": True,
            "updated_wall_time_ns": 1_000,
            "connections": {
                "0": {
                    "bytes": 400,
                    "final": True,
                    "updated_wall_time_ns": 900,
                },
                "1": {
                    "bytes": 100,
                    "final": False,
                    "updated_wall_time_ns": 950,
                },
            },
        }

        enrich_upload_target_observer(row, snapshot, 1.5)

        self.assertEqual(row["in_band_target_confirmed_bytes"], 25)
        self.assertEqual(row["bytes"], 500)
        self.assertEqual(row["target_confirmed_bytes"], 500)
        self.assertEqual(row["upload_goodput_mbps"], 0.003)
        self.assertEqual(row["target_observer_connections"], 2)
        self.assertEqual(row["target_observer_final_connections"], 1)
        self.assertEqual(row["complete_streams"], 1)
        self.assertEqual(row["failed_streams"], 1)
        self.assertFalse(row["upload_accounting_exact"])
        self.assertTrue(row["upload_accounting_lower_bound"])
        self.assertFalse(row["complete"])
        self.assertEqual(row["status"], "loss")
        self.assertEqual(row["exit_code"], 0)
        self.assertFalse(is_exact_upload_measurement(row))

    def test_target_observer_rejects_unexpected_connections_transactionally(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 2,
            "time_s": 1.0,
            "target_confirmed_bytes": 10,
            "local_accepted_bytes": 1_000,
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
        }
        snapshot = {
            "version": 2,
            "quiesced": True,
            "finalized": True,
            "updated_wall_time_ns": 1_000,
            "connections": {
                "0": {
                    "bytes": 300,
                    "final": True,
                    "updated_wall_time_ns": 900,
                },
                "1": {
                    "bytes": 200,
                    "final": False,
                    "updated_wall_time_ns": 950,
                },
                "2": {
                    "bytes": 100,
                    "final": True,
                    "updated_wall_time_ns": 975,
                },
            },
        }
        original = dict(row)

        with self.assertRaisesRegex(ValueError, "unexpected connections"):
            enrich_upload_target_observer(row, snapshot, 1.1)
        self.assertEqual(row, original)

    def test_target_observer_zero_snapshot_is_fail(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 2,
            "time_s": 1.0,
            "target_confirmed_bytes": 0,
            "local_accepted_bytes": 1_000,
            "upload_probe_errors": [],
            "upload_ack_accounting_valid": True,
        }

        enrich_upload_target_observer(
            row,
            {
                "version": 2,
                "quiesced": True,
                "finalized": True,
                "updated_wall_time_ns": 1_000,
                "connections": {},
            },
            1.1,
        )

        self.assertEqual(row["bytes"], 0)
        self.assertEqual(row["upload_goodput_mbps"], 0)
        self.assertEqual(row["target_observer_connections"], 0)
        self.assertEqual(row["target_observer_final_connections"], 0)
        self.assertEqual(row["complete_streams"], 0)
        self.assertEqual(row["failed_streams"], 2)
        self.assertFalse(row["upload_accounting_exact"])
        self.assertFalse(row["upload_accounting_lower_bound"])
        self.assertFalse(row["complete"])
        self.assertEqual(row["status"], "fail")
        self.assertEqual(row["exit_code"], 1)

    def test_v4_probe_error_prevents_exact_and_bounds_stream_counters(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 2,
            "time_s": 1.0,
            "target_confirmed_bytes": 100,
            "local_accepted_bytes": 300,
            "streams": 2,
            "upload_probe_errors": ["stream 1: invalid ACK"],
            "upload_ack_accounting_valid": False,
        }
        snapshot = {
            "version": 2,
            "quiesced": True,
            "finalized": True,
            "updated_wall_time_ns": 1_000,
            "connections": {
                "0": {
                    "bytes": 120,
                    "final": True,
                    "updated_wall_time_ns": 900,
                },
                "1": {
                    "bytes": 180,
                    "final": True,
                    "updated_wall_time_ns": 950,
                },
            },
        }

        enrich_upload_target_observer(row, snapshot, 1.25)

        self.assertFalse(row["upload_accounting_exact"])
        self.assertTrue(row["upload_accounting_lower_bound"])
        self.assertIsNone(row["upload_interval_accounting_source"])
        self.assertEqual(row["streams"], 2)
        self.assertEqual(row["streams_with_delivery"], 2)
        self.assertEqual(row["complete_streams"], 1)
        self.assertEqual(row["failed_streams"], 1)
        self.assertEqual(row["status"], "loss")
        self.assertFalse(is_exact_upload_measurement(row))

    def test_v3_hot_observer_remains_receiver_evidence_but_not_exact(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 1,
            "time_s": 1.0,
            "target_confirmed_bytes": 100,
            "local_accepted_bytes": 100,
        }

        enrich_upload_target_observer(
            row,
            {
                "version": 1,
                "connections": {"0": {"bytes": 100, "final": True}},
            },
            1.1,
        )

        self.assertEqual(row["upload_metric_version"], 3)
        self.assertTrue(is_proven_upload_measurement(row))
        self.assertFalse(is_exact_upload_measurement(row))
        self.assertFalse(row["upload_accounting_exact"])
        self.assertTrue(row["upload_accounting_lower_bound"])
        self.assertEqual(row["status"], "loss")

    def test_invalid_target_observer_preserves_ack_accounting(self):
        original = {
            "protocol": "tcp-upload",
            "parallel_uploads": 1,
            "time_s": 1.0,
            "bytes": 25,
            "target_confirmed_bytes": 25,
            "local_accepted_bytes": 100,
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
            "upload_accounting_exact": False,
            "upload_accounting_lower_bound": True,
            "status": "loss",
        }
        invalid_snapshots = (
            "",
            "{",
            {"version": 3, "connections": {}},
            {
                "version": 2,
                "quiesced": False,
                "finalized": True,
                "connections": {},
            },
            {"version": 1, "connections": {"0": {"bytes": 10, "final": True}}},
        )

        for snapshot in invalid_snapshots:
            with self.subTest(snapshot=snapshot):
                row = dict(original)
                with self.assertRaises((ValueError, json.JSONDecodeError)):
                    enrich_upload_target_observer(row, snapshot, 1.1)
                self.assertEqual(row, original)

    def test_target_observer_rejects_shorter_elapsed_transactionally(self):
        row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 1,
            "time_s": 2.0,
            "target_confirmed_bytes": 10,
            "local_accepted_bytes": 10,
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
        }
        original = dict(row)

        with self.assertRaisesRegex(ValueError, "shorter than probe"):
            enrich_upload_target_observer(
                row,
                {
                    "version": 2,
                    "quiesced": True,
                    "finalized": True,
                    "connections": {"0": {"bytes": 10, "final": True}},
                },
                1.9,
            )
        self.assertEqual(row, original)

    def test_v4_observer_rejects_fractional_schema_integers_transactionally(self):
        base_row = {
            "protocol": "tcp-upload",
            "parallel_uploads": 1,
            "time_s": 1.0,
            "target_confirmed_bytes": 10,
            "local_accepted_bytes": 10,
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
            "upload_probe_errors": [],
            "upload_ack_accounting_valid": True,
        }
        base_snapshot = {
            "version": 2,
            "quiesced": True,
            "finalized": True,
            "updated_wall_time_ns": 100,
            "connections": {
                "0": {
                    "bytes": 10,
                    "final": True,
                    "updated_wall_time_ns": 90,
                }
            },
        }
        row_mutations = (
            ("parallel_uploads", 1.5),
            ("target_confirmed_bytes", 9.5),
            ("local_accepted_bytes", 10.5),
        )
        for field, value in row_mutations:
            with self.subTest(field=field):
                row = {**base_row, field: value}
                original = dict(row)
                with self.assertRaisesRegex(ValueError, "invalid"):
                    enrich_upload_target_observer(row, base_snapshot, 1.1)
                self.assertEqual(row, original)

        snapshot_mutations = (
            ("bytes", 10.5),
            ("connection_timestamp", 90.5),
            ("snapshot_timestamp", 100.5),
        )
        for field, value in snapshot_mutations:
            with self.subTest(field=field):
                row = dict(base_row)
                original = dict(row)
                snapshot = json.loads(json.dumps(base_snapshot))
                if field == "bytes":
                    snapshot["connections"]["0"]["bytes"] = value
                elif field == "connection_timestamp":
                    snapshot["connections"]["0"]["updated_wall_time_ns"] = value
                else:
                    snapshot["updated_wall_time_ns"] = value
                with self.assertRaisesRegex(ValueError, "invalid"):
                    enrich_upload_target_observer(row, snapshot, 1.1)
                self.assertEqual(row, original)

    def test_upload_proof_requires_v2_receiver_confirmed_accounting(self):
        self.assertTrue(
            is_proven_upload_measurement(
                {
                    "upload_metric_version": 2,
                    "upload_accounting_source": "target_sink_ack",
                }
            )
        )
        self.assertTrue(
            is_proven_upload_measurement(
                {
                    "upload_metric_version": 2,
                    "upload_accounting_source": "target_sink_observer",
                }
            )
        )
        self.assertTrue(
            is_proven_upload_measurement(
                {
                    "upload_metric_version": 3.0,
                    "upload_accounting_source": "target_sink_ack",
                }
            )
        )
        for row in (
            {"upload_accounting_source": "target_sink_ack"},
            {
                "upload_metric_version": 1,
                "upload_accounting_source": "target_sink_ack",
            },
            {
                "upload_metric_version": 2,
                "upload_accounting_source": "local_socket_acceptance",
            },
        ):
            self.assertFalse(is_proven_upload_measurement(row))

    def test_exact_upload_requires_v2_ack_or_v4_final_observer(self):
        base = {
            "protocol": "tcp-upload",
            "status": "ok",
            "bytes": 100,
            "complete": True,
            "failed_streams": 0,
            "upload_accounting_exact": True,
            "upload_accounting_lower_bound": False,
        }
        v2 = {
            **base,
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
        }
        v3 = {
            **base,
            "upload_metric_version": 3,
            "upload_accounting_source": "target_sink_observer",
        }
        v4 = {
            **base,
            "upload_metric_version": 4,
            "upload_accounting_source": "target_sink_observer",
            "upload_probe_errors": [],
            "upload_ack_accounting_valid": True,
            "probe_elapsed_s": 1.0,
            "observer_elapsed_s": 1.25,
            "time_s": 1.25,
            "target_observer_snapshot_version": 2,
            "target_observer_quiesced": True,
            "target_observer_finalized": True,
        }

        self.assertTrue(is_exact_upload_measurement(v2))
        self.assertFalse(
            is_exact_upload_measurement(
                {**v2, "upload_observer_error": "unexpected connections"}
            )
        )
        self.assertFalse(is_exact_upload_measurement(v3))
        self.assertTrue(is_exact_upload_measurement(v4))
        self.assertFalse(
            is_exact_upload_measurement(
                {**v4, "upload_probe_errors": ["worker failed"]}
            )
        )
        self.assertFalse(
            is_exact_upload_measurement({**v4, "target_observer_quiesced": False})
        )
        self.assertFalse(
            is_exact_upload_measurement(
                {**v4, "observer_elapsed_s": 0.9, "time_s": 0.9}
            )
        )

    def test_upload_app_payload_uses_receiver_bytes_not_local_acceptance(self):
        row = {
            "protocol": "tcp-upload",
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
            "bytes": 1_000,
            "local_accepted_bytes": 9_000,
            "mixed_app_payload_bytes": 8_000,
            "bulk_bytes": 7_000,
        }
        telemetry = {
            "services": {
                "client": {"delta_rx_bytes": 100, "delta_tx_bytes": 1_000},
            }
        }

        self.assertEqual(application_payload_bytes(row), (1_000, "bytes"))
        enrich_traffic_overhead(row, telemetry)
        self.assertEqual(row["app_payload_bytes"], 1_000)
        self.assertEqual(row["app_payload_source"], "bytes")

    def test_upload_app_payload_does_not_fall_back_to_local_diagnostics(self):
        row = {
            "protocol": "tcp-upload",
            "upload_metric_version": 2,
            "upload_accounting_source": "target_sink_ack",
            "bytes": 0,
            "local_accepted_bytes": 9_000,
            "mixed_app_payload_bytes": 8_000,
            "bulk_bytes": 7_000,
        }

        self.assertEqual(application_payload_bytes(row), (None, None))

    def test_legacy_upload_bytes_are_not_treated_as_delivered_payload(self):
        row = {
            "protocol": "tcp-upload",
            "bytes": 9_000,
            "upload_goodput_mbps": 72.0,
        }

        self.assertEqual(application_payload_bytes(row), (None, None))

    def test_instrumentation_metadata_records_exact_filter(self):
        row = {}
        enrich_instrumentation(
            row, "yes", "0", " path_timeout,stream_open,path_timeout "
        )

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

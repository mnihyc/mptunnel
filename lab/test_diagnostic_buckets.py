import unittest
from collections import Counter
from pathlib import Path
from tempfile import TemporaryDirectory
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from diagnostic_buckets import analyze_row, collect_metrics, dominant_bucket, score_buckets


def base_metrics():
    return {
        "event_counts": {"server_sender_dispatch": 100},
        "teardown_hits": {},
        "server_sender_enqueue_count": 100,
        "server_sender_dispatch_count": 100,
        "server_sender_dispatch_p95_ms": 120,
        "server_sender_dispatch_max_ms": 2_000,
        "server_sender_conformance_delta": 0,
        "receive_hole_events": 0,
        "receive_hole_significant": False,
        "receive_hole_max_ratio": None,
        "receive_hole_max_bytes": 0,
        "stream_ordering_debt_max_bytes": 0,
        "selected_underlays": {},
        "repair_bytes_after_max": None,
        "repair_debt_has_hole_evidence": False,
        "udp_datagram_timeouts": 0,
        "reliable_stream_open_timeouts": 0,
        "path_queue_control_max_ms": 0,
    }


class DiagnosticBucketTests(unittest.TestCase):
    def test_artifact_and_matching_tail_are_not_double_counted(self):
        line = "mptunnel_lab_diag ts_ms=1 event=receive_hole reorder_bytes=524288\n"
        with TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text(line * 60, encoding="utf-8")
            row = {
                "status": "ok",
                "lab_diagnostics_enabled": True,
                "lab_diagnostic_events": ["*"],
                "server_log_tail": line * 60,
            }
            result = analyze_row(
                row,
                {"services": {"server": {"file": str(path)}}},
                {},
            )

        self.assertEqual(result["metrics"]["receive_hole_events"], 60)

    def test_runner_client_headers_do_not_defeat_tail_deduplication(self):
        line = "mptunnel_lab_diag ts_ms=1 event=receive_hole reorder_bytes=524288\n"
        with TemporaryDirectory() as directory:
            path = Path(directory) / "client.log"
            path.write_text(
                "== client:mptunnel-client-case.log ==\n" + line * 60,
                encoding="utf-8",
            )
            row = {
                "status": "ok",
                "lab_diagnostics_enabled": True,
                "lab_diagnostic_events": ["*"],
                "client_log_tail": "== mptunnel-client-case.log ==\n" + line * 60,
            }
            result = analyze_row(
                row,
                {"services": {"client": {"file": str(path)}}},
                {},
            )

        self.assertEqual(result["metrics"]["receive_hole_events"], 60)

    def test_distinct_tail_is_kept_when_same_service_artifact_exists(self):
        enqueue = (
            "mptunnel_lab_diag ts_ms=1 event=server_sender_enqueue "
            "queue_bytes=64\n"
        )
        dispatch = (
            "mptunnel_lab_diag ts_ms=2 event=server_sender_dispatch "
            "queue_delay_ms=1\n"
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text(enqueue, encoding="utf-8")
            row = {
                "status": "ok",
                "lab_diagnostics_enabled": True,
                "lab_diagnostic_events": ["*"],
                "server_log_tail": dispatch,
            }
            result = analyze_row(
                row,
                {"services": {"server": {"file": str(path)}}},
                {},
            )

        self.assertEqual(result["metrics"]["server_sender_enqueue_count"], 1)
        self.assertEqual(result["metrics"]["server_sender_dispatch_count"], 1)

    def test_partial_tail_overlap_keeps_only_new_events(self):
        enqueue = (
            "mptunnel_lab_diag ts_ms=1 event=server_sender_enqueue "
            "queue_bytes=64\n"
        )
        dispatch = (
            "mptunnel_lab_diag ts_ms=2 event=server_sender_dispatch "
            "queue_delay_ms=1\n"
        )
        receive_hole = (
            "mptunnel_lab_diag ts_ms=3 event=receive_hole reorder_bytes=524288\n"
        )
        with TemporaryDirectory() as directory:
            path = Path(directory) / "server.log"
            path.write_text(enqueue + dispatch, encoding="utf-8")
            row = {
                "status": "ok",
                "lab_diagnostics_enabled": True,
                "lab_diagnostic_events": ["*"],
                "server_log_tail": dispatch + receive_hole,
            }
            result = analyze_row(
                row,
                {"services": {"server": {"file": str(path)}}},
                {},
            )

        self.assertEqual(result["metrics"]["server_sender_enqueue_count"], 1)
        self.assertEqual(result["metrics"]["server_sender_dispatch_count"], 1)
        self.assertEqual(result["metrics"]["receive_hole_events"], 1)

    def test_filtered_events_mark_excluded_metrics_unavailable(self):
        row = {
            "status": "ok",
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["reliable_stream_open_timeout"],
        }
        metrics = collect_metrics(row, [], {})

        self.assertEqual(metrics["reliable_stream_open_timeouts"], 0)
        self.assertIsNone(metrics["receive_hole_events"])
        self.assertIsNone(metrics["server_sender_conformance_delta"])
        self.assertIsNone(metrics["candidate_reasons"])
        score_buckets(row, metrics)

    def test_wildcard_filter_keeps_zero_counts_observed(self):
        row = {
            "status": "ok",
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["*"],
        }
        metrics = collect_metrics(row, [], {})

        self.assertEqual(metrics["receive_hole_events"], 0)
        self.assertEqual(metrics["server_sender_conformance_delta"], 0)
        self.assertEqual(metrics["candidate_reasons"], {})

    def test_candidate_only_filter_does_not_compare_excluded_receive_holes(self):
        row = {
            "status": "ok",
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["server_bulk_output_candidate"],
        }
        logs = [
            (
                "server",
                "mptunnel_lab_diag ts_ms=1 event=server_bulk_output_candidate "
                "reason=ordered_owner_tail_debt stream_ordering_debt=1048576\n",
            )
        ]
        metrics = collect_metrics(row, logs, {})

        self.assertIsNone(metrics["receive_hole_events"])
        scores, _ = score_buckets(row, metrics)
        self.assertEqual(scores.get("harmful_admission", 0), 0)

    def test_conformance_only_filter_uses_summary_event(self):
        row = {
            "status": "ok",
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["sender_service_conformance"],
        }
        logs = [
            (
                "server",
                "mptunnel_lab_diag ts_ms=1 event=sender_service_conformance "
                "session_id=7 stream_id=9 server_response_stream_data_frames=5 "
                "server_sender_service_stream_data_decisions=4\n",
            )
        ]
        metrics = collect_metrics(row, logs, {})

        self.assertEqual(metrics["server_response_stream_data_frames"], 5)
        self.assertEqual(metrics["server_sender_service_stream_data_decisions"], 4)
        self.assertEqual(metrics["server_sender_conformance_delta"], 1)
        scores, _ = score_buckets(row, metrics)
        self.assertGreater(scores.get("sender_starvation", 0), 0)

    def test_full_trace_prefers_raw_conformance_for_open_streams(self):
        row = {
            "status": "ok",
            "lab_diagnostics_enabled": True,
            "lab_diagnostic_events": ["*"],
        }
        frame = (
            "mptunnel_lab_diag ts_ms=1 event=server_response_stream_data_frame "
            "session_id=7 stream_id=9 offset=0 payload_bytes=64"
        )
        decision = (
            "mptunnel_lab_diag ts_ms=1 event=sender_service_decision role=server "
            "session_id=7 stream_id=9 decision_kind=data_service "
            "frame_kind=stream_data payload_bytes=64"
        )
        summary = (
            "mptunnel_lab_diag ts_ms=1 event=sender_service_conformance "
            "session_id=8 stream_id=10 server_response_stream_data_frames=5 "
            "server_sender_service_stream_data_decisions=5"
        )
        logs = [("server", "\n".join([frame] * 10 + [decision] * 9 + [summary]))]
        metrics = collect_metrics(row, logs, {})

        self.assertEqual(metrics["server_response_stream_data_frames"], 10)
        self.assertEqual(metrics["server_sender_service_stream_data_decisions"], 9)
        self.assertEqual(metrics["server_sender_conformance_delta"], 1)

    def test_isolated_dispatch_tail_delay_is_not_sender_starvation(self):
        scores, _ = score_buckets({"status": "ok"}, base_metrics())
        self.assertEqual(scores.get("sender_starvation", 0), 0)

    def test_sustained_dispatch_delay_is_sender_starvation(self):
        metrics = base_metrics()
        metrics["server_sender_dispatch_p95_ms"] = 400
        scores, _ = score_buckets({"status": "ok"}, metrics)
        self.assertGreater(scores.get("sender_starvation", 0), 0)

    def test_successful_row_with_only_teardown_noise_has_no_dominant_failure(self):
        scores = Counter({"carrier_teardown": 3})
        self.assertEqual(dominant_bucket({"status": "ok"}, base_metrics(), scores), "none")


if __name__ == "__main__":
    unittest.main()

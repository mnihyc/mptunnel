import unittest
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from diagnostic_buckets import dominant_bucket, score_buckets


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

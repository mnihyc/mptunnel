import json
import sys
import tempfile
import unittest
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(LAB_DIR))

from derive_performance_series import CASE_SERIES  # noqa: E402
from evaluate_release_series import main as evaluate_main  # noqa: E402
import test_performance_series as performance_fixtures  # noqa: E402

SOURCE_COMMIT = "b" * 40
HOST_SNAPSHOT = "a" * 64
RATES = {
    "mpp_tcp": 100.0,
    "mpp_quic": 100.0,
    "mpp_default": 100.0,
    "xray_vmess": 90.0,
    "hysteria2": 100.0,
}
P95 = {
    "mpp_tcp": 100.0,
    "mpp_quic": 105.0,
    "mpp_default": 105.0,
    "xray_vmess": 100.0,
    "hysteria2": 100.0,
}


def set_latency(record, latency_ms):
    for attempt in record["interactive_attempt_series"]:
        attempt["end_offset_s"] = attempt["start_offset_s"] + latency_ms / 1000.0
        attempt["latency_ms"] = latency_ms
    record["interactive_p50_ms"] = latency_ms
    record["interactive_p95_ms"] = latency_ms
    record["interactive_max_ms"] = latency_ms


def product_record(series_id, case):
    record = performance_fixtures.PerformanceSeriesDerivationTests.record_for(
        series_id, case
    )
    rate = RATES[series_id]
    record.update(
        source_commit=SOURCE_COMMIT,
        host_snapshot_sha256=HOST_SNAPSHOT,
        bulk_error=None,
        interactive_error=None,
        bulk_interval_goodput_raw_mbps=[rate] * 200,
        bulk_interval_goodput_mbps=[rate] * 194,
    )
    set_latency(record, P95[series_id])
    return record


def direct_trace():
    fixtures = performance_fixtures.PerformanceSeriesDerivationTests
    condition = fixtures.dynamic_loss_condition()
    metadata = fixtures.dynamic_loss_trace(condition)
    metadata["topology_mode"] = "direct"
    metadata["dynamic_role_to_service"] = {
        "client-egress": "client",
        "remote-egress": "target",
    }
    metadata["constant_service_loss_percent"] = {}
    for event, loss in zip(metadata["events"], condition["loss_percent"]):
        endpoint = event["endpoints"]["remote-egress"]
        endpoint["service"] = "target"
        endpoint["readback_base64"] = fixtures.qdisc_readback(condition, "target", loss)
    return metadata


def raw_record():
    record = performance_fixtures.PerformanceSeriesDerivationTests.valid_raw_record()
    record.update(
        case="baseline_raw_tcp_bulk_interactive_balanced",
        protocol="raw-tcp",
        mode="direct",
        target="172.31.15.30:8080",
        tcp_echo_target="172.31.15.30:10022",
        source_commit=SOURCE_COMMIT,
        host_snapshot_sha256=HOST_SNAPSHOT,
        bulk_error=None,
        interactive_error=None,
        bulk_interactive_dynamic_loss=direct_trace(),
        bulk_interval_goodput_raw_mbps=[110.0] * 200,
        bulk_interval_goodput_mbps=[110.0] * 194,
    )
    set_latency(record, 95.0)
    return record


def write_result(root, name, records):
    result_dir = root / name
    result_dir.mkdir()
    result = result_dir / "results.jsonl"
    result.write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )
    (result_dir / "run-manifest.json").write_text(
        json.dumps(performance_fixtures.PerformanceSeriesDerivationTests.manifest()),
        encoding="utf-8",
    )
    return result


def write_cohort(root):
    products = []
    raws = []
    for repetition in range(2):
        products.append(
            write_result(
                root,
                f"product-{repetition + 1}",
                [
                    product_record(series_id, case)
                    for series_id, _label, case in CASE_SERIES
                ],
            )
        )
        raws.append(write_result(root, f"raw-{repetition + 1}", [raw_record()]))
    return products, raws


def invocation(products, raws, output, candidate_commit=SOURCE_COMMIT):
    arguments = ["--candidate-commit", candidate_commit]
    for product in products:
        arguments.extend(("--product-repetition", str(product)))
    for raw in raws:
        arguments.extend(("--raw-control", str(raw)))
    arguments.extend(("--output", str(output)))
    return arguments


class ReleaseSeriesEvaluationTests(unittest.TestCase):
    def test_two_paired_repetitions_emit_complete_passing_verdict(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            output = root / "verdict.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        self.assertEqual(verdict["status"], "pass")
        self.assertEqual(verdict["candidate_commit"], SOURCE_COMMIT)
        self.assertEqual(verdict["source_commit"], SOURCE_COMMIT)
        self.assertEqual(len(verdict["repetitions"]), 2)
        for repetition in verdict["repetitions"]:
            self.assertEqual(repetition["status"], "pass")
            self.assertTrue(repetition["raw_control"]["pass"])
            self.assertEqual(repetition["raw_control"]["trajectory_windows"], 38)
            self.assertEqual(len(repetition["comparisons"]), 11)
            self.assertTrue(all(item["pass"] for item in repetition["comparisons"]))
            self.assertFalse(
                any(
                    item["id"].startswith(("xray_vmess.loss_", "hysteria2.loss_"))
                    for item in repetition["comparisons"]
                )
            )

    def test_failed_rate_gate_returns_one_and_retains_exact_comparison(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[1].read_text().splitlines()]
            quic = next(row for row in rows if row["case"].startswith("mptunnel_quic_"))
            quic["bulk_interval_goodput_raw_mbps"] = [50.0] * 200
            quic["bulk_interval_goodput_mbps"] = [50.0] * 194
            products[1].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            output = root / "failed-verdict.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 1)
        self.assertEqual(verdict["status"], "fail")
        self.assertEqual(verdict["failed_repetitions"], ["repetition-2"])
        failed = {item["id"]: item for item in verdict["repetitions"][1]["comparisons"]}
        comparison = failed["mpp_quic.over_faster_external_goodput"]
        self.assertFalse(comparison["pass"])
        self.assertEqual(comparison["actual"], 50.0)
        self.assertEqual(comparison["reference"], 100.0)
        self.assertEqual(comparison["ratio"], 0.5)

    def test_epoch_recurrence_excludes_transition_first_second(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[0].read_text().splitlines()]
            tcp = next(row for row in rows if row["case"].startswith("mptunnel_tcp_"))
            tcp["bulk_interval_goodput_raw_mbps"][125:130] = [0.0] * 5
            tcp["bulk_interval_goodput_mbps"] = tcp["bulk_interval_goodput_raw_mbps"][
                3:-3
            ]
            products[0].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            output = root / "verdict.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        comparisons = {
            item["id"]: item for item in verdict["repetitions"][0]["comparisons"]
        }
        recurrence = comparisons["mpp_tcp.loss_1_percent.epoch_5_over_0"]
        self.assertEqual(recurrence["actual"], 100.0)
        self.assertEqual(recurrence["reference"], 100.0)
        self.assertEqual(recurrence["ratio"], 1.0)

    def test_competitor_recurrence_is_context_not_a_release_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[0].read_text().splitlines()]
            xray = next(
                row for row in rows if row["case"].startswith("baseline_vmess_")
            )
            xray["bulk_interval_goodput_raw_mbps"][130:150] = [0.0] * 20
            xray["bulk_interval_goodput_mbps"] = xray["bulk_interval_goodput_raw_mbps"][
                3:-3
            ]
            products[0].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            output = root / "verdict.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        ids = {item["id"] for item in verdict["repetitions"][0]["comparisons"]}
        self.assertFalse(
            any(identifier.startswith("xray_vmess.loss_") for identifier in ids)
        )

    def test_external_echo_loss_invalidates_p95_and_retains_artifact(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[0].read_text().splitlines()]
            hysteria = next(
                row for row in rows if row["case"].startswith("baseline_hysteria2_")
            )
            hysteria["status"] = "loss"
            hysteria["interactive_ok"] = 79
            hysteria["interactive_fail"] = 1
            hysteria["interactive_attempt_series"][0].update(
                outcome="timeout", latency_ms=None
            )
            products[0].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            output = root / "invalid-verdict.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 2)
        self.assertEqual(verdict["status"], "invalid")
        self.assertEqual(verdict["repetitions"][0]["status"], "invalid")
        self.assertIn(
            "application status is not ok", verdict["repetitions"][0]["errors"][0]
        )
        self.assertEqual(verdict["repetitions"][1]["status"], "pass")

    def test_exact_pair_cardinality_and_raw_source_are_mandatory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            cardinality_output = root / "cardinality.json"
            cardinality_status = evaluate_main(
                invocation(products, raws[:1], cardinality_output)
            )
            cardinality = json.loads(cardinality_output.read_text(encoding="utf-8"))

            raw = json.loads(raws[1].read_text(encoding="utf-8"))
            raw["source_commit"] = "c" * 40
            raws[1].write_text(json.dumps(raw) + "\n", encoding="utf-8")
            source_output = root / "source.json"
            source_status = evaluate_main(invocation(products, raws, source_output))
            source = json.loads(source_output.read_text(encoding="utf-8"))

        self.assertEqual(cardinality_status, 2)
        self.assertEqual(cardinality["status"], "invalid")
        self.assertIn("exactly two", cardinality["errors"][0])
        self.assertEqual(source_status, 2)
        self.assertEqual(source["repetitions"][1]["status"], "invalid")
        self.assertIn("source commit differs", source["repetitions"][1]["errors"][0])

    def test_zero_ratio_reference_invalidates_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[0].read_text().splitlines()]
            tcp = next(row for row in rows if row["case"].startswith("mptunnel_tcp_"))
            tcp["bulk_interval_goodput_raw_mbps"][5:25] = [0.0] * 20
            tcp["bulk_interval_goodput_mbps"] = tcp["bulk_interval_goodput_raw_mbps"][
                3:-3
            ]
            products[0].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            output = root / "zero-reference.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 2)
        self.assertEqual(verdict["repetitions"][0]["status"], "invalid")
        self.assertIn("positive reference", verdict["repetitions"][0]["errors"][0])
        self.assertEqual(verdict["repetitions"][1]["status"], "pass")

    def test_coherent_stale_cohort_cannot_substitute_for_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            output = root / "wrong-candidate.json"

            status = evaluate_main(
                invocation(products, raws, output, candidate_commit="c" * 40)
            )
            verdict = json.loads(output.read_text(encoding="utf-8"))

            malformed_output = root / "malformed-candidate.json"
            malformed_status = evaluate_main(
                invocation(products, raws, malformed_output, candidate_commit="B" * 40)
            )
            malformed = json.loads(malformed_output.read_text(encoding="utf-8"))

        self.assertEqual(status, 2)
        self.assertEqual(verdict["status"], "invalid")
        self.assertEqual(verdict["candidate_commit"], "c" * 40)
        self.assertIn("does not match --candidate-commit", verdict["errors"][0])
        self.assertEqual(malformed_status, 2)
        self.assertIn("40-character lowercase", malformed["errors"][0])


if __name__ == "__main__":
    unittest.main()

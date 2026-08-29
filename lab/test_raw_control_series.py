import copy
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
ROOT = LAB_DIR.parent
RAW_DATASET_PATH = (
    ROOT
    / "docs"
    / "assets"
    / "performance"
    / "sustained-random-internet-raw-control.json"
)
RAW_SVG_PATH = (
    ROOT
    / "docs"
    / "assets"
    / "performance"
    / "sustained-random-internet-raw-control.svg"
)
RELEASE_VERDICT_PATH = (
    ROOT
    / "docs"
    / "assets"
    / "performance"
    / "sustained-random-internet-release-verdict.json"
)

sys.path.insert(0, str(LAB_DIR))

from derive_performance_series import (  # noqa: E402
    DerivationError,
    derive_dataset as derive_product_dataset,
)
from derive_raw_control_series import main as derive_raw_main  # noqa: E402
from evaluate_release_series import main as evaluate_main  # noqa: E402
from render_performance_series import (  # noqa: E402
    load_dataset,
    render_svg,
    validate_dataset,
)
import test_evaluate_release_series as evaluation_fixtures  # noqa: E402
from test_performance_series import contract_fixture  # noqa: E402

PRODUCT_SVG_SHA256 = "adabcdd5a10d5ab9f1d2cb1387fc1a98f354c4755e163e641dcecb6e96af5876"


def _rewrite_raw_trajectory(path, repetition_index):
    record = json.loads(path.read_text(encoding="utf-8"))
    raw = [float(index + repetition_index * 2) for index in range(200)]
    record["bulk_interval_goodput_raw_mbps"] = raw
    record["bulk_interval_goodput_mbps"] = raw[3:-3]
    latencies = []
    for attempt in record["interactive_attempt_series"]:
        latency = 80.0 + attempt["index"] * 0.25 + repetition_index
        attempt["latency_ms"] = latency
        attempt["end_offset_s"] = attempt["start_offset_s"] + latency / 1000.0
        latencies.append(latency)
    record["interactive_p95_ms"] = sorted(latencies)[round((len(latencies) - 1) * 0.95)]
    path.write_text(json.dumps(record) + "\n", encoding="utf-8")


def _write_passing_evidence(root):
    products, raws = evaluation_fixtures.write_cohort(root)
    for index, raw in enumerate(raws):
        _rewrite_raw_trajectory(raw, index)
    verdict = root / RELEASE_VERDICT_PATH.name
    status = evaluate_main(evaluation_fixtures.invocation(products, raws, verdict))
    if status != 0:
        raise AssertionError(verdict.read_text(encoding="utf-8"))
    return products, raws, verdict


def _derive_invocation(raws, verdict, output, candidate=None):
    arguments = [
        "--release-verdict",
        str(verdict),
        "--candidate-commit",
        candidate or evaluation_fixtures.SOURCE_COMMIT,
        "--output",
        str(output),
        "--cohort-id",
        "synthetic-raw-control",
        "--title",
        "Raw TCP direct control",
        "--subtitle",
        "Synthetic contract evidence; never a performance claim.",
        "--condition-note",
        "Direct route under the fixed dynamic law.",
    ]
    for raw in raws:
        arguments.extend(("--raw-control", str(raw)))
    return arguments


class RawControlSeriesTests(unittest.TestCase):
    def test_exact_two_passing_paired_controls_derive_real_trajectories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _products, raws, verdict = _write_passing_evidence(root)
            first_output = root / "raw-control-a.json"
            second_output = root / "raw-control-b.json"

            first_status = derive_raw_main(
                _derive_invocation(raws, verdict, first_output)
            )
            second_status = derive_raw_main(
                _derive_invocation(raws, verdict, second_output)
            )
            first_bytes = first_output.read_bytes()
            second_bytes = second_output.read_bytes()
            dataset = json.loads(first_bytes)

        self.assertEqual(first_status, 0)
        self.assertEqual(second_status, 0)
        self.assertEqual(first_bytes, second_bytes)
        self.assertEqual([entry["id"] for entry in dataset["series"]], ["raw_tcp"])
        self.assertEqual(dataset["provenance"]["valid_repetitions"], 2)
        self.assertEqual(
            dataset["provenance"]["comparison_scope"],
            {
                "role": "context-only",
                "route": "direct-client-to-target",
                "included_in_exact_five": False,
                "included_in_product_comparisons": False,
            },
        )
        goodput = dataset["series"][0]["goodput"]
        latency = dataset["series"][0]["latency"]
        self.assertEqual(len(goodput), 38)
        self.assertEqual([goodput[0]["time_s"], goodput[-1]["time_s"]], [1.5, 38.5])
        self.assertEqual([goodput[0]["median"], goodput[-1]["median"]], [8.0, 193.0])
        self.assertEqual(len(latency), 80)
        self.assertNotEqual(latency[0]["median"], latency[-1]["median"])
        self.assertTrue(all(sample["available"] == 2 for sample in latency))

    def test_cardinality_verdict_sha_and_pairing_are_mandatory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _products, raws, verdict = _write_passing_evidence(root)

            one_output = root / "one.json"
            one_status = derive_raw_main(
                _derive_invocation(raws[:1], verdict, one_output)
            )
            wrong_sha_output = root / "wrong-sha.json"
            wrong_sha_status = derive_raw_main(
                _derive_invocation(raws, verdict, wrong_sha_output, candidate="c" * 40)
            )
            swapped_output = root / "swapped.json"
            swapped_status = derive_raw_main(
                _derive_invocation(list(reversed(raws)), verdict, swapped_output)
            )
            failed_verdict = root / "failed-release-verdict.json"
            failed_payload = json.loads(verdict.read_text(encoding="utf-8"))
            failed_payload["status"] = "fail"
            failed_verdict.write_text(json.dumps(failed_payload), encoding="utf-8")
            failed_output = root / "failed-verdict-output.json"
            failed_status = derive_raw_main(
                _derive_invocation(raws, failed_verdict, failed_output)
            )

        self.assertEqual(one_status, 2)
        self.assertFalse(one_output.exists())
        self.assertEqual(wrong_sha_status, 2)
        self.assertFalse(wrong_sha_output.exists())
        self.assertEqual(swapped_status, 2)
        self.assertFalse(swapped_output.exists())
        self.assertEqual(failed_status, 2)
        self.assertFalse(failed_output.exists())

    def test_direct_route_and_exact_raw_case_exclude_product_contamination(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _products, raws, verdict = _write_passing_evidence(root)
            original = raws[0].read_text(encoding="utf-8")

            wrong_route = json.loads(original)
            wrong_route["target"] = "172.31.40.30:8080"
            raws[0].write_text(json.dumps(wrong_route) + "\n", encoding="utf-8")
            route_output = root / "wrong-route.json"
            route_status = derive_raw_main(
                _derive_invocation(raws, verdict, route_output)
            )

            raws[0].write_text(original, encoding="utf-8")
            contamination = evaluation_fixtures.product_record(
                "mpp_tcp", "mptunnel_tcp_bulk_interactive_balanced"
            )
            with raws[0].open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(contamination) + "\n")
            contamination_output = root / "contaminated.json"
            contamination_status = derive_raw_main(
                _derive_invocation(raws, verdict, contamination_output)
            )

            with self.assertRaisesRegex(DerivationError, "unexpected case"):
                derive_product_dataset(
                    [[raws[0]], [raws[1]]],
                    "title",
                    "subtitle",
                    "condition",
                    "raw-cannot-enter-product",
                )

        self.assertEqual(route_status, 2)
        self.assertFalse(route_output.exists())
        self.assertEqual(contamination_status, 2)
        self.assertFalse(contamination_output.exists())

    def test_raw_svg_is_deterministic_and_product_svg_is_byte_stable(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _products, raws, verdict = _write_passing_evidence(root)
            output = root / "raw-control.json"
            self.assertEqual(
                derive_raw_main(_derive_invocation(raws, verdict, output)), 0
            )
            dataset = json.loads(output.read_text(encoding="utf-8"))

        validate_dataset(dataset)
        first_svg = render_svg(dataset)
        second_svg = render_svg(copy.deepcopy(dataset))
        self.assertEqual(first_svg, second_svg)
        self.assertIn('data-series="raw_tcp"', first_svg)
        self.assertEqual(
            hashlib.sha256(render_svg(contract_fixture()).encode()).hexdigest(),
            PRODUCT_SVG_SHA256,
        )

    def test_publication_paths_are_pre_registered_without_fake_assets(self):
        self.assertEqual(
            RAW_DATASET_PATH.name,
            "sustained-random-internet-raw-control.json",
        )
        self.assertEqual(
            RAW_SVG_PATH.name,
            "sustained-random-internet-raw-control.svg",
        )
        self.assertEqual(
            RELEASE_VERDICT_PATH.name,
            "sustained-random-internet-release-verdict.json",
        )


@unittest.skipUnless(
    RAW_DATASET_PATH.exists(), "accepted raw-control dataset not derived yet"
)
class CheckedInRawControlSeriesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = load_dataset(RAW_DATASET_PATH)

    def test_passing_verdict_is_present_and_content_addressed(self):
        self.assertTrue(
            RELEASE_VERDICT_PATH.exists(),
            "passing release verdict is not checked in",
        )
        expected = self.dataset["provenance"]["release_verdict"]["sha256"]
        self.assertEqual(
            hashlib.sha256(RELEASE_VERDICT_PATH.read_bytes()).hexdigest(),
            expected,
        )
        verdict = json.loads(RELEASE_VERDICT_PATH.read_text(encoding="utf-8"))
        self.assertEqual(verdict["status"], "pass")
        self.assertEqual(
            verdict["candidate_commit"],
            self.dataset["provenance"]["candidate_commit"],
        )
        self.assertEqual(
            verdict["runtime_identity"],
            self.dataset["provenance"]["paired_candidate_runtime_identity"],
        )

    def test_checked_in_svg_is_exactly_regenerated(self):
        self.assertTrue(RAW_SVG_PATH.exists(), "raw-control SVG is not checked in")
        self.assertEqual(
            RAW_SVG_PATH.read_text(encoding="utf-8"),
            render_svg(self.dataset),
        )


if __name__ == "__main__":
    unittest.main()

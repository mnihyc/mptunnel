import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from performance_ledger import (
    APPROVED_TRADEOFF,
    DELTA_INCONCLUSIVE,
    FAIL,
    INCONCLUSIVE,
    PASS,
    COMPARISON_KIND,
    PROVEN_IMPROVEMENT,
    PROVEN_REGRESSION,
    REGISTRATION_KIND,
    TRADEOFF_KIND,
    LedgerError,
    compare_candidate,
    new_ledger,
    register_champion,
    subject_identity_from_result,
    validate_ledger,
)
from result_enrichment import (
    HOST_VALIDITY_RULES_VERSION,
    MPTUNNEL_CARRIER_PRESENTATION,
    MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE,
    MPTUNNEL_PROTOCOL_VERSION,
    RESULT_SCHEMA_VERSION,
    RUN_MANIFEST_SCHEMA_VERSION,
)

LAB_DIR = Path(__file__).resolve().parent
BASELINE_LOCK = LAB_DIR / "baseline-lock.json"
BASELINE_SHA256 = hashlib.sha256(BASELINE_LOCK.read_bytes()).hexdigest()
SCRIPT = LAB_DIR / "performance_ledger.py"


def subject(digit):
    return {
        "result_schema_version": RESULT_SCHEMA_VERSION,
        "run_manifest_schema_version": RUN_MANIFEST_SCHEMA_VERSION,
        "host_snapshot_schema_version": 1,
        "host_validity_rules_version": HOST_VALIDITY_RULES_VERSION,
        "host_snapshot_sha256": "e" * 64,
        "host_valid": True,
        "source_commit": digit * 40,
        "source_tree_dirty": False,
        "source_snapshot_sha256": digit * 64,
        "mptunnel_build_profile": "release",
        "mptunnel_build_features": [],
        "mptunnel_protocol_version": MPTUNNEL_PROTOCOL_VERSION,
        "mptunnel_carrier_presentation": MPTUNNEL_CARRIER_PRESENTATION,
        "mptunnel_client_runtime": "native",
        "mptunnel_client_runtime_version": "native",
        "mptunnel_client_target": "x86_64-unknown-linux-gnu",
        "mptunnel_client_sha256": digit * 64,
        "mptunnel_server_target": "x86_64-unknown-linux-gnu",
        "mptunnel_server_sha256": digit * 64,
        "cargo_lock_sha256": "c" * 64,
        "rustc_version": "rustc 1.90.0",
        "rustc_executable_sha256": "a" * 64,
        "cargo_version": "cargo 1.90.0",
        "cargo_executable_sha256": "b" * 64,
    }


def cell():
    return {
        "id": "tcp.equal-balanced.download",
        "dimensions": {
            "case": "mptunnel_tcp_multipath_equal_balanced",
            "direction": "download",
            "host_epoch": "lab-host-v1",
            "instrumentation": "off",
            "platform": "linux-amd64",
            "protocol": "tcp",
            "topology": "equal-balanced",
            "workload": {"connections": 2, "duration_seconds": 30},
        },
    }


def metric(direction="higher"):
    return {
        "id": "goodput_mbps" if direction == "higher" else "p95_ms",
        "unit": "Mbit/s" if direction == "higher" else "ms",
        "direction": direction,
    }


def inference():
    return {
        "method": "deterministic-paired-bootstrap-percentile-v2",
        "preregistered": True,
        "confidence_level": 0.95,
        "bootstrap_iterations": 10_000,
        "bootstrap_seed": 20260726,
        "alternative": "two_sided",
        "test_statistic": "paired_median_normalized_delta",
    }


def registration(
    champion,
    *,
    metric_value=None,
    values=None,
    registration_id="f0-initial-champion",
):
    metric_value = metric_value or metric()
    values = values or [100.0] * 7
    return {
        "schema_version": 2,
        "kind": REGISTRATION_KIND,
        "registration_id": registration_id,
        "cell": cell(),
        "metric": metric_value,
        "inference": inference(),
        "baseline_lock_sha256": BASELINE_SHA256,
        "subject": champion,
        "required_pair_count": 7,
        "declared_repeat_count": len(values),
        "repetitions": [
            {"repeat_id": f"r{index:02d}", "value": value}
            for index, value in enumerate(values, 1)
        ],
    }


def comparison(
    candidate,
    parent,
    champion,
    *,
    metric_value=None,
    candidate_values=None,
    parent_values=None,
    champion_values=None,
    comparison_id="f0-candidate-comparison",
):
    metric_value = metric_value or metric()
    candidate_values = candidate_values or [110.0] * 7
    parent_values = parent_values or [100.0] * 7
    champion_values = champion_values or [100.0] * 7
    assert len(candidate_values) == len(parent_values) == len(champion_values)
    return {
        "schema_version": 2,
        "kind": COMPARISON_KIND,
        "comparison_id": comparison_id,
        "cell": cell(),
        "metric": metric_value,
        "inference": inference(),
        "baseline_lock_sha256": BASELINE_SHA256,
        "candidate": candidate,
        "parent": parent,
        "champion": champion,
        "required_pair_count": 7,
        "declared_pair_count": len(candidate_values),
        "pairs": [
            {
                "pair_id": f"p{index:02d}",
                "candidate": candidate_value,
                "parent": parent_value,
                "champion": champion_value,
            }
            for index, (candidate_value, parent_value, champion_value) in enumerate(
                zip(candidate_values, parent_values, champion_values), 1
            )
        ],
    }


def tradeoff(
    comparison_value,
    *,
    candidate=None,
):
    return {
        "schema_version": 2,
        "kind": TRADEOFF_KIND,
        "record_id": "tradeoff-low-latency-queue",
        "comparison_id": comparison_value["comparison_id"],
        "cell": copy.deepcopy(comparison_value["cell"]),
        "metric": copy.deepcopy(comparison_value["metric"]),
        "candidate": copy.deepcopy(candidate or comparison_value["candidate"]),
        "preregistered": True,
        "approved": True,
        "approved_by": "project-owner",
        "theoretical_necessity": (
            "A lower queue bound necessarily spends peak bulk throughput to "
            "bound persistent queueing delay under the registered load."
        ),
        "benefit": {
            "kind": "latency",
            "metric_id": "interactive_p99_ms",
            "unit": "ms",
            "required_gain": 20.0,
            "observed_gain": 25.0,
        },
        "pareto_evidence_sha256": "a" * 64,
        "ablation_evidence_sha256": "b" * 64,
    }


def registered_ledger(metric_value=None, values=None):
    ledger = new_ledger(BASELINE_LOCK)
    first = subject("1")
    register_champion(
        ledger,
        registration(first, metric_value=metric_value, values=values),
        BASELINE_LOCK,
    )
    return ledger, first


class PerformanceLedgerTests(unittest.TestCase):
    def test_subject_identity_matches_current_result_schema_and_requires_valid_host(
        self,
    ):
        row = {
            **subject("1"),
            "case": "ignored",
            "status": "ok",
            "goodput_mbps": 100.0,
        }

        self.assertEqual(subject_identity_from_result(row), subject("1"))

        standard = subject("1")
        standard["mptunnel_carrier_presentation"] = (
            MPTUNNEL_CARRIER_PRESENTATION_BY_PROFILE["standard"]
        )
        self.assertEqual(subject_identity_from_result(standard), standard)

        row["source_tree_dirty"] = True
        self.assertTrue(subject_identity_from_result(row)["source_tree_dirty"])

        row["host_valid"] = False
        with self.assertRaisesRegex(LedgerError, "host_valid must be true"):
            subject_identity_from_result(row)

    def test_registration_is_deterministic_and_retains_immutable_initial_champion(
        self,
    ):
        first = subject("1")
        registration_value = registration(
            first, values=[97.0, 101.0, 98.0, 100.0, 104.0, 99.0, 103.0]
        )
        reversed_value = copy.deepcopy(registration_value)
        reversed_value["repetitions"].reverse()
        left = new_ledger(BASELINE_LOCK)
        right = new_ledger(BASELINE_LOCK)

        left_decision = register_champion(left, registration_value, BASELINE_LOCK)
        right_decision = register_champion(right, reversed_value, BASELINE_LOCK)

        self.assertEqual(left, right)
        self.assertEqual(left_decision, right_decision)
        self.assertEqual(left_decision["candidate_median"], 100.0)
        self.assertEqual(len(left["cells"][0]["champion_history"]), 1)
        validate_ledger(left, BASELINE_LOCK)

    def test_higher_is_better_requires_proven_improvement(self):
        ledger, first = registered_ledger()
        candidate = subject("2")

        result = compare_candidate(
            ledger,
            comparison(candidate, first, first),
            BASELINE_LOCK,
            commit=True,
        )

        self.assertEqual(result["status"], PASS)
        self.assertEqual(
            result["immediate_parent"]["paired_median_normalized_delta"], 10.0
        )
        self.assertEqual(
            result["historical_champion"]["paired_median_normalized_delta"], 10.0
        )
        parent_inference = result["immediate_parent"]["inference"]
        self.assertEqual(
            parent_inference["test_statistic"],
            "paired_median_normalized_delta",
        )
        self.assertEqual(parent_inference["test_statistic_value"], 10.0)
        self.assertEqual(
            parent_inference["two_sided_confidence_interval_95"],
            [10.0, 10.0],
        )
        self.assertEqual(
            parent_inference["classification"], PROVEN_IMPROVEMENT
        )
        self.assertTrue(result["promoted_to_champion"])
        self.assertEqual(len(ledger["cells"][0]["accepted_history"]), 2)
        self.assertEqual(len(ledger["cells"][0]["champion_history"]), 2)

    def test_lower_is_better_normalizes_improvements_to_positive(self):
        lower_metric = metric("lower")
        ledger, first = registered_ledger(metric_value=lower_metric, values=[10.0] * 7)
        candidate = subject("2")

        result = compare_candidate(
            ledger,
            comparison(
                candidate,
                first,
                first,
                metric_value=lower_metric,
                candidate_values=[9.0] * 7,
                parent_values=[10.0] * 7,
                champion_values=[10.0] * 7,
            ),
            BASELINE_LOCK,
        )

        self.assertEqual(result["status"], PASS)
        self.assertEqual(
            result["immediate_parent"]["paired_median_normalized_delta"], 1.0
        )
        self.assertEqual(
            result["historical_champion"]["paired_median_normalized_delta"], 1.0
        )

    def test_exact_all_zero_equivalence_passes_without_replacing_champion(self):
        ledger, first = registered_ledger()
        candidate = subject("2")

        result = compare_candidate(
            ledger,
            comparison(
                candidate,
                first,
                first,
                candidate_values=[100.0] * 7,
            ),
            BASELINE_LOCK,
            commit=True,
        )

        self.assertEqual(result["status"], PASS)
        self.assertEqual(
            result["historical_champion"]["paired_median_normalized_delta"], 0.0
        )
        self.assertFalse(result["promoted_to_champion"])
        self.assertEqual(len(ledger["cells"][0]["champion_history"]), 1)

    def test_overlapping_interval_is_inconclusive_and_never_committed(self):
        ledger, first = registered_ledger()
        before = copy.deepcopy(ledger)
        candidate = subject("2")
        wide_deltas = [-100.0, -100.0, -100.0, 0.0, 100.0, 100.0, 100.0]
        value = comparison(
            candidate,
            first,
            first,
            candidate_values=[100.0 + delta for delta in wide_deltas],
        )

        result = compare_candidate(ledger, value, BASELINE_LOCK, commit=True)

        self.assertEqual(result["status"], INCONCLUSIVE)
        self.assertFalse(result["committed"])
        self.assertEqual(ledger, before)
        self.assertEqual(
            result["immediate_parent"]["paired_median_normalized_delta"], 0.0
        )
        self.assertEqual(
            result["immediate_parent"]["inference"]["classification"],
            DELTA_INCONCLUSIVE,
        )
        with self.assertRaisesRegex(LedgerError, "proven repeated regression"):
            compare_candidate(
                ledger,
                value,
                BASELINE_LOCK,
                tradeoff_value=tradeoff(value),
            )

    def test_positive_median_with_overlapping_interval_is_inconclusive(self):
        ledger, first = registered_ledger()
        before = copy.deepcopy(ledger)
        deltas = [-5.0, -5.0, -5.0, 1.0, 5.0, 5.0, 5.0]

        result = compare_candidate(
            ledger,
            comparison(
                subject("2"),
                first,
                first,
                candidate_values=[100.0 + delta for delta in deltas],
            ),
            BASELINE_LOCK,
            commit=True,
        )

        self.assertEqual(result["status"], INCONCLUSIVE)
        self.assertFalse(result["promoted_to_champion"])
        self.assertFalse(result["committed"])
        self.assertEqual(ledger, before)

    def test_bootstrap_result_is_deterministic(self):
        ledger, first = registered_ledger()
        value = comparison(
            subject("2"),
            first,
            first,
            candidate_values=[97.0, 99.0, 100.0, 102.0, 103.0, 104.0, 106.0],
        )

        first_result = compare_candidate(ledger, value, BASELINE_LOCK)
        second_result = compare_candidate(ledger, value, BASELINE_LOCK)

        self.assertEqual(first_result, second_result)

    def test_worse_paired_median_fails_and_never_mutates_ledger(self):
        ledger, first = registered_ledger()
        before = copy.deepcopy(ledger)

        result = compare_candidate(
            ledger,
            comparison(
                subject("2"),
                first,
                first,
                candidate_values=[99.0] * 7,
            ),
            BASELINE_LOCK,
            commit=True,
        )

        self.assertEqual(result["status"], FAIL)
        self.assertFalse(result["committed"])
        self.assertEqual(ledger, before)

    def test_approved_tradeoff_advances_parent_without_overwriting_champion(self):
        ledger, first = registered_ledger()
        second = subject("2")
        passing = comparison(second, first, first, comparison_id="pass-second")
        compare_candidate(ledger, passing, BASELINE_LOCK, commit=True)
        historical_champions = copy.deepcopy(ledger["cells"][0]["champion_history"])
        third = subject("3")
        regressed = comparison(
            third,
            second,
            second,
            candidate_values=[105.0] * 7,
            parent_values=[110.0] * 7,
            champion_values=[110.0] * 7,
            comparison_id="approved-third",
        )

        result = compare_candidate(
            ledger,
            regressed,
            BASELINE_LOCK,
            tradeoff_value=tradeoff(regressed),
            commit=True,
        )

        self.assertEqual(result["status"], APPROVED_TRADEOFF)
        self.assertTrue(result["committed"])
        self.assertFalse(result["promoted_to_champion"])
        self.assertEqual(result["observed_normalized_regression"], 5.0)
        entry = ledger["cells"][0]
        self.assertEqual(entry["champion_history"], historical_champions)
        self.assertEqual(entry["accepted_history"][-1]["subject"], third)
        self.assertEqual(
            entry["accepted_history"][-1]["observed_normalized_regression"],
            5.0,
        )
        self.assertEqual(
            entry["champion_history"][-1]["subject"],
            second,
        )

        fourth = subject("4")
        followup = comparison(
            fourth,
            third,
            second,
            candidate_values=[111.0] * 7,
            parent_values=[105.0] * 7,
            champion_values=[110.0] * 7,
            comparison_id="followup-fourth",
        )
        self.assertEqual(
            compare_candidate(ledger, followup, BASELINE_LOCK)["status"], PASS
        )

    def test_tradeoff_records_observed_cost_and_must_match_identity(self):
        ledger, first = registered_ledger()
        value = comparison(
            subject("2"),
            first,
            first,
            candidate_values=[90.0] * 7,
        )

        result = compare_candidate(
            ledger,
            value,
            BASELINE_LOCK,
            tradeoff_value=tradeoff(value),
        )
        self.assertEqual(result["status"], APPROVED_TRADEOFF)
        self.assertEqual(result["observed_normalized_regression"], 10.0)

        wrong_candidate = tradeoff(value, candidate=subject("3"))
        with self.assertRaisesRegex(LedgerError, "candidate identity"):
            compare_candidate(
                ledger,
                value,
                BASELINE_LOCK,
                tradeoff_value=wrong_candidate,
            )

    def test_tradeoff_is_rejected_when_candidate_already_passes(self):
        ledger, first = registered_ledger()
        value = comparison(subject("2"), first, first)

        with self.assertRaisesRegex(LedgerError, "proven repeated regression"):
            compare_candidate(
                ledger,
                value,
                BASELINE_LOCK,
                tradeoff_value=tradeoff(value),
            )

    def test_parent_champion_cell_and_metric_identities_must_match_exactly(self):
        ledger, first = registered_ledger()
        value = comparison(subject("2"), first, first)
        value["parent"] = subject("3")
        with self.assertRaisesRegex(LedgerError, "immediate accepted parent"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["champion"] = subject("3")
        with self.assertRaisesRegex(LedgerError, "active historical champion"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["cell"]["dimensions"]["host_epoch"] = "another-host"
        with self.assertRaisesRegex(LedgerError, "cell identity"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["metric"]["unit"] = "bytes/s"
        with self.assertRaisesRegex(LedgerError, "metric identity"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["inference"]["bootstrap_seed"] += 1
        with self.assertRaisesRegex(LedgerError, "inference contract"):
            compare_candidate(ledger, value, BASELINE_LOCK)

    def test_baseline_lock_digest_and_document_are_exact_identities(self):
        ledger, first = registered_ledger()
        value = comparison(subject("2"), first, first)
        value["baseline_lock_sha256"] = "0" * 64

        with self.assertRaisesRegex(LedgerError, "baseline lock identity"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        modified = copy.deepcopy(ledger)
        modified["baseline_lock"]["document"]["tools"]["xray"]["release"] = "changed"
        with self.assertRaisesRegex(LedgerError, "exactly match"):
            validate_ledger(modified, BASELINE_LOCK)

    def test_pair_count_declaration_minimum_and_uniqueness_are_enforced(self):
        ledger, first = registered_ledger()
        value = comparison(subject("2"), first, first)
        value["declared_pair_count"] = 8
        with self.assertRaisesRegex(LedgerError, "does not match pairs"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["pairs"] = value["pairs"][:6]
        value["declared_pair_count"] = 6
        with self.assertRaisesRegex(LedgerError, "7 are required"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["pairs"][1]["pair_id"] = value["pairs"][0]["pair_id"]
        with self.assertRaisesRegex(LedgerError, "duplicate comparison pair_id"):
            compare_candidate(ledger, value, BASELINE_LOCK)

    def test_registration_rejects_duplicate_and_too_few_repetitions(self):
        first = subject("1")
        ledger = new_ledger(BASELINE_LOCK)
        value = registration(first, values=[100.0] * 6)
        with self.assertRaisesRegex(LedgerError, "7 are required"):
            register_champion(ledger, value, BASELINE_LOCK)

        value = registration(first)
        value["repetitions"][1]["repeat_id"] = value["repetitions"][0]["repeat_id"]
        with self.assertRaisesRegex(LedgerError, "duplicate registration repeat_id"):
            register_champion(ledger, value, BASELINE_LOCK)

    def test_ledger_hash_chain_detects_historical_record_tampering(self):
        ledger, _ = registered_ledger()
        ledger["cells"][0]["accepted_history"][0]["candidate_median"] = 99.0

        with self.assertRaisesRegex(LedgerError, "record hash"):
            validate_ledger(ledger, BASELINE_LOCK)

    def test_v1_and_removed_threshold_fields_are_rejected(self):
        ledger, first = registered_ledger()
        value = comparison(subject("2"), first, first)
        value["schema_version"] = 1
        with self.assertRaisesRegex(LedgerError, "schema_version must be 2"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(subject("2"), first, first)
        value["inference"]["noninferiority_margin"] = 5.0
        with self.assertRaisesRegex(LedgerError, "unknown"):
            compare_candidate(ledger, value, BASELINE_LOCK)

        value = comparison(
            subject("2"),
            first,
            first,
            candidate_values=[90.0] * 7,
        )
        old_tradeoff = tradeoff(value)
        old_tradeoff["maximum_normalized_regression"] = 10.0
        with self.assertRaisesRegex(LedgerError, "unknown"):
            compare_candidate(
                ledger,
                value,
                BASELINE_LOCK,
                tradeoff_value=old_tradeoff,
            )

    def test_cli_register_compare_and_fail_exit_contract(self):
        first = subject("1")
        second = subject("2")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "ledger.json"
            registration_path = root / "registration.json"
            passing_path = root / "passing.json"
            failing_path = root / "failing.json"
            registration_path.write_text(
                json.dumps(registration(first)), encoding="utf-8"
            )
            passing_path.write_text(
                json.dumps(comparison(second, first, first)), encoding="utf-8"
            )
            failing_path.write_text(
                json.dumps(
                    comparison(
                        second,
                        first,
                        first,
                        candidate_values=[99.0] * 7,
                        comparison_id="failing-comparison",
                    )
                ),
                encoding="utf-8",
            )

            init_result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "init",
                    "--ledger",
                    str(ledger_path),
                    "--baseline-lock",
                    str(BASELINE_LOCK),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(init_result.returncode, 0, init_result.stderr)

            register_result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "register",
                    "--ledger",
                    str(ledger_path),
                    "--baseline-lock",
                    str(BASELINE_LOCK),
                    "--evidence",
                    str(registration_path),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(register_result.returncode, 0, register_result.stderr)
            before_fail = ledger_path.read_bytes()

            fail_result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "compare",
                    "--ledger",
                    str(ledger_path),
                    "--baseline-lock",
                    str(BASELINE_LOCK),
                    "--evidence",
                    str(failing_path),
                    "--commit",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(fail_result.returncode, 1, fail_result.stderr)
            self.assertEqual(json.loads(fail_result.stdout)["status"], FAIL)
            self.assertEqual(ledger_path.read_bytes(), before_fail)

            pass_result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "compare",
                    "--ledger",
                    str(ledger_path),
                    "--baseline-lock",
                    str(BASELINE_LOCK),
                    "--evidence",
                    str(passing_path),
                    "--commit",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(pass_result.returncode, 0, pass_result.stderr)
            self.assertTrue(json.loads(pass_result.stdout)["committed"])
            saved = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(len(saved["cells"][0]["accepted_history"]), 2)
            self.assertEqual(len(saved["cells"][0]["champion_history"]), 2)


if __name__ == "__main__":
    unittest.main()

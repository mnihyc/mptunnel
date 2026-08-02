import copy
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

try:
    from validate_performance_declaration import (
        BASE_COMPLETENESS,
        ContractError,
        classify_paths,
        load_json,
        required_payload,
        scope_requirements,
        validate_declaration,
        validate_registry,
    )
except ModuleNotFoundError:
    from lab.validate_performance_declaration import (
        BASE_COMPLETENESS,
        ContractError,
        classify_paths,
        load_json,
        required_payload,
        scope_requirements,
        validate_declaration,
        validate_registry,
    )


LAB_DIR = Path(__file__).resolve().parent
REGISTRY_PATH = LAB_DIR / "performance-impact-registry.json"
EXAMPLE_PATH = LAB_DIR / "performance-change.example.json"
VALIDATOR_PATH = LAB_DIR / "validate_performance_declaration.py"


def declaration_for_scope(registry, scope, changed_path):
    coverage = scope_requirements(registry, [scope])
    return {
        "schema_version": 2,
        "registry_revision": registry["registry_revision"],
        "change_id": f"test-{scope}",
        "impact_status": "affected",
        "uncertainty": "bounded",
        "rationale": "Test fixture with explicit affected cell and metric coverage.",
        "changed_paths": [changed_path],
        "impact_scopes": [scope],
        "affected": [
            {"cell_id": cell_id, "metrics": sorted(metrics)}
            for cell_id, metrics in sorted(coverage.items())
        ],
        "quick_triage": {"status": "not_run", "artifact_sha256": []},
        "acceptance_evidence": [],
    }


def evidence_for_declaration(
    registry,
    declaration,
    *,
    lane_override=None,
    valid_pairs_delta=0,
    triggered_events_delta=0,
    definition_dual_run=False,
):
    records = []
    for index, affected in enumerate(declaration["affected"]):
        cell = registry["cells"][affected["cell_id"]]
        raw_digest = hashlib.sha256(f"raw-{index}".encode()).hexdigest()
        summary_digest = hashlib.sha256(f"summary-{index}".encode()).hexdigest()
        completeness = {field: True for field in BASE_COMPLETENESS}
        completeness["definition_dual_run"] = definition_dual_run
        records.append(
            {
                "cell_id": affected["cell_id"],
                "metrics": list(affected["metrics"]),
                "lane": lane_override or cell["minimum_lane"],
                "covered_profiles": list(cell["profiles"]),
                "valid_pairs": max(
                    0, cell["minimum_valid_pairs"] + valid_pairs_delta
                ),
                "triggered_events": max(
                    0, cell["minimum_triggered_events"] + triggered_events_delta
                ),
                "comparison_references": list(cell["required_references"]),
                "artifacts": [
                    {"kind": "raw", "sha256": raw_digest},
                    {"kind": "summary", "sha256": summary_digest},
                ],
                "completeness": completeness,
                "statistical_test": {
                    "method": "paired_bootstrap_two_sided_95",
                    "hypothesis": "directional_zero_classification",
                    "preregistered": True,
                },
                "result": "pass",
            }
        )
    return records


class PerformanceDeclarationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.registry = load_json(REGISTRY_PATH)
        validate_registry(cls.registry)

    def test_registry_makes_quick_triage_only(self):
        quick = self.registry["lanes"]["quick"]

        self.assertFalse(quick["acceptance_capable"])
        self.assertEqual(quick["role"], "triage_only")
        self.assertGreaterEqual(len(self.registry["cells"]), 20)
        self.assertGreaterEqual(len(self.registry["metrics"]), 50)

    def test_path_classification_uses_specific_first_match_and_ignores_tests(self):
        classified = classify_paths(
            self.registry,
            [
                "src/runtime/datagram/quic.rs",
                "src/scheduler/policy.rs",
                "src/scheduler/tests_policy.rs",
                "docs/ARCHITECTURE.md",
            ],
        )

        self.assertEqual(
            classified["matches"]["src/runtime/datagram/quic.rs"][
                "required_scopes"
            ],
            ["datagram-engine", "quic-carrier"],
        )
        self.assertEqual(
            classified["matches"]["src/scheduler/policy.rs"]["required_scopes"],
            ["scheduler-and-recovery"],
        )
        self.assertIn("src/scheduler/tests_policy.rs", classified["ignored"])
        self.assertIn("docs/ARCHITECTURE.md", classified["ignored"])

    def test_performance_envelope_requires_full_matrix(self):
        changed_paths = ["src/performance.rs"]

        payload = required_payload(self.registry, changed_paths)

        self.assertTrue(payload["declaration_required"])
        self.assertEqual(payload["required_scopes"], ["full-matrix"])
        for path in changed_paths:
            self.assertEqual(
                payload["path_classification"]["matches"][path],
                {
                    "rule": "performance-envelope",
                    "required_scopes": ["full-matrix"],
                },
            )
        self.assertEqual(payload["path_classification"]["unclassified"], [])
        affected_cells = {
            affected["cell_id"] for affected in payload["affected"]
        }
        self.assertIn("reliable.tcp.single.download", affected_cells)
        self.assertIn("platform.android.packaged", affected_cells)

    def test_required_payload_expands_scopes_to_cells_and_explicit_metrics(self):
        payload = required_payload(
            self.registry, ["src/runtime/datagram/quic.rs"]
        )

        self.assertTrue(payload["declaration_required"])
        self.assertEqual(
            payload["required_scopes"], ["datagram-engine", "quic-carrier"]
        )
        cells = {entry["cell_id"]: entry for entry in payload["affected"]}
        self.assertIn("datagram.quic.single", cells)
        self.assertIn("reliable.quic.single.download", cells)
        self.assertIn("datagram_p95_ms", cells["datagram.quic.single"]["metrics"])
        self.assertIn(
            "receiver_goodput_mbps",
            cells["reliable.quic.single.download"]["metrics"],
        )

    def test_example_is_a_valid_candidate_declaration(self):
        declaration = load_json(EXAMPLE_PATH)

        result = validate_declaration(self.registry, declaration)

        self.assertEqual(result["impact_status"], "affected")
        self.assertEqual(set(result["coverage"]), {"diagnostic.instrumentation"})

    def test_sensitive_path_cannot_claim_no_performance_impact(self):
        declaration = {
            "schema_version": 2,
            "registry_revision": self.registry["registry_revision"],
            "change_id": "test-false-no-impact",
            "impact_status": "not_affected",
            "uncertainty": "bounded",
            "rationale": "Invalid fixture.",
            "changed_paths": ["src/scheduler.rs"],
            "impact_scopes": [],
            "affected": [],
            "quick_triage": {"status": "not_run", "artifact_sha256": []},
            "acceptance_evidence": [],
        }

        with self.assertRaisesRegex(
            ContractError, "cannot be declared not_affected"
        ):
            validate_declaration(self.registry, declaration)

    def test_declaration_cannot_omit_path_required_scope(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["changed_paths"] = ["src/runtime/datagram/quic.rs"]

        with self.assertRaisesRegex(ContractError, "omits path-required"):
            validate_declaration(self.registry, declaration)

    def test_declaration_cannot_omit_a_required_metric(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["affected"][0]["metrics"].remove(
            "diagnostic_event_drop_pct"
        )

        with self.assertRaisesRegex(ContractError, "omits required metrics"):
            validate_declaration(self.registry, declaration)

    def test_uncertain_impact_requires_full_matrix(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["uncertainty"] = "uncertain"

        with self.assertRaisesRegex(ContractError, "requires the full-matrix"):
            validate_declaration(self.registry, declaration)

    def test_authoritative_changed_paths_must_match_exactly(self):
        declaration = load_json(EXAMPLE_PATH)

        with self.assertRaisesRegex(ContractError, "do not exactly match"):
            validate_declaration(
                self.registry,
                declaration,
                external_changed_paths=[
                    "src/lab_diagnostics.rs",
                    "src/scheduler.rs",
                ],
            )

    def test_clear_quick_triage_without_repeated_evidence_cannot_accept(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["quick_triage"]["status"] = "clear"

        with self.assertRaisesRegex(
            ContractError, "requires complete repeated evidence"
        ):
            validate_declaration(
                self.registry, declaration, phase="acceptance"
            )

    def test_quick_lane_evidence_cannot_accept(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["acceptance_evidence"] = evidence_for_declaration(
            self.registry, declaration, lane_override="quick"
        )

        with self.assertRaisesRegex(ContractError, "triage only"):
            validate_declaration(
                self.registry, declaration, phase="acceptance"
            )

    def test_primary_runtime_cell_requires_seven_valid_pairs(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["acceptance_evidence"] = evidence_for_declaration(
            self.registry, declaration, valid_pairs_delta=-1
        )

        with self.assertRaisesRegex(ContractError, "at least 7 valid pairs"):
            validate_declaration(
                self.registry, declaration, phase="acceptance"
            )

    def test_complete_repeated_evidence_accepts(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["quick_triage"]["status"] = "signal"
        declaration["acceptance_evidence"] = evidence_for_declaration(
            self.registry, declaration
        )

        validated = validate_declaration(
            self.registry, declaration, phase="acceptance"
        )

        self.assertEqual(
            set(validated["coverage"]), {"diagnostic.instrumentation"}
        )

    def test_v1_declaration_and_old_quick_filter_are_rejected(self):
        declaration = load_json(EXAMPLE_PATH)
        declaration["schema_version"] = 1
        with self.assertRaisesRegex(ContractError, "schema_version must be 2"):
            validate_declaration(self.registry, declaration)

        declaration = load_json(EXAMPLE_PATH)
        declaration["quick_filter"] = declaration.pop("quick_triage")
        with self.assertRaisesRegex(ContractError, "quick_triage must be an object"):
            validate_declaration(self.registry, declaration)

    def test_fault_cell_requires_thirty_triggered_events(self):
        registry = copy.deepcopy(self.registry)
        registry["cell_groups"]["fault-test"] = ["fault.reliable.download"]
        registry["impact_scopes"]["fault-test"] = {
            "cell_groups": ["fault-test"],
            "metric_sets": ["*"],
        }
        declaration = declaration_for_scope(
            registry, "fault-test", "research/fault-change.txt"
        )
        declaration["acceptance_evidence"] = evidence_for_declaration(
            registry, declaration, triggered_events_delta=-1
        )

        with self.assertRaisesRegex(
            ContractError, "at least 30 triggered events"
        ):
            validate_declaration(registry, declaration, phase="acceptance")

    def test_platform_cell_cannot_accept_nightly_evidence(self):
        registry = copy.deepcopy(self.registry)
        registry["cell_groups"]["platform-test"] = [
            "platform.windows.packaged"
        ]
        registry["impact_scopes"]["platform-test"] = {
            "cell_groups": ["platform-test"],
            "metric_sets": ["*"],
        }
        declaration = declaration_for_scope(
            registry, "platform-test", "research/platform-change.txt"
        )
        declaration["acceptance_evidence"] = evidence_for_declaration(
            registry, declaration, lane_override="nightly"
        )

        with self.assertRaisesRegex(ContractError, "requires release evidence"):
            validate_declaration(registry, declaration, phase="acceptance")

    def test_measurement_contract_requires_dual_run(self):
        declaration = declaration_for_scope(
            self.registry, "measurement-contract", "lab/container_stats.py"
        )
        declaration["acceptance_evidence"] = evidence_for_declaration(
            self.registry, declaration, definition_dual_run=False
        )

        with self.assertRaisesRegex(ContractError, "require dual-run"):
            validate_declaration(
                self.registry, declaration, phase="acceptance"
            )

        for evidence in declaration["acceptance_evidence"]:
            evidence["completeness"]["definition_dual_run"] = True
        validate_declaration(
            self.registry, declaration, phase="acceptance"
        )

    def test_cli_registry_check_and_non_sensitive_path_check(self):
        checked = subprocess.run(
            [sys.executable, str(VALIDATOR_PATH), "--check-registry"],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertIn("valid performance registry", checked.stdout)

        with tempfile.TemporaryDirectory() as temp_dir:
            changed = Path(temp_dir) / "changed.txt"
            changed.write_text("docs/ARCHITECTURE.md\n", encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR_PATH),
                    "--changed-paths-file",
                    str(changed),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("no performance declaration required", result.stdout)

    def test_cli_requires_manifest_for_sensitive_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            changed = Path(temp_dir) / "changed.txt"
            changed.write_text("src/scheduler.rs\n", encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR_PATH),
                    "--changed-paths-file",
                    str(changed),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("performance declaration required", result.stderr)

    def test_cli_requires_manifest_for_extracted_core_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            changed = Path(temp_dir) / "changed.txt"
            changed.write_text(
                "src/scheduler/policy.rs\n", encoding="utf-8"
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR_PATH),
                    "--changed-paths-file",
                    str(changed),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 2)
        self.assertIn("performance declaration required", result.stderr)

    def test_print_required_is_machine_readable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            changed = Path(temp_dir) / "changed.txt"
            changed.write_text("src/lab_diagnostics.rs\n", encoding="utf-8")
            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATOR_PATH),
                    "--changed-paths-file",
                    str(changed),
                    "--print-required",
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        payload = json.loads(result.stdout)
        self.assertTrue(payload["declaration_required"])
        self.assertEqual(
            payload["required_scopes"], ["diagnostic-instrumentation"]
        )


if __name__ == "__main__":
    unittest.main()

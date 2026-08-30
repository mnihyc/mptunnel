import json
import sys
import tempfile
import unittest
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(LAB_DIR))

from derive_performance_series import CASE_SERIES  # noqa: E402
from evaluate_release_series import RAW_CASE, main as evaluate_main  # noqa: E402
from result_enrichment import (  # noqa: E402
    MPTUNNEL_CARRIER_PRESENTATION,
    MPTUNNEL_PROTOCOL_VERSION,
    RUN_MANIFEST_SCHEMA_VERSION,
)
import test_performance_series as performance_fixtures  # noqa: E402

SOURCE_COMMIT = "b" * 40
HOST_SNAPSHOT = "a" * 64
CLIENT_SHA256 = "1" * 64
SERVER_SHA256 = CLIENT_SHA256
CLIENT_RUNTIME = "native"
CLIENT_RUNTIME_VERSION = "native"
CLIENT_TARGET = "x86_64-unknown-linux-gnu"
SERVER_TARGET = "x86_64-unknown-linux-gnu"
CONTAINER_IMAGE_IDS = {
    "client": "sha256:" + "5" * 64,
    "server": "sha256:" + "6" * 64,
    "target": "sha256:" + "7" * 64,
}
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
        mptunnel_build_features=[],
        mptunnel_build_profile="release",
        mptunnel_protocol_version=MPTUNNEL_PROTOCOL_VERSION,
        mptunnel_carrier_presentation=MPTUNNEL_CARRIER_PRESENTATION,
        mptunnel_client_runtime=CLIENT_RUNTIME,
        mptunnel_client_runtime_version=CLIENT_RUNTIME_VERSION,
        mptunnel_client_target=CLIENT_TARGET,
        mptunnel_client_sha256=CLIENT_SHA256,
        mptunnel_server_target=SERVER_TARGET,
        mptunnel_server_sha256=SERVER_SHA256,
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
    metadata["endpoint_clocks"]["remote-egress"]["service"] = "target"
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
        mptunnel_build_features=[],
        mptunnel_build_profile="release",
        mptunnel_protocol_version=MPTUNNEL_PROTOCOL_VERSION,
        mptunnel_carrier_presentation=MPTUNNEL_CARRIER_PRESENTATION,
        mptunnel_client_runtime=CLIENT_RUNTIME,
        mptunnel_client_runtime_version=CLIENT_RUNTIME_VERSION,
        mptunnel_client_target=CLIENT_TARGET,
        mptunnel_client_sha256=CLIENT_SHA256,
        mptunnel_server_target=SERVER_TARGET,
        mptunnel_server_sha256=SERVER_SHA256,
        bulk_error=None,
        interactive_error=None,
        bulk_interactive_dynamic_loss=direct_trace(),
        bulk_interval_goodput_raw_mbps=[110.0] * 200,
        bulk_interval_goodput_mbps=[110.0] * 194,
    )
    set_latency(record, 95.0)
    return record


def release_manifest():
    manifest = performance_fixtures.PerformanceSeriesDerivationTests.manifest()
    manifest["schema_version"] = RUN_MANIFEST_SCHEMA_VERSION
    manifest["safe_environment_overrides"].update(
        MPTUNNEL_LAB_NETEM_MODE="apply",
        MPTUNNEL_LAB_INTERNET_SEED="mptunnel-random-internet-v1",
        MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES="0",
    )
    manifest["workload"].update(object_mib=4096, bulk_connections=1)
    manifest["execution"].update(
        build_product=False,
        build_lab_images=False,
        lab_diagnostics="0",
        lab_perf="0",
        management_snapshots="0",
        container_stats="1",
        use_path_hints=False,
        require_competitor_baselines=True,
    )
    manifest["containers"] = {
        role: {"image_id": image_id}
        for role, image_id in CONTAINER_IMAGE_IDS.items()
    }
    manifest["product"] = {
        "mptunnel_build_profile": "release",
        "mptunnel_build_features": [],
        "mptunnel_protocol_version": MPTUNNEL_PROTOCOL_VERSION,
        "mptunnel_transport_profile": "shared-secret",
        "mptunnel_carrier_presentation": MPTUNNEL_CARRIER_PRESENTATION,
    }
    return manifest


def write_result(root, name, records):
    result_dir = root / name
    result_dir.mkdir()
    result = result_dir / "results.jsonl"
    result.write_text(
        "".join(json.dumps(record) + "\n" for record in records), encoding="utf-8"
    )
    (result_dir / "run-manifest.json").write_text(
        json.dumps(release_manifest()),
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
        self.assertEqual(
            verdict["runtime_identity"],
            {
                "mptunnel_client_runtime": CLIENT_RUNTIME,
                "mptunnel_client_runtime_version": CLIENT_RUNTIME_VERSION,
                "mptunnel_client_target": CLIENT_TARGET,
                "mptunnel_client_sha256": CLIENT_SHA256,
                "mptunnel_server_target": SERVER_TARGET,
                "mptunnel_server_sha256": SERVER_SHA256,
            },
        )
        self.assertEqual(verdict["container_image_identity"], CONTAINER_IMAGE_IDS)
        self.assertEqual(len(verdict["repetitions"]), 2)
        for index, repetition in enumerate(verdict["repetitions"], 1):
            self.assertEqual(repetition["status"], "pass")
            self.assertTrue(repetition["raw_control"]["pass"])
            self.assertEqual(repetition["raw_control"]["source"], f"raw-{index}")
            self.assertEqual(repetition["product_sources"], [f"product-{index}"])
            self.assertEqual(repetition["raw_control"]["trajectory_windows"], 38)
            self.assertEqual(len(repetition["comparisons"]), 11)
            self.assertTrue(all(item["pass"] for item in repetition["comparisons"]))
            self.assertFalse(
                any("raw" in item["id"] for item in repetition["comparisons"])
            )
            self.assertFalse(
                any(
                    item["id"].startswith(("xray_vmess.loss_", "hysteria2.loss_"))
                    for item in repetition["comparisons"]
                )
            )
        self.assertNotIn(str(root), json.dumps(verdict))

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

    def test_product_repetition_is_one_indivisible_results_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            accepted_output = root / "accepted.json"
            accepted_status = evaluate_main(
                invocation(products, raws, accepted_output)
            )

            split_products = [f"{products[0]},{products[1]}", products[1]]
            rejected_output = root / "rejected.json"
            rejected_status = evaluate_main(
                invocation(split_products, raws, rejected_output)
            )
            rejected = json.loads(rejected_output.read_text(encoding="utf-8"))

        self.assertEqual(accepted_status, 0)
        self.assertEqual(rejected_status, 2)
        self.assertEqual(rejected["status"], "invalid")
        self.assertEqual(rejected["repetitions"], [])
        self.assertIn("exactly one results.jsonl path", rejected["errors"][0])

    def test_all_four_runs_require_one_complete_container_image_triple(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            manifest_path = raws[1].parent / "run-manifest.json"
            original = json.loads(manifest_path.read_text(encoding="utf-8"))

            mismatched = json.loads(json.dumps(original))
            mismatched["containers"]["server"]["image_id"] = "sha256:" + "8" * 64
            manifest_path.write_text(json.dumps(mismatched), encoding="utf-8")
            mismatch_output = root / "image-mismatch.json"
            mismatch_status = evaluate_main(
                invocation(products, raws, mismatch_output)
            )
            mismatch = json.loads(mismatch_output.read_text(encoding="utf-8"))

            incomplete = json.loads(json.dumps(original))
            del incomplete["containers"]["target"]
            manifest_path.write_text(json.dumps(incomplete), encoding="utf-8")
            incomplete_output = root / "image-incomplete.json"
            incomplete_status = evaluate_main(
                invocation(products, raws, incomplete_output)
            )
            incomplete_verdict = json.loads(
                incomplete_output.read_text(encoding="utf-8")
            )

            malformed = json.loads(json.dumps(original))
            malformed["containers"]["target"]["image_id"] = "latest"
            manifest_path.write_text(json.dumps(malformed), encoding="utf-8")
            malformed_output = root / "image-malformed.json"
            malformed_status = evaluate_main(
                invocation(products, raws, malformed_output)
            )
            malformed_verdict = json.loads(
                malformed_output.read_text(encoding="utf-8")
            )

        self.assertEqual(mismatch_status, 2)
        self.assertEqual(mismatch["status"], "invalid")
        self.assertEqual(mismatch["repetitions"], [])
        self.assertIn("one container image-ID triple", mismatch["errors"][0])
        self.assertEqual(incomplete_status, 2)
        self.assertEqual(incomplete_verdict["status"], "invalid")
        self.assertEqual(incomplete_verdict["repetitions"], [])
        self.assertIn(
            "incomplete or invalid container image-ID triple",
            incomplete_verdict["errors"][0],
        )
        self.assertEqual(malformed_status, 2)
        self.assertEqual(malformed_verdict["status"], "invalid")
        self.assertIn(
            "incomplete or invalid container image-ID triple",
            malformed_verdict["errors"][0],
        )

    def test_all_four_manifests_require_the_exact_release_profile(self):
        invalid_fields = (
            (("workload", "object_mib"), 2048),
            (("workload", "bulk_connections"), 2),
            (("execution", "build_product"), True),
            (("execution", "build_lab_images"), True),
            (("execution", "lab_diagnostics"), "1"),
            (("execution", "lab_perf"), "1"),
            (("execution", "management_snapshots"), "1"),
            (("execution", "container_stats"), "0"),
            (("execution", "use_path_hints"), True),
            (("execution", "require_competitor_baselines"), False),
            (
                ("safe_environment_overrides", "MPTUNNEL_LAB_NETEM_MODE"),
                "internet-five-path-epoch-0",
            ),
            (
                ("safe_environment_overrides", "MPTUNNEL_LAB_INTERNET_SEED"),
                "different-seed",
            ),
            (
                (
                    "safe_environment_overrides",
                    "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES",
                ),
                "1",
            ),
        )
        for index, (keys, value) in enumerate(invalid_fields):
            with self.subTest(field=".".join(keys)), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                products, raws = write_cohort(root)
                manifest_path = raws[1].parent / "run-manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest[keys[0]][keys[1]] = value
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                output = root / f"invalid-profile-{index}.json"

                status = evaluate_main(invocation(products, raws, output))
                verdict = json.loads(output.read_text(encoding="utf-8"))

                self.assertEqual(status, 2)
                self.assertEqual(verdict["status"], "invalid")
                self.assertEqual(verdict["repetitions"], [])
                self.assertIn("frozen v0.4.7 release profile", verdict["errors"][0])

    def test_release_profile_rejects_missing_build_provenance(self):
        for flag in ("build_product", "build_lab_images"):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                products, raws = write_cohort(root)
                manifest_path = products[0].parent / "run-manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                del manifest["execution"][flag]
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                output = root / f"missing-{flag}.json"

                status = evaluate_main(invocation(products, raws, output))
                verdict = json.loads(output.read_text(encoding="utf-8"))

                self.assertEqual(status, 2)
                self.assertEqual(verdict["status"], "invalid")
                self.assertEqual(verdict["repetitions"], [])
                self.assertIn("frozen v0.4.7 release profile", verdict["errors"][0])

    def test_release_profile_rejects_stale_or_missing_manifest_schema(self):
        for schema_version in (3, None):
            with (
                self.subTest(schema_version=schema_version),
                tempfile.TemporaryDirectory() as temporary,
            ):
                root = Path(temporary)
                products, raws = write_cohort(root)
                manifest_path = raws[0].parent / "run-manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                if schema_version is None:
                    del manifest["schema_version"]
                else:
                    manifest["schema_version"] = schema_version
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                output = root / f"schema-{schema_version}.json"

                status = evaluate_main(invocation(products, raws, output))
                verdict = json.loads(output.read_text(encoding="utf-8"))

                self.assertEqual(status, 2)
                self.assertEqual(verdict["status"], "invalid")
                self.assertEqual(verdict["repetitions"], [])
                self.assertIn("run-manifest schema 4", verdict["errors"][0])

    def test_release_wire_and_native_runtime_identity_are_exact(self):
        invalid_wire_fields = (
            ("mptunnel_build_profile", "debug"),
            ("mptunnel_build_features", ["lab-diagnostics"]),
            ("mptunnel_protocol_version", 7),
            ("mptunnel_transport_profile", "standard"),
            ("mptunnel_carrier_presentation", "invalid-carrier"),
        )
        for field, value in invalid_wire_fields:
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                products, raws = write_cohort(root)
                manifest_path = raws[0].parent / "run-manifest.json"
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
                manifest["product"][field] = value
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                output = root / f"invalid-{field}.json"

                status = evaluate_main(invocation(products, raws, output))
                verdict = json.loads(output.read_text(encoding="utf-8"))

                self.assertEqual(status, 2)
                self.assertIn("frozen release wire/build identity", verdict["errors"][0])

        invalid_runtime_updates = (
            {
                "mptunnel_client_runtime": "wine",
                "mptunnel_client_runtime_version": "wine-9.0",
            },
            {"mptunnel_client_runtime_version": "native-v2"},
            {"mptunnel_server_target": "aarch64-unknown-linux-gnu"},
            {"mptunnel_server_sha256": "2" * 64},
        )
        for index, updates in enumerate(invalid_runtime_updates):
            with self.subTest(runtime=index), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                products, raws = write_cohort(root)
                for path in (*products, *raws):
                    rows = [json.loads(line) for line in path.read_text().splitlines()]
                    for row in rows:
                        row.update(updates)
                    path.write_text(
                        "".join(json.dumps(row) + "\n" for row in rows),
                        encoding="utf-8",
                    )
                output = root / f"invalid-runtime-{index}.json"

                status = evaluate_main(invocation(products, raws, output))
                verdict = json.loads(output.read_text(encoding="utf-8"))

                self.assertEqual(status, 2)
                self.assertIn("symmetric native", verdict["errors"][0])

    def test_paired_raw_and_product_attempt_counts_must_match(self):
        def shorten(path, case):
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            record = next(row for row in rows if row["case"] == case)
            record["interactive_attempt_series"].pop()
            last = record["interactive_attempt_series"][-1]
            last["start_offset_s"] = 39.5
            last["end_offset_s"] = 39.5 + last["latency_ms"] / 1000.0
            record["interactive_count"] = 79
            record["interactive_ok"] = 79
            path.write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            raw_original = raws[1].read_text(encoding="utf-8")
            shorten(raws[1], RAW_CASE)
            raw_output = root / "raw-attempt-mismatch.json"
            raw_status = evaluate_main(invocation(products, raws, raw_output))
            raw_verdict = json.loads(raw_output.read_text(encoding="utf-8"))

            raws[1].write_text(raw_original, encoding="utf-8")
            product_case = CASE_SERIES[0][2]
            shorten(products[1], product_case)
            product_output = root / "product-attempt-mismatch.json"
            product_status = evaluate_main(
                invocation(products, raws, product_output)
            )
            product_verdict = json.loads(product_output.read_text(encoding="utf-8"))

        self.assertEqual(raw_status, 2)
        self.assertEqual(raw_verdict["status"], "invalid")
        self.assertIn("raw controls", raw_verdict["errors"][0])
        self.assertEqual(product_status, 2)
        self.assertEqual(product_verdict["status"], "invalid")
        self.assertIn("paired mpp_tcp", product_verdict["errors"][0])

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

    def test_stale_or_feature_altered_product_binary_invalidates_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            rows = [json.loads(line) for line in products[1].read_text().splitlines()]
            for row in rows:
                row["mptunnel_server_sha256"] = "3" * 64
            products[1].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            stale_output = root / "stale-runtime.json"
            stale_status = evaluate_main(invocation(products, raws, stale_output))
            stale = json.loads(stale_output.read_text(encoding="utf-8"))

            feature_root = root / "feature-cohort"
            feature_root.mkdir()
            products, raws = write_cohort(feature_root)
            rows = [json.loads(line) for line in products[0].read_text().splitlines()]
            rows[0]["mptunnel_build_features"] = ["lab-diagnostics"]
            products[0].write_text(
                "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8"
            )
            feature_output = root / "feature-runtime.json"
            feature_status = evaluate_main(invocation(products, raws, feature_output))
            feature = json.loads(feature_output.read_text(encoding="utf-8"))

        self.assertEqual(stale_status, 2)
        self.assertEqual(stale["status"], "invalid")
        self.assertIn("one client/server runtime binary pair", stale["errors"][0])
        self.assertEqual(feature_status, 2)
        self.assertEqual(feature["status"], "invalid")
        self.assertIn("frozen release wire/build identity", feature["errors"][0])

    def test_paired_raw_control_must_record_the_clean_complete_runtime_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            products, raws = write_cohort(root)
            original = json.loads(raws[1].read_text(encoding="utf-8"))
            row = dict(original)
            row["mptunnel_client_sha256"] = "4" * 64
            raws[1].write_text(json.dumps(row) + "\n", encoding="utf-8")
            output = root / "raw-runtime.json"

            status = evaluate_main(invocation(products, raws, output))
            verdict = json.loads(output.read_text(encoding="utf-8"))

            incomplete = dict(original)
            del incomplete["mptunnel_client_runtime_version"]
            raws[1].write_text(json.dumps(incomplete) + "\n", encoding="utf-8")
            incomplete_output = root / "raw-incomplete-runtime.json"
            incomplete_status = evaluate_main(
                invocation(products, raws, incomplete_output)
            )
            incomplete_verdict = json.loads(
                incomplete_output.read_text(encoding="utf-8")
            )

            altered = dict(original)
            altered["mptunnel_build_features"] = ["lab-diagnostics"]
            raws[1].write_text(json.dumps(altered) + "\n", encoding="utf-8")
            altered_output = root / "raw-feature-runtime.json"
            altered_status = evaluate_main(invocation(products, raws, altered_output))
            altered_verdict = json.loads(altered_output.read_text(encoding="utf-8"))

        self.assertEqual(status, 2)
        self.assertEqual(verdict["repetitions"][1]["status"], "invalid")
        self.assertIn(
            "raw control runtime identity differs",
            verdict["repetitions"][1]["errors"][0],
        )
        self.assertEqual(incomplete_status, 2)
        self.assertIn(
            "complete client/server runtime identity",
            incomplete_verdict["repetitions"][1]["errors"][0],
        )
        self.assertEqual(altered_status, 2)
        self.assertIn(
            "frozen release wire/build identity",
            altered_verdict["repetitions"][1]["errors"][0],
        )


if __name__ == "__main__":
    unittest.main()

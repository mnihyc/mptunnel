import base64
import copy
import contextlib
import io
import json
import sys
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

LAB_DIR = Path(__file__).resolve().parent
ROOT = LAB_DIR.parent
DATASET_PATH = (
    ROOT / "docs" / "assets" / "performance" / "sustained-random-internet-series.json"
)
SVG_PATH = (
    ROOT / "docs" / "assets" / "performance" / "sustained-random-internet-series.svg"
)
EXPECTED_IDS = [
    "mpp_tcp",
    "mpp_quic",
    "mpp_default",
    "xray_vmess",
    "hysteria2",
]

sys.path.insert(0, str(LAB_DIR))
from render_performance_series import (  # noqa: E402
    DatasetError,
    EXPECTED_STYLE,
    load_dataset,
    render_svg,
    validate_dataset,
)
from derive_performance_series import (  # noqa: E402
    CASE_SERIES,
    DerivationError,
    _goodput_series,
    _latency_series,
    _validate_record,
    derive_dataset,
    main as derive_main,
    parse_args,
)
from result_enrichment import (  # noqa: E402
    BULK_INTERACTIVE_DYNAMIC_LOSS_CONDITION,
    validate_bulk_interactive_dynamic_loss_metadata,
    validate_bulk_interactive_probe_route,
)


def contract_fixture():
    goodput = [
        {
            "time_s": float(index),
            "low": 8.0 + index,
            "median": 9.0 + index,
            "high": 10.0 + index,
            "available": 2,
            "repetitions": 2,
        }
        for index in range(1, 5)
    ]
    latency = copy.deepcopy(goodput)
    for sample in latency:
        sample["outcomes"] = {"success": 2}
    latency[1].update(
        low=None,
        median=None,
        high=None,
        available=0,
        outcomes={"io_error": 2},
    )
    return {
        "schema_version": 1,
        "figure": {
            "title": "Structural renderer contract",
            "subtitle": "Synthetic values exercise rendering only, not performance claims.",
            "condition_note": "Unit-test fixture; never publication evidence.",
            "variability_note": "Line: median; band: range.",
        },
        "provenance": {
            "aggregation": "pointwise_median_min_max",
            "goodput_window_alignment": "symmetric_full_windows",
            "valid_repetitions": 2,
            "source_runs": [
                {"id": "repetition-1", "result_dirs": ["fixture-a"]},
                {"id": "repetition-2", "result_dirs": ["fixture-b"]},
            ],
            "condition": {
                "netem_mode": "test-only",
                "internet_seed": "test-only",
                "include_outages": "0",
                "mpp_path_hints": "0",
                "hysteria_client_rate": "1mbit",
                "hysteria_server_rate": "1mbit",
                "load_duration_s": 4.0,
                "bulk_connections": 1,
                "object_mib": 1,
                "case_isolation": True,
                "container_isolation": True,
                "probe": {
                    "mode": "socks5",
                    "target": "target:8080",
                    "tcp_echo_target": "target:10022",
                    "test_duration_s": 4.0,
                    "bulk_load_duration_s": 4.0,
                    "bulk_interval_seconds": 0.2,
                    "bulk_interval_trim_discard_each_end": 3,
                    "interactive_interval_ms": 500,
                    "interactive_timeout_ms": 5000,
                    "interactive_payload_bytes": 64,
                },
            },
        },
        "series": [
            {
                "id": series_id,
                "label": series_id,
                "valid_repetitions": 2,
                "implementation": {
                    "tool": (
                        "mptunnel"
                        if series_id.startswith("mpp_")
                        else "xray" if series_id == "xray_vmess" else "hysteria2"
                    ),
                    "carrier": series_id,
                    **(
                        {"protocol_version": 1, "build_profile": "release"}
                        if series_id.startswith("mpp_")
                        else {"release": "test-only"}
                    ),
                },
                "goodput": copy.deepcopy(goodput),
                "latency": copy.deepcopy(latency),
            }
            for series_id in EXPECTED_STYLE
        ],
    }


class PerformanceSeriesContractTests(unittest.TestCase):
    def test_null_latency_breaks_lines_and_marks_unavailability(self):
        svg = render_svg(contract_fixture())
        root = ET.fromstring(svg)
        namespace = {"svg": "http://www.w3.org/2000/svg"}
        goodput_lines = root.findall(".//svg:path[@data-metric='goodput']", namespace)
        latency_lines = root.findall(".//svg:path[@data-metric='latency']", namespace)
        unavailable = root.findall(
            ".//svg:path[@class='availability-marker unavailable']", namespace
        )

        self.assertEqual(len(goodput_lines), 5)
        self.assertEqual(len(latency_lines), 5)
        self.assertEqual(len(unavailable), 5)
        self.assertEqual(len({marker.attrib["d"] for marker in unavailable}), 5)
        self.assertNotIn("isolated-observation", svg)
        self.assertNotIn("nan", svg.lower())
        self.assertNotIn("null", svg.lower())

    def test_unavailable_latency_cannot_be_published_as_zero(self):
        dataset = contract_fixture()
        missing = dataset["series"][0]["latency"][1]
        missing.update(low=0, median=0, high=0)

        with self.assertRaisesRegex(DatasetError, "must remain null"):
            validate_dataset(dataset)

    def test_scalar_summary_is_not_a_time_series(self):
        dataset = contract_fixture()
        dataset["series"][0]["goodput"] = dataset["series"][0]["goodput"][:1]

        with self.assertRaisesRegex(DatasetError, "real ordered series"):
            validate_dataset(dataset)

    def test_repetition_outcomes_must_account_for_every_run(self):
        dataset = contract_fixture()
        dataset["series"][0]["latency"][0]["outcomes"] = {"success": 1}

        with self.assertRaisesRegex(DatasetError, "every repetition"):
            validate_dataset(dataset)

    def test_latency_outcomes_are_required_and_match_available_samples(self):
        dataset = contract_fixture()
        del dataset["series"][0]["latency"][1]["outcomes"]
        with self.assertRaisesRegex(DatasetError, "must be an object"):
            validate_dataset(dataset)

        dataset = contract_fixture()
        dataset["series"][0]["latency"][0]["outcomes"] = {"io_error": 2}
        with self.assertRaisesRegex(DatasetError, "match availability"):
            validate_dataset(dataset)

    def test_exact_five_series_and_isolated_conditions_are_required(self):
        dataset = contract_fixture()
        dataset["series"].pop()
        with self.assertRaisesRegex(DatasetError, "exact five"):
            validate_dataset(dataset)

        dataset = contract_fixture()
        dataset["provenance"]["condition"]["case_isolation"] = False
        with self.assertRaisesRegex(DatasetError, "case and container isolation"):
            validate_dataset(dataset)

        dataset = contract_fixture()
        dataset["provenance"]["condition"]["probe"]["test_duration_s"] = 3.0
        with self.assertRaisesRegex(DatasetError, "probe workload"):
            validate_dataset(dataset)

    def test_provenance_requires_structured_unique_repetition_sources(self):
        dataset = contract_fixture()
        dataset["provenance"]["source_runs"] = ["fixture-a", "fixture-b"]
        with self.assertRaisesRegex(DatasetError, "must be an object"):
            validate_dataset(dataset)

        dataset = contract_fixture()
        dataset["provenance"]["source_runs"][1]["result_dirs"] = ["fixture-a"]
        with self.assertRaisesRegex(DatasetError, "unique directory names"):
            validate_dataset(dataset)


class PerformanceSeriesDerivationTests(unittest.TestCase):
    @staticmethod
    def dynamic_loss_condition():
        return copy.deepcopy(BULK_INTERACTIVE_DYNAMIC_LOSS_CONDITION)

    @staticmethod
    def qdisc_readback(condition, service, loss):
        payload = (
            "interface=eth-balanced\n"
            f"address={condition['service_addresses'][service]}\n"
            "qdisc netem 1: root refcnt 2 limit 1000 "
            f"rate 500Mbit delay 50ms 20ms loss random {loss}%\n"
        )
        return base64.b64encode(payload.encode()).decode()

    @staticmethod
    def dynamic_loss_trace(condition):
        def endpoint(role, service, start_ms, end_ms, loss):
            return {
                "role": role,
                "service": service,
                "start_offset_ms": start_ms,
                "end_offset_ms": end_ms,
                "command_exit_code": 0,
                "apply_exit_code": 0,
                "readback_exit_code": 0,
                "readback_base64": PerformanceSeriesDerivationTests.qdisc_readback(
                    condition, service, loss
                ),
            }

        events = []
        epoch_ms = condition["epoch_seconds"] * 1000
        for index, loss in enumerate(condition["loss_percent"]):
            planned_offset_ms = index * epoch_ms
            events.append(
                {
                    "index": index,
                    "loss_percent": loss,
                    "planned_offset_ms": planned_offset_ms,
                    "endpoints": {
                        "client-egress": endpoint(
                            "client-egress", "client", planned_offset_ms,
                            planned_offset_ms + 1, loss,
                        ),
                        "remote-egress": endpoint(
                            "remote-egress", "server", planned_offset_ms,
                            planned_offset_ms + 2, loss,
                        ),
                    },
                }
            )
        return {
            "condition": copy.deepcopy(condition),
            "probe_started_monotonic_ms": 1000,
            "schedule_origin": "probe-started-file-clock-monotonic-ms",
            "clock_name": "CLOCK_MONOTONIC",
            "host_time_namespace_id": "time:[4026531834]",
            "client_time_namespace_id": "time:[4026532522]",
            "host_monotonic_offset": {"seconds": 0, "nanoseconds": 0},
            "client_monotonic_offset": {"seconds": 0, "nanoseconds": 0},
            "topology_mode": "proxy",
            "dynamic_role_to_service": {
                "client-egress": "client",
                "remote-egress": "server",
            },
            "schedule_exit_code": 0,
            "schedule_completed_offset_ms": condition["duration_seconds"] * 1000,
            "applied_event_count": len(events),
            "events": events,
            "trace_complete": True,
        }

    @staticmethod
    def valid_raw_record():
        attempts = [
            {
                "index": index,
                "start_offset_s": index * 0.5,
                "end_offset_s": index * 0.5 + (100.0 + index) / 1000.0,
                "latency_ms": 100.0 + index,
                "outcome": "success",
            }
            for index in range(80)
        ]
        raw_goodput = [1.0] * 200
        return {
            "status": "ok",
            "bulk_status": "ok",
            "protocol": "bulk-interactive",
            "host_valid": True,
            "source_tree_dirty": False,
            "workload_mode": "bulk-interactive",
            "mode": "socks5",
            "target": "172.31.40.30:8080",
            "tcp_echo_target": "172.31.40.30:10022",
            "test_duration_s": 40.0,
            "bulk_time_s": 40.0,
            "bulk_load_duration_s": 40.0,
            "bulk_bytes": 1,
            "bulk_interval_seconds": 0.2,
            "bulk_interval_trim_discard_each_end": 3,
            "bulk_interval_goodput_raw_mbps": raw_goodput,
            "bulk_interval_goodput_mbps": raw_goodput[3:-3],
            "interactive_interval_ms": 500,
            "interactive_timeout_ms": 5000,
            "interactive_payload_bytes": 64,
            "interactive_time_s": 40.0,
            "interactive_attempt_series": attempts,
            "interactive_count": 80,
            "interactive_ok": 80,
            "interactive_fail": 0,
        }

    @classmethod
    def record_for(cls, series_id, case, value_offset=0.0):
        record = cls.valid_raw_record()
        condition = cls.dynamic_loss_condition()
        record.update(
            {
                "case": case,
                "source_commit": "accepted-source-commit",
                "bulk_interactive_dynamic_loss": cls.dynamic_loss_trace(condition),
                "bulk_interval_trim_discard_each_end": 3,
                "bulk_interval_goodput_raw_mbps": [1.0 + value_offset] * 200,
                "bulk_interval_goodput_mbps": [
                    value + value_offset
                    for value in record["bulk_interval_goodput_mbps"]
                ],
            }
        )
        if series_id.startswith("mpp_"):
            record.update(
                {
                    "performance_comparable": True,
                    "mptunnel_protocol_version": 8,
                    "mptunnel_build_profile": "release",
                }
            )
        else:
            tool = "xray" if series_id == "xray_vmess" else "hysteria2"
            endpoint = {"tool": tool, "verified": True, "release": "test-v1"}
            record["baseline_identity"] = {
                "tool": tool,
                "lock_sha256": "locked-baseline",
                "client": copy.deepcopy(endpoint),
                "server": copy.deepcopy(endpoint),
            }
        return record

    @staticmethod
    def manifest(seed="series-seed"):
        return {
            "safe_environment_overrides": {
                "MPTUNNEL_LAB_NETEM_MODE": "internet-five-path-epoch-0",
                "MPTUNNEL_LAB_INTERNET_SEED": seed,
                "MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES": "0",
                "MPTUNNEL_LAB_USE_PATH_HINTS": "0",
                "MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE": "844465kbit",
                "MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE": "130963kbit",
            },
            "workload": {
                "load_duration_seconds": 40.0,
                "bulk_connections": 1,
                "bulk_streams": 1,
                "object_mib": 1,
                "bulk_interactive_dynamic_loss": (
                    PerformanceSeriesDerivationTests.dynamic_loss_condition()
                ),
            },
            "execution": {
                "isolate_cases": True,
                "isolate_containers_per_case": True,
            },
        }

    @staticmethod
    def write_result_dir(root, name, records, manifest):
        result_dir = root / name
        result_dir.mkdir()
        result_path = result_dir / "results.jsonl"
        result_path.write_text(
            "".join(json.dumps(record) + "\n" for record in records),
            encoding="utf-8",
        )
        (result_dir / "run-manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8"
        )
        return result_path

    @classmethod
    def write_split_repetition(cls, root, repetition, seed="series-seed"):
        paths = []
        for case_index, (series_id, _label, case) in enumerate(CASE_SERIES):
            paths.append(
                cls.write_result_dir(
                    root,
                    f"{repetition}-{series_id}",
                    [cls.record_for(series_id, case, case_index)],
                    cls.manifest(seed),
                )
            )
        return paths

    @classmethod
    def write_complete_repetition(cls, root, repetition):
        records = [
            cls.record_for(series_id, case, case_index)
            for case_index, (series_id, _label, case) in enumerate(CASE_SERIES)
        ]
        return cls.write_result_dir(root, repetition, records, cls.manifest())

    def test_goodput_uses_real_one_second_windows_before_cohort_bounds(self):
        first = {
            "bulk_interval_seconds": 0.2,
            "bulk_interval_trim_discard_each_end": 3,
            "bulk_interval_goodput_mbps": [float(value) for value in range(15)],
        }
        second = copy.deepcopy(first)
        second["bulk_interval_goodput_mbps"] = [
            value + 10 for value in first["bulk_interval_goodput_mbps"]
        ]

        series = _goodput_series([first, second], 2)

        self.assertEqual([sample["time_s"] for sample in series], [1.1, 2.1, 3.1])
        self.assertEqual(series[0]["low"], 2.0)
        self.assertEqual(series[0]["median"], 7.0)
        self.assertEqual(series[0]["high"], 12.0)

    def test_goodput_symmetrically_discards_partial_edge_windows(self):
        record = {
            "bulk_interval_seconds": 0.2,
            "bulk_interval_trim_discard_each_end": 3,
            # A deterministic 30-second grid has 150 raw bins and 144 after
            # the declared three-bin edge trim.
            "bulk_interval_goodput_mbps": [10.0] * 144,
        }

        series = _goodput_series([record, copy.deepcopy(record)], 2)

        self.assertEqual(len(series), 28)
        self.assertEqual(series[0]["time_s"], 1.5)
        self.assertEqual(series[-1]["time_s"], 28.5)
        self.assertTrue(all(sample["median"] == 10.0 for sample in series))

    def test_missing_echo_repetition_is_counted_but_never_becomes_zero(self):
        success = {
            "index": 0,
            "start_offset_s": 0.01,
            "latency_ms": 100.0,
            "outcome": "success",
        }
        failed = {
            "index": 1,
            "start_offset_s": 0.51,
            "latency_ms": None,
            "outcome": "io_error",
        }
        first = {"interactive_attempt_series": [success, failed]}
        second = {"interactive_attempt_series": [copy.deepcopy(success)]}

        series = _latency_series([first, second], 2)

        self.assertEqual(series[1]["available"], 0)
        self.assertIsNone(series[1]["median"])
        self.assertEqual(series[1]["outcomes"], {"io_error": 1, "not_recorded": 1})

    def test_baseline_uses_verified_identity_not_mpp_comparability_flag(self):
        record = self.valid_raw_record()
        record["performance_comparable"] = None
        record["baseline_identity"] = {
            "tool": "xray",
            "lock_sha256": "locked",
            "client": {"tool": "xray", "verified": True},
            "server": {"tool": "xray", "verified": True},
        }

        _validate_record(record, "xray_vmess", "case", "fixture")

        record["baseline_identity"]["server"]["verified"] = False
        with self.assertRaisesRegex(DerivationError, "server xray identity"):
            _validate_record(record, "xray_vmess", "case", "fixture")

    def test_mpp_requires_explicit_performance_comparability(self):
        record = self.valid_raw_record()
        record["performance_comparable"] = None

        with self.assertRaisesRegex(DerivationError, "MPP row"):
            _validate_record(record, "mpp_quic", "case", "fixture")

    def test_declared_duration_requires_persistent_goodput_and_echo_grids(self):
        record = self.record_for(
            "xray_vmess",
            "baseline_vmess_tcp_bulk_interactive_balanced",
        )
        record.update(test_duration_s=60.0, bulk_time_s=60.0)
        with self.assertRaisesRegex(DerivationError, "goodput grid"):
            _validate_record(
                record,
                "xray_vmess",
                "baseline_vmess_tcp_bulk_interactive_balanced",
                "fixture",
            )

        record["bulk_interval_trim_discard_each_end"] = 0
        record["bulk_interval_goodput_raw_mbps"] = [1.0] * 300
        record["bulk_interval_goodput_mbps"] = [1.0] * 300
        record["interactive_time_s"] = 60.0
        with self.assertRaisesRegex(DerivationError, "echo series does not persist"):
            _validate_record(
                record,
                "xray_vmess",
                "baseline_vmess_tcp_bulk_interactive_balanced",
                "fixture",
            )

    def test_accounted_competitor_echo_loss_is_a_gap_but_mpp_loss_is_rejected(self):
        record = self.record_for(
            "hysteria2",
            "baseline_hysteria2_udp_bulk_interactive_balanced",
        )
        record.update(status="loss", interactive_ok=79, interactive_fail=1)
        record["interactive_attempt_series"][1].update(
            latency_ms=None,
            outcome="timeout",
        )

        _validate_record(
            record,
            "hysteria2",
            "baseline_hysteria2_udp_bulk_interactive_balanced",
            "fixture",
        )
        latency = _latency_series([record, copy.deepcopy(record)], 2)
        self.assertEqual(latency[1]["available"], 0)
        self.assertIsNone(latency[1]["median"])
        self.assertEqual(latency[1]["outcomes"], {"timeout": 2})

        record.update(
            performance_comparable=True,
            mptunnel_protocol_version=8,
            mptunnel_build_profile="release",
        )
        with self.assertRaisesRegex(DerivationError, "MPP echo continuity"):
            _validate_record(
                record,
                "mpp_quic",
                "mptunnel_quic_bulk_interactive_balanced",
                "fixture",
            )

    def test_split_files_form_two_complete_logical_repetitions(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            repetitions = [
                self.write_split_repetition(root, "run-a"),
                self.write_split_repetition(root, "run-b"),
            ]
            output = root / "derived.json"
            status = derive_main(
                [
                    "--repetition",
                    ",".join(str(path) for path in repetitions[0]),
                    "--repetition",
                    ",".join(str(path) for path in repetitions[1]),
                    "--output",
                    str(output),
                    "--cohort-id",
                    "split-files",
                    "--title",
                    "title",
                    "--subtitle",
                    "subtitle",
                    "--condition-note",
                    "condition",
                ]
            )
            dataset = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        self.assertEqual(dataset["provenance"]["valid_repetitions"], 2)
        self.assertEqual(
            dataset["provenance"]["source_runs"],
            [
                {
                    "id": "repetition-1",
                    "result_dirs": [
                        "run-a-mpp_tcp",
                        "run-a-mpp_quic",
                        "run-a-mpp_default",
                        "run-a-xray_vmess",
                        "run-a-hysteria2",
                    ],
                },
                {
                    "id": "repetition-2",
                    "result_dirs": [
                        "run-b-mpp_tcp",
                        "run-b-mpp_quic",
                        "run-b-mpp_default",
                        "run-b-xray_vmess",
                        "run-b-hysteria2",
                    ],
                },
            ],
        )
        self.assertEqual([entry["id"] for entry in dataset["series"]], EXPECTED_IDS)

    def test_complete_result_file_cli_remains_compatible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_complete_repetition(root, "complete-a")
            second = self.write_complete_repetition(root, "complete-b")
            output = root / "derived.json"

            status = derive_main(
                [
                    "--result",
                    str(first),
                    "--result",
                    str(second),
                    "--output",
                    str(output),
                    "--cohort-id",
                    "compatibility",
                    "--title",
                    "title",
                    "--subtitle",
                    "subtitle",
                    "--condition-note",
                    "condition",
                ]
            )
            dataset = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(status, 0)
        self.assertEqual(
            dataset["provenance"]["source_runs"],
            [
                {"id": "repetition-1", "result_dirs": ["complete-a"]},
                {"id": "repetition-2", "result_dirs": ["complete-b"]},
            ],
        )

    def test_derivation_rejects_incomplete_dynamic_loss_trace(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            row = json.loads(first[0].read_text(encoding="utf-8"))
            row["bulk_interactive_dynamic_loss"]["trace_complete"] = False
            first[0].write_text(json.dumps(row) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(DerivationError, "dynamic-loss trace"):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "incomplete-dynamic-loss",
                )

    def test_derivation_rejects_dynamic_loss_trace_manifest_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            row = json.loads(first[0].read_text(encoding="utf-8"))
            row["bulk_interactive_dynamic_loss"]["condition"][
                "condition_id"
            ] = "different-condition"
            first[0].write_text(json.dumps(row) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(DerivationError, "dynamic-loss trace"):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "dynamic-loss-mismatch",
                )

    def test_dynamic_loss_trace_rejects_missing_or_wrong_qdisc_readback(self):
        condition = self.dynamic_loss_condition()
        metadata = self.dynamic_loss_trace(condition)
        validate_bulk_interactive_dynamic_loss_metadata(metadata)

        missing = copy.deepcopy(metadata)
        missing["events"][0]["endpoints"]["client-egress"][
            "readback_base64"
        ] = "-"
        with self.assertRaisesRegex(ValueError, "readback is invalid"):
            validate_bulk_interactive_dynamic_loss_metadata(missing)

        valid_payload = base64.b64decode(
            metadata["events"][0]["endpoints"]["client-egress"][
                "readback_base64"
            ]
        ).decode()
        for old, new in (
            ("500Mbit", "499Mbit"),
            ("50ms", "51ms"),
            ("20ms", "21ms"),
            ("loss random 1%", "loss random 6%"),
        ):
            wrong_profile = copy.deepcopy(metadata)
            wrong_profile["events"][0]["endpoints"]["client-egress"][
                "readback_base64"
            ] = base64.b64encode(valid_payload.replace(old, new).encode()).decode()
            with self.subTest(field=old), self.assertRaisesRegex(
                ValueError, "qdisc does not match"
            ):
                validate_bulk_interactive_dynamic_loss_metadata(wrong_profile)

    def test_dynamic_loss_trace_accepts_distinct_namespaces_with_equal_offsets(self):
        metadata = self.dynamic_loss_trace(self.dynamic_loss_condition())
        validate_bulk_interactive_dynamic_loss_metadata(metadata)

    def test_dynamic_loss_trace_rejects_effective_monotonic_offset_mismatch(self):
        metadata = self.dynamic_loss_trace(self.dynamic_loss_condition())
        for same_namespace in (True, False):
            mismatched_clock = copy.deepcopy(metadata)
            if same_namespace:
                mismatched_clock["client_time_namespace_id"] = (
                    mismatched_clock["host_time_namespace_id"]
                )
            mismatched_clock["client_monotonic_offset"] = {
                "seconds": 1,
                "nanoseconds": 0,
            }
            with self.subTest(same_namespace=same_namespace), self.assertRaisesRegex(
                ValueError, "monotonic offsets differ"
            ):
                validate_bulk_interactive_dynamic_loss_metadata(mismatched_clock)

    def test_dynamic_loss_trace_rejects_missing_or_malformed_offsets(self):
        metadata = self.dynamic_loss_trace(self.dynamic_loss_condition())
        invalid_values = (
            None,
            {},
            {"seconds": 0},
            {"seconds": 0, "nanoseconds": -1},
            {"seconds": 0, "nanoseconds": 1_000_000_000},
            {"seconds": "0", "nanoseconds": 0},
            {"seconds": True, "nanoseconds": 0},
            {"seconds": 0, "nanoseconds": 0, "extra": 0},
        )
        for field in ("host_monotonic_offset", "client_monotonic_offset"):
            for value in invalid_values:
                malformed = copy.deepcopy(metadata)
                malformed[field] = value
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                    ValueError, "monotonic offset is invalid"
                ):
                    validate_bulk_interactive_dynamic_loss_metadata(malformed)

    def test_dynamic_loss_trace_rejects_malformed_namespace_ids(self):
        metadata = self.dynamic_loss_trace(self.dynamic_loss_condition())
        for field in ("host_time_namespace_id", "client_time_namespace_id"):
            for value in (None, "", "time:4026531834", "time:[invalid]"):
                malformed = copy.deepcopy(metadata)
                malformed[field] = value
                with self.subTest(field=field, value=value), self.assertRaisesRegex(
                    ValueError, "clock provenance is invalid"
                ):
                    validate_bulk_interactive_dynamic_loss_metadata(malformed)

    def test_dynamic_loss_trace_rejects_late_transition(self):
        metadata = self.dynamic_loss_trace(self.dynamic_loss_condition())

        late = copy.deepcopy(metadata)
        late["events"][0]["endpoints"]["remote-egress"]["end_offset_ms"] = 251
        with self.assertRaisesRegex(ValueError, "did not complete on time"):
            validate_bulk_interactive_dynamic_loss_metadata(late)

    def test_dynamic_loss_condition_declares_only_route_matched_impairments(self):
        condition = self.dynamic_loss_condition()
        proxy_target = self.valid_raw_record()["target"].rsplit(":", 1)[0]
        balanced_target = condition["service_addresses"]["target"].split("/", 1)[0]

        self.assertEqual(condition["schema_version"], 4)
        self.assertEqual(condition["condition_id"], "balanced-dynamic-loss-8x5-v4")
        self.assertEqual(proxy_target, "172.31.40.30")
        self.assertEqual(balanced_target, "172.31.15.30")
        self.assertNotEqual(proxy_target, balanced_target)
        self.assertEqual(
            condition["series_topology"]["proxy"]["dynamic_role_to_service"],
            {"client-egress": "client", "remote-egress": "server"},
        )
        self.assertEqual(
            condition["series_topology"]["direct"]["dynamic_role_to_service"],
            {"client-egress": "client", "remote-egress": "target"},
        )
        self.assertEqual(
            condition["series_topology"]["proxy"]["probe"],
            {
                "mode": "socks5",
                "target": "172.31.40.30:8080",
                "tcp_echo_target": "172.31.40.30:10022",
                "protocol": "bulk-interactive",
            },
        )
        self.assertEqual(
            condition["series_topology"]["direct"]["probe"],
            {
                "mode": "direct",
                "target": "172.31.15.30:8080",
                "tcp_echo_target": "172.31.15.30:10022",
                "protocol": "raw-tcp",
            },
        )
        self.assertNotIn("constant_service_loss_percent", json.dumps(condition))

        legacy_claim = self.dynamic_loss_trace(condition)
        legacy_claim["constant_service_loss_percent"] = {"target": 3}
        with self.assertRaisesRegex(ValueError, "off-contract impairment"):
            validate_bulk_interactive_dynamic_loss_metadata(legacy_claim)

    def test_raw_tcp_trace_requires_its_direct_route_and_target_role(self):
        condition = self.dynamic_loss_condition()
        metadata = self.dynamic_loss_trace(condition)
        metadata["topology_mode"] = "direct"
        metadata["dynamic_role_to_service"] = {
            "client-egress": "client",
            "remote-egress": "target",
        }
        for event, loss in zip(metadata["events"], condition["loss_percent"]):
            endpoint = event["endpoints"]["remote-egress"]
            endpoint["service"] = "target"
            endpoint["readback_base64"] = self.qdisc_readback(
                condition, "target", loss
            )
        row = {
            "mode": "direct",
            "target": "172.31.15.30:8080",
            "tcp_echo_target": "172.31.15.30:10022",
            "protocol": "raw-tcp",
        }
        validate_bulk_interactive_dynamic_loss_metadata(metadata)
        validate_bulk_interactive_probe_route(row, metadata)

        wrong_mapping = copy.deepcopy(metadata)
        wrong_mapping["dynamic_role_to_service"]["remote-egress"] = "server"
        with self.assertRaisesRegex(ValueError, "role-to-service mapping"):
            validate_bulk_interactive_dynamic_loss_metadata(wrong_mapping)

        wrong_route = dict(row, target="172.31.40.30:8080")
        with self.assertRaisesRegex(ValueError, "probe route"):
            validate_bulk_interactive_probe_route(wrong_route, metadata)

    def test_cli_forbids_mixing_complete_and_split_forms(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                parse_args(
                    [
                        "--result",
                        "complete/results.jsonl",
                        "--repetition",
                        "split-a/results.jsonl,split-b/results.jsonl",
                        "--output",
                        "out.json",
                        "--cohort-id",
                        "mixed",
                        "--title",
                        "title",
                        "--subtitle",
                        "subtitle",
                        "--condition-note",
                        "condition",
                    ]
                )

    def test_split_repetition_rejects_case_contamination_and_incompleteness(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            contaminated = self.write_result_dir(
                root,
                "run-a-contamination",
                [{"case": "unrelated_case"}],
                self.manifest(),
            )
            with self.assertRaisesRegex(DerivationError, "unexpected case"):
                derive_dataset(
                    [first + [contaminated], second],
                    "title",
                    "subtitle",
                    "condition",
                    "contaminated",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")[:-1]
            second = self.write_split_repetition(root, "run-b")
            with self.assertRaisesRegex(DerivationError, "omits required cases"):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "incomplete",
                )

    def test_split_repetition_rejects_condition_and_directory_mismatch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            mismatched_manifest = first[-1].parent / "run-manifest.json"
            manifest = self.manifest()
            manifest["workload"]["bulk_interactive_dynamic_loss"]["profile"][
                "jitter"
            ] = "21ms"
            mismatched_manifest.write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            with self.assertRaisesRegex(DerivationError, "invalid dynamic-loss condition"):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "mismatched",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            row = json.loads(first[-1].read_text(encoding="utf-8"))
            row["interactive_interval_ms"] = 1000
            first[-1].write_text(json.dumps(row) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(
                DerivationError, "probe conditions differ within"
            ):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "probe-mismatch",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a", "seed-a")
            second = self.write_split_repetition(root, "run-b", "seed-b")
            with self.assertRaisesRegex(
                DerivationError, "conditions differ across logical repetitions"
            ):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "cross-repetition-mismatch",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            with self.assertRaisesRegex(DerivationError, "duplicate result directory"):
                derive_dataset(
                    [first, first],
                    "title",
                    "subtitle",
                    "condition",
                    "duplicated",
                )

    def test_derivation_requires_isolation_and_manifest_probe_duration_match(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            manifest_path = first[0].parent / "run-manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["execution"]["isolate_cases"] = False
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(DerivationError, "did not isolate"):
                derive_dataset(
                    [first, second],
                    "title",
                    "subtitle",
                    "condition",
                    "not-isolated",
                )

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_complete_repetition(root, "complete-a")
            second = self.write_complete_repetition(root, "complete-b")
            for result in (first, second):
                rows = [
                    json.loads(line)
                    for line in result.read_text(encoding="utf-8").splitlines()
                    if line
                ]
                for row in rows:
                    row["test_duration_s"] = 2.0
                    row["bulk_load_duration_s"] = 2.0
                result.write_text(
                    "".join(json.dumps(row) + "\n" for row in rows),
                    encoding="utf-8",
                )
            with self.assertRaisesRegex(
                DerivationError, "manifest and probe durations"
            ):
                derive_dataset(
                    [[first], [second]],
                    "title",
                    "subtitle",
                    "condition",
                    "duration-mismatch",
                )

    def test_split_repetition_rejects_duplicate_required_case(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = self.write_split_repetition(root, "run-a")
            second = self.write_split_repetition(root, "run-b")
            series_id, _label, case = CASE_SERIES[0]
            duplicate = self.write_result_dir(
                root,
                "run-a-duplicate-case",
                [self.record_for(series_id, case)],
                self.manifest(),
            )
            with self.assertRaisesRegex(DerivationError, "repeats case"):
                derive_dataset(
                    [first + [duplicate], second],
                    "title",
                    "subtitle",
                    "condition",
                    "duplicated-case",
                )


@unittest.skipUnless(
    DATASET_PATH.exists(), "accepted five-series dataset not derived yet"
)
class CheckedInPerformanceSeriesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = load_dataset(DATASET_PATH)

    def test_dataset_has_exact_requested_series_and_real_trajectories(self):
        self.assertEqual(
            [series["id"] for series in self.dataset["series"]], EXPECTED_IDS
        )
        self.assertEqual(self.dataset["provenance"]["valid_repetitions"], 2)
        for series in self.dataset["series"]:
            self.assertEqual(len(series["goodput"]), 38)
            self.assertGreaterEqual(len(series["latency"]), 3)
            self.assertTrue(
                all(sample["available"] == 2 for sample in series["latency"]),
                f"{series['id']} does not retain both accepted latency trajectories",
            )

    def test_checked_in_svg_is_exact_renderer_output(self):
        self.assertTrue(SVG_PATH.exists(), "derived SVG is not checked in")
        self.assertEqual(
            SVG_PATH.read_text(encoding="utf-8"),
            render_svg(self.dataset),
        )

    def test_dataset_cites_each_valid_source_run(self):
        provenance = self.dataset["provenance"]
        source_runs = provenance["source_runs"]
        self.assertEqual(len(source_runs), provenance["valid_repetitions"])
        self.assertEqual(len({entry["id"] for entry in source_runs}), len(source_runs))
        result_dirs = [
            directory for entry in source_runs for directory in entry["result_dirs"]
        ]
        self.assertEqual(len(set(result_dirs)), len(result_dirs))
        self.assertTrue(provenance["source_commit"])


if __name__ == "__main__":
    unittest.main()

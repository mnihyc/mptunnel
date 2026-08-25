import json
import os
import re
import subprocess
import sys
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from internet_condition_schedule import (
    DIRECTIONS,
    GENERATOR_ID,
    STRATA,
    TSV_COLUMNS,
    UINT32_MAX,
    build_schedule,
    canonical_json,
    load_schedule,
    render_rows,
    rows_for,
    schedule_metadata,
    schedule_sha256,
    validate_schedule,
)


SCRIPT = Path(__file__).resolve().with_name("internet_condition_schedule.py")
RATE_TOKEN = re.compile(r"^[1-9][0-9]*kbit$")
TIME_TOKEN = re.compile(r"^(?:0|[1-9][0-9]*)ms$")
PERCENT_TOKEN = re.compile(r"^(?:0|[1-9][0-9]*)(?:\.[0-9]{1,4})?%$")


def cli(*arguments: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *arguments],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


class InternetConditionScheduleTests(unittest.TestCase):
    def test_same_seed_has_fixed_canonical_identity(self):
        first = build_schedule("lab-seed-2026", 3)
        second = build_schedule("lab-seed-2026", 3)

        self.assertEqual(first, second)
        self.assertEqual(canonical_json(first), canonical_json(second))
        self.assertEqual(first["schedule_sha256"], schedule_sha256(first))
        # This fixture catches accidental generator, serialization, or draw drift.
        self.assertEqual(
            first["schedule_sha256"],
            "2eb4d599082d3748e37c8acf7745d280ce102f3fb303c2da51ea56cb4a5f697e",
        )

    def test_different_seed_epoch_and_direction_change_rows(self):
        base = render_rows("seed-a", 0, "client")
        self.assertNotEqual(base, render_rows("seed-b", 0, "client"))
        self.assertNotEqual(base, render_rows("seed-a", 1, "client"))
        self.assertNotEqual(base, render_rows("seed-a", 0, "server"))

    def test_each_epoch_direction_is_stratified_across_five_paths(self):
        expected_strata = {str(item["name"]) for item in STRATA}
        for epoch in range(8):
            for direction in DIRECTIONS:
                with self.subTest(epoch=epoch, direction=direction):
                    rows = render_rows("stratification", epoch, direction)
                    self.assertEqual(len(rows), 5)
                    self.assertEqual(
                        {str(row["stratum"]) for row in rows}, expected_strata
                    )
                    self.assertEqual(
                        [int(row["path_index"]) for row in rows], [1, 2, 3, 4, 5]
                    )
                    self.assertEqual(
                        len({str(row["subnet_prefix"]) for row in rows}), 5
                    )

    def test_rows_are_tc_legal_and_obey_cross_field_invariants(self):
        rows = build_schedule("legal-tc-values", 25, include_outages=True)["rows"]
        seeds: list[int] = []
        observed_nonzero = {
            "reorder": False,
            "duplicate": False,
            "corrupt": False,
        }
        for row in rows:
            self.assertRegex(str(row["rate"]), RATE_TOKEN)
            self.assertRegex(str(row["delay"]), TIME_TOKEN)
            self.assertRegex(str(row["jitter"]), TIME_TOKEN)
            for field in (
                "delay_correlation",
                "loss",
                "loss_correlation",
                "reorder",
                "reorder_correlation",
                "duplicate",
                "corrupt",
            ):
                self.assertRegex(str(row[field]), PERCENT_TOKEN)
            delay = int(str(row["delay"])[:-2])
            jitter = int(str(row["jitter"])[:-2])
            self.assertLessEqual(jitter, delay)
            if str(row["jitter"]) == "0ms":
                self.assertEqual(row["delay_correlation"], "0%")
            if row["loss"] == "0%":
                self.assertEqual(row["loss_correlation"], "0%")
            if row["reorder"] == "0%":
                self.assertEqual(row["reorder_correlation"], "0%")
            else:
                self.assertGreater(delay, 0)
            if row["outage"]:
                self.assertEqual(row["loss"], "100%")
                self.assertEqual(row["loss_correlation"], "0%")
            else:
                self.assertNotEqual(row["loss"], "100%")
            for field in observed_nonzero:
                observed_nonzero[field] |= row[field] != "0%"
            seed = int(row["netem_seed"])
            self.assertGreaterEqual(seed, 1)
            self.assertLessEqual(seed, UINT32_MAX)
            seeds.append(seed)
        self.assertTrue(all(observed_nonzero.values()))
        # The keyed Feistel permutation makes seeds collision-free throughout the
        # generator's supported epoch range, not merely unlikely to collide.
        self.assertEqual(len(seeds), len(set(seeds)))

    def test_outages_are_optional_and_one_local_row_per_epoch(self):
        without = build_schedule("outage-policy", 12, include_outages=False)
        with_outages = build_schedule("outage-policy", 12, include_outages=True)
        self.assertFalse(any(row["outage"] for row in without["rows"]))
        for epoch in range(12):
            rows = [row for row in with_outages["rows"] if row["epoch"] == epoch]
            self.assertEqual(sum(bool(row["outage"]) for row in rows), 1)
        self.assertNotEqual(
            without["schedule_sha256"], with_outages["schedule_sha256"]
        )

    def test_validation_rejects_digest_drift_and_forged_seeded_content(self):
        schedule = build_schedule("tamper-evident", 2)
        self.assertIs(validate_schedule(schedule), schedule)

        mutated = deepcopy(schedule)
        mutated["rows"][0]["rate"] = "999999kbit"
        with self.assertRaisesRegex(ValueError, "schedule_sha256"):
            validate_schedule(mutated)

        forged = deepcopy(mutated)
        forged["schedule_sha256"] = schedule_sha256(forged)
        with self.assertRaisesRegex(ValueError, "seeded replay"):
            validate_schedule(forged)

    def test_validation_rejects_noncanonical_and_invalid_tokens(self):
        replacements = {
            "rate": "1mbit",
            "delay": "1.0ms",
            "jitter": "-1ms",
            "loss": "00.1%",
            "netem_seed": 0,
        }
        for field, value in replacements.items():
            with self.subTest(field=field):
                schedule = build_schedule("invalid-token", 1)
                schedule["rows"][0][field] = value
                schedule["schedule_sha256"] = schedule_sha256(schedule)
                with self.assertRaises(ValueError):
                    validate_schedule(schedule)

    def test_metadata_is_bounded_and_records_common_schedule_identity(self):
        schedule = build_schedule("metadata", 200, include_outages=True)
        metadata = schedule_metadata(schedule)

        self.assertEqual(metadata["schedule_sha256"], schedule["schedule_sha256"])
        self.assertEqual(metadata["application_scope"], "protocol-neutral-network-conditions")
        self.assertEqual(metadata["row_count"], 2_000)
        self.assertEqual(metadata["outage_count"], 200)
        self.assertEqual(metadata["direction_row_counts"], {"client": 1_000, "server": 1_000})
        self.assertEqual(
            metadata["stratum_counts"],
            {
                "congested": 400,
                "fiber": 400,
                "fixed-wireless": 400,
                "mobile": 400,
                "satellite": 400,
            },
        )
        self.assertNotIn("rows", metadata)
        self.assertLess(len(canonical_json(metadata)), 2048)

    def test_load_and_rows_for_replay_exact_artifact(self):
        schedule = build_schedule("artifact-replay", 4, include_outages=True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schedule.json"
            path.write_text(canonical_json(schedule) + "\n", encoding="utf-8")
            loaded = load_schedule(path)

        self.assertEqual(loaded, schedule)
        self.assertEqual(
            rows_for(loaded, 2, "server"),
            render_rows("artifact-replay", 2, "server", include_outages=True),
        )
        with self.assertRaisesRegex(ValueError, "outside"):
            rows_for(loaded, 4, "server")

    def test_render_tsv_contract_is_exact_and_shell_friendly(self):
        completed = cli(
            "render",
            "--seed",
            "tsv-contract",
            "--epoch",
            "3",
            "--direction",
            "client",
            "--topology",
            "five-path",
            "--format",
            "tsv",
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        lines = completed.stdout.rstrip("\n").splitlines()
        self.assertEqual(len(lines), 5)
        self.assertTrue(all(len(line.split("\t")) == len(TSV_COLUMNS) for line in lines))
        self.assertEqual(
            [line.split("\t")[0] for line in lines],
            ["172.31.10", "172.31.15", "172.31.16", "172.31.20", "172.31.30"],
        )
        self.assertTrue(all(line.split("\t")[-1] == "0" for line in lines))

        headed = cli(
            "render",
            "--seed",
            "tsv-contract",
            "--epoch",
            "3",
            "--direction",
            "client",
            "--format",
            "tsv",
            "--header",
        )
        self.assertEqual(headed.returncode, 0, headed.stderr)
        self.assertEqual(headed.stdout.splitlines()[0], "\t".join(TSV_COLUMNS))

    def test_generate_validate_metadata_and_replay_cli_round_trip(self):
        generated = cli(
            "generate",
            "--seed",
            "cli-round-trip",
            "--epochs",
            "3",
            "--include-outages",
        )
        self.assertEqual(generated.returncode, 0, generated.stderr)
        schedule = json.loads(generated.stdout)
        self.assertEqual(schedule["generator"], GENERATOR_ID)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schedule.json"
            path.write_text(generated.stdout, encoding="utf-8")
            validated = cli("validate", "--schedule", str(path))
            metadata = cli("metadata", "--schedule", str(path))
            replayed = cli(
                "replay",
                "--schedule",
                str(path),
                "--expect-sha256",
                schedule["schedule_sha256"],
                "--epoch",
                "1",
                "--direction",
                "server",
                "--format",
                "tsv",
            )

        self.assertEqual(validated.returncode, 0, validated.stderr)
        self.assertEqual(
            json.loads(validated.stdout),
            {"schedule_sha256": schedule["schedule_sha256"], "valid": True},
        )
        self.assertEqual(metadata.returncode, 0, metadata.stderr)
        self.assertEqual(
            json.loads(metadata.stdout)["schedule_sha256"], schedule["schedule_sha256"]
        )
        direct = cli(
            "render",
            "--seed",
            "cli-round-trip",
            "--epoch",
            "1",
            "--direction",
            "server",
            "--include-outages",
            "--format",
            "tsv",
        )
        self.assertEqual(replayed.stdout, direct.stdout)

    def test_replay_rejects_an_artifact_with_the_wrong_expected_identity(self):
        schedule = build_schedule("expected-artifact", 1)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "schedule.json"
            path.write_text(canonical_json(schedule) + "\n", encoding="utf-8")
            replayed = cli(
                "replay",
                "--schedule",
                str(path),
                "--expect-sha256",
                "0" * 64,
                "--epoch",
                "0",
                "--direction",
                "client",
                "--format",
                "tsv",
            )

        self.assertEqual(replayed.returncode, 2)
        self.assertIn("does not match --expect-sha256", replayed.stderr)

    def test_cli_is_stable_across_python_hash_seeds(self):
        outputs = []
        for python_hash_seed in ("1", "987654321", "random"):
            environment = os.environ.copy()
            environment["PYTHONHASHSEED"] = python_hash_seed
            completed = cli(
                "generate",
                "--seed",
                "cross-process",
                "--epochs",
                "4",
                "--include-outages",
                env=environment,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            outputs.append(completed.stdout)
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(outputs[1], outputs[2])

    def test_invalid_generation_inputs_fail_closed(self):
        for arguments in (
            ("generate", "--seed", "", "--epochs", "1"),
            ("generate", "--seed", "x", "--epochs", "0"),
            (
                "render",
                "--seed",
                "x",
                "--epoch",
                "-1",
                "--direction",
                "client",
            ),
        ):
            with self.subTest(arguments=arguments):
                completed = cli(*arguments)
                self.assertEqual(completed.returncode, 2)
                self.assertTrue(completed.stderr.startswith("usage:"))


if __name__ == "__main__":
    unittest.main()

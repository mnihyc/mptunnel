import os
import subprocess
import tempfile
import unittest
from pathlib import Path


LAB_DIR = Path(__file__).resolve().parent
NETEM_SCRIPT = LAB_DIR / "configure-netem.sh"
NETEM_SOURCE = NETEM_SCRIPT.read_text(encoding="utf-8")


BASE_ROWS = (
    "172.31.10\t80mbit\t20ms\t2ms\t10%\t1%\t20%\t0.5%\t30%\t0.1%\t0.01%\t101\t0",
    "172.31.15\t120mbit\t55ms\t8ms\t11%\t2%\t21%\t0.6%\t31%\t0.2%\t0.02%\t102\t0",
    "172.31.16\t45mbit\t90ms\t12ms\t12%\t3%\t22%\t0.7%\t32%\t0.3%\t0.03%\t103\t0",
    "172.31.20\t250mbit\t170ms\t20ms\t13%\t4%\t23%\t0.8%\t33%\t0.4%\t0.04%\t104\t0",
    "172.31.30\t8mbit\t480ms\t90ms\t14%\t5%\t24%\t0.9%\t34%\t0.5%\t0.05%\t105\t1",
)


class ConfigureNetemTests(unittest.TestCase):
    def run_mode(
        self,
        mode: str,
        rows=BASE_ROWS,
        *,
        seed_supported: bool = True,
        extra_env: dict[str, str] | None = None,
    ) -> tuple[subprocess.CompletedProcess[str], list[str], list[str]]:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            tc_log = root / "tc.log"
            generator_log = root / "generator.log"

            (fake_bin / "ip").write_text(
                """#!/usr/bin/env bash
cat <<'EOF'
1: if10 inet 172.31.10.2/24 brd 172.31.10.255 scope global if10
2: if15 inet 172.31.15.2/24 brd 172.31.15.255 scope global if15
3: if16 inet 172.31.16.2/24 brd 172.31.16.255 scope global if16
4: if20 inet 172.31.20.2/24 brd 172.31.20.255 scope global if20
5: if30 inet 172.31.30.2/24 brd 172.31.30.255 scope global if30
EOF
""",
                encoding="utf-8",
            )
            (fake_bin / "tc").write_text(
                """#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$FAKE_TC_LOG"
if [[ "$*" == *" netem help" ]]; then
  if [[ "$FAKE_TC_SEED_SUPPORTED" == "1" ]]; then
    echo 'Usage: ... netem ... seed SEED' >&2
  else
    echo 'Usage: ... netem' >&2
  fi
  exit 1
fi
""",
                encoding="utf-8",
            )
            generator = root / "internet_condition_schedule.py"
            generator.write_text(
                """import os
import sys
from pathlib import Path

Path(os.environ["FAKE_GENERATOR_LOG"]).write_text(
    "\\n".join(sys.argv[1:]), encoding="utf-8"
)
print(os.environ["FAKE_SCHEDULE_TSV"], end="")
""",
                encoding="utf-8",
            )
            (fake_bin / "ip").chmod(0o755)
            (fake_bin / "tc").chmod(0o755)

            env = os.environ.copy()
            env.update(
                {
                    "PATH": f"{fake_bin}:{env['PATH']}",
                    "FAKE_TC_LOG": str(tc_log),
                    "FAKE_TC_SEED_SUPPORTED": "1" if seed_supported else "0",
                    "FAKE_GENERATOR_LOG": str(generator_log),
                    "FAKE_SCHEDULE_TSV": "\n".join(rows),
                    "MPTUNNEL_LAB_INTERNET_SCHEDULE_SCRIPT": str(generator),
                    "MPTUNNEL_LAB_INTERNET_SEED": "unit-seed",
                }
            )
            if extra_env:
                env.update(extra_env)
            completed = subprocess.run(
                ["bash", str(NETEM_SCRIPT), mode],
                cwd=LAB_DIR.parent,
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            tc_calls = tc_log.read_text(encoding="utf-8").splitlines() if tc_log.exists() else []
            generator_args = (
                generator_log.read_text(encoding="utf-8").splitlines()
                if generator_log.exists()
                else []
            )
            return completed, tc_calls, generator_args

    def test_epoch_zero_renders_exact_contract_and_replaces_all_five_paths(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-epoch-0-client"
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            generator_args,
            [
                "render",
                "--seed",
                "unit-seed",
                "--epoch",
                "0",
                "--direction",
                "client",
                "--topology",
                "five-path",
                "--format",
                "tsv",
            ],
        )
        self.assertEqual(len(tc_calls), 6)
        self.assertEqual(tc_calls[0], "qdisc add dev if10 root netem help")
        applications = tc_calls[1:]
        self.assertTrue(all(call.startswith("qdisc replace dev ") for call in applications))
        self.assertEqual(
            {call.split()[3] for call in applications},
            {"if10", "if15", "if16", "if20", "if30"},
        )
        low_latency = next(call for call in applications if " dev if10 " in f" {call} ")
        for fragment in (
            "rate 80mbit",
            "delay 20ms 2ms 10% distribution normal",
            "loss random 1% 20%",
            "reorder 0.5% 30%",
            "duplicate 0.1%",
            "corrupt 0.01%",
            "seed 101",
        ):
            self.assertIn(fragment, low_latency)

        outage = next(call for call in applications if " dev if30 " in f" {call} ")
        self.assertIn("rate 8mbit", outage)
        self.assertIn("delay 480ms 90ms 14% distribution normal", outage)
        self.assertIn("loss random 100% 24%", outage)
        self.assertIn("reorder 0.9% 34%", outage)
        self.assertIn("duplicate 0.5%", outage)
        self.assertIn("corrupt 0.05%", outage)
        self.assertIn("seed 105", outage)

    def test_balanced_dynamic_transition_changes_the_existing_qdisc(self):
        completed, tc_calls, generator_args = self.run_mode(
            "change-balanced",
            extra_env={
                "MPTUNNEL_LAB_BALANCED_RATE": "500mbit",
                "MPTUNNEL_LAB_BALANCED_DELAY": "50ms",
                "MPTUNNEL_LAB_BALANCED_JITTER": "20ms",
                "MPTUNNEL_LAB_BALANCED_LOSS": "6%",
            },
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(generator_args, [])
        self.assertEqual(len(tc_calls), 1)
        self.assertTrue(tc_calls[0].startswith("qdisc change dev if15 root netem "))
        for fragment in (
            "rate 500mbit",
            "delay 50ms 20ms distribution normal",
            "loss 6%",
        ):
            self.assertIn(fragment, tc_calls[0])

    def test_later_epoch_is_an_independent_seeded_replacement(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-epoch-7-server"
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("7", generator_args)
        self.assertIn("server", generator_args)
        self.assertTrue(
            all(call.startswith("qdisc replace dev ") for call in tc_calls[1:])
        )

    def test_optional_load_coupled_mode_keeps_schedule_and_adds_finite_bottleneck(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-load-coupled-epoch-0-client"
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            generator_args[0:6],
            ["render", "--seed", "unit-seed", "--epoch", "0", "--direction"],
        )
        self.assertEqual(tc_calls[0], "qdisc add dev if10 root netem help")
        low_latency = tc_calls[1:5]
        self.assertEqual(low_latency[0], "qdisc del dev if10 root")
        self.assertEqual(
            low_latency[1], "qdisc add dev if10 root handle 1: htb default 10"
        )
        self.assertIn(
            "class add dev if10 parent 1: classid 1:10 htb rate 80mbit ceil 80mbit",
            low_latency[2],
        )
        self.assertIn("burst 20000b cburst 20000b quantum 1514", low_latency[2])
        self.assertIn(
            "qdisc add dev if10 parent 1:10 handle 10: netem limit 840",
            low_latency[3],
        )
        self.assertNotIn(" rate ", f" {low_latency[3]} ")
        self.assertIn("delay 20ms 2ms 10% distribution normal", low_latency[3])
        self.assertIn("loss random 1% 20%", low_latency[3])
        self.assertIn("seed 101", low_latency[3])
        self.assertEqual(len(tc_calls), 21)

    def test_load_coupled_queue_horizon_must_be_positive_before_mutation(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-load-coupled-epoch-0-client",
            extra_env={"MPTUNNEL_LAB_INTERNET_LOAD_QUEUE_DELAY": "0ms"},
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("must be a positive duration", completed.stderr)
        self.assertEqual(generator_args, [])
        self.assertEqual(tc_calls, [])

    def test_zero_jitter_omits_the_tc_distribution_clause(self):
        rows = list(BASE_ROWS)
        fields = rows[0].split("\t")
        fields[3] = "0ms"
        fields[4] = "0%"
        rows[0] = "\t".join(fields)

        completed, tc_calls, _ = self.run_mode(
            "internet-five-path-epoch-0-client", rows
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        low_latency = next(
            call
            for call in tc_calls
            if call.startswith("qdisc replace dev if10 ")
        )
        self.assertIn("delay 20ms", low_latency)
        self.assertNotIn("distribution", low_latency)
        self.assertIn("loss random 1% 20%", low_latency)

    def test_outages_are_an_explicit_generator_contract(self):
        completed, _, generator_args = self.run_mode(
            "internet-five-path-epoch-0-client",
            extra_env={"MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES": "1"},
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(generator_args[-1], "--include-outages")

    def test_artifact_replay_uses_the_exact_expected_schedule_identity(self):
        expected_sha256 = "a" * 64
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-epoch-7-server",
            extra_env={
                "MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE": "/workspace/.tmp/schedule.json",
                "MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256": expected_sha256,
            },
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            generator_args,
            [
                "replay",
                "--schedule",
                "/workspace/.tmp/schedule.json",
                "--expect-sha256",
                expected_sha256,
                "--epoch",
                "7",
                "--direction",
                "server",
                "--format",
                "tsv",
            ],
        )
        self.assertEqual(len(tc_calls), 6)

    def test_artifact_replay_requires_a_bounded_expected_identity(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-epoch-0-client",
            extra_env={
                "MPTUNNEL_LAB_INTERNET_SCHEDULE_FILE": "/workspace/.tmp/schedule.json",
                "MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256": "not-a-digest",
            },
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("artifact replay requires", completed.stderr)
        self.assertEqual(generator_args, [])
        self.assertEqual(tc_calls, [])

    def test_invalid_outage_setting_fails_before_generator_or_qdisc(self):
        completed, tc_calls, generator_args = self.run_mode(
            "internet-five-path-epoch-0-client",
            extra_env={"MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES": "yes"},
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("must be 0 or 1", completed.stderr)
        self.assertEqual(generator_args, [])
        self.assertEqual(tc_calls, [])

    def test_current_generator_output_is_accepted_by_shell_contract(self):
        rendered = subprocess.run(
            [
                "python3",
                str(LAB_DIR / "internet_condition_schedule.py"),
                "render",
                "--seed",
                "shell-contract-test",
                "--epoch",
                "3",
                "--direction",
                "client",
                "--topology",
                "five-path",
                "--format",
                "tsv",
            ],
            text=True,
            capture_output=True,
            check=True,
        )
        completed, tc_calls, _ = self.run_mode(
            "internet-five-path-epoch-3-client",
            tuple(rendered.stdout.splitlines()),
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(len(tc_calls), 6)

    def test_rejects_bad_tokens_before_any_qdisc_mutation(self):
        for field_index, invalid_token, message in (
            (1, "45mbit;touch-pwned", "invalid rate"),
            (1, "0mbit", "invalid rate"),
            (2, "20ms extra", "invalid delay or jitter"),
            (4, "100.1%", "not a 0..100% token"),
            (12, "true", "invalid outage flag"),
        ):
            with self.subTest(token=invalid_token):
                bad_rows = list(BASE_ROWS)
                fields = bad_rows[2].split("\t")
                fields[field_index] = invalid_token
                bad_rows[2] = "\t".join(fields)

                completed, tc_calls, _ = self.run_mode(
                    "internet-five-path-epoch-0-client", bad_rows
                )

                self.assertEqual(completed.returncode, 2)
                self.assertIn(message, completed.stderr)
                self.assertEqual(tc_calls, [])

    def test_rejects_zero_or_out_of_range_uint32_seed_before_mutation(self):
        for invalid_seed in ("0", "4294967296"):
            with self.subTest(seed=invalid_seed):
                bad_rows = list(BASE_ROWS)
                fields = bad_rows[0].split("\t")
                fields[11] = invalid_seed
                bad_rows[0] = "\t".join(fields)

                completed, tc_calls, _ = self.run_mode(
                    "internet-five-path-epoch-0-client", bad_rows
                )

                self.assertEqual(completed.returncode, 2)
                self.assertIn("invalid or zero uint32 netem seed", completed.stderr)
                self.assertEqual(tc_calls, [])

    def test_rejects_duplicate_or_incomplete_five_path_schedule(self):
        duplicate_rows = list(BASE_ROWS)
        duplicate_rows[-1] = duplicate_rows[0]
        for rows, message in (
            (duplicate_rows, "duplicate subnet prefix"),
            (BASE_ROWS[:-1], "expected 5"),
        ):
            with self.subTest(message=message):
                completed, tc_calls, _ = self.run_mode(
                    "internet-five-path-epoch-0-client", rows
                )
                self.assertEqual(completed.returncode, 2)
                self.assertIn(message, completed.stderr)
                self.assertEqual(tc_calls, [])

    def test_seed_feature_is_required_instead_of_silently_dropped(self):
        completed, tc_calls, _ = self.run_mode(
            "internet-five-path-epoch-0-client", seed_supported=False
        )

        self.assertEqual(completed.returncode, 2)
        self.assertIn("does not advertise seed support", completed.stderr)
        self.assertEqual(tc_calls, ["qdisc add dev if10 root netem help"])

    def test_existing_mode_does_not_invoke_generator_or_seed_probe(self):
        completed, tc_calls, generator_args = self.run_mode("apply-lowlat")

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(generator_args, [])
        self.assertEqual(len(tc_calls), 1)
        self.assertTrue(tc_calls[0].startswith("qdisc replace dev if10 root netem"))

    def test_implementation_uses_array_arguments_and_never_eval(self):
        function = NETEM_SOURCE.split(
            "apply_internet_profile_to_interface() {", 1
        )[1].split("\n}", 1)[0]
        self.assertIn("local -a qdisc_args", function)
        self.assertIn('"${qdisc_args[@]}"', function)
        self.assertNotIn("eval", function)


if __name__ == "__main__":
    unittest.main()

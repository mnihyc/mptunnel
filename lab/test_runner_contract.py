import unittest
from pathlib import Path


SCRIPT = (Path(__file__).resolve().parent / "run-heterogeneous-ablation.sh").read_text(
    encoding="utf-8"
)


class RunnerContractTests(unittest.TestCase):
    def test_diagnostic_build_uses_shared_truthy_parser(self):
        build_function = SCRIPT.split("build_mptunnel_binary() {", 1)[1].split("\n}", 1)[0]

        self.assertIn('if flag_enabled "$lab_diagnostics"; then', build_function)
        self.assertIn("--features lab-diagnostics", build_function)

    def test_direct_and_product_mixed_rows_have_explicit_scope(self):
        self.assertIn(
            'append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 0',
            SCRIPT,
        )
        self.assertIn(
            'append_mixed_probe_result "$case_name" "$exit_code" "$output" "" "" 1',
            SCRIPT,
        )

    def test_flapper_cannot_be_stopped_by_background_terminal_signals(self):
        flapper = SCRIPT.split("start_random_flapping() {", 1)[1].split(
            "\n}\n\nshould_run_case()", 1
        )[0]

        self.assertIn("trap '' TTOU TTIN TSTP", flapper)
        self.assertIn(") </dev/null &", flapper)


if __name__ == "__main__":
    unittest.main()

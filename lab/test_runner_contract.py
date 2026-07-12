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

    def test_equal_fat_mptcp_cases_match_bulk_and_exact_upload_contracts(self):
        download = SCRIPT.split("run_mptcp_baseline_case() {", 1)[1].split(
            "\n}\n\nrun_mptcp_baseline_upload_case()", 1
        )[0]
        upload = SCRIPT.split("run_mptcp_baseline_upload_case() {", 1)[1].split(
            "\n}\n\nappend_mixed_probe_result()", 1
        )[0]

        self.assertIn("--parallel-downloads '${bulk_connections}'", download)
        self.assertIn('start_mptcp_evidence "$case_name"', download)
        self.assertIn('stop_mptcp_evidence "$case_name"', download)
        self.assertIn('mptcp_evidence_summary "$case_name"', download)
        self.assertIn("restart_target_tcp_sink mptcp", upload)
        self.assertIn("--protocol 'mptcp-upload' --mptcp", upload)
        self.assertIn("--parallel-uploads '${bulk_connections}'", upload)
        self.assertIn("freeze_target_tcp_sink", upload)
        self.assertIn("append_upload_probe_result", upload)
        self.assertIn('start_mptcp_evidence "$case_name"', upload)
        self.assertIn('stop_mptcp_evidence "$case_name"', upload)
        self.assertIn('mptcp_evidence_summary "$case_name"', upload)
        self.assertIn(
            'run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_equal_fat" ideal-all-fat',
            SCRIPT,
        )
        self.assertIn(
            'run_mptcp_baseline_upload_case "baseline_mptcp_tcp_multipath_equal_fat_upload" ideal-all-fat',
            SCRIPT,
        )

    def test_mptcp_endpoint_setup_cannot_silently_accept_missing_addresses(self):
        configure = SCRIPT.split("configure_mptcp_endpoints() {", 1)[1].split(
            "\n}\n\ncheck_mptcp_baseline_case()", 1
        )[0]

        self.assertIn("no interface owns requested MPTCP address", configure)
        self.assertIn("failed to add MPTCP endpoint", configure)
        self.assertIn("kernel endpoint table did not retain", configure)
        self.assertNotIn("|| true", configure)
        self.assertIn("client MPTCP endpoint configuration failed:", SCRIPT)
        self.assertIn("target MPTCP endpoint configuration failed:", SCRIPT)

    def test_mixed_single_equal_fat_controls_use_one_tcp_and_one_udp_endpoint(self):
        self.assertIn(
            'run_reliable_ideal_download_case "mptunnel_reliable_mixed_single_equal_fat" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"',
            SCRIPT,
        )
        self.assertIn(
            'run_reliable_ideal_upload_case "mptunnel_reliable_mixed_single_equal_fat_upload" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"',
            SCRIPT,
        )


if __name__ == "__main__":
    unittest.main()

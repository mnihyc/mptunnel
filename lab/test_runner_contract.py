import unittest
from pathlib import Path


SCRIPT = (Path(__file__).resolve().parent / "run-heterogeneous-ablation.sh").read_text(
    encoding="utf-8"
)
COMPOSE = (Path(__file__).resolve().parent / "docker-compose.yml").read_text(
    encoding="utf-8"
)
DOCKERFILE = (Path(__file__).resolve().parent / "docker" / "Dockerfile").read_text(
    encoding="utf-8"
)


class RunnerContractTests(unittest.TestCase):
    def test_diagnostic_build_uses_shared_truthy_parser(self):
        build_function = SCRIPT.split("build_mptunnel_binary() {", 1)[1].split("\n}", 1)[0]

        self.assertIn('if flag_enabled "$lab_diagnostics"; then', build_function)
        self.assertIn("--features lab-diagnostics", build_function)

    def test_wine_runtime_is_opt_in_and_client_only(self):
        build_function = SCRIPT.split("build_mptunnel_binary() {", 1)[1].split(
            "\n}", 1
        )[0]
        client_command = SCRIPT.split("client_mptunnel_command() {", 1)[1].split(
            "\n}", 1
        )[0]
        server_start = SCRIPT.split("start_server() {", 1)[1].split("\n}", 1)[0]

        self.assertIn('MPTUNNEL_LAB_CLIENT_RUNTIME:-native', SCRIPT)
        self.assertIn('target "$client_target"', build_function)
        self.assertIn("WINEPREFIX=%q wine %q", client_command)
        self.assertIn("/workspace/target/release/mptunnel", server_start)
        self.assertIn("MPTUNNEL_LAB_INSTALL_WINE", COMPOSE)
        self.assertIn('MPTUNNEL_LAB_INSTALL_WINE=0', DOCKERFILE)

    def test_wine_results_record_runtime_and_binary_identity(self):
        provenance = SCRIPT.split("refresh_result_reproducibility() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn('"mptunnel_client_runtime"', provenance)
        self.assertIn('"mptunnel_client_runtime_version"', provenance)
        self.assertIn('"mptunnel_client_sha256"', provenance)
        self.assertIn('"mptunnel_server_sha256"', provenance)
        self.assertIn("prepare_client_runtime", SCRIPT)

    def test_wine_runtime_excludes_tun_or_rejects_explicit_selection(self):
        selector = SCRIPT.split("should_run_case() {", 1)[1].split("\n}", 1)[0]
        validation = SCRIPT.split(
            "validate_client_runtime_case_filter() {", 1
        )[1].split("\n}", 1)[0]

        self.assertIn('"$case_name" == mptunnel_tun_*', selector)
        self.assertIn("Wine client runtime cannot run TUN case", validation)
        self.assertIn("validate_client_runtime_case_filter", SCRIPT)

    def test_proxy_client_waits_for_listener_and_reports_early_exit(self):
        readiness = SCRIPT.split("wait_for_client_proxy() {", 1)[1].split(
            "\n}", 1
        )[0]
        start = SCRIPT.split("start_client_with_netem() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn("/proc/net/tcp /proc/net/tcp6", readiness)
        self.assertIn('kill -0 \\"\\$pid\\"', readiness)
        self.assertIn("tail -n 80", readiness)
        self.assertIn("wait_for_client_proxy", start)

    def test_release_artifacts_retain_configs_qdisc_and_run_inputs(self):
        config = SCRIPT.split("persist_redacted_config() {", 1)[1].split(
            "\n}", 1
        )[0]
        telemetry = SCRIPT.split("start_case_telemetry() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn("write_run_manifest(sys.argv[1], os.environ)", SCRIPT)
        self.assertIn("compose-config.yaml", SCRIPT)
        self.assertIn("secret|token", config)
        self.assertIn("record_config_checksum", config)
        self.assertIn("config-sha256.txt", SCRIPT)
        self.assertIn("retain_active_client_config_for_case", telemetry)
        self.assertIn("capture_qdisc_snapshot", telemetry)
        self.assertIn('normalize_lab_result_path "run-${timestamp}-$$"', SCRIPT)
        self.assertIn(': > "$result_dir/config-sha256.txt"', SCRIPT)

    def test_external_baseline_rows_record_verified_running_executables(self):
        capture = SCRIPT.split("capture_baseline_identity() {", 1)[1].split(
            "\n}\n\nrun_vmess_baseline_case()", 1
        )[0]

        self.assertIn("identity-${tool}", capture)
        self.assertIn("BASELINE_LOCK_SHA256", capture)
        wrappers = (
            (
                "run_vmess_baseline_case() {",
                "run_vmess_baseline_upload_case() {",
                "capture_baseline_identity xray",
                'run_baseline_download_probe_case "$case_name" "vmess" "$baseline_proxy_port" "$baseline_identity_json"',
            ),
            (
                "run_vmess_baseline_upload_case() {",
                "run_hysteria2_baseline_case() {",
                "capture_baseline_identity xray",
                'run_baseline_upload_probe_case "$case_name" "vmess" "$baseline_proxy_port" "$baseline_identity_json"',
            ),
            (
                "run_hysteria2_baseline_case() {",
                "run_hysteria2_baseline_upload_case() {",
                "capture_baseline_identity hysteria2",
                'run_baseline_download_probe_case "$case_name" "hysteria2" "$baseline_proxy_port" "$baseline_identity_json"',
            ),
            (
                "run_hysteria2_baseline_upload_case() {",
                "configure_mptcp_endpoints() {",
                "capture_baseline_identity hysteria2",
                'run_baseline_upload_probe_case "$case_name" "hysteria2" "$baseline_proxy_port" "$baseline_identity_json"',
            ),
        )
        for start, end, capture_call, probe_call in wrappers:
            body = SCRIPT.split(start, 1)[1].split(end, 1)[0]
            self.assertIn(capture_call, body)
            self.assertIn(probe_call, body)
        download_probe = SCRIPT.split("run_baseline_download_probe_case() {", 1)[
            1
        ].split("\n}\n\nrun_baseline_upload_probe_case()", 1)[0]
        upload_probe = SCRIPT.split("run_baseline_upload_probe_case() {", 1)[1].split(
            "\n}\n\nensure_baseline_tool()", 1
        )[0]
        self.assertIn(
            'append_row_with_telemetry "$case_name" "$output" "$protocol" 0 "" "$baseline_identity_json"',
            download_probe,
        )
        self.assertIn(
            'append_download_probe_result "$case_name" "$exit_code" "" "$probe_stderr" 0 "$protocol" "$baseline_identity_json"',
            download_probe,
        )
        self.assertIn(
            '"$observer_freeze_exit_code" "" "$baseline_identity_json"',
            upload_probe,
        )

    def test_wine_shutdown_waits_before_reusing_the_proxy_port(self):
        stop = SCRIPT.split("stop_client() {", 1)[1].split("\n}", 1)[0]

        self.assertIn("wineserver -k", stop)
        self.assertIn("wineserver -w", stop)
        self.assertIn("timeout ${client_start_timeout_seconds}s", stop)

    def test_wine_initialization_is_bounded(self):
        prepare = SCRIPT.split("prepare_client_runtime() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn("timeout ${client_start_timeout_seconds}s", prepare)
        self.assertIn("timed out initializing the Wine client runtime", prepare)

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

    def test_mixed_family_contention_upload_orders_quic_before_two_tcp_paths(self):
        self.assertIn(
            'run_reliable_ideal_upload_case "mptunnel_reliable_mixed_family_contention_equal_fat_upload" "fat" "$udp_endpoint_fat $tcp_endpoint_lowlat $tcp_endpoint_balanced"',
            SCRIPT,
        )


if __name__ == "__main__":
    unittest.main()

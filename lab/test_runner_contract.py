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
BASELINE_TOOLS = (
    Path(__file__).resolve().parent / "baseline-tools.sh"
).read_text(encoding="utf-8")
EXHAUSTIVE = (
    Path(__file__).resolve().parent / "run-exhaustive-experiments.sh"
).read_text(encoding="utf-8")
NETEM = (Path(__file__).resolve().parent / "configure-netem.sh").read_text(
    encoding="utf-8"
)
RANDOM_INTERNET = (
    Path(__file__).resolve().parent / "run-random-internet-experiments.sh"
).read_text(encoding="utf-8")


class RunnerContractTests(unittest.TestCase):
    def test_only_optional_baselines_can_emit_skipped_rows(self):
        optional_protocols = (
            '"vmess"',
            '"vmess-upload"',
            '"hysteria2"',
            '"hysteria2-upload"',
            '"$protocol"',
            '"mptcp"',
            '"mptcp-upload"',
        )
        skip_calls = [
            line.strip()
            for line in SCRIPT.splitlines()
            if 'append_skipped_result "$case_name"' in line
        ]

        self.assertGreater(len(skip_calls), 0)
        for call in skip_calls:
            self.assertTrue(
                any(protocol in call for protocol in optional_protocols),
                f"non-optional case emits a skipped row: {call}",
            )
        self.assertIn('{"ok", "loss", "skipped"}', SCRIPT)
        skipped = SCRIPT.split("append_skipped_result() {", 1)[1].split(
            "\n}", 1
        )[0]
        self.assertIn("MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES", SCRIPT)
        self.assertIn(
            "baseline_vmess_*|baseline_hysteria2_*|baseline_mptcp_*",
            skipped,
        )
        self.assertIn('status="fail"', skipped)
        self.assertIn(
            'MPTUNNEL_LAB_FAIL_ON_BAD_STATUS:-1',
            EXHAUSTIVE,
        )

    def test_current_wire_and_resource_contract_is_explicit(self):
        provenance = SCRIPT.split("refresh_result_reproducibility() {", 1)[1].split(
            "\n}", 1
        )[0]
        resources = SCRIPT.split("resource_config_toml() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn("MPTUNNEL_CARRIER_PRESENTATION", provenance)
        self.assertIn("mptunnel_carrier_presentation", provenance)
        self.assertIn(
            'MPTUNNEL_LAB_SHARED_TRANSPORT_SECRET:-1',
            SCRIPT,
        )
        self.assertIn('mptunnel_transport_profile="shared-secret"', SCRIPT)
        self.assertIn('mptunnel_transport_profile="standard"', SCRIPT)
        self.assertIn(
            'MPTUNNEL_TRANSPORT_PROFILE="$mptunnel_transport_profile"',
            SCRIPT,
        )
        self.assertIn("lab evidence supports only MPP wire protocol", SCRIPT)
        for name in (
            "MPTUNNEL_MAX_QUIC_CONCURRENT_BIDI_STREAMS",
            "MPTUNNEL_MAX_REINJECTION_CACHE_CHUNKS",
            "MPTUNNEL_MAX_REORDER_BUFFER_CHUNKS",
            "MPTUNNEL_MAX_RETAINED_RECEIVE_RANGES",
            "MPTUNNEL_QUIC_PATH_KEEP_ALIVE_INTERVAL_S",
            "MPTUNNEL_QUIC_PATH_IDLE_TIMEOUT_S",
        ):
            self.assertIn(name, resources)

        self.assertIn(".tmp/lab/baselines", BASELINE_TOOLS)
        self.assertNotIn(
            'MPTUNNEL_LAB_BASELINE_DIR:-/tmp/',
            BASELINE_TOOLS,
        )

    def test_diagnostic_build_uses_shared_truthy_parser(self):
        build_function = SCRIPT.split("build_mptunnel_binary() {", 1)[1].split("\n}", 1)[0]

        self.assertIn('if flag_enabled "$lab_diagnostics"; then', build_function)
        self.assertIn("--features lab-diagnostics", build_function)

    def test_configured_build_root_controls_cargo_output(self):
        build_function = SCRIPT.split("build_mptunnel_binary() {", 1)[1].split("\n}", 1)[0]

        self.assertEqual(
            build_function.count('CARGO_TARGET_DIR="$host_build_root"'),
            2,
        )

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

    def test_tcp_carrier_qos_cohort_is_opt_in_paired_and_reproducible(self):
        server_config = SCRIPT.split("server_config_toml() {", 1)[1].split(
            "\n}", 1
        )[0]
        qos_case = SCRIPT.split("run_tcp_carrier_qos_case() {", 1)[1].split(
            "\n}", 1
        )[0]
        qos_dispatch = SCRIPT.rsplit(
            'if flag_enabled "$tcp_carrier_qos_cohort"; then', 1
        )[1].split("\nfi", 1)[0]
        per_flow_profile = NETEM.split("apply_tcp_per_flow_qos() {", 1)[1].split(
            "\n}", 1
        )[0]
        shared_profile = NETEM.split(
            "apply_tcp_shared_bottleneck() {", 1
        )[1].split("\n}", 1)[0]

        self.assertIn(
            'tcp_carrier_qos_cohort="${MPTUNNEL_LAB_TCP_CARRIER_QOS_COHORT:-0}"',
            SCRIPT,
        )
        self.assertIn("tcp_carrier_qos_duration_seconds=30", SCRIPT)
        self.assertIn("tcp_carrier_qos_workers=3", SCRIPT)
        self.assertIn(
            "for qos_topology in range_1_1 range_1_3 range_3_3 three_endpoints_1_1",
            qos_dispatch,
        )
        self.assertIn('"per_flow_qos:tcp-per-flow-qos"', qos_dispatch)
        self.assertIn('"shared_bottleneck:tcp-shared-bottleneck"', qos_dispatch)
        self.assertIn('"unconstrained:unconstrained"', qos_dispatch)
        self.assertIn("range_3_3)", qos_case)
        self.assertIn("three_endpoints_1_1)", qos_case)
        self.assertEqual(qos_case.count("?max-tcp-carriers=1"), 4)
        self.assertNotIn("max-tcp-carriers", server_config)
        self.assertIn("fixed", qos_case)
        self.assertIn("$tcp_carrier_qos_workers", qos_case)
        self.assertIn("$tcp_carrier_qos_duration_seconds", qos_case)
        self.assertIn("--synchronized-start", SCRIPT)

        self.assertIn("root handle 1: netem", per_flow_profile)
        self.assertIn("parent 1:1 handle 10: fq", per_flow_profile)
        self.assertIn('flow_limit "$flow_limit_packets"', per_flow_profile)
        self.assertIn('maxrate "$maxrate"', per_flow_profile)
        self.assertIn('tc qdisc del dev "$iface" root', per_flow_profile)
        self.assertIn("root handle 1: netem", shared_profile)
        self.assertIn('rate "$rate"', shared_profile)
        self.assertIn('tc qdisc del dev "$iface" root', shared_profile)
        self.assertIn(
            'tcp_per_flow_qos_rate="${MPTUNNEL_LAB_TCP_PER_FLOW_QOS_RATE:-500mbit}"',
            NETEM,
        )
        self.assertIn(
            'tcp_shared_bottleneck_rate="${MPTUNNEL_LAB_TCP_SHARED_BOTTLENECK_RATE:-200mbit}"',
            NETEM,
        )
        self.assertIn("tc -s -d qdisc show dev", NETEM)

    def test_versioned_host_snapshot_is_captured_before_result_identity(self):
        capture = SCRIPT.split("capture_host_snapshot() {", 1)[1].split(
            "\n}", 1
        )[0]
        provenance = SCRIPT.split("refresh_result_reproducibility() {", 1)[1].split(
            "\n}", 1
        )[0]
        startup = SCRIPT.split("prepare_client_runtime\n", 1)[1]

        self.assertIn('host_snapshot_file="$result_dir/host-snapshot.json"', SCRIPT)
        self.assertIn("host_snapshot.py", capture)
        self.assertIn("--exclude-container-id", capture)
        self.assertIn("MPTUNNEL_LAB_REQUIRE_VALID_HOST", (
            Path(__file__).resolve().parent / "host_snapshot.py"
        ).read_text(encoding="utf-8"))
        self.assertIn("load_host_reproducibility", provenance)
        self.assertLess(
            startup.index("capture_host_snapshot"),
            startup.index("refresh_result_reproducibility"),
        )
        self.assertIn("HOST_SNAPSHOT_SHA256", SCRIPT)

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

    def test_bulk_interactive_latency_series_cases_share_one_exact_workload(self):
        probe = SCRIPT.split("run_bulk_interactive_probe_case() {", 1)[1].split(
            "\n}\n\nrun_baseline_upload_probe_case()", 1
        )[0]
        self.assertIn("--workload-mode bulk-interactive", probe)
        self.assertIn("--http-target 172.31.40.30:8080", probe)
        self.assertIn("--tcp-echo-target 172.31.40.30:10022", probe)
        self.assertIn("--tcp-echo-interval-ms '${tcp_echo_interval_ms}'", probe)
        self.assertNotIn("--small-path", probe)
        self.assertNotIn("--udp-target", probe)
        self.assertIn('local mptunnel_row="${3:-1}"', probe)
        self.assertIn('local baseline_identity_json="${4:-}"', probe)
        self.assertIn('"$mptunnel_row" "$baseline_identity_json"', probe)

        for case_name in (
            "baseline_vmess_tcp_bulk_interactive_balanced",
            "baseline_hysteria2_udp_bulk_interactive_balanced",
            "mptunnel_tcp_bulk_interactive_balanced",
            "mptunnel_quic_bulk_interactive_balanced",
            "mptunnel_tcp_quic_bulk_interactive_balanced",
        ):
            self.assertIn(f'if should_run_case "{case_name}";', SCRIPT)

        self.assertIn(
            'start_client "tcp_bulk_interactive_balanced" "$tcp_balanced"',
            SCRIPT,
        )
        self.assertIn(
            'start_client "quic_bulk_interactive_balanced" "$udp_balanced"',
            SCRIPT,
        )
        self.assertIn(
            '"tcp_quic_bulk_interactive_balanced" "$tcp_balanced $udp_balanced"',
            SCRIPT,
        )

        hysteria_case = SCRIPT.split(
            'if should_run_case "baseline_hysteria2_udp_bulk_interactive_balanced";',
            1,
        )[1].split("\nfi", 1)[0]
        self.assertIn("MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE", hysteria_case)
        self.assertIn("MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE", hysteria_case)
        self.assertIn('"$hysteria_up_rate"', hysteria_case)
        self.assertIn('"$hysteria_down_rate"', hysteria_case)

    def test_bulk_interactive_baselines_retain_verified_identity_and_mpp_scope(self):
        vmess = SCRIPT.split("run_vmess_baseline_case() {", 1)[1].split(
            "\n}\n\nrun_vmess_baseline_upload_case()", 1
        )[0]
        hysteria = SCRIPT.split("run_hysteria2_baseline_case() {", 1)[1].split(
            "\n}\n\nrun_hysteria2_baseline_upload_case()", 1
        )[0]
        append = SCRIPT.split("append_mixed_probe_result() {", 1)[1].split(
            "\n}\n\nrecord_mixed_probe_case()", 1
        )[0]

        for wrapper in (vmess, hysteria):
            self.assertIn('"$case_name" "$baseline_proxy_port" 0 "$baseline_identity_json"', wrapper)
        self.assertIn('BASELINE_IDENTITY="$baseline_identity_json"', append)
        self.assertIn("enrich_baseline_identity", append)

    def test_flapper_cannot_be_stopped_by_background_terminal_signals(self):
        flapper = SCRIPT.split("start_random_flapping() {", 1)[1].split(
            "\n}\n\nshould_run_case()", 1
        )[0]

        self.assertIn("trap '' TTOU TTIN TSTP", flapper)
        self.assertIn(") </dev/null &", flapper)

    def test_flapper_uses_complete_non_cumulative_condition_epochs(self):
        flapper = SCRIPT.split("start_random_flapping() {", 1)[1].split(
            "\n}\n\nshould_run_case()", 1
        )[0]

        self.assertIn(
            'flap_initial_stable_seconds="${MPTUNNEL_LAB_FLAP_INITIAL_STABLE_SECONDS:-10}"',
            SCRIPT,
        )
        self.assertIn("initial_hold_deadline_ms", flapper)
        self.assertIn("initial_stable_seconds * 1000", flapper)
        self.assertIn("exec_netem client apply", flapper)
        self.assertIn('exec_netem client "$mode"', flapper)
        self.assertIn("exec_netem server apply", flapper)
        self.assertIn('exec_netem server "$mode"', flapper)

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

    def test_asymmetric_product_baselines_keep_one_fixed_link(self):
        apply_netem = SCRIPT.split("apply_netem() {", 1)[1].split(
            "\n}", 1
        )[0]
        asymmetric_download = SCRIPT.split(
            "run_asymmetric_download_case() {", 1
        )[1].split("\n}", 1)[0]
        asymmetric_upload = SCRIPT.split(
            "run_asymmetric_upload_case() {", 1
        )[1].split("\n}", 1)[0]

        self.assertIn('[[ "$mode" == "asymmetric" ]]', apply_netem)
        self.assertIn("apply_asymmetric_netem", apply_netem)
        self.assertIn(
            'start_client_with_netem "$case_name" asymmetric',
            asymmetric_download,
        )
        self.assertIn(
            'start_client_with_netem "$case_name" asymmetric',
            asymmetric_upload,
        )
        self.assertNotIn("apply_asymmetric_netem", asymmetric_download)
        self.assertNotIn("apply_asymmetric_netem", asymmetric_upload)
        self.assertIn(
            '"baseline_vmess_tcp_single_asymmetric_download_reference"',
            SCRIPT,
        )
        self.assertIn(
            '"baseline_hysteria2_udp_single_asymmetric_download_reference"',
            SCRIPT,
        )
        self.assertIn(
            '"baseline_vmess_tcp_single_asymmetric_upload_reference"',
            SCRIPT,
        )
        self.assertIn(
            '"baseline_hysteria2_udp_single_asymmetric_upload_reference"',
            SCRIPT,
        )
        vmess_upload = SCRIPT.split(
            'if should_run_case "baseline_vmess_tcp_single_asymmetric_upload_reference";',
            1,
        )[1].split("fi", 1)[0]
        hysteria_upload = SCRIPT.split(
            'if should_run_case "baseline_hysteria2_udp_single_asymmetric_upload_reference";',
            1,
        )[1].split("fi", 1)[0]
        self.assertIn('"172.31.10.20"', vmess_upload)
        self.assertIn('"172.31.10.20"', hysteria_upload)
        self.assertIn('"20 mbps"', SCRIPT)
        self.assertIn('"200 mbps"', SCRIPT)

    def test_seeded_random_netem_is_directional_and_protocol_neutral(self):
        apply_netem = SCRIPT.split("apply_netem() {", 1)[1].split(
            "\n}", 1
        )[0]
        startup = SCRIPT.split("write_run_manifest\n", 1)[1]

        self.assertIn(
            'default_netem_mode="${MPTUNNEL_LAB_NETEM_MODE:-apply}"',
            SCRIPT,
        )
        self.assertIn(
            '[[ "$mode" =~ ^internet-five-path-epoch-([0-9]+)$ ]]',
            apply_netem,
        )
        self.assertIn('exec_netem client "${mode}-client"', apply_netem)
        self.assertIn('exec_netem server "${mode}-server"', apply_netem)
        self.assertIn('exec_netem target "${mode}-server"', apply_netem)
        self.assertIn(
            '-e MPTUNNEL_LAB_INTERNET_SEED="$internet_seed"', SCRIPT
        )
        self.assertIn(
            '-e MPTUNNEL_LAB_INTERNET_INCLUDE_OUTAGES="$internet_include_outages"',
            SCRIPT,
        )
        self.assertIn('apply_netem "$default_netem_mode"', startup)

        for default_argument in (
            'local netem_mode="${3:-$default_netem_mode}"',
            'local netem_mode="${2:-$default_netem_mode}"',
        ):
            self.assertIn(default_argument, SCRIPT)
        start_client = SCRIPT.split("start_client() {", 1)[1].split(
            "\n}", 1
        )[0]
        self.assertIn(
            'start_client_with_netem "$profile" "$default_netem_mode"',
            start_client,
        )
        self.assertIn(
            'start_client_with_netem "$case_name" asymmetric', SCRIPT
        )
        self.assertIn(
            'run_mptcp_baseline_case "baseline_mptcp_tcp_multipath_equal_fat" ideal-all-fat',
            SCRIPT,
        )

    def test_random_internet_matrix_replays_one_canonical_schedule(self):
        self.assertIn(
            'schedule_script="$script_dir/internet_condition_schedule.py"',
            RANDOM_INTERNET,
        )
        self.assertIn('python3 "$schedule_script" generate', RANDOM_INTERNET)
        self.assertIn(
            'validate --schedule "$schedule_file"', RANDOM_INTERNET
        )
        self.assertIn(
            'metadata --schedule "$schedule_file"', RANDOM_INTERNET
        )
        self.assertIn(
            'MPTUNNEL_LAB_INTERNET_EPOCHS:-7', RANDOM_INTERNET
        )
        self.assertIn(
            'MPTUNNEL_LAB_NETEM_MODE="$netem_mode"', RANDOM_INTERNET
        )
        self.assertIn(
            'MPTUNNEL_LAB_INTERNET_SCHEDULE_SHA256="$schedule_sha256"',
            RANDOM_INTERNET,
        )
        self.assertIn(
            'MPTUNNEL_LAB_REQUIRE_VALID_HOST="${MPTUNNEL_LAB_REQUIRE_VALID_HOST:-1}"',
            RANDOM_INTERNET,
        )
        self.assertIn(
            'MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES="${MPTUNNEL_LAB_REQUIRE_COMPETITOR_BASELINES:-1}"',
            RANDOM_INTERNET,
        )
        self.assertIn('BUILD_PRODUCT="$build_product"', RANDOM_INTERNET)
        self.assertIn("first_run=0", RANDOM_INTERNET)
        self.assertIn('RESULT_DIR="$run_result_dir"', RANDOM_INTERNET)
        for subject in (
            "direct_balanced",
            "baseline_vmess_tcp_single_balanced",
            "baseline_hysteria2_udp_single_balanced",
            "baseline_mptcp_tcp_multipath_all",
            "mptunnel_tcp_single_balanced",
            "mptunnel_udp_stream_single_balanced",
            "mptunnel_tcp_multipath_all",
            "mptunnel_udp_stream_multipath_all",
            "mptunnel_udp_single_balanced",
            "mptunnel_udp_multipath_all",
            "mptunnel_client_direct_balanced",
            "mptunnel_client_direct_balanced_upload",
        ):
            self.assertIn(subject, RANDOM_INTERNET)

    def test_active_mpp_client_keeps_an_explicit_direct_outbound_control(self):
        client = SCRIPT.split("socks_client_config_toml() {", 1)[1].split(
            "\n}", 1
        )[0]

        self.assertIn('name = "lab-client-direct"', client)
        self.assertIn('protocol = "direct"', client)
        direct_rule = client.split(
            'name = "allow-lab-client-direct-control"', 1
        )[1].split("[[routing.rules]]", 1)[0]
        self.assertIn('destination_cidrs = ["172.31.15.30/32"]', direct_rule)
        self.assertIn('outbound = "lab-client-direct"', direct_rule)
        self.assertLess(
            client.index('name = "allow-lab-client-direct-control"'),
            client.index('name = "allow-lab-private-targets"'),
        )
        self.assertIn('"mptunnel_client_direct_balanced"', SCRIPT)
        self.assertIn('"mptunnel_client_direct_balanced_upload"', SCRIPT)

    def test_random_hysteria_brutal_rates_match_scheduled_directions(self):
        self.assertIn('if row["subnet_prefix"] == "172.31.15"', RANDOM_INTERNET)
        self.assertIn('rates["client"]', RANDOM_INTERNET)
        self.assertIn('rates["server"]', RANDOM_INTERNET)
        self.assertIn(
            'MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE="$hysteria_client_rate"',
            RANDOM_INTERNET,
        )
        self.assertIn(
            'MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE="$hysteria_server_rate"',
            RANDOM_INTERNET,
        )
        balanced_case = SCRIPT.split(
            'if should_run_case "baseline_hysteria2_udp_single_balanced";', 1
        )[1].split("\nfi", 1)[0]
        self.assertIn("MPTUNNEL_LAB_HYSTERIA_BALANCED_CLIENT_RATE", balanced_case)
        self.assertIn("MPTUNNEL_LAB_HYSTERIA_BALANCED_SERVER_RATE", balanced_case)
        self.assertIn('"$default_netem_mode"', balanced_case)

    def test_hysteria2_product_baselines_enable_brutal_at_shaped_rate(self):
        self.assertIn("hysteria_bandwidth_from_netem_rate", SCRIPT)
        self.assertIn("${MPTUNNEL_LAB_BALANCED_RATE:-200mbit}", SCRIPT)
        self.assertIn("${MPTUNNEL_LAB_FAT_RATE:-500mbit}", SCRIPT)
        self.assertIn("bandwidth:", BASELINE_TOOLS)
        self.assertIn("disableLossCompensation: false", BASELINE_TOOLS)

    def test_mixed_single_equal_fat_controls_use_one_tcp_and_one_udp_endpoint(self):
        self.assertIn(
            'run_reliable_ideal_download_case "mptunnel_reliable_mixed_single_equal_fat" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"',
            SCRIPT,
        )
        self.assertIn(
            'run_reliable_ideal_upload_case "mptunnel_reliable_mixed_single_equal_fat_upload" "fat" "$tcp_endpoint_fat $udp_endpoint_fat"',
            SCRIPT,
        )

    def test_public_mixed_comparisons_use_matched_transport_pairs(self):
        self.assertIn(
            'start_client "reliable_mixed_single_cross_continent_high_bandwidth" "$tcp_fat $udp_fat"',
            SCRIPT,
        )
        self.assertIn(
            'start_client "reliable_mixed_single_cross_continent_high_bandwidth_upload" "$tcp_fat $udp_fat"',
            SCRIPT,
        )
        self.assertIn(
            'run_reliable_ideal_download_case "mptunnel_reliable_mixed_paired_multipath_equal_fat" "fat" "$tcp_equal_all $udp_equal_all"',
            SCRIPT,
        )
        self.assertIn(
            'run_reliable_ideal_upload_case "mptunnel_reliable_mixed_paired_multipath_equal_fat_upload" "fat" "$tcp_equal_all $udp_equal_all"',
            SCRIPT,
        )
        self.assertIn(
            '"mptunnel_reliable_mixed_two_links_equal_fat"',
            SCRIPT,
        )
        self.assertIn(
            '"$tcp_endpoint_lowlat $udp_endpoint_lowlat $tcp_endpoint_fat $udp_endpoint_fat"',
            SCRIPT,
        )

    def test_path_choice_controls_pair_both_transports_on_each_link(self):
        self.assertIn(
            '"mptunnel_mixed_single_cross_continent_high_bandwidth"',
            SCRIPT,
        )
        self.assertIn(
            '"mptunnel_mixed_two_links_lowlat_fat"',
            SCRIPT,
        )
        self.assertIn(
            '"$tcp_lowlat $udp_lowlat $tcp_fat $udp_fat"',
            SCRIPT,
        )

    def test_single_tcp_local_ceiling_has_exact_one_carrier_control(self):
        self.assertIn(
            '"mptunnel_tcp_single_unconstrained_range_1_1"',
            SCRIPT,
        )
        self.assertIn(
            '"--path \'tcp://172.31.10.20:${server_port}?max-tcp-carriers=1\'"',
            SCRIPT,
        )

    def test_mixed_family_contention_upload_orders_quic_before_two_tcp_paths(self):
        self.assertIn(
            'run_reliable_ideal_upload_case "mptunnel_reliable_mixed_family_contention_equal_fat_upload" "fat" "$udp_endpoint_fat $tcp_endpoint_lowlat $tcp_endpoint_balanced"',
            SCRIPT,
        )

    def test_all_tcp_cohorts_can_override_carrier_ceiling(self):
        self.assertIn(
            'tcp_carrier_max="${MPTUNNEL_LAB_TCP_CARRIER_MAX:-}"',
            SCRIPT,
        )
        self.assertIn(
            'tcp_carrier_hint_query="&max-tcp-carriers=${tcp_carrier_max}"',
            SCRIPT,
        )
        self.assertIn(
            'scale_tcp_carrier_max="${MPTUNNEL_LAB_SCALE_TCP_CARRIER_MAX:-$tcp_carrier_max}"',
            SCRIPT,
        )

    def test_large_varying_and_asymmetric_cases_have_complete_topology_and_load(self):
        server = SCRIPT.split("server_config_toml() {", 1)[1].split(
            "\n}", 1
        )[0]
        endpoints = SCRIPT.split('tcp_scale_all="', 1)[1].split(
            '\n\nif should_run_case "direct_low_latency"', 1
        )[0]
        asymmetric = SCRIPT.split("apply_asymmetric_netem() {", 1)[1].split(
            "\n}", 1
        )[0]
        browser = SCRIPT.split("run_browser_probe_case() {", 1)[1].split(
            "\n}", 1
        )[0]
        browser_batches = SCRIPT.split("run_browser_batch_case() {", 1)[1].split(
            "\n}", 1
        )[0]
        browser_load = SCRIPT.split("run_browser_full_load_case() {", 1)[1].split(
            "\n}", 1
        )[0]
        varying_download = SCRIPT.split(
            "run_varying_links_download_case() {", 1
        )[1].split("\n}", 1)[0]
        schedule = SCRIPT.split("run_path_variation_schedule() {", 1)[1].split(
            "\n}", 1
        )[0]
        initial = SCRIPT.split(
            "prepare_path_variation_initial_epoch() {", 1
        )[1].split("\n}", 1)[0]

        self.assertEqual(endpoints.count("--path 'tcp://"), 10)
        self.assertEqual(endpoints.count("--path 'quic://"), 10)
        self.assertEqual(endpoints.count("${scale_tcp_carrier_query}"), 10)
        for prefix in range(41, 46):
            self.assertIn(f'"tcp://172.31.{prefix}.20:${{server_port}}"', server)
        for prefix in range(51, 61):
            self.assertIn(f'"quic://172.31.{prefix}.20:${{server_port}}"', server)
        self.assertIn("exec_netem client asymmetric-client", asymmetric)
        self.assertIn("exec_netem server asymmetric-server", asymmetric)
        self.assertIn("--small-batch-size \"$browser_batch_size\"", browser_batches)
        self.assertIn("--small-batch-period-ms \"$browser_batch_period_ms\"", browser_batches)
        self.assertIn("--small-response-budget-ms \"$browser_batch_deadline_ms\"", browser_batches)
        self.assertIn("--require-small-response-budget", browser_batches)
        self.assertIn("--browser-only", browser)
        self.assertIn("--browser-full-load", browser_load)
        self.assertIn("--small-batch-size \"$browser_load_concurrency\"", browser_load)
        self.assertIn("\"$browser_load_duration_seconds\"", browser_load)
        self.assertNotIn("browser_batch_deadline_ms", browser_load)
        self.assertIn("COMPLETION_TIMEOUT_SECONDS", browser)
        self.assertIn("probe_process_timeout_seconds", browser)
        self.assertNotIn("--bulk-path", browser)
        self.assertNotIn("--tcp-echo-target", browser)
        self.assertNotIn("--udp-payload-bytes", browser)
        self.assertIn("scale-${rate_band}-epoch-0-client", initial)
        self.assertIn("scale-${rate_band}-epoch-0-server", initial)
        self.assertIn(
            'start_client_with_netem "$case_name" "scale-${rate_band}-epoch-0"',
            SCRIPT,
        )
        self.assertIn("preconditioned", schedule)
        self.assertIn(
            'exec_netem client "scale-${rate_band}-epoch-${epoch}-client"',
            schedule,
        )
        self.assertIn(
            'exec_netem server "scale-${rate_band}-epoch-${epoch}-server"',
            schedule,
        )
        self.assertIn(
            "scale_rate_bands=(access gigabit multi-gigabit)", SCRIPT
        )
        self.assertIn("run_browser_batch_case", SCRIPT)
        self.assertIn('start_client "$case_name" "$tcp_all $udp_all"', browser)
        self.assertIn("run_browser_full_load_case", SCRIPT)
        self.assertIn(
            'run_varying_links_download_case "$scale_rate_band"', SCRIPT
        )
        self.assertIn(
            'run_varying_links_upload_case "$scale_rate_band"', SCRIPT
        )
        self.assertIn("--parallel-downloads '${bulk_connections}'", varying_download)
        self.assertIn("run_path_variation_schedule", varying_download)
        self.assertIn('--rate-band "$rate_band"', SCRIPT)
        self.assertIn("path_variation_metadata", SCRIPT)
        self.assertIn("attach_path_variation_metadata", SCRIPT)


if __name__ == "__main__":
    unittest.main()

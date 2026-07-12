import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent))
from mptcp_evidence import parse_mptcp_socket_line, parse_subflow_line, summarize_rows


class MptcpEvidenceTests(unittest.TestCase):
    def test_mptcp_socket_parser_reads_kernel_reported_subflow_total(self):
        row = parse_mptcp_socket_line(
            "ESTAB 0 0 172.31.10.10:40000 172.31.10.30:18081 "
            "remote_key token:abc123 subflows_total:5 last_data_sent:20"
        )

        self.assertEqual(row["token"], "abc123")
        self.assertIsNone(row["additional_subflows"])
        self.assertEqual(row["subflows_total"], 5)

    def test_mptcp_socket_parser_reads_additional_subflow_counter_shapes(self):
        raw_lines = (
            "ESTAB 0 0 172.31.10.10:47790 172.31.10.30:10023 ino:1 sk:1 "
            "cgroup:/ <-> subflows:1 add_addr_signal:1 add_addr_accepted:1 "
            "subflows_max:5 token:d6c702fc",
            "ESTAB 0 0 172.31.10.10:47790 172.31.10.30:10023 ino:1 sk:1 "
            "cgroup:/ <-> subflows:2 add_addr_signal:2 add_addr_accepted:2 "
            "subflows_max:5 token:d6c702fc",
            "ESTAB 0 0 172.31.10.10:47790 172.31.10.30:10023 ino:1 sk:1 "
            "cgroup:/ <-> subflows:4 add_addr_signal:4 add_addr_accepted:4 "
            "subflows_max:5 token:d6c702fc",
        )

        for expected, raw in zip((1, 2, 4), raw_lines, strict=True):
            with self.subTest(expected=expected):
                row = parse_mptcp_socket_line(raw)
                self.assertEqual(row["additional_subflows"], expected)
                self.assertIsNone(row["subflows_total"])

    def test_mptcp_socket_parser_keeps_additional_and_total_counts_separate(self):
        row = parse_mptcp_socket_line(
            "ESTAB 0 0 172.31.10.10:47790 172.31.10.30:10023 "
            "subflows:4 remote_key token:d6c702fc subflows_total:5"
        )

        self.assertEqual(row["additional_subflows"], 4)
        self.assertEqual(row["subflows_total"], 5)

        result = summarize_rows(
            [
                {
                    "kind": "sample",
                    "service": "client",
                    "mptcp_socket_query_exit_code": 0,
                    "mptcp_subflow_query_exit_code": 0,
                    "mptcp_socket_count": 1,
                    "mptcp_subflow_count": 0,
                    "mptcp_sockets": [row],
                    "mptcp_subflows": [],
                },
                {
                    "kind": "terminal",
                    "service": "client",
                    "stop_reason": "requested",
                },
            ],
            "mptcp-evidence.jsonl",
        )
        client = result["services"]["client"]
        self.assertEqual(
            client["max_reported_additional_subflows_per_connection"], 4
        )
        self.assertEqual(client["max_reported_total_subflows_per_connection"], 5)

    def test_subflow_parser_extracts_token_and_endpoint_addresses(self):
        row = parse_subflow_line(
            "ESTAB 0 0 172.31.10.10:40000 172.31.10.30:18081 "
            "tcp-ulp-mptcp flags:Mm token:111(1)/222(2)"
        )

        self.assertEqual(row["local_address"], "172.31.10.10")
        self.assertEqual(row["peer_address"], "172.31.10.30")
        self.assertEqual(row["token"], "111(1)/222(2)")

    def test_summary_proves_multipath_only_with_distinct_pairs_under_one_token(self):
        rows = [
            {
                "kind": "sample",
                "service": "client",
                "mptcp_socket_query_exit_code": 0,
                "mptcp_subflow_query_exit_code": 0,
                "mptcp_socket_count": 1,
                "mptcp_subflow_count": 2,
                "mptcp_sockets": [{"token": "one", "subflows_total": 2}],
                "mptcp_subflows": [
                    {
                        "local_address": "172.31.10.10",
                        "peer_address": "172.31.10.30",
                        "token": "one",
                    },
                    {
                        "local_address": "172.31.20.10",
                        "peer_address": "172.31.20.30",
                        "token": "one",
                    },
                ],
            },
            {
                "kind": "terminal",
                "service": "client",
                "stop_reason": "requested",
            },
        ]

        result = summarize_rows(rows, "mptcp-evidence.jsonl")

        self.assertTrue(result["collection_ok"])
        self.assertTrue(result["multipath_observed"])
        self.assertEqual(result["aggregation_evidence"], "observed")
        client = result["services"]["client"]
        self.assertEqual(client["max_mptcp_socket_count"], 1)
        self.assertEqual(client["max_mptcp_subflow_count"], 2)
        self.assertIsNone(
            client["max_reported_additional_subflows_per_connection"]
        )
        self.assertEqual(client["max_reported_total_subflows_per_connection"], 2)
        self.assertEqual(client["max_subflows_per_token"], 2)
        self.assertEqual(client["max_distinct_endpoint_pairs_per_token"], 2)

    def test_summary_recovers_additional_subflows_from_legacy_raw_artifact(self):
        rows = [
            {
                "kind": "sample",
                "service": "client",
                "mptcp_socket_query_exit_code": 0,
                "mptcp_subflow_query_exit_code": 0,
                "mptcp_socket_count": 1,
                "mptcp_subflow_count": 0,
                "mptcp_sockets": [
                    {
                        "token": "d6c702fc",
                        "subflows_total": None,
                        "raw": (
                            "ESTAB 0 0 172.31.10.10:47790 172.31.10.30:10023 "
                            "ino:1 sk:1 cgroup:/ <-> subflows:4 add_addr_signal:4 "
                            "add_addr_accepted:4 subflows_max:5 token:d6c702fc"
                        ),
                    }
                ],
                "mptcp_subflows": [],
            },
            {
                "kind": "terminal",
                "service": "client",
                "stop_reason": "requested",
            },
        ]

        result = summarize_rows(rows, "mptcp-evidence.jsonl")

        self.assertTrue(result["multipath_observed"])
        self.assertEqual(result["aggregation_evidence"], "observed")
        client = result["services"]["client"]
        self.assertEqual(
            client["max_reported_additional_subflows_per_connection"], 4
        )
        self.assertIsNone(client["max_reported_total_subflows_per_connection"])

    def test_one_additional_subflow_already_proves_multipath(self):
        rows = [
            {
                "kind": "sample",
                "service": "client",
                "mptcp_socket_query_exit_code": 0,
                "mptcp_subflow_query_exit_code": 0,
                "mptcp_socket_count": 1,
                "mptcp_subflow_count": 0,
                "mptcp_sockets": [{"additional_subflows": 1}],
                "mptcp_subflows": [],
            },
            {
                "kind": "terminal",
                "service": "client",
                "stop_reason": "requested",
            },
        ]

        result = summarize_rows(rows, "mptcp-evidence.jsonl")

        self.assertTrue(result["multipath_observed"])

    def test_parallel_connections_without_token_linkage_do_not_prove_multipath(self):
        rows = [
            {
                "kind": "sample",
                "service": "client",
                "mptcp_socket_query_exit_code": 0,
                "mptcp_subflow_query_exit_code": 0,
                "mptcp_socket_count": 2,
                "mptcp_subflow_count": 2,
                "mptcp_subflows": [
                    {
                        "local_address": "172.31.10.10",
                        "peer_address": "172.31.10.30",
                        "token": None,
                    },
                    {
                        "local_address": "172.31.20.10",
                        "peer_address": "172.31.20.30",
                        "token": None,
                    },
                ],
            },
            {
                "kind": "terminal",
                "service": "client",
                "stop_reason": "requested",
            },
        ]

        result = summarize_rows(rows, "mptcp-evidence.jsonl")

        self.assertFalse(result["multipath_observed"])
        self.assertEqual(result["aggregation_evidence"], "not_observed")
        self.assertIsNone(result["services"]["client"]["max_subflows_per_token"])

    def test_failed_collection_reports_evidence_unavailable(self):
        result = summarize_rows(
            [
                {
                    "kind": "sampler_error",
                    "service": "client",
                    "error": "ss unavailable",
                }
            ],
            "mptcp-evidence.jsonl",
        )

        self.assertFalse(result["collection_ok"])
        self.assertFalse(result["multipath_observed"])
        self.assertEqual(result["aggregation_evidence"], "unavailable")


if __name__ == "__main__":
    unittest.main()

import argparse
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import management_snapshots


class ManagementSnapshotTests(unittest.TestCase):
    def test_container_snapshot_parses_management_and_interfaces(self):
        payload = {
            "interfaces": {"eth0": {"ipv4": "172.31.10.10", "tx_bytes": 42}},
            "management": {
                "schema": "mptunnel.management.v4",
                "role": "client",
                "summary": {
                    "path_count": 1,
                    "active_paths": 1,
                    "active_flows": 1,
                },
                "traffic": {
                    "rates": {
                        "to_peer_bps": "12000000",
                        "from_peer_bps": "0",
                    }
                },
                "paths": [
                    {
                        "service": "mpp_outbound",
                        "service_index": 0,
                        "path": "primary",
                        "underlay": "tcp",
                        "tcp_carrier_ordinal": 1,
                        "state": "active",
                        "delivery_samples": 3,
                    }
                ],
                "sessions": [
                    {
                        "service": "mpp_outbound",
                        "service_index": 0,
                        "session_id": "17",
                        "state": "active",
                        "carrier_count": 1,
                    }
                ],
                "flows": [
                    {
                        "session_id": "17",
                        "flow_kind": "reliable",
                        "flow_id": "23",
                        "network": "tcp",
                    }
                ],
            },
        }
        completed = subprocess.CompletedProcess(
            ["docker"], 0, stdout=json.dumps(payload), stderr=""
        )

        with mock.patch.object(
            management_snapshots, "run", return_value=completed
        ) as run_mock:
            snapshot = management_snapshots.container_snapshot(
                "container-id", 17600, "lab-management-token"
            )

        self.assertEqual(snapshot, payload)
        command = run_mock.call_args.args[0]
        self.assertIn("/api/v4/status", command[5])
        self.assertIn("mptunnel.management.v4", command[5])
        self.assertIn("Authorization", command[5])
        self.assertEqual(command[-2:], ["17600", "lab-management-token"])

    def test_sample_records_each_service_once_before_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "snapshots.jsonl"
            stop_file = root / "stop"
            args = argparse.Namespace(
                output=str(output),
                stop_file=str(stop_file),
                compose_file="compose.yml",
                services=["client", "server"],
                case="equal-fat",
                interval=1.0,
                port=17600,
                token="lab-management-token",
            )

            def fake_snapshot(container_id, port, token):
                self.assertEqual(token, "lab-management-token")
                if container_id == "server-id":
                    stop_file.touch()
                return {"management": {"role": container_id}, "interfaces": {}}

            with (
                mock.patch.object(
                    management_snapshots,
                    "compose_container_ids",
                    return_value={"client": "client-id", "server": "server-id"},
                ),
                mock.patch.object(
                    management_snapshots,
                    "container_snapshot",
                    side_effect=fake_snapshot,
                ),
            ):
                result = management_snapshots.sample(args)

            rows = [json.loads(line) for line in output.read_text().splitlines()]

        self.assertEqual(result, 0)
        self.assertEqual([row["service"] for row in rows], ["client", "server"])
        self.assertEqual(rows[0]["management"]["role"], "client-id")
        for row in rows:
            self.assertIsInstance(row["sample_started_monotonic_ns"], int)
            self.assertIsInstance(row["sample_finished_monotonic_ns"], int)
            self.assertLessEqual(
                row["sample_started_monotonic_ns"],
                row["sample_finished_monotonic_ns"],
            )


if __name__ == "__main__":
    unittest.main()

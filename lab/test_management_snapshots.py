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
            "management": {"paths": [{"index": 0, "delivery_samples": 3}]},
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
        self.assertIn("/api/diagnostics", command[5])
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


if __name__ == "__main__":
    unittest.main()

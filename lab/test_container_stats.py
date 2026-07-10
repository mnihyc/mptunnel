import argparse
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import container_stats


class ContainerStatsStoppingTests(unittest.TestCase):
    def test_compose_lookup_stops_between_services(self):
        stopped = False
        calls = []

        def fake_run(argv, timeout=3.0):
            nonlocal stopped
            calls.append(argv[-1])
            stopped = True
            return subprocess.CompletedProcess(argv, 0, stdout="client-id\n", stderr="")

        with mock.patch.object(container_stats, "run", side_effect=fake_run):
            ids = container_stats.compose_container_ids(
                "compose.yml",
                ["client", "server", "target"],
                lambda: stopped,
            )

        self.assertEqual(ids, {"client": "client-id"})
        self.assertEqual(calls, ["client"])

    def test_wait_stops_during_poll(self):
        with tempfile.TemporaryDirectory() as directory:
            stop_file = Path(directory) / "stop"
            sleeps = []

            def fake_sleep(delay):
                sleeps.append(delay)
                stop_file.touch()

            with (
                mock.patch.object(container_stats.time, "monotonic", return_value=10.75),
                mock.patch.object(container_stats.time, "sleep", side_effect=fake_sleep),
            ):
                stopped = container_stats.wait_for_stop_or_deadline(
                    stop_file,
                    deadline_monotonic=11.0,
                    poll_interval=1.0,
                )

        self.assertTrue(stopped)
        self.assertEqual(sleeps, [0.25])

    def test_sample_stops_before_reading_next_service(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "stats.jsonl"
            stop_file = root / "stop"
            args = argparse.Namespace(
                output=str(output),
                stop_file=str(stop_file),
                compose_file="compose.yml",
                services=["client", "server", "target"],
                case="stopping-test",
                interval=1.0,
            )
            reads = []

            def fake_read_netdev(container_id):
                reads.append(container_id)
                stop_file.touch()
                return {
                    "rx_bytes": 1,
                    "rx_packets": 1,
                    "tx_bytes": 1,
                    "tx_packets": 1,
                }

            with (
                mock.patch.object(
                    container_stats,
                    "compose_container_ids",
                    return_value={
                        "client": "client-id",
                        "server": "server-id",
                        "target": "target-id",
                    },
                ),
                mock.patch.object(container_stats, "docker_stats", return_value={}),
                mock.patch.object(
                    container_stats,
                    "read_netdev",
                    side_effect=fake_read_netdev,
                ),
            ):
                result = container_stats.sample(args)

            rows = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(result, 0)
        self.assertEqual(reads, ["client-id"])
        self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()

import argparse
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import container_stats


class ContainerStatsNetdevTests(unittest.TestCase):
    NETDEV = """\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 50 5 0 0 0 0 0 0 60 6 0 0 0 0 0 0
  eth0: 100 10 0 0 0 0 0 0 200 20 0 0 0 0 0 0
  eth1: 300 30 0 0 0 0 0 0 400 40 0 0 0 0 0 0
"""

    def test_boundary_snapshot_keeps_aggregate_and_interface_counters(self):
        stdout = (
            self.NETDEV
            + f"\n{container_stats.IPV4_MARKER}\n"
            + "2: eth0@if12 inet 172.31.10.10/24 brd 172.31.10.255 scope global eth0\n"
        )
        completed = subprocess.CompletedProcess(["docker"], 0, stdout=stdout, stderr="")

        with mock.patch.object(container_stats, "run", return_value=completed) as run:
            snapshot = container_stats.read_netdev_snapshot("container-id")

        self.assertEqual(snapshot["rx_bytes"], 400)
        self.assertEqual(snapshot["tx_packets"], 60)
        self.assertEqual(
            snapshot["interfaces"]["eth0"],
            {
                "ipv4": "172.31.10.10",
                "rx_bytes": 100,
                "rx_packets": 10,
                "tx_bytes": 200,
                "tx_packets": 20,
            },
        )
        self.assertIsNone(snapshot["interfaces"]["eth1"]["ipv4"])
        run.assert_called_once()
        self.assertIn("ip -o -4 addr show", run.call_args.args[0][-1])

    def test_snapshot_deltas_are_per_interface_and_nonnegative(self):
        services = {"client": {"samples": 2}}
        before = {
            "services": {
                "client": {
                    "rx_bytes": 1_000,
                    "rx_packets": 100,
                    "tx_bytes": 2_000,
                    "tx_packets": 200,
                    "interfaces": {
                        "eth0": {
                            "ipv4": "172.31.10.10",
                            "rx_bytes": 100,
                            "rx_packets": 10,
                            "tx_bytes": 200,
                            "tx_packets": 20,
                        },
                        "eth1": {
                            "ipv4": "172.31.20.10",
                            "rx_bytes": 300,
                            "rx_packets": 30,
                            "tx_bytes": 400,
                            "tx_packets": 40,
                        },
                    },
                }
            }
        }
        after = {
            "services": {
                "client": {
                    "rx_bytes": 900,
                    "rx_packets": 150,
                    "tx_bytes": 2_500,
                    "tx_packets": 190,
                    "interfaces": {
                        "eth0": {
                            "ipv4": "172.31.10.10",
                            "rx_bytes": 175,
                            "rx_packets": 9,
                            "tx_bytes": 260,
                            "tx_packets": 28,
                        },
                        "eth1": {
                            "ipv4": "172.31.20.10",
                            "rx_bytes": 350,
                            "rx_packets": 39,
                            "tx_bytes": 480,
                            "tx_packets": 50,
                        },
                        "eth2": {
                            "ipv4": "172.31.30.10",
                            "rx_bytes": 999,
                            "rx_packets": 99,
                            "tx_bytes": 999,
                            "tx_packets": 99,
                        },
                    },
                }
            }
        }

        container_stats.apply_snapshot_deltas(services, before, after)

        client = services["client"]
        self.assertEqual(client["delta_rx_bytes"], 0)
        self.assertEqual(client["delta_rx_packets"], 50)
        self.assertEqual(client["delta_tx_bytes"], 500)
        self.assertEqual(client["delta_tx_packets"], 0)
        self.assertEqual(
            client["interfaces"]["eth0"],
            {
                "ipv4": "172.31.10.10",
                "delta_rx_bytes": 75,
                "delta_rx_packets": 0,
                "delta_tx_bytes": 60,
                "delta_tx_packets": 8,
            },
        )
        self.assertEqual(client["interfaces"]["eth1"]["delta_tx_bytes"], 80)
        self.assertNotIn("eth2", client["interfaces"])
        self.assertEqual(
            client["netdev_interface_delta_source"],
            "case_before_after_snapshot",
        )

    def test_legacy_boundary_snapshots_keep_aggregate_contract(self):
        services = {}
        before = {
            "services": {
                "server": {
                    "rx_bytes": 10,
                    "rx_packets": 1,
                    "tx_bytes": 20,
                    "tx_packets": 2,
                }
            }
        }
        after = {
            "services": {
                "server": {
                    "rx_bytes": 30,
                    "rx_packets": 3,
                    "tx_bytes": 50,
                    "tx_packets": 5,
                }
            }
        }

        container_stats.apply_snapshot_deltas(services, before, after)

        self.assertEqual(services["server"]["delta_rx_bytes"], 20)
        self.assertNotIn("interfaces", services["server"])


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

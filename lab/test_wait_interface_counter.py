import tempfile
import unittest
from pathlib import Path

from wait_interface_counter import (
    interface_for_ipv4,
    read_counter,
    wait_for_counter_delta,
)


class WaitInterfaceCounterTests(unittest.TestCase):
    def test_interface_lookup_uses_exact_ipv4_owner(self):
        addresses = [
            {
                "ifname": "eth0",
                "addr_info": [{"family": "inet", "local": "172.31.10.20"}],
            },
            {
                "ifname": "eth3",
                "addr_info": [{"family": "inet", "local": "172.31.20.20"}],
            },
        ]
        self.assertEqual(interface_for_ipv4(addresses, "172.31.20.20"), "eth3")

    def test_wait_requires_both_minimum_time_and_counter_delta(self):
        now = [0.0]
        values = iter([100, 120, 180, 220])

        def clock():
            return now[0]

        def sleep(duration):
            now[0] += duration

        result = wait_for_counter_delta(
            lambda: next(values),
            required_delta=50,
            min_wait=0.2,
            timeout=1.0,
            interval=0.1,
            clock=clock,
            sleep=sleep,
        )
        self.assertEqual(result["status"], "triggered")
        self.assertEqual(result["delta"], 120)
        self.assertEqual(result["elapsed_s"], 0.2)

    def test_timeout_is_bounded_and_reports_observed_delta(self):
        now = [0.0]

        def sleep(duration):
            now[0] += duration

        result = wait_for_counter_delta(
            lambda: 100,
            required_delta=1,
            min_wait=0.0,
            timeout=0.3,
            interval=0.1,
            clock=lambda: now[0],
            sleep=sleep,
        )
        self.assertEqual(result["status"], "timeout")
        self.assertEqual(result["elapsed_s"], 0.3)

    def test_read_counter_uses_structured_sysfs_path(self):
        with tempfile.TemporaryDirectory() as directory:
            counter = Path(directory) / "eth3" / "statistics" / "tx_bytes"
            counter.parent.mkdir(parents=True)
            counter.write_text("12345\n", encoding="ascii")
            self.assertEqual(read_counter("eth3", "tx_bytes", Path(directory)), 12345)


if __name__ == "__main__":
    unittest.main()

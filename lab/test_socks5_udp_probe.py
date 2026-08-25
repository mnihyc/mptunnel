import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from socks5_udp_probe import attempt_has_response_budget


class Socks5UdpProbeTests(unittest.TestCase):
    def test_attempt_requires_full_response_budget_before_load_deadline(self):
        self.assertTrue(attempt_has_response_budget(7.0, 10.0, 2.5))
        self.assertTrue(attempt_has_response_budget(7.5, 10.0, 2.5))
        self.assertFalse(attempt_has_response_budget(7.6, 10.0, 2.5))


if __name__ == "__main__":
    unittest.main()

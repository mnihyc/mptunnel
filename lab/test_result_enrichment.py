import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from result_enrichment import application_payload_bytes


class ResultEnrichmentTests(unittest.TestCase):
    def test_mixed_payload_uses_explicit_all_lane_app_bytes(self):
        row = {
            "protocol": "mixed",
            "bulk_bytes": 1000,
            "mixed_app_payload_bytes": 1420,
        }

        self.assertEqual(
            application_payload_bytes(row),
            (1420, "mixed_app_payload_bytes"),
        )


if __name__ == "__main__":
    unittest.main()

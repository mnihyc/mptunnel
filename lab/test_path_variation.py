import json
import tempfile
import unittest
from pathlib import Path

from path_variation import PATHS, QUALITY_PROFILES, RATE_BANDS, profiles, trace_metadata


class PathVariationTests(unittest.TestCase):
    def test_rate_bands_use_the_same_normalized_schedule(self):
        seed = "deterministic-scale"
        self.assertEqual(
            RATE_BANDS,
            {
                "access": (30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
                "gigabit": (300, 350, 400, 450, 500, 600, 700, 800, 900, 1000),
                "multi-gigabit": (
                    3000,
                    3500,
                    4000,
                    4500,
                    5000,
                    6000,
                    7000,
                    8000,
                    9000,
                    10000,
                ),
            },
        )
        self.assertEqual(sum(transport == "tcp" for transport, _ in PATHS), 10)
        self.assertEqual(sum(transport == "udp" for transport, _ in PATHS), 10)

        for epoch in range(4):
            for direction in ("client", "server"):
                by_band = {
                    rate_band: profiles(seed, epoch, direction, rate_band)
                    for rate_band in RATE_BANDS
                }
                for rate_band, rows in by_band.items():
                    rates = RATE_BANDS[rate_band]
                    for transport in ("tcp", "udp"):
                        transport_rows = [
                            row for row in rows if row["transport"] == transport
                        ]
                        self.assertEqual(
                            sorted(row["rate_mbps"] for row in transport_rows),
                            sorted(rates),
                        )
                        self.assertEqual(
                            sorted(
                                (
                                    row["delay_ms"],
                                    row["jitter_ms"],
                                    row["loss_percent"],
                                )
                                for row in transport_rows
                            ),
                            sorted(QUALITY_PROFILES),
                        )

                access_by_prefix = {
                    row["subnet_prefix"]: row for row in by_band["access"]
                }
                for rate_band, multiplier in (("gigabit", 10), ("multi-gigabit", 100)):
                    for row in by_band[rate_band]:
                        access = access_by_prefix[row["subnet_prefix"]]
                        self.assertEqual(row["rate_mbps"], access["rate_mbps"] * multiplier)
                        self.assertEqual(
                            (
                                row["delay_ms"],
                                row["jitter_ms"],
                                row["loss_percent"],
                            ),
                            (
                                access["delay_ms"],
                                access["jitter_ms"],
                                access["loss_percent"],
                            ),
                        )

    def test_seeded_epochs_change_the_highest_rate_path_for_each_transport(self):
        seed = "deterministic-scale"
        for rate_band in RATE_BANDS:
            for direction in ("client", "server"):
                for transport in ("tcp", "udp"):
                    highest_rate = [
                        max(
                            (
                                row
                                for row in profiles(seed, epoch, direction, rate_band)
                                if row["transport"] == transport
                            ),
                            key=lambda row: row["rate_mbps"],
                        )["subnet_prefix"]
                        for epoch in range(5)
                    ]
                    self.assertGreater(len(set(highest_rate)), 1)

    def test_trace_metadata_rejects_drift_and_accepts_exact_applied_epochs(self):
        seed = "deterministic-scale"
        rate_band = "gigabit"
        rows = []
        for epoch in range(3):
            origin = epoch * 6000
            rows.append(
                {
                    "epoch": epoch,
                    "event_start_offset_ms": origin,
                    "client_apply_start_offset_ms": origin,
                    "client_apply_end_offset_ms": origin + 10,
                    "client_exit_code": 0,
                    "server_apply_start_offset_ms": origin + 1,
                    "server_apply_end_offset_ms": origin + 9,
                    "server_exit_code": 0,
                    "preconditioned": epoch == 0,
                    "client_profiles": profiles(seed, epoch, "client", rate_band),
                    "server_profiles": profiles(seed, epoch, "server", rate_band),
                }
            )
        with tempfile.TemporaryDirectory() as directory:
            trace = Path(directory) / "trace.jsonl"
            trace.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )
            accepted = trace_metadata(trace, seed, rate_band, expected_epochs=3)
            self.assertTrue(accepted["trace_complete"])
            self.assertEqual(accepted["rate_band"], rate_band)
            self.assertEqual(accepted["minimum_link_rate_mbps"], 300)
            self.assertEqual(accepted["maximum_link_rate_mbps"], 1000)

            rows[1]["server_profiles"][0]["rate_mbps"] = 999
            trace.write_text(
                "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
                encoding="utf-8",
            )
            rejected = trace_metadata(trace, seed, rate_band, expected_epochs=3)
            self.assertFalse(rejected["trace_complete"])
            self.assertIn("gigabit rate band", rejected["trace_error"])


if __name__ == "__main__":
    unittest.main()

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from flapping_schedule import (
    attach_metadata_to_result,
    build_metadata,
    generate_schedule,
    normalize_bounds,
    normalize_modes,
    schedule_digest,
)


class FlappingScheduleTests(unittest.TestCase):
    def test_same_seed_replays_exact_schedule(self):
        modes = normalize_modes("apply-lowlat,spike-fat,blackhole-poor")
        first = list(generate_schedule("repeatable-42", modes, 1, 4, 12))
        second = list(generate_schedule("repeatable-42", modes, 1, 4, 12))

        self.assertEqual(first, second)
        self.assertEqual(schedule_digest(first), schedule_digest(second))
        self.assertEqual(
            schedule_digest(first),
            "ce57ae43c77740425466f86140445faf3308a8382079c1ca837b54cac5cbbea8",
        )

    def test_schedule_stays_within_configured_modes_and_bounds(self):
        modes = normalize_modes("apply-balanced,blackhole-lowlat")
        events = list(generate_schedule("bounds", modes, 2, 5, 100))

        self.assertEqual({event["mode"] for event in events}, set(modes))
        self.assertTrue(
            all(2 <= int(event["hold_seconds"]) <= 5 for event in events)
        )
        self.assertEqual(normalize_bounds(0, 0), (1, 1))

    def test_different_seeds_have_different_schedule_identity(self):
        modes = normalize_modes("apply-lowlat,blackhole-fat")
        first = list(generate_schedule("seed-a", modes, 1, 4, 20))
        second = list(generate_schedule("seed-b", modes, 1, 4, 20))

        self.assertNotEqual(schedule_digest(first), schedule_digest(second))

    def test_metadata_embeds_applied_trace_and_stable_schedule_identity(self):
        modes = "apply-lowlat,blackhole-fat"
        schedule = list(
            generate_schedule("metadata-seed", normalize_modes(modes), 1, 1, 8)
        )
        applied = [
            {
                **event,
                "event_start_offset_ms": 10_000 + index * 1_100,
                "client_apply_start_offset_ms": 10_001 + index * 1_100,
                "client_apply_end_offset_ms": 10_006 + index * 1_100,
                "client_command_exit_code": 0,
                "server_apply_start_offset_ms": 10_007 + index * 1_100,
                "server_apply_end_offset_ms": 10_012 + index * 1_100,
                "server_command_exit_code": 0,
            }
            for index, event in enumerate(schedule[:3])
        ]

        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            trace_path.write_text(
                "".join(json.dumps(event) + "\n" for event in applied),
                encoding="utf-8",
            )

            metadata = build_metadata(
                seed="metadata-seed",
                seed_source="configured",
                raw_modes=modes,
                min_seconds=1,
                max_seconds=1,
                trace_path=trace_path,
                initial_stable_seconds=10,
                probe_started_unix_seconds="1783630000.125000000",
                schedule_origin_unix_ms="1783630000125",
                schedule_origin_monotonic_ms="123456789",
                stop_requested_offset_ms=13_250,
                worker_exit_code=0,
                restore_exit_code=0,
            )

        self.assertEqual(metadata["seed"], "metadata-seed")
        self.assertEqual(metadata["applied_event_count"], 3)
        self.assertNotIn("events", metadata)
        self.assertTrue(metadata["applied_schedule_matches_plan"])
        self.assertEqual(metadata["command_failure_count"], 0)
        self.assertEqual(metadata["stop_requested_offset_ms"], 13_250)
        self.assertEqual(metadata["schedule_origin_monotonic_ms"], "123456789")
        self.assertEqual(metadata["initial_stable_seconds"], 10)
        self.assertEqual(metadata["first_event_start_offset_ms"], 10_000)
        self.assertTrue(metadata["initial_stable_timing_valid"])
        self.assertTrue(metadata["completed_dwell_timing_valid"])
        self.assertTrue(metadata["trace_complete"])
        self.assertEqual(
            metadata["applied_schedule_sha256"], schedule_digest(applied)
        )

    def test_truncated_trace_is_retained_but_marked_incomplete(self):
        modes = "apply-lowlat,blackhole-fat"
        event = next(generate_schedule("truncated", normalize_modes(modes), 1, 3, 1))
        event.update(
            {
                "event_start_offset_ms": 1,
                "client_apply_start_offset_ms": 2,
                "client_apply_end_offset_ms": 3,
                "client_command_exit_code": 0,
                "server_apply_start_offset_ms": 4,
                "server_apply_end_offset_ms": 5,
                "server_command_exit_code": 0,
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            trace_path.write_text(
                json.dumps(event) + "\n" + '{"index":1,"mode":',
                encoding="utf-8",
            )
            metadata = build_metadata(
                seed="truncated",
                seed_source="configured",
                raw_modes=modes,
                min_seconds=1,
                max_seconds=3,
                trace_path=trace_path,
                worker_exit_code=0,
                restore_exit_code=0,
            )

        self.assertEqual(metadata["applied_event_count"], 1)
        self.assertFalse(metadata["trace_parse_complete"])
        self.assertFalse(metadata["trace_complete"])
        self.assertIn("invalid trace row 2", metadata["trace_error"])

        row = {"status": "ok", "failure_reason": "probe failed first"}
        attach_metadata_to_result(row, metadata)
        self.assertEqual(row["probe_status_before_flapping_validation"], "ok")
        self.assertEqual(row["status"], "fail")
        self.assertEqual(row["failure_reason"], "probe failed first")
        self.assertIn("incomplete", row["flapping_failure_reason"])

    def test_command_failure_invalidates_flapping_result(self):
        modes = "apply-lowlat,blackhole-fat"
        event = next(generate_schedule("command-failure", normalize_modes(modes), 1, 3, 1))
        event.update(
            {
                "event_start_offset_ms": 1,
                "client_apply_start_offset_ms": 2,
                "client_apply_end_offset_ms": 3,
                "client_command_exit_code": 17,
                "server_apply_start_offset_ms": 4,
                "server_apply_end_offset_ms": 5,
                "server_command_exit_code": 0,
            }
        )
        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            trace_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            metadata = build_metadata(
                seed="command-failure",
                seed_source="configured",
                raw_modes=modes,
                min_seconds=1,
                max_seconds=3,
                trace_path=trace_path,
                worker_exit_code=0,
                restore_exit_code=0,
            )

        self.assertEqual(metadata["command_failure_count"], 1)
        self.assertFalse(metadata["trace_complete"])
        row = {"status": "ok"}
        attach_metadata_to_result(row, metadata)
        self.assertEqual(row["status"], "fail")

    def test_long_trace_keeps_result_metadata_bounded(self):
        modes = "apply-lowlat,blackhole-fat"
        events = []
        for event in generate_schedule(
            "long-trace", normalize_modes(modes), 1, 1, 500
        ):
            offset_ms = int(event["planned_offset_seconds"]) * 1000
            events.append(
                {
                    **event,
                    "event_start_offset_ms": offset_ms,
                    "client_apply_start_offset_ms": offset_ms,
                    "client_apply_end_offset_ms": offset_ms + 10,
                    "client_command_exit_code": 0,
                    "server_apply_start_offset_ms": offset_ms + 10,
                    "server_apply_end_offset_ms": offset_ms + 20,
                    "server_command_exit_code": 0,
                }
            )
        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            trace_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            metadata = build_metadata(
                seed="long-trace",
                seed_source="configured",
                raw_modes=modes,
                min_seconds=1,
                max_seconds=1,
                trace_path=trace_path,
                worker_exit_code=0,
                restore_exit_code=0,
            )

        self.assertEqual(metadata["applied_event_count"], 500)
        self.assertNotIn("events", metadata)
        self.assertLess(len(json.dumps(metadata)), 4096)
        self.assertTrue(metadata["trace_complete"])

    def test_compressed_completed_dwell_invalidates_trace(self):
        modes = "apply-lowlat,blackhole-fat"
        events = []
        for index, event in enumerate(
            generate_schedule("compressed", normalize_modes(modes), 1, 1, 2)
        ):
            offset_ms = index * 500
            events.append(
                {
                    **event,
                    "event_start_offset_ms": offset_ms,
                    "client_apply_start_offset_ms": offset_ms,
                    "client_apply_end_offset_ms": offset_ms + 10,
                    "client_command_exit_code": 0,
                    "server_apply_start_offset_ms": offset_ms + 10,
                    "server_apply_end_offset_ms": offset_ms + 20,
                    "server_command_exit_code": 0,
                }
            )
        with tempfile.TemporaryDirectory() as tmp:
            trace_path = Path(tmp) / "trace.jsonl"
            trace_path.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            metadata = build_metadata(
                seed="compressed",
                seed_source="configured",
                raw_modes=modes,
                min_seconds=1,
                max_seconds=1,
                trace_path=trace_path,
                worker_exit_code=0,
                restore_exit_code=0,
            )

        self.assertFalse(metadata["completed_dwell_timing_valid"])
        self.assertFalse(metadata["trace_complete"])

    def test_invalid_or_empty_mode_names_are_rejected(self):
        for modes in ("", "apply-lowlat,", "apply lowlat", "not-a-netem-mode"):
            with self.subTest(modes=modes), self.assertRaises(ValueError):
                normalize_modes(modes)


if __name__ == "__main__":
    unittest.main()

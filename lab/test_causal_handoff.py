#!/usr/bin/env python3

import unittest

from causal_handoff import (
    parse_diagnostic_line,
    verify_exact_handoff,
    verify_negative_control,
)


def event(name, **fields):
    return {"event": name, **{key: str(value) for key, value in fields.items()}}


def exact_chain():
    common_path = {
        "session_id": "7",
        "path_id": "1",
        "path_instance_id": "91",
        "calibration_id": "44",
    }
    proof_evidence = {
        "train_bytes": "100",
        "sample_floor_bytes": "80",
        "warmup_bytes": "20",
        "required_proof_bytes": "60",
        "written_data_frame_count": "2",
        "received_bytes": "100",
        "rate_bps": "9000",
    }
    identity = {
        "session_id": "7",
        "binding_instance_id": "3",
        "handoff_mode": "PerformanceOverride",
        "capacity_proof_authority": "exact_receipt",
        "capacity_proof_token": "44",
        "from_underlay": "Tcp",
        "from_path_id": "0",
        "from_path_instance_id": "70",
        "from_incarnation": "2",
        "to_underlay": "Udp",
        "to_path_id": "1",
        "to_path_instance_id": "91",
        "to_incarnation": "5",
    }
    return [
        event(
            "response_quic_capacity_calibration",
            phase="started",
            binding_instance_id="3",
            train_bytes="100",
            sample_floor_bytes="80",
            accounting_slack_bytes="5",
            carrier_window_bytes="20",
            fresh_strict_window_bytes="60",
            proof_validity_ms="1000",
            seq="1",
            **common_path,
        ),
        event(
            "quic_capacity_receipt",
            role="server",
            phase="confirmed",
            received_payload_bytes="100",
            seq="2",
            **common_path,
        ),
        event(
            "response_quic_capacity_calibration",
            phase="completed",
            reason="exact_carrier_proof",
            binding_instance_id="3",
            accounting_slack_bytes="5",
            proof_validity_ms="1000",
            written_bytes="100",
            receipt_confirmed="true",
            seq="3",
            **common_path,
            **proof_evidence,
        ),
        event(
            "quic_capacity_proof",
            phase="accepted",
            seq="4",
            **common_path,
            **proof_evidence,
        ),
        event(
            "quic_capacity_probe_retired",
            proof_accepted="true",
            carrier_retired="true",
            seq="5",
            **common_path,
        ),
        event("response_service_handoff", phase="drain_started", seq="6", **identity),
        event(
            "server_bulk_output_selected",
            reason="service_handoff",
            session_id="7",
            binding_instance_id="3",
            path_underlay="Udp",
            path_id="1",
            role="Service",
            work="OwnerData",
            seq="7",
        ),
        event("response_service_handoff", phase="committed", seq="8", **identity),
    ]


class CausalHandoffTest(unittest.TestCase):
    def test_parse_diagnostic_line(self):
        parsed = parse_diagnostic_line(
            "prefix mptunnel_lab_diag seq=9 event=sample phase=accepted"
        )
        self.assertEqual(
            parsed, {"seq": "9", "event": "sample", "phase": "accepted"}
        )

    def test_exact_receipt_chain_passes(self):
        result = verify_exact_handoff(exact_chain())
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["chain"]["calibration_id"], "44")

    def test_session_path_proof_can_move_another_binding(self):
        events = exact_chain()
        events[0]["binding_instance_id"] = "4"
        events[2]["binding_instance_id"] = "4"
        result = verify_exact_handoff(events)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["chain"]["calibration_binding_instance_id"], "4")

    def test_proof_observer_can_log_after_drain_publication(self):
        events = exact_chain()
        drain = events.pop(5)
        events.insert(3, drain)
        self.assertEqual(verify_exact_handoff(events)["status"], "ok")

    def test_wrong_receipt_token_breaks_chain(self):
        events = exact_chain()
        events[1]["calibration_id"] = "45"
        self.assertEqual(verify_exact_handoff(events)["status"], "fail")

    def test_source_owner_selection_during_drain_breaks_chain(self):
        events = exact_chain()
        events.insert(
            -1,
            event(
                "server_bulk_output_selected",
                reason="service_first",
                session_id="7",
                binding_instance_id="3",
                path_underlay="Tcp",
                path_id="0",
                role="Service",
                work="OwnerData",
            ),
        )
        self.assertEqual(verify_exact_handoff(events)["status"], "fail")

    def test_exact_cohort_rejects_a_new_udp_binding(self):
        initial = [
            event(
                "server_bulk_output_selected",
                session_id="7",
                binding_instance_id=binding,
                path_underlay="Tcp",
                role="Service",
                work="OwnerData",
            )
            for binding in ("3", "4")
        ]
        events = initial + exact_chain()
        self.assertEqual(
            verify_exact_handoff(events, expected_product_bindings=2)["status"],
            "ok",
        )
        events.append(
            event(
                "server_bulk_output_selected",
                session_id="7",
                binding_instance_id="5",
                path_underlay="Udp",
                role="Service",
                work="OwnerData",
            )
        )
        result = verify_exact_handoff(events, expected_product_bindings=2)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(
            result["reason"], "product binding cohort changed during causal row"
        )

    def test_negative_control_requires_exercised_rejection(self):
        events = [
            event(
                "server_bulk_output_selected",
                path_underlay="Tcp",
                role="Service",
                work="OwnerData",
            ),
            event(
                "response_service_handoff",
                phase="evaluation",
                service_underlay="Tcp",
                target_underlay="Udp",
                first_failed_gate="family_or_gain",
            ),
        ]
        self.assertEqual(verify_negative_control(events)["status"], "ok")
        events.append(
            event(
                "response_service_handoff",
                phase="committed",
                from_underlay="Tcp",
                to_underlay="Udp",
            )
        )
        self.assertEqual(verify_negative_control(events)["status"], "fail")


if __name__ == "__main__":
    unittest.main()

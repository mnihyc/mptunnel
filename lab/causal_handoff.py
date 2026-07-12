#!/usr/bin/env python3
"""Wait for and verify causal response Service handoffs in lab diagnostics."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Iterable


DIAGNOSTIC_PREFIX = "mptunnel_lab_diag "


def parse_diagnostic_line(line: str) -> dict[str, str] | None:
    marker = line.find(DIAGNOSTIC_PREFIX)
    if marker < 0:
        return None
    fields: dict[str, str] = {}
    for token in line[marker + len(DIAGNOSTIC_PREFIX) :].split():
        key, separator, value = token.partition("=")
        if separator and key:
            fields[key] = value
    return fields if "event" in fields else None


def load_diagnostics(path: Path, after_byte: int = 0) -> list[dict[str, str]]:
    with path.open("rb") as handle:
        size = handle.seek(0, 2)
        offset = after_byte if 0 <= after_byte <= size else 0
        handle.seek(offset)
        raw = handle.read().decode("utf-8", errors="replace")
    return [
        event
        for line in raw.splitlines()
        if (event := parse_diagnostic_line(line)) is not None
    ]


def fields_match(event: dict[str, str], required: dict[str, str]) -> bool:
    return all(event.get(key) == value for key, value in required.items())


def parse_fields(raw_fields: Iterable[str]) -> dict[str, str]:
    fields: dict[str, str] = {}
    for raw in raw_fields:
        key, separator, value = raw.partition("=")
        if not separator or not key:
            raise ValueError(f"field must use key=value syntax: {raw}")
        fields[key] = value
    return fields


def wait_for_events(args: argparse.Namespace) -> int:
    required = parse_fields(args.field)
    required["event"] = args.event
    deadline = time.monotonic() + args.timeout
    while True:
        events = [
            event
            for event in load_diagnostics(args.log, args.after_byte)
            if fields_match(event, required)
        ]
        if args.distinct_field:
            distinct = sorted(
                {
                    event[args.distinct_field]
                    for event in events
                    if args.distinct_field in event
                }
            )
            observed = len(distinct)
        else:
            distinct = []
            observed = len(events)
        if observed >= args.count:
            print(
                json.dumps(
                    {
                        "status": "ok",
                        "event": args.event,
                        "matched": len(events),
                        "distinct_values": distinct,
                    },
                    sort_keys=True,
                )
            )
            return 0
        if time.monotonic() >= deadline:
            print(
                json.dumps(
                    {
                        "status": "fail",
                        "reason": "timeout",
                        "event": args.event,
                        "matched": len(events),
                        "distinct_values": distinct,
                        "required": required,
                    },
                    sort_keys=True,
                )
            )
            return 1
        time.sleep(args.poll_interval)


def _matching_index(
    events: list[dict[str, str]],
    start: int,
    stop: int,
    required: dict[str, str],
) -> int | None:
    return next(
        (
            index
            for index in range(start, stop)
            if fields_match(events[index], required)
        ),
        None,
    )


def _exact_handoff_chain(
    events: list[dict[str, str]], drain_index: int
) -> dict[str, object] | None:
    drain = events[drain_index]
    token = drain.get("capacity_proof_token")
    session = drain.get("session_id")
    binding = drain.get("binding_instance_id")
    target_path = drain.get("to_path_id")
    target_instance = drain.get("to_path_instance_id")
    if not all((token, session, binding, target_path, target_instance)):
        return None

    calibration_required = {
        "event": "response_quic_capacity_calibration",
        "phase": "started",
        "session_id": session,
        "path_id": target_path,
        "path_instance_id": target_instance,
        "calibration_id": token,
    }
    calibration_index = _matching_index(events, 0, drain_index, calibration_required)
    if calibration_index is None:
        return None

    receipt_index = _matching_index(
        events,
        calibration_index + 1,
        drain_index,
        {
            "event": "quic_capacity_receipt",
            "role": "server",
            "phase": "confirmed",
            "session_id": session,
            "path_id": target_path,
            "path_instance_id": target_instance,
            "calibration_id": token,
        },
    )
    if receipt_index is None:
        return None
    calibration = events[calibration_index]
    completed_index = _matching_index(
        events,
        receipt_index + 1,
        drain_index,
        {
            "event": "response_quic_capacity_calibration",
            "phase": "completed",
            "reason": "exact_carrier_proof",
            "session_id": session,
            "binding_instance_id": calibration.get("binding_instance_id", ""),
            "path_id": target_path,
            "path_instance_id": target_instance,
            "calibration_id": token,
        },
    )
    if completed_index is None:
        return None
    completed = events[completed_index]
    receipt = events[receipt_index]
    equal_fields = (
        ("train_bytes", "train_bytes"),
        ("sample_floor_bytes", "sample_floor_bytes"),
        ("accounting_slack_bytes", "accounting_slack_bytes"),
        ("carrier_window_bytes", "warmup_bytes"),
        ("fresh_strict_window_bytes", "required_proof_bytes"),
        ("proof_validity_ms", "proof_validity_ms"),
    )
    if any(calibration.get(left) != completed.get(right) for left, right in equal_fields):
        return None
    train_bytes = completed.get("train_bytes")
    if (
        not train_bytes
        or train_bytes == "0"
        or receipt.get("received_payload_bytes") != train_bytes
        or completed.get("written_bytes") != train_bytes
        or completed.get("received_bytes") != train_bytes
        or completed.get("receipt_confirmed") != "true"
        or int(completed.get("written_data_frame_count", "0")) <= 0
        or int(completed.get("rate_bps", "0")) <= 0
    ):
        return None

    proof_index = _matching_index(
        events,
        completed_index + 1,
        len(events),
        {
            "event": "quic_capacity_proof",
            "phase": "accepted",
            "session_id": session,
            "path_id": target_path,
            "path_instance_id": target_instance,
            "calibration_id": token,
        },
    )
    if proof_index is None:
        return None
    retirement_index = _matching_index(
        events,
        proof_index + 1,
        len(events),
        {
            "event": "quic_capacity_probe_retired",
            "session_id": session,
            "path_id": target_path,
            "path_instance_id": target_instance,
            "calibration_id": token,
            "proof_accepted": "true",
            "carrier_retired": "true",
        },
    )
    if retirement_index is None:
        return None
    proof = events[proof_index]
    proof_equal_fields = (
        "train_bytes",
        "sample_floor_bytes",
        "warmup_bytes",
        "required_proof_bytes",
        "written_data_frame_count",
        "received_bytes",
        "rate_bps",
    )
    if any(proof.get(field) != completed.get(field) for field in proof_equal_fields):
        return None
    if any(
        event.get("calibration_id") == token
        and event.get("session_id") == session
        and (
            (event.get("event") == "quic_capacity_proof" and event.get("phase") == "rejected")
            or (
                event.get("event") == "response_quic_capacity_calibration"
                and event.get("phase") in {"cancelled", "retired"}
            )
        )
        for event in events
    ):
        return None

    identity_fields = (
        "session_id",
        "binding_instance_id",
        "handoff_mode",
        "capacity_proof_authority",
        "capacity_proof_token",
        "from_underlay",
        "from_path_id",
        "from_path_instance_id",
        "from_incarnation",
        "to_underlay",
        "to_path_id",
        "to_path_instance_id",
        "to_incarnation",
    )
    commit_required = {
        "event": "response_service_handoff",
        "phase": "committed",
        **{field: drain[field] for field in identity_fields if field in drain},
    }
    commit_index = _matching_index(events, drain_index + 1, len(events), commit_required)
    if commit_index is None:
        return None

    source_owner = {
        "event": "server_bulk_output_selected",
        "session_id": session,
        "binding_instance_id": binding,
        "path_underlay": drain["from_underlay"],
        "path_id": drain["from_path_id"],
        "role": "Service",
        "work": "OwnerData",
    }
    if _matching_index(events, drain_index + 1, commit_index, source_owner) is not None:
        return None

    selection_index = _matching_index(
        events,
        drain_index + 1,
        commit_index,
        {
            "event": "server_bulk_output_selected",
            "reason": "service_handoff",
            "session_id": session,
            "binding_instance_id": binding,
            "path_underlay": drain["to_underlay"],
            "path_id": target_path,
            "role": "Service",
            "work": "OwnerData",
        },
    )
    if selection_index is None:
        return None

    indexes = {
        "calibration": calibration_index,
        "receipt": receipt_index,
        "completion": completed_index,
        "proof": proof_index,
        "retirement": retirement_index,
        "drain": drain_index,
        "selection": selection_index,
        "commit": commit_index,
    }
    return {
        "session_id": session,
        "binding_instance_id": binding,
        "calibration_binding_instance_id": calibration.get("binding_instance_id"),
        "calibration_id": token,
        "handoff_mode": drain.get("handoff_mode"),
        "from": f"{drain.get('from_underlay')}:{drain.get('from_path_id')}",
        "to": f"{drain.get('to_underlay')}:{target_path}",
        "event_indexes": indexes,
        "event_sequences": {
            name: events[index].get("seq") for name, index in indexes.items()
        },
    }


def _product_binding_cohort(
    events: list[dict[str, str]],
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    tcp_bindings: set[tuple[str, str]] = set()
    all_bindings: set[tuple[str, str]] = set()
    for event in events:
        if not fields_match(
            event,
            {
                "event": "server_bulk_output_selected",
                "role": "Service",
                "work": "OwnerData",
            },
        ):
            continue
        identity = (event.get("session_id", ""), event.get("binding_instance_id", ""))
        if not all(identity):
            continue
        all_bindings.add(identity)
        if event.get("path_underlay") == "Tcp":
            tcp_bindings.add(identity)
    return tcp_bindings, all_bindings


def verify_exact_handoff(
    events: list[dict[str, str]], expected_product_bindings: int | None = None
) -> dict[str, object]:
    if expected_product_bindings is not None:
        tcp_bindings, all_bindings = _product_binding_cohort(events)
        if (
            len(tcp_bindings) != expected_product_bindings
            or all_bindings != tcp_bindings
        ):
            return {
                "status": "fail",
                "reason": "product binding cohort changed during causal row",
                "expected_tcp_product_binding_count": expected_product_bindings,
                "tcp_product_bindings": sorted(tcp_bindings),
                "all_product_bindings": sorted(all_bindings),
            }

    drains = [
        index
        for index, event in enumerate(events)
        if fields_match(
            event,
            {
                "event": "response_service_handoff",
                "phase": "drain_started",
                "capacity_proof_authority": "exact_receipt",
                "from_underlay": "Tcp",
                "to_underlay": "Udp",
            },
        )
    ]
    for drain_index in drains:
        if chain := _exact_handoff_chain(events, drain_index):
            return {"status": "ok", "chain": chain}
    return {
        "status": "fail",
        "reason": "no complete exact-receipt TCP-to-UDP handoff chain",
        "exact_receipt_drain_count": len(drains),
    }


def verify_negative_control(events: list[dict[str, str]]) -> dict[str, object]:
    tcp_owner = any(
        fields_match(
            event,
            {
                "event": "server_bulk_output_selected",
                "path_underlay": "Tcp",
                "role": "Service",
                "work": "OwnerData",
            },
        )
        for event in events
    )
    rejected_opportunity = any(
        event.get("event") == "response_service_handoff"
        and event.get("phase") == "evaluation"
        and event.get("service_underlay") == "Tcp"
        and event.get("target_underlay") == "Udp"
        and event.get("first_failed_gate") == "family_or_gain"
        for event in events
    )
    forbidden = [
        event
        for event in events
        if event.get("event") == "response_service_handoff"
        and event.get("phase") in {"drain_started", "committed"}
        and event.get("from_underlay") == "Tcp"
        and event.get("to_underlay") == "Udp"
    ]
    if tcp_owner and rejected_opportunity and not forbidden:
        return {
            "status": "ok",
            "tcp_owner_observed": True,
            "slower_udp_opportunity_rejected": True,
        }
    return {
        "status": "fail",
        "reason": "negative control was not both exercised and preserved",
        "tcp_owner_observed": tcp_owner,
        "slower_udp_opportunity_rejected": rejected_opportunity,
        "forbidden_transition_count": len(forbidden),
    }


def verify_log(args: argparse.Namespace) -> int:
    events = load_diagnostics(args.log, args.after_byte)
    result = (
        verify_exact_handoff(events, args.expected_product_bindings)
        if args.mode == "exact-receipt"
        else verify_negative_control(events)
    )
    result["diagnostic_event_count"] = len(events)
    print(json.dumps(result, sort_keys=True))
    return 0 if result["status"] == "ok" else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    wait_parser = subparsers.add_parser("wait")
    wait_parser.add_argument("--log", type=Path, required=True)
    wait_parser.add_argument("--event", required=True)
    wait_parser.add_argument("--field", action="append", default=[])
    wait_parser.add_argument("--distinct-field")
    wait_parser.add_argument("--count", type=int, default=1)
    wait_parser.add_argument("--after-byte", type=int, default=0)
    wait_parser.add_argument("--timeout", type=float, default=10.0)
    wait_parser.add_argument("--poll-interval", type=float, default=0.05)
    wait_parser.set_defaults(handler=wait_for_events)

    verify_parser = subparsers.add_parser("verify")
    verify_parser.add_argument("--log", type=Path, required=True)
    verify_parser.add_argument(
        "--mode", choices=("exact-receipt", "negative-control"), required=True
    )
    verify_parser.add_argument("--after-byte", type=int, default=0)
    verify_parser.add_argument("--expected-product-bindings", type=int)
    verify_parser.set_defaults(handler=verify_log)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if getattr(args, "count", 1) < 1:
        raise SystemExit("--count must be positive")
    if (
        getattr(args, "expected_product_bindings", None) is not None
        and args.expected_product_bindings < 1
    ):
        raise SystemExit("--expected-product-bindings must be positive")
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())

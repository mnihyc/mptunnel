#!/usr/bin/env python3
"""Wait for a byte counter delta on the interface owning an IPv4 address."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import time


ALLOWED_COUNTERS = {"rx_bytes", "tx_bytes"}


def interface_for_ipv4(addresses, address):
    for interface in addresses:
        for info in interface.get("addr_info", []):
            if info.get("family") == "inet" and info.get("local") == address:
                name = interface.get("ifname")
                if name:
                    return name
    raise ValueError(f"no interface owns IPv4 address {address}")


def load_interface_addresses():
    output = subprocess.check_output(
        ["ip", "-j", "-4", "address", "show"],
        text=True,
    )
    return json.loads(output)


def read_counter(interface, counter, sysfs_root=Path("/sys/class/net")):
    if counter not in ALLOWED_COUNTERS:
        raise ValueError(f"unsupported interface counter {counter!r}")
    return int((sysfs_root / interface / "statistics" / counter).read_text().strip())


def wait_for_counter_delta(
    read_value,
    required_delta,
    min_wait,
    timeout,
    interval,
    clock=time.monotonic,
    sleep=time.sleep,
):
    started = clock()
    baseline = read_value()
    while True:
        now = clock()
        current = read_value()
        elapsed = now - started
        delta = current - baseline
        if delta < 0:
            raise RuntimeError("interface counter moved backwards")
        if elapsed >= min_wait and delta >= required_delta:
            return {
                "status": "triggered",
                "baseline": baseline,
                "current": current,
                "delta": delta,
                "elapsed_s": round(elapsed, 6),
            }
        if elapsed >= timeout:
            return {
                "status": "timeout",
                "baseline": baseline,
                "current": current,
                "delta": delta,
                "elapsed_s": round(elapsed, 6),
            }
        sleep(min(interval, max(0.0, timeout - elapsed)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--address", required=True)
    parser.add_argument("--counter", choices=sorted(ALLOWED_COUNTERS), required=True)
    parser.add_argument("--delta-bytes", type=int, required=True)
    parser.add_argument("--min-wait", type=float, default=0.0)
    parser.add_argument("--timeout", type=float, required=True)
    parser.add_argument("--interval", type=float, default=0.02)
    args = parser.parse_args()
    if args.delta_bytes <= 0:
        parser.error("--delta-bytes must be positive")
    if args.min_wait < 0 or args.timeout <= 0 or args.min_wait >= args.timeout:
        parser.error("require 0 <= --min-wait < --timeout")
    if args.interval <= 0:
        parser.error("--interval must be positive")

    try:
        interface = interface_for_ipv4(load_interface_addresses(), args.address)
        result = wait_for_counter_delta(
            lambda: read_counter(interface, args.counter),
            args.delta_bytes,
            args.min_wait,
            args.timeout,
            args.interval,
        )
        result.update(
            {
                "address": args.address,
                "interface": interface,
                "counter": args.counter,
                "required_delta_bytes": args.delta_bytes,
                "min_wait_s": args.min_wait,
                "timeout_s": args.timeout,
            }
        )
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "triggered" else 124
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "address": args.address,
                    "counter": args.counter,
                    "error": str(exc),
                },
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())

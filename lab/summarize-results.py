#!/usr/bin/env python3
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def fmt_float(value, digits=3):
    if value is None:
        return "-"
    return f"{value:.{digits}f}"


def median(values):
    clean = [value for value in values if isinstance(value, (int, float))]
    if not clean:
        return None
    return float(statistics.median(clean))


def mean(values):
    clean = [value for value in values if isinstance(value, (int, float))]
    if not clean:
        return None
    return float(statistics.mean(clean))


def collect_files(paths):
    files = []
    for path_text in paths:
        path = Path(path_text)
        if path.is_dir():
            files.extend(sorted(path.glob("*.jsonl")))
        else:
            files.append(path)
    return files


def load_records(files):
    records = []
    for path in files:
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if "case" not in record:
                    continue
                record["_source"] = str(path)
                record["_line"] = line_number
                records.append(record)
    return records


def grouped(records):
    by_case = defaultdict(list)
    for record in records:
        by_case[record.get("case", "unknown")].append(record)
    return dict(sorted(by_case.items()))


def tcp_rows(by_case):
    rows = []
    for case, records in by_case.items():
        if not any(record.get("protocol") == "tcp" for record in records):
            continue
        ok = [record for record in records if record.get("status") == "ok"]
        rows.append(
            {
                "case": case,
                "runs": len(records),
                "ok": len(ok),
                "fail": len(records) - len(ok),
                "median_goodput": median([record.get("goodput_mbps") for record in ok]),
                "best_goodput": max(
                    [record.get("goodput_mbps") for record in ok if isinstance(record.get("goodput_mbps"), (int, float))],
                    default=None,
                ),
                "median_time": median([record.get("time_s") for record in ok]),
            }
        )
    return rows


def udp_rows(by_case):
    rows = []
    for case, records in by_case.items():
        if not any(record.get("protocol") == "udp" for record in records):
            continue
        rows.append(
            {
                "case": case,
                "runs": len(records),
                "ok": sum(1 for record in records if record.get("status") == "ok"),
                "loss": sum(1 for record in records if record.get("status") == "loss"),
                "fail": sum(1 for record in records if record.get("status") not in ("ok", "loss")),
                "received": median([record.get("received") for record in records]),
                "count": median([record.get("count") for record in records]),
                "avg_loss_rate": mean([record.get("loss_rate") for record in records]),
                "median_p50_ms": median([record.get("p50_ms") for record in records]),
                "median_p95_ms": median([record.get("p95_ms") for record in records]),
            }
        )
    return rows


def best_goodput(records, prefix):
    candidates = [
        record
        for record in records
        if record.get("protocol") == "tcp"
        and record.get("status") == "ok"
        and str(record.get("case", "")).startswith(prefix)
        and isinstance(record.get("goodput_mbps"), (int, float))
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda record: record["goodput_mbps"])


def by_source(records):
    buckets = defaultdict(list)
    for record in records:
        buckets[record["_source"]].append(record)
    return dict(sorted(buckets.items()))


def source_comparisons(records):
    rows = []
    for source, source_records in by_source(records).items():
        best_raw = best_goodput(source_records, "direct_")
        best_single = best_goodput(source_records, "mptunnel_tcp_single_")
        multipath = next(
            (
                record
                for record in source_records
                if record.get("case") == "mptunnel_tcp_multipath_all"
                and record.get("status") == "ok"
                and isinstance(record.get("goodput_mbps"), (int, float))
            ),
            None,
        )
        failover = next(
            (
                record
                for record in source_records
                if record.get("case") == "mptunnel_tcp_multipath_failover_blackhole_fat"
            ),
            None,
        )
        udp_multi = next(
            (
                record
                for record in source_records
                if record.get("case") == "mptunnel_udp_multipath_all"
            ),
            None,
        )
        udp_low = next(
            (
                record
                for record in source_records
                if record.get("case") == "mptunnel_udp_single_low_latency"
            ),
            None,
        )
        rows.append(
            {
                "source": Path(source).name,
                "best_raw_case": best_raw.get("case") if best_raw else None,
                "best_raw_goodput": best_raw.get("goodput_mbps") if best_raw else None,
                "best_single_case": best_single.get("case") if best_single else None,
                "best_single_goodput": best_single.get("goodput_mbps") if best_single else None,
                "multipath_goodput": multipath.get("goodput_mbps") if multipath else None,
                "multipath_vs_raw": (
                    multipath["goodput_mbps"] / best_raw["goodput_mbps"]
                    if multipath and best_raw and best_raw.get("goodput_mbps")
                    else None
                ),
                "multipath_vs_single": (
                    multipath["goodput_mbps"] / best_single["goodput_mbps"]
                    if multipath and best_single and best_single.get("goodput_mbps")
                    else None
                ),
                "failover_status": failover.get("status") if failover else None,
                "failover_time": failover.get("time_s") if failover else None,
                "udp_multi_loss": udp_multi.get("loss_rate") if udp_multi else None,
                "udp_multi_p95": udp_multi.get("p95_ms") if udp_multi else None,
                "udp_low_p95": udp_low.get("p95_ms") if udp_low else None,
            }
        )
    return rows


def render_markdown(records):
    by_case = grouped(records)
    lines = [
        "# mptunnel Lab Summary",
        "",
        f"records: {len(records)}",
        f"sources: {len(by_source(records))}",
        "",
        "## TCP Downloads",
        "",
        "| case | runs | ok | fail | median Mbps | best Mbps | median seconds |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in tcp_rows(by_case):
        lines.append(
            "| {case} | {runs} | {ok} | {fail} | {median_goodput} | {best_goodput} | {median_time} |".format(
                case=row["case"],
                runs=row["runs"],
                ok=row["ok"],
                fail=row["fail"],
                median_goodput=fmt_float(row["median_goodput"]),
                best_goodput=fmt_float(row["best_goodput"]),
                median_time=fmt_float(row["median_time"]),
            )
        )

    lines.extend(
        [
            "",
            "## UDP Probes",
            "",
            "| case | runs | ok | loss | fail | median received | median count | avg loss | median p50 ms | median p95 ms |",
            "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in udp_rows(by_case):
        lines.append(
            "| {case} | {runs} | {ok} | {loss} | {fail} | {received} | {count} | {avg_loss_rate} | {median_p50_ms} | {median_p95_ms} |".format(
                case=row["case"],
                runs=row["runs"],
                ok=row["ok"],
                loss=row["loss"],
                fail=row["fail"],
                received=fmt_float(row["received"], 1),
                count=fmt_float(row["count"], 1),
                avg_loss_rate=fmt_float(row["avg_loss_rate"], 3),
                median_p50_ms=fmt_float(row["median_p50_ms"]),
                median_p95_ms=fmt_float(row["median_p95_ms"]),
            )
        )

    lines.extend(
        [
            "",
            "## Per-Run Comparisons",
            "",
            "| source | best raw | best single | multipath Mbps | mp/raw | mp/single | failover | failover seconds | UDP multi loss | UDP multi p95 ms | UDP low p95 ms |",
            "| --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
        ]
    )
    for row in source_comparisons(records):
        lines.append(
            "| {source} | {best_raw} | {best_single} | {multipath} | {mp_raw} | {mp_single} | {failover_status} | {failover_time} | {udp_loss} | {udp_multi_p95} | {udp_low_p95} |".format(
                source=row["source"],
                best_raw=(
                    f"{row['best_raw_case']} {fmt_float(row['best_raw_goodput'])}"
                    if row["best_raw_case"]
                    else "-"
                ),
                best_single=(
                    f"{row['best_single_case']} {fmt_float(row['best_single_goodput'])}"
                    if row["best_single_case"]
                    else "-"
                ),
                multipath=fmt_float(row["multipath_goodput"]),
                mp_raw=fmt_float(row["multipath_vs_raw"], 2),
                mp_single=fmt_float(row["multipath_vs_single"], 2),
                failover_status=row["failover_status"] or "-",
                failover_time=fmt_float(row["failover_time"]),
                udp_loss=fmt_float(row["udp_multi_loss"], 3),
                udp_multi_p95=fmt_float(row["udp_multi_p95"]),
                udp_low_p95=fmt_float(row["udp_low_p95"]),
            )
        )
    lines.append("")
    return "\n".join(lines)


def render_json(records):
    by_case = grouped(records)
    payload = {
        "records": len(records),
        "sources": len(by_source(records)),
        "tcp": tcp_rows(by_case),
        "udp": udp_rows(by_case),
        "comparisons": source_comparisons(records),
    }
    return json.dumps(payload, indent=2, sort_keys=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("paths", nargs="+", help="JSONL files or directories")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    args = parser.parse_args()

    files = collect_files(args.paths)
    records = load_records(files)
    if args.format == "json":
        print(render_json(records))
    else:
        print(render_markdown(records))


if __name__ == "__main__":
    main()

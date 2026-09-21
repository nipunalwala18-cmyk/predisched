#!/usr/bin/env python3
"""Merge every node's event log and show physical order beside Lamport order.

This is the lab Exp 3 demo. Each node writes logs/<node>.jsonl through EventLog, with both its
physical time (its own skewed clock) and its Lamport time on every event. Printing the same events
ordered two ways shows where wall-clock order disagrees with causal order.

Usage:
    python scripts/merge-events.py [logs_dir] [--task TASK_ID] [--limit N]
"""
import argparse
import glob
import json
import os
import sys


def load(logs_dir):
    events = []
    for path in sorted(glob.glob(os.path.join(logs_dir, "*.jsonl"))):
        with open(path, encoding="utf8") as handle:
            for line_no, line in enumerate(handle, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"skipping malformed line {path}:{line_no}", file=sys.stderr)
    return events


def label(event):
    task = event.get("task_id") or ""
    short_task = task[-8:] if task else "-"
    extra = ""
    if event.get("type") == "EXECUTE_START":
        extra = " " + event.get("thread", "")
    elif event.get("type") == "RESULT":
        extra = " " + event.get("exec_ms", "") + "ms"
    elif event.get("type") == "CLOCK_SYNC":
        extra = " spread {0}->{1}ms".format(
            event.get("spread_before_ms", "?"), event.get("spread_after_ms", "?")
        )
    return f"{event.get('node','?'):<12} {event.get('type',''):<14} {short_task}{extra}"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs_dir", nargs="?", default="logs")
    parser.add_argument("--task", help="only events for this task id")
    parser.add_argument("--limit", type=int, default=40, help="rows to print (default 40)")
    args = parser.parse_args()

    events = load(args.logs_dir)
    if args.task:
        events = [e for e in events if e.get("task_id") == args.task]
    if not events:
        print(f"No events found in {args.logs_dir}")
        return 1

    by_physical = sorted(events, key=lambda e: (e.get("physical_ms", 0), e.get("node", "")))
    by_lamport = sorted(events, key=lambda e: (e.get("lamport", 0), e.get("node", "")))

    by_physical = by_physical[-args.limit:]
    by_lamport = by_lamport[-args.limit:]

    width = 56
    print(f"{len(events)} events from {args.logs_dir}\n")
    print(f"{'PHYSICAL TIME ORDER (each node its own clock)':<{width}} | LAMPORT ORDER")
    print("-" * width + "-+-" + "-" * width)
    for left, right in zip(by_physical, by_lamport):
        left_text = f"{left.get('physical_ms',0) % 100000:>6} {label(left)}"
        right_text = f"{right.get('lamport',0):>6} {label(right)}"
        print(f"{left_text:<{width}} | {right_text}")

    disagreements = sum(
        1 for a, b in zip(by_physical, by_lamport)
        if (a.get("node"), a.get("type"), a.get("task_id"))
        != (b.get("node"), b.get("type"), b.get("task_id"))
    )
    print()
    print(f"{disagreements} of {len(by_physical)} rows differ between the two orderings.")
    print("Physical order can misplace events because each node's clock carries its own offset;")
    print("Lamport order always respects causality: a message is received after it was sent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

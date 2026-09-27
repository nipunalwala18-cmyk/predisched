"""Dumps PrediSched's execution history and events from PostgreSQL for the Spark jobs (no Spark).

    python spark/export_history.py [--dsn "host=localhost dbname=predisched user=postgres password=predisched"]

Writes ``spark/data/execution_history.csv`` (every column, with a header) and
``spark/data/events.jsonl`` (one event per line; ``details`` as a JSON object).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import psycopg

DEFAULT_DSN = "host=localhost port=5432 dbname=predisched user=postgres password=predisched"


def export(dsn: str, out_dir: Path) -> tuple[int, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    history = out_dir / "execution_history.csv"
    events = out_dir / "events.jsonl"
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        with history.open("wb") as out:
            with cur.copy("COPY (SELECT * FROM execution_history ORDER BY dispatched_at, id)"
                          " TO STDOUT WITH (FORMAT csv, HEADER true)") as copy:
                for block in copy:
                    out.write(block)
        cur.execute("SELECT count(*) FROM execution_history")
        history_rows = cur.fetchone()[0]
        cur.execute("SELECT node_id, lamport_time, ts, event_type, task_id, trace_id, details"
                    " FROM events ORDER BY ts, id")
        event_rows = 0
        with events.open("w", encoding="utf-8") as out:
            for node, lamport, ts, event_type, task_id, trace_id, details in cur:
                out.write(json.dumps({
                    "node": node, "lamport": lamport, "ts": ts.isoformat(),
                    "event_type": event_type, "task_id": task_id, "trace_id": trace_id,
                    "details": details or {},
                }) + "\n")
                event_rows += 1
    return history_rows, event_rows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", default=os.environ.get("PREDISCHED_DSN", DEFAULT_DSN))
    parser.add_argument("--output", default=str(Path(__file__).resolve().parent / "data"))
    args = parser.parse_args(argv)
    history_rows, event_rows = export(args.dsn, Path(args.output))
    print(f"exported {history_rows} execution_history rows and {event_rows} events"
          f" to {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

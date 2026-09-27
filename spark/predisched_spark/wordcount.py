"""The Exp 7 warm-up: word count over the event log (event types and detail values).

    flatMap  each JSON event -> its words (the event type, then every word of every detail value)
    map      word -> (word, 1)
    reduce   reduceByKey adds the ones

    spark-submit --master local[*] spark/predisched_spark/wordcount.py \
        --input spark/data/events.jsonl --output spark/out
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from predisched_spark.common import parse_args, print_table, session, stop, write_csv  # noqa: E402

WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")


def words(line: str) -> list[str]:
    """flatMap: the words of one event."""
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return []
    found = [event.get("event_type") or event.get("type") or ""]
    details = event.get("details") or {}
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except json.JSONDecodeError:
            details = {"raw": details}
    for value in details.values():
        found.extend(WORD.findall(str(value)))
    return [w for w in found if w]


def compute(sc, input_path: str) -> list[tuple[str, int]]:
    counts = (sc.textFile(input_path)
              .flatMap(words)
              .map(lambda word: (word, 1))
              .reduceByKey(lambda a, b: a + b)
              .collect())
    return sorted(counts, key=lambda pair: (-pair[1], pair[0]))


def main(argv=None) -> int:
    args = parse_args("Exp 7 warm-up: word count over events",
                      "spark/data/events.jsonl", "spark/out", argv)
    spark = session("predisched-wordcount", args.master)
    try:
        counts = compute(spark.sparkContext, args.input)
    finally:
        stop(spark, args)
    rows = write_csv(Path(args.output) / "wordcount.csv", ("word", "count"), counts)
    print_table("Top 15 words", ("word", "count"), counts[:15])
    print(f"ROWS wordcount={rows} output={args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

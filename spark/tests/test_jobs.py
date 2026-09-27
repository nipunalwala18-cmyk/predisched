"""Spec §17 Spark row: the MapReduce results match a pandas check of the same data."""

import json

import pandas as pd
import pytest

from predisched_spark import exec_stats, features, wordcount


def test_exec_stats_equals_a_pandas_groupby_mean(spark, history_csv):
    result = exec_stats.compute(spark.sparkContext, history_csv)
    frame = pd.read_csv(history_csv)
    frame = frame[frame["status"] == "COMPLETED"]
    for group, column in (("type", "task_type"), ("worker", "worker_id")):
        expected = frame.groupby(column)["exec_time_ms"].agg(["mean", "count"])
        got = {name: (count, mean) for name, count, mean, _ in result[group]}
        assert set(got) == set(expected.index)
        for name, row in expected.iterrows():
            assert got[name][0] == row["count"], (group, name)
            assert got[name][1] == pytest.approx(row["mean"]), (group, name)


def test_the_failed_row_is_left_out(spark, history_csv):
    result = exec_stats.compute(spark.sparkContext, history_csv)
    assert sum(count for _, count, _, _ in result["type"]) == 19


def test_features_have_the_expected_columns_and_no_nulls_for_workers_with_data(spark, history_csv):
    df = features.load(spark, history_csv)
    stats = features.type_worker_stats(df).toPandas()
    assert list(stats.columns) == ["task_type", "worker_id", "tasks", "exec_mean_ms",
                                   "exec_p50_ms", "exec_p95_ms", "exec_std_ms"]
    assert not stats.isnull().any().any()
    assert stats["tasks"].sum() == 19
    windows = features.worker_windows(df).toPandas()
    assert list(windows.columns) == ["worker_id", "window_start", "window_end", "arrival_rate",
                                     "queue_len_mean", "queue_len_max"]
    assert not windows.isnull().any().any()
    assert set(windows["worker_id"]) == {"worker-1", "worker-2", "worker-3"}
    # Every dispatch falls in two 10 s windows sliding by 5 s.
    assert windows["arrival_rate"].sum() * 10 == pytest.approx(2 * 19)


def test_wordcount_counts_event_types_and_detail_words(spark, tmp_path):
    events = tmp_path / "events.jsonl"
    events.write_text("\n".join(json.dumps(e) for e in [
        {"event_type": "DISPATCH", "details": {"worker": "worker-1"}},
        {"event_type": "DISPATCH", "details": {"worker": "worker-2"}},
        {"event_type": "RESULT", "details": {"worker": "worker-1", "success": "true"}},
    ]) + "\n", encoding="utf-8")
    counts = dict(wordcount.compute(spark.sparkContext, str(events)))
    assert counts["DISPATCH"] == 2
    assert counts["worker-1"] == 2
    assert counts["RESULT"] == 1

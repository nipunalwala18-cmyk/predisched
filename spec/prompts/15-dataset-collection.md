# Prompt 15: Collect the ML dataset

## Goal

A scripted campaign runs varied saved workloads through the non-predictive strategies on
heterogeneous workers and produces a clean, labelled dataset of at least 10,000 task executions with
every column in spec §12.1, including the future-looking targets.

## Read first

- Spec §12.1, §7.3, §7.4, §13 (controls), RULES rules 7 and 8

## Build

1. **Heterogeneous workers**: `configs/campaign.yaml` defines worker profiles (pool size, and CPU /
   memory limits when run in Docker via `cpus:` / `mem_limit:`). Locally, heterogeneity comes from
   pool sizes and a `worker.slowdown` factor that scales executor work (documented as simulated).
2. **Campaign runner** `scripts/collect-dataset.py`:
   - For each combination of profile (`cpu_heavy`, `io_heavy`, `mixed`, `bursty`, `long_tail`,
     `deadline`), pattern (steady, bursty, periodic) and strategy (the four from prompt 08), generate
     one trace with a fixed seed (saved under `workloads/campaign/`), replay it, and tag the rows with
     a `run_id`.
   - Resumable: skips runs already present in the database.
   - Prints progress and a final row count.
3. **Label builder** `ml/build_dataset.py`:
   - Reads `execution_history` and `worker_metrics` (via the prompt 12 export or directly).
   - Computes `queue_len_future` (worker queue length N seconds after dispatch, N from config,
     default 5) and `overloaded_future` (CPU > 85 % or queue > threshold within N seconds) from the
     metrics time series.
   - Adds `resource_profile`, joins the Spark features from `ml/data/features/`.
   - Drops rows with missing labels, logs how many and why.
   - Writes `ml/data/dataset.parquet` plus `ml/data/dataset-card.md`: row count, date, runs, class
     balance of `overloaded_future`, exec-time distribution per task type.
4. **Hold-out**: mark one workload pattern (default `periodic`) and one task type (default
   `GRAPH_TASK`) as held out in the card, for generalisation and cold-start tests in prompt 16.

## Tests

- `pytest` in `ml/`: label computation on a hand-made metrics series (known future queue length and
  overload flag); no future leakage (every feature timestamp ≤ dispatch time).
- The campaign runner's plan (list of runs) is deterministic for a given config.

## Acceptance checks

```bash
python scripts/collect-dataset.py --config configs/campaign.yaml
```
```bash
python ml/build_dataset.py
```

Paste the dataset card. The row count must be at least 10,000; if it is not, extend the campaign
before committing.

## Docs and commit

- `docs/components/dataset.md`: collection design, label definitions, the dataset card.
- Commit: `Data: campaign runner and labelled execution dataset for the prediction models`

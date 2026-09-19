# Prompt 16: Train and evaluate the prediction models

## Goal

Three models are trained and evaluated on held-out data: M1 execution time (regression), M2 queue
forecast (regression) and M3 overload risk (classification). Each goes through the progression
baseline → linear → random forest → XGBoost, and the simplest model that clearly beats the baseline is
kept. Metrics, plots and versioned model files exist. `ML_INFER_TASK` works.

## Read first

- Spec §12.2, §12.1, §5 (ML quality), §7.4, §17 (ML row), §19

## Build

1. **`ml/`** (package `predisched_ml`, pinned `requirements.txt`: pandas, NumPy, scikit-learn,
   XGBoost, joblib, matplotlib, pyarrow):
   - `features.py`: one function building the feature matrix from a dataset frame, used by training
     and by the prediction server so the two can never drift. One-hot for task type and resource
     profile; numeric input size parsed per type; log-transform exec time for M1.
   - `train.py --model m1|m2|m3|all`:
     - time-based split (train on the earliest 80 % by dispatch time, test on the rest; no shuffle);
     - plus the held-out pattern and task type from the dataset card as separate test sets;
     - candidates in order: baseline (mean per task type, or majority class), linear / logistic
       regression, random forest, XGBoost; small grid search with time-series CV;
     - picks the simplest candidate whose primary metric beats the baseline by a margin from config
       (default 10 %), and records why.
   - `evaluate.py`: MAE, RMSE, R² for M1 and M2; precision, recall, F1, ROC-AUC for M3; per task type
     breakdown; plots (predicted vs actual, residuals, ROC, feature importance) in `ml/reports/`.
   - Models saved as `ml/models/<m>/v<N>/model.joblib` with `meta.json` (features, metrics, data
     hash, training date, seed). `ml/models/registry.json` lists versions and which is live.
2. **Cold start**: when a task type was not in training, M1 falls back to the type's running mean
   from the scheduler (or a global mean) and flags `cold_start=true`. Evaluate on the held-out type.
3. **`ML_INFER_TASK`** executor: input `model=exec_time, batch=…` calls a small Python entry point
   (`python -m predisched_ml.infer`) on a batch drawn from the dataset and returns timing and a
   checksum. Resource profile `CPU_BOUND`.

## Tests

- `pytest`: `features.py` gives identical columns for training and single-row serving input; the
  time split has no overlap and preserves order; a trained model round-trips through joblib; the
  baseline is always evaluated.

## Acceptance checks

```bash
python -m predisched_ml.train --model all
```
```bash
python -m predisched_ml.evaluate --model all
```

Paste the metrics table for every candidate of every model on the time-split test set and on the
held-out pattern and type, and name the kept model with its reason. If no model beats its baseline,
say so plainly; do not tune on the test set to change that.

## Docs and commit

- `docs/components/ml-models.md`: features, split, candidates, the full metrics tables, plots.
- CI: add `ml/` pytest.
- Commit: `ML: execution-time, queue-forecast and overload models trained and evaluated`

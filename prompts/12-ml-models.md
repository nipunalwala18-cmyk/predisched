# Prompt 12 — Prediction models: training and evaluation

**Spec sections:** §5 (ML quality), §10.1, §10.2, §17 (Risks)
**Depends on:** 11
**Lab topics:** none directly (the product's research core)
**Commit message:** `ML: execution-time, queue-forecast and overload-risk models trained and evaluated against baselines`

---

## Prompt

Read `CLAUDE.md` and the spec sections above. Train the three models the predictive scheduler needs,
and evaluate them honestly. A model ships only if it beats its naive baseline on held-out data.

### Build (`ml/`)

- `ml/predisched_ml/features.py` — one function `make_matrix(df, model) -> (X, y)` used by **both**
  training and serving, so features can never drift between them. Categorical columns one-hot encoded
  with a fixed, saved category list. Unknown categories at serving time map to an "other" column.
- `ml/predisched_ml/split.py` — time-based split: train on the first 70% by dispatch time, validate on
  the next 15%, test on the last 15%. Plus a **held-out pattern** split: train without `periodic`,
  test only on `periodic` (spec §10.2).
- `ml/predisched_ml/train.py --model {exec_time,queue_forecast,overload}` runs the progression from
  §10.2 for that model:
  - baseline: historical mean per task type (regression) / majority class or "overloaded now" rule
    (classification);
  - Linear / Logistic Regression;
  - Random Forest;
  - XGBoost, with a small grid tuned on the validation split only.
  Seeds fixed. Class imbalance for `overload` handled with class weights and a threshold chosen on the
  validation split to maximise F1.
- Selection rule: the simplest model whose validation error is within 2% of the best **and** beats
  the baseline by a clear margin (≥ 10% lower MAE, or ≥ 0.05 higher F1). If nothing beats the
  baseline, ship the baseline and say so.
- Log-transform `exec_time_ms` for training if residuals are skewed; report metrics on the original
  scale.
- `ml/predisched_ml/evaluate.py` — on the test and held-out-pattern splits: MAE, RMSE, R² (regression);
  precision, recall, F1, ROC-AUC (classification); plus per-task-type MAE, predicted-vs-actual scatter,
  residual histogram, ROC curve, and feature importances. Writes `results/ml/<model>/metrics.json` and
  PNGs.
- `ml/models/<model>/v<N>/` — `model.joblib`, `features.json` (column order + categories),
  `metadata.json` (training data hash, row counts, metrics, chosen algorithm, created at). A
  `ml/models/<model>/CURRENT` file names the version to serve.
- `ml/notebooks/exploration.ipynb` — distributions and correlations; not imported by any code.

### Tests (`ml/tests`)

- `make_matrix` yields identical columns for a training frame and a single serving row.
- Time split has no overlap and preserves order.
- The selection rule picks the baseline when a model does not beat it (fixture with pure-noise features).
- A saved model loads and predicts the same values as before saving.

### Acceptance checks

```bash
python -m ml.predisched_ml.train --model exec_time
python -m ml.predisched_ml.train --model queue_forecast
python -m ml.predisched_ml.train --model overload
python -m ml.predisched_ml.evaluate --all
python -m pytest ml/tests
```

### Docs

`docs/components/ml-models.md`: one table per model comparing baseline → LR → RF → XGBoost on test and
held-out-pattern splits, what was chosen and why, and the limitations from spec §17 that the results
confirm or refute.

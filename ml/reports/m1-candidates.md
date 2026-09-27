# m1: exec_time (exec_time_ms), v1

Kept: **linear(alpha=0.1)**. linear(alpha=0.1) is the simplest candidate whose CV mae (26.0195) beats the best baseline (mean per task_type, 79.8627) by at least 10%; random forest scores better (17.9240) but is more complex, xgboost scores better (22.4533) but is more complex.

Split: 5,978 train / 1,494 test rows (time split), 3,578 held-out-pattern rows, 268 held-out-type rows.

## Cross-validation (training part only; drives the selection)

| family | candidate | CV mae |
| --- | --- | --- |
| baseline | mean per task_type | 79.8627 |
| linear | linear(alpha=0.1) | 26.0195 |
| linear | linear(alpha=1.0) | 26.9934 |
| linear | linear(alpha=10.0) | 32.6257 |
| random_forest | random forest(max_depth=8, min_samples_leaf=1, n_estimators=200) | 21.1729 |
| random_forest | random forest(max_depth=8, min_samples_leaf=5, n_estimators=200) | 22.9333 |
| random_forest | random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 17.9240 |
| random_forest | random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 20.3023 |
| xgboost | xgboost(learning_rate=0.05, max_depth=3, n_estimators=300) | 28.2609 |
| xgboost | xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 22.4533 |
| xgboost | xgboost(learning_rate=0.1, max_depth=3, n_estimators=300) | 27.3102 |
| xgboost | xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 23.8441 |

## Time-split test set

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| mean per task_type | 92.736 | 197.654 | 0.358 |
| **linear(alpha=0.1)** (kept) | 28.016 | 96.637 | 0.847 |
| random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 15.691 | 57.151 | 0.946 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 20.439 | 61.116 | 0.939 |
| linear(alpha=0.1) + cold-start fallback | 28.016 | 96.637 | 0.847 |

## Held-out pattern

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| mean per task_type | 83.731 | 174.573 | 0.406 |
| **linear(alpha=0.1)** (kept) | 20.865 | 63.509 | 0.921 |
| random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 19.294 | 80.904 | 0.873 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 20.533 | 69.968 | 0.905 |
| linear(alpha=0.1) + cold-start fallback | 20.865 | 63.509 | 0.921 |

## Held-out task type

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| mean per task_type | 95.128 | 115.522 | -0.225 |
| **linear(alpha=0.1)** (kept) | 37.151 | 78.660 | 0.432 |
| random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 64.924 | 121.043 | -0.345 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 64.506 | 120.369 | -0.330 |
| linear(alpha=0.1) + cold-start fallback [268 cold-start rows] | 60.694 | 103.192 | 0.023 |

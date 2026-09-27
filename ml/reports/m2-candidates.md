# m2: queue_forecast (queue_len_future), v1

Kept: **mean per worker_id**. no candidate beat the best baseline (mean per worker_id, CV mae 0.1017) by 10%: the baseline is kept.

Split: 5,978 train / 1,494 test rows (time split), 3,578 held-out-pattern rows, 268 held-out-type rows.

## Cross-validation (training part only; drives the selection)

| family | candidate | CV mae |
| --- | --- | --- |
| baseline | persistence (queue_len now) | 0.1140 |
| baseline | mean per worker_id | 0.1017 |
| linear | linear(alpha=0.1) | 0.1000 |
| linear | linear(alpha=1.0) | 0.1000 |
| linear | linear(alpha=10.0) | 0.0999 |
| random_forest | random forest(max_depth=8, min_samples_leaf=1, n_estimators=200) | 0.1087 |
| random_forest | random forest(max_depth=8, min_samples_leaf=5, n_estimators=200) | 0.1056 |
| random_forest | random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 0.1102 |
| random_forest | random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.1050 |
| xgboost | xgboost(learning_rate=0.05, max_depth=3, n_estimators=300) | 0.1098 |
| xgboost | xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 0.1105 |
| xgboost | xgboost(learning_rate=0.1, max_depth=3, n_estimators=300) | 0.1143 |
| xgboost | xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 0.1080 |

## Time-split test set

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| persistence (queue_len now) | 0.187 | 0.607 | -0.601 |
| **mean per worker_id** (kept) | 0.149 | 0.450 | 0.118 |
| linear(alpha=10.0) | 0.183 | 0.415 | 0.252 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.167 | 0.434 | 0.179 |
| xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 0.219 | 0.534 | -0.238 |

## Held-out pattern

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| persistence (queue_len now) | 0.054 | 0.281 | -1.331 |
| **mean per worker_id** (kept) | 0.078 | 0.194 | -0.109 |
| linear(alpha=10.0) | 0.079 | 0.200 | -0.178 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.120 | 0.330 | -2.213 |
| xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 0.126 | 0.348 | -2.587 |

## Held-out task type

| candidate | MAE | RMSE | R2 |
| --- | --- | --- | --- |
| persistence (queue_len now) | 0.112 | 0.432 | 0.013 |
| **mean per worker_id** (kept) | 0.158 | 0.412 | 0.104 |
| linear(alpha=10.0) | 0.115 | 0.409 | 0.116 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.112 | 0.363 | 0.302 |
| xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 0.089 | 0.327 | 0.433 |

# m3: overload (overloaded_future), v1

Kept: **linear(C=1.0)**. linear(C=1.0) is the simplest candidate whose CV f1 (0.5679) beats the best baseline (rate per worker_id, 0.4875) by at least 10%; xgboost scores better (0.5721) but is more complex.

Split: 5,978 train / 1,494 test rows (time split), 3,578 held-out-pattern rows, 268 held-out-type rows.

## Cross-validation (training part only; drives the selection)

| family | candidate | CV f1 |
| --- | --- | --- |
| baseline | majority class | 0.0000 (threshold 0.500) |
| baseline | rate per worker_id | 0.4875 (threshold 0.137) |
| linear | linear(C=0.1) | 0.5492 (threshold 0.794) |
| linear | linear(C=1.0) | 0.5679 (threshold 0.806) |
| linear | linear(C=10.0) | 0.5571 (threshold 0.729) |
| random_forest | random forest(max_depth=8, min_samples_leaf=1, n_estimators=200) | 0.5304 (threshold 0.167) |
| random_forest | random forest(max_depth=8, min_samples_leaf=5, n_estimators=200) | 0.5344 (threshold 0.247) |
| random_forest | random forest(max_depth=16, min_samples_leaf=1, n_estimators=200) | 0.5161 (threshold 0.075) |
| random_forest | random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.5520 (threshold 0.216) |
| xgboost | xgboost(learning_rate=0.05, max_depth=3, n_estimators=300) | 0.5425 (threshold 0.088) |
| xgboost | xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 0.5721 (threshold 0.016) |
| xgboost | xgboost(learning_rate=0.1, max_depth=3, n_estimators=300) | 0.5411 (threshold 0.015) |
| xgboost | xgboost(learning_rate=0.1, max_depth=6, n_estimators=300) | 0.5655 (threshold 0.009) |

## Time-split test set

| candidate | PRECISION | RECALL | F1 | ROC-AUC |
| --- | --- | --- | --- | --- |
| majority class | 0.000 | 0.000 | 0.000 | 0.500 |
| rate per worker_id | 0.544 | 1.000 | 0.705 | 0.937 |
| **linear(C=1.0)** (kept) | 0.866 | 0.923 | 0.894 | 0.990 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.614 | 1.000 | 0.761 | 0.992 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 0.698 | 1.000 | 0.822 | 0.983 |

## Held-out pattern

| candidate | PRECISION | RECALL | F1 | ROC-AUC |
| --- | --- | --- | --- | --- |
| majority class | 0.000 | 0.000 | 0.000 | 0.500 |
| rate per worker_id | 0.125 | 1.000 | 0.223 | 0.869 |
| **linear(C=1.0)** (kept) | 0.180 | 0.357 | 0.240 | 0.903 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.169 | 0.837 | 0.281 | 0.909 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 0.150 | 0.767 | 0.251 | 0.899 |

## Held-out task type

| candidate | PRECISION | RECALL | F1 | ROC-AUC |
| --- | --- | --- | --- | --- |
| majority class | 0.000 | 0.000 | 0.000 | 0.500 |
| rate per worker_id | 0.301 | 1.000 | 0.463 | 0.865 |
| **linear(C=1.0)** (kept) | 0.583 | 0.250 | 0.350 | 0.877 |
| random forest(max_depth=16, min_samples_leaf=5, n_estimators=200) | 0.489 | 0.786 | 0.603 | 0.942 |
| xgboost(learning_rate=0.05, max_depth=6, n_estimators=300) | 0.513 | 0.714 | 0.597 | 0.937 |

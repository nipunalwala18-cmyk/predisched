-- Prompt 18: explainable decisions (F13) and live prediction accuracy.

-- Per-candidate breakdown of every placement: predicted wait, predicted execution, overload
-- probability and penalty, cost, and whether the candidate was skipped and why.
ALTER TABLE scheduling_decisions
    ADD COLUMN breakdown       jsonb,
    ADD COLUMN fallback        boolean NOT NULL DEFAULT false,
    ADD COLUMN fallback_reason text,
    ADD COLUMN model_versions  text;

-- One row per completed task the predictive strategy placed: the prediction for the worker it
-- ran on next to the actual execution time, with the rolling MAE at that point (dashboard, drift).
CREATE TABLE prediction_outcomes (
    id              bigserial PRIMARY KEY,
    task_id         text             NOT NULL,
    task_type       text             NOT NULL,
    worker_id       text             NOT NULL,
    pred_exec_ms    double precision NOT NULL,
    actual_exec_ms  bigint           NOT NULL,
    abs_error_ms    double precision NOT NULL,
    cold_start      boolean          NOT NULL,
    model_versions  text,
    rolling_mae_ms  double precision,
    rolling_type_mae_ms double precision,
    ts              timestamptz      NOT NULL
);
CREATE INDEX prediction_outcomes_ts_idx ON prediction_outcomes (ts);
CREATE INDEX prediction_outcomes_type_ts_idx ON prediction_outcomes (task_type, ts);

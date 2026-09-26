-- PrediSched storage (spec §14, prompt 11). Times are timestamptz from the node's physical clock;
-- Lamport times are kept alongside where ordering across nodes matters.

CREATE TABLE tasks (
    task_id       text PRIMARY KEY,
    type          text        NOT NULL,
    input         text        NOT NULL,
    priority      int         NOT NULL,
    status        text        NOT NULL,
    worker_id     text,
    submitted_at  timestamptz NOT NULL,
    started_at    timestamptz,
    completed_at  timestamptz,
    result        text,
    exec_time_ms  bigint,
    attempts      int         NOT NULL DEFAULT 0,
    deadline_at   timestamptz,
    sla_met       boolean,
    strategy      text,
    client_id     text,
    trace_id      text,
    workflow_id   text,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX tasks_status_idx ON tasks (status);
CREATE INDEX tasks_deadline_idx ON tasks (completed_at) WHERE deadline_at IS NOT NULL;

CREATE TABLE workers (
    worker_id      text PRIMARY KEY,
    host           text        NOT NULL,
    port           int         NOT NULL,
    cores          int         NOT NULL,
    memory_mb      bigint      NOT NULL,
    pool_size      int         NOT NULL,
    status         text        NOT NULL,
    last_heartbeat timestamptz NOT NULL
);

CREATE TABLE worker_metrics (
    id              bigserial PRIMARY KEY,
    worker_id       text             NOT NULL,
    ts              timestamptz      NOT NULL,
    cpu_pct         double precision NOT NULL,
    mem_pct         double precision NOT NULL,
    active_threads  int              NOT NULL,
    queue_len       int              NOT NULL,
    tasks_completed bigint           NOT NULL,
    avg_exec_ms     double precision NOT NULL
);
CREATE INDEX worker_metrics_worker_ts_idx ON worker_metrics (worker_id, ts);

-- One row per finished attempt; the feature columns (spec §12.1) are captured at dispatch time.
-- queue_len_future and overloaded_future are targets filled in later by the Spark job (prompt 15).
CREATE TABLE execution_history (
    id                          bigserial PRIMARY KEY,
    task_id                     text             NOT NULL,
    task_type                   text             NOT NULL,
    resource_profile            text,
    attempt                     int              NOT NULL,
    status                      text             NOT NULL,
    strategy                    text,
    priority                    int              NOT NULL,
    input_size                  bigint           NOT NULL,
    worker_id                   text             NOT NULL,
    worker_cores                int,
    concurrent_tasks_on_worker  int,
    cpu_pct                     double precision,
    mem_pct                     double precision,
    active_threads              int,
    queue_len                   int,
    arrival_rate                double precision,
    avg_exec_recent             double precision,
    dispatched_at               timestamptz      NOT NULL,
    wait_time_ms                bigint           NOT NULL,
    exec_time_ms                bigint           NOT NULL,
    queue_len_future            int,
    overloaded_future           boolean
);
CREATE INDEX execution_history_task_idx ON execution_history (task_id);
CREATE INDEX execution_history_worker_ts_idx ON execution_history (worker_id, dispatched_at);

CREATE TABLE predictions (
    id             bigserial PRIMARY KEY,
    task_id        text             NOT NULL,
    worker_id      text             NOT NULL,
    pred_exec_ms   double precision,
    overload_prob  double precision,
    model_version  text,
    ts             timestamptz      NOT NULL
);
CREATE INDEX predictions_task_idx ON predictions (task_id);
CREATE INDEX predictions_worker_ts_idx ON predictions (worker_id, ts);

CREATE TABLE scheduling_decisions (
    id             bigserial PRIMARY KEY,
    task_id        text             NOT NULL,
    strategy       text             NOT NULL,
    chosen_worker  text             NOT NULL,
    cost           double precision,
    scores         jsonb,
    decision_us    bigint           NOT NULL,
    ts             timestamptz      NOT NULL
);
CREATE INDEX scheduling_decisions_task_idx ON scheduling_decisions (task_id);
CREATE INDEX scheduling_decisions_worker_ts_idx ON scheduling_decisions (chosen_worker, ts);

CREATE TABLE events (
    id            bigserial PRIMARY KEY,
    node_id       text        NOT NULL,
    lamport_time  bigint      NOT NULL,
    ts            timestamptz NOT NULL,
    event_type    text        NOT NULL,
    task_id       text,
    trace_id      text,
    details       jsonb
);
CREATE INDEX events_task_idx ON events (task_id);
CREATE INDEX events_node_ts_idx ON events (node_id, ts);

CREATE TABLE replication_log (
    id            bigserial PRIMARY KEY,
    seq_no        bigint      NOT NULL,
    node_id       text        NOT NULL,
    op            text        NOT NULL,
    task_id       text        NOT NULL,
    version       bigint      NOT NULL,
    lamport_time  bigint      NOT NULL,
    applied_at    timestamptz NOT NULL
);
CREATE INDEX replication_log_task_idx ON replication_log (task_id);
CREATE INDEX replication_log_node_seq_idx ON replication_log (node_id, seq_no);

CREATE TABLE clock_sync (
    id                bigserial PRIMARY KEY,
    node_id           text        NOT NULL,
    ts                timestamptz NOT NULL,
    offset_before_ms  bigint      NOT NULL,
    offset_after_ms   bigint      NOT NULL,
    algorithm         text        NOT NULL
);
CREATE INDEX clock_sync_node_ts_idx ON clock_sync (node_id, ts);

CREATE TABLE failures (
    id            bigserial PRIMARY KEY,
    node_id       text        NOT NULL,
    type          text        NOT NULL,
    detected_at   timestamptz NOT NULL,
    recovered_at  timestamptz,
    details       text
);
CREATE INDEX failures_node_idx ON failures (node_id, detected_at);

CREATE TABLE benchmark_runs (
    run_id      text PRIMARY KEY,
    strategy    text,
    scenario    text,
    metrics     jsonb       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- F6: results of deterministic tasks, keyed by SHA-256 of type + normalised input.
CREATE TABLE result_cache (
    key         text PRIMARY KEY,
    task_type   text        NOT NULL,
    output      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    hits        bigint      NOT NULL DEFAULT 0
);

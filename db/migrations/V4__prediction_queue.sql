-- Prompt 22: the queue forecast (M2) beside each logged prediction, for the dashboard's
-- actual-vs-predicted queue chart (GET /api/queue/forecast).
ALTER TABLE predictions ADD COLUMN pred_queue_len double precision;

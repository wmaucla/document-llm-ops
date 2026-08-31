-- Append-only diagnostic history. Deliberately never read for control-flow
-- decisions -- the counters on `documents` are -- so it can be scanned freely
-- and pruned on a schedule without affecting behaviour.

-- name: log_attempt
INSERT INTO attempt_log (doc_id, stage, attempt_no, producer_or_model, outcome,
                         error_class, error_msg, started_at, ended_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);

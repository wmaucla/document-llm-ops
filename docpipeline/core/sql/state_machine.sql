-- The guarded state machine. One statement, and it is the whole concurrency
-- control for document state in this system.

-- name: transition
-- arch diagram: "Postgres ledger". The `WHERE state = ANY(...)` and RETURNING
-- *are* the guard, not decoration: the row lock serialises concurrent movers,
-- and an empty RETURNING means the row was not in a legal predecessor state,
-- which the caller raises IllegalTransition on. This is what makes a re-drive
-- unable to produce two live workers -- the second one to try matches zero rows.
UPDATE documents
   SET state = %(to)s, state_updated_at = now()
 WHERE doc_id = %(doc_id)s AND state = ANY(%(allowed)s)
RETURNING state;

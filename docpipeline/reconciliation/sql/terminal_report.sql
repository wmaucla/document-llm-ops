-- The scheduled report on documents parked in a terminal state.

-- name: top_reasons
-- Grouped on a prefix rather than the whole string: last_error carries attempt
-- counts and doc-specific detail, so full text is near-unique and would group
-- into buckets of one.
SELECT left(coalesce(last_error, '(none)'), %s) AS reason, count(*) AS n
  FROM documents
 WHERE state = %s
 GROUP BY reason
 ORDER BY n DESC
 LIMIT %s;

-- name: count_not_replayable
-- Documents dlq_replay will NOT pick up, because their version already matches
-- current. These are the ones genuinely needing a human or a new deploy, as
-- opposed to the ones a future deploy will sweep up on its own.
SELECT count(*) AS n FROM documents
 WHERE state = 'failed'
   AND build_sha IS NOT DISTINCT FROM %s
   AND prompt_version IS NOT DISTINCT FROM %s;

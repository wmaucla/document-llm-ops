-- Stuck-state recovery. Claims documents that have sat in an in-flight state
-- longer than their stage's threshold.

-- name: claim_stuck_batch
-- Two thresholds, not one, and that split is the fix for a real bug: text
-- production is fast in every mode, but a real extraction legitimately takes
-- minutes, so a single 30s threshold made the sweeper claim documents that were
-- being processed successfully -- burning their attempt budget and duplicating
-- work onto the one shared Ollama pod (AGENT.md bug #3).
--
-- SKIP LOCKED lets this run alongside itself safely; the batch cap keeps a
-- mistyped threshold from re-driving the entire table in one tick.
SELECT doc_id, state, gcs_path, page_count, has_text_layer, shards_total,
       priority, text_attempts, extract_attempts
  FROM documents
 WHERE (
         (state = ANY(%(text_states)s)
          AND state_updated_at < now() - (%(text_threshold)s || ' seconds')::interval)
      OR (state = ANY(%(extract_states)s)
          AND state_updated_at < now() - (%(extract_threshold)s || ' seconds')::interval)
       )
 ORDER BY priority DESC, state_updated_at ASC
 LIMIT %(cap)s
 FOR UPDATE SKIP LOCKED;

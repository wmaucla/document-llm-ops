-- Committing what the extraction funnel produced, after the quality gates have
-- ruled on it.

-- name: commit_extraction_result
-- arch diagram: the write after "5 quality gates". `AND extraction_result IS
-- NULL` is first-writer-wins: if a document was re-driven while this worker was
-- still running, the late finisher matches zero rows and its answer is
-- discarded rather than overwriting the committed one. The caller logs
-- extract_divergence_detected when the two disagree.
UPDATE documents
   SET extraction_result = %s, gate_results = %s, state = %s, state_updated_at = now()
 WHERE doc_id = %s AND extraction_result IS NULL
RETURNING doc_id;

-- name: route_without_writing
-- arch diagram: "5 quality gates" routing to the review pill. For routes that
-- carry no extraction_result yet (e.g. a completeness failure before extraction
-- ever ran), so it deliberately does not touch the first-writer-wins guard.
UPDATE documents
   SET gate_results = %s, state = %s, state_updated_at = now()
 WHERE doc_id = %s
RETURNING doc_id;

-- Re-driving documents that failed under an older build or prompt.

-- name: find_replayable
-- Only `failed`, and only where the code or prompt has moved since the document
-- last failed -- "we shipped a fix, try again". Re-running the same input
-- through the same version would just fail identically, which is why this is
-- gated rather than periodic. `review` is deliberately not included: a document
-- there was judged, not defeated, and leaving it needs a human.
SELECT doc_id, build_sha, prompt_version FROM documents
 WHERE state = 'failed'
   AND (build_sha IS DISTINCT FROM %s OR prompt_version IS DISTINCT FROM %s);

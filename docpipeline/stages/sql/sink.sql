-- The terminal sink. "Exactly one row per document, ever" is the assertion this
-- table exists to support.

-- name: post_document
-- ON CONFLICT DO NOTHING is what makes at-least-once delivery safe here: the
-- relay may redeliver document.extracted after a rollback, and a second arrival
-- must not produce a second posted row. Returns no row when it was a duplicate,
-- which the caller reports rather than treating as success.
INSERT INTO posted_documents (doc_id, route, fields)
VALUES (%(doc_id)s, %(route)s, %(fields)s)
ON CONFLICT (doc_id) DO NOTHING
RETURNING doc_id;

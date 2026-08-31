-- Document intake. Triage is the only writer of the initial row.

-- name: insert_initial_document
-- arch diagram: "Triage" creating the row in "Postgres ledger". ON CONFLICT DO
-- NOTHING is the content-checksum dedupe: doc_id is the object's crc32c, so
-- re-uploading identical bytes is a no-op rather than a second document.
-- Returns no row when the document already existed.
INSERT INTO documents (doc_id, gcs_path, state, page_count, has_text_layer,
                       priority, shards_total, last_error, doc_type)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (doc_id) DO NOTHING
RETURNING doc_id;

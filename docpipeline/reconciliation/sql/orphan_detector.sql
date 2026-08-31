-- Local ingest: the orphan detector polls GCS because fake-gcs-server has no
-- bucket-notification wiring. This is the standard fallback for that, not a
-- deviation.

-- name: already_pending_triage
-- Skips objects that already have an unpublished triage.requests message, so a
-- slow triage consumer does not cause the detector to enqueue the same document
-- again on its next 10s pass. A row's existence is its pending state -- the
-- relay deletes on delivery ack rather than flagging published.
SELECT DISTINCT doc_id FROM outbox
 WHERE topic = 'triage.requests' AND doc_id = ANY(%s);

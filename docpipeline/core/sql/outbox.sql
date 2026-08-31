-- The transactional outbox relay's claim.

-- name: claim_outbox_batch
-- arch diagram: "Outbox → sink". SKIP LOCKED is what lets two relay replicas
-- poll the same table without double-publishing: each skips rows the other has
-- locked. There is no `published_at IS NULL` filter because a row's *existence*
-- is its pending state -- the relay deletes on delivery ack rather than
-- flagging, so the table is bounded by the backlog rather than by history.
SELECT id, doc_id, topic, payload, headers FROM outbox
 ORDER BY id
 LIMIT %s
 FOR UPDATE SKIP LOCKED;

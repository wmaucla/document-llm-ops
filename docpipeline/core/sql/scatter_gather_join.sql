-- The scatter-gather join: deciding when every shard of a sharded document has
-- landed. Two statements that must stay exactly as they are -- this is the one
-- place in the repo where getting the SQL wrong is a correctness bug rather
-- than a style nit.

-- name: claim_shard
-- Step 1 of 2. The unique index makes a duplicate Kafka delivery a no-op: the
-- second arrival returns no row and the caller exits before touching
-- shards_done, so a redelivery cannot inflate the count and fire the join early.
INSERT INTO document_shards (doc_id, shard_idx)
VALUES (%s, %s)
ON CONFLICT (doc_id, shard_idx) DO NOTHING
RETURNING shard_idx;

-- name: increment_shards_done
-- Step 2 of 2, and the reason the design is correct. The increment and the
-- read-back happen in ONE statement under the parent row's lock, so concurrent
-- final shards serialise and read 1, 2, 3 -- exactly one sees done == total and
-- becomes the winner that publishes ocr.completed.
--
-- Never split this into a SELECT and an UPDATE. A separate count can observe a
-- stale value while another shard is mid-commit, producing two winners or none.
UPDATE documents SET shards_done = shards_done + 1
 WHERE doc_id = %s
RETURNING shards_done, shards_total;

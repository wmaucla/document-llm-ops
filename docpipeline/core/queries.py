"""Every SQL statement the ledger and outbox issue, in one place.

Split out of `ledger.py` so the SQL can be read as SQL. In this pipeline the
statements *are* the design rather than incidental plumbing — the row locks,
`ON CONFLICT`, `SKIP LOCKED` and `RETURNING` clauses are what make concurrency
safe, not the Python around them — so the reasoning lives here with each
statement rather than in the calling function. Read this file to understand
the correctness argument; read `ledger.py` to see when each one fires.

Everything here is a plain string constant with `%s` / `%(name)s` placeholders.
Nothing is built by formatting, and `tests/test_sql_safety.py` enforces that on
every run: the model's own output (`vendor`, `invoice_no`) reaches these
statements only as bound parameters, so a prompt-injection payload in a
document can influence the *model* but never the database.
"""

from __future__ import annotations

# ── state machine ────────────────────────────────────────────────────────────
# arch diagram: "Postgres ledger". Every state move in the system is this one
# statement. The `WHERE state = ANY(...)` and `RETURNING` *are* the concurrency
# control: the row lock serialises concurrent movers, and an empty RETURNING
# means the row was not in a legal predecessor state, which the caller raises
# on. This is what makes a re-drive unable to produce two live workers — the
# second one to try matches zero rows.
TRANSITION = """
    UPDATE documents
       SET state = %(to)s, state_updated_at = now()
     WHERE doc_id = %(doc_id)s AND state = ANY(%(allowed)s)
    RETURNING state
"""

# arch diagram: "Triage" creating the row in "Postgres ledger". ON CONFLICT DO
# NOTHING is the content-checksum dedupe: doc_id is the object's crc32c, so
# re-uploading identical bytes is a no-op rather than a second document.
INSERT_INITIAL_DOCUMENT = """
    INSERT INTO documents (doc_id, gcs_path, state, page_count, has_text_layer,
                            priority, shards_total, last_error, doc_type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (doc_id) DO NOTHING
    RETURNING doc_id
"""

# ── transactional outbox ─────────────────────────────────────────────────────
# arch diagram: "Outbox → sink". Written in the *caller's* transaction, which is
# the entire point: the message and the state change commit together or not at
# all, so there is no window where the database moved on and the message was
# never queued.
ENQUEUE = "INSERT INTO outbox (doc_id, topic, payload, headers) VALUES (%s, %s, %s, %s)"

# Same, batched for fan-out (N shard messages from one split).
ENQUEUE_MANY = "INSERT INTO outbox (doc_id, topic, payload) VALUES (%s, %s, %s)"

# The relay's claim. SKIP LOCKED is what lets two relay replicas poll the same
# table without double-publishing: each skips rows the other has locked. A row's
# existence is its pending state — delivered rows are deleted, not flagged.
CLAIM_OUTBOX_BATCH = """
    SELECT id, doc_id, topic, payload, headers FROM outbox
     ORDER BY id
     LIMIT %s
     FOR UPDATE SKIP LOCKED
"""

# Delete on ack, in the same transaction as the claim and after flush() has
# confirmed delivery — so a slow or partitioned broker rolls back and the rows
# stay queued for the next tick.
DELETE_PUBLISHED = "DELETE FROM outbox WHERE id = ANY(%s)"

# The single most important relay metric: a dead relay stalls the whole pipeline
# while every other dashboard stays green.
OLDEST_PENDING_AGE = "SELECT extract(epoch from (now() - min(created_at))) AS age FROM outbox"

OUTBOX_DEPTH = "SELECT count(*) AS n FROM outbox"

# ── scatter-gather join ──────────────────────────────────────────────────────
# arch diagram: "Scatter-gather join", step 1 of 2. Claims this shard. The
# unique index makes a duplicate Kafka delivery a no-op: the second arrival
# returns no row and exits before touching shards_done, so a redelivery cannot
# inflate the count and fire the join early.
CLAIM_SHARD = """
    INSERT INTO document_shards (doc_id, shard_idx)
    VALUES (%s, %s)
    ON CONFLICT (doc_id, shard_idx) DO NOTHING
    RETURNING shard_idx
"""

# Step 2 of 2, and the reason this design is correct. The increment and the
# read-back happen in ONE statement under the parent row's lock, so concurrent
# final shards serialise and read 1, 2, 3 — exactly one sees done == total and
# becomes the winner. A separate `SELECT count(*)` could observe a stale count
# while another shard is mid-commit, producing two winners or none. Never
# refactor this into two queries.
INCREMENT_SHARDS_DONE = """
    UPDATE documents SET shards_done = shards_done + 1
     WHERE doc_id = %s
    RETURNING shards_done, shards_total
"""

SELECT_SHARD_INDEXES = "SELECT shard_idx FROM document_shards WHERE doc_id = %s"

# ── extraction result ────────────────────────────────────────────────────────
# arch diagram: the write after "5 quality gates". `AND extraction_result IS
# NULL` is first-writer-wins: if a document was re-driven mid-flight, the late
# finisher matches zero rows and its answer is discarded rather than overwriting
# the committed one. Divergence between the two is logged by the caller.
COMMIT_EXTRACTION_RESULT = """
    UPDATE documents
       SET extraction_result = %s, gate_results = %s, state = %s, state_updated_at = now()
     WHERE doc_id = %s AND extraction_result IS NULL
    RETURNING doc_id
"""

# arch diagram: "5 quality gates" routing to the review pill. For routes that
# carry no extraction_result yet, so it deliberately does not touch the
# first-writer-wins guard above.
ROUTE_WITHOUT_WRITING = """
    UPDATE documents
       SET gate_results = %s, state = %s, state_updated_at = now()
     WHERE doc_id = %s
    RETURNING doc_id
"""

# ── per-document columns ─────────────────────────────────────────────────────
SET_LAST_ERROR = "UPDATE documents SET last_error = %s WHERE doc_id = %s"
SET_GATE_RESULTS = "UPDATE documents SET gate_results = %s WHERE doc_id = %s"
RESET_REPAIR_ATTEMPTS = "UPDATE documents SET repair_attempts = 0 WHERE doc_id = %s"
STAMP_BUILD_INFO = "UPDATE documents SET build_sha = %s, prompt_version = %s WHERE doc_id = %s"
GET_DOCUMENT = "SELECT * FROM documents WHERE doc_id = %s"

# The one statement that cannot be a constant: a column *name* is an identifier,
# and identifiers cannot be bound parameters. `ledger.increment_attempts`
# allowlists the column against a fixed tuple before interpolating, and
# tests/test_sql_safety.py exempts it by name.
INCREMENT_ATTEMPTS_TEMPLATE = "UPDATE documents SET {column} = {column} + 1 WHERE doc_id = %s RETURNING {column}"

# ── attempt log ──────────────────────────────────────────────────────────────
# Append-only diagnostic history. Deliberately not read for control-flow
# decisions (the counters on `documents` are), so it can be scanned freely.
LOG_ATTEMPT = """
    INSERT INTO attempt_log (doc_id, stage, attempt_no, producer_or_model, outcome,
                              error_class, error_msg, started_at, ended_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# ── feature flags ────────────────────────────────────────────────────────────
# The auto-post kill switch lives in a table rather than in config, so it can be
# flipped without a redeploy — which an import-time constant cannot do.
GET_FEATURE_FLAG = "SELECT value FROM feature_flags WHERE key = %s"
SET_FEATURE_FLAG = (
    "INSERT INTO feature_flags (key, value) VALUES (%s, %s) "
    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
)

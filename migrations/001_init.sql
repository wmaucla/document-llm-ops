-- Document pipeline ledger — see the design doc's "Concrete specification" and
-- "The document ledger" sections. Run as a superuser/owner (make init-db);
-- creates the pipeline_rw/pipeline_ro roles.

-- ============================================================================
-- Roles
--
-- pipeline_ro deliberately has no INSERT on outbox and no Kafka producer
-- credentials at the application layer. Since every publish in this system
-- goes through the outbox, that one missing grant is what makes the read-only
-- operator lane (replay, bake-off) structurally incapable of affecting
-- production — see "Enforcement is structural, not policy".
-- ============================================================================

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_rw') THEN
    CREATE ROLE pipeline_rw LOGIN PASSWORD 'pipeline_rw_pw';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_ro') THEN
    CREATE ROLE pipeline_ro LOGIN PASSWORD 'pipeline_ro_pw';
  END IF;
END
$$;

-- ============================================================================
-- documents — one row per document. Triage is the only writer of the initial
-- row; there is no "received" state (see "1 · State machine").
-- ============================================================================

CREATE TABLE IF NOT EXISTS documents (
  doc_id              text PRIMARY KEY,               -- GCS-provided crc32c checksum
  gcs_path            text NOT NULL,
  state               text NOT NULL CHECK (state IN (
                         'text_pending', 'text_running',
                         'extract_pending', 'extract_running',
                         'complete', 'review', 'failed'
                       )),
  state_updated_at    timestamptz NOT NULL DEFAULT now(),  -- DB-server time only

  shards_total        int NOT NULL DEFAULT 1,
  shards_done         int NOT NULL DEFAULT 0,

  text_attempts       int NOT NULL DEFAULT 0,
  extract_attempts    int NOT NULL DEFAULT 0,
  repair_attempts     int NOT NULL DEFAULT 0,          -- resets each new extract_attempt

  spend_cents         int NOT NULL DEFAULT 0,

  extraction_result   jsonb,                           -- NULL = first-writer-wins guard
  gate_results        jsonb NOT NULL DEFAULT '{}'::jsonb,

  page_count          int,
  priority            int NOT NULL DEFAULT 0,          -- higher = more urgent
  has_text_layer      boolean,
  doc_type            text NOT NULL DEFAULT 'invoice', -- known pre-extraction; see gates.classify_doc_type

  vendor              text,
  invoice_no          text,

  last_error          text,
  build_sha           text,
  prompt_version      text,
  funnel_version      int,

  created_at          timestamptz NOT NULL DEFAULT now()
);

-- The sweeper's whole query in one index. Bounded by concurrency (in-flight
-- rows), not by history — see "The sweeper query needs a partial index".
CREATE INDEX IF NOT EXISTS documents_inflight_idx ON documents (state_updated_at)
  WHERE state IN ('text_pending', 'text_running', 'extract_pending', 'extract_running');

-- Business-level dedupe: content-checksum dedupe (the PK) does not catch a
-- rescanned/re-emailed duplicate invoice. See "Quality gates — Business dedupe".
CREATE UNIQUE INDEX IF NOT EXISTS documents_vendor_invoice_no_idx ON documents (vendor, invoice_no)
  WHERE vendor IS NOT NULL AND invoice_no IS NOT NULL;

CREATE INDEX IF NOT EXISTS documents_gcs_path_idx ON documents (gcs_path);

-- ============================================================================
-- document_shards — transient join bookkeeping. The unique index is what
-- makes the scatter-gather join duplicate-safe.
-- ============================================================================

CREATE TABLE IF NOT EXISTS document_shards (
  doc_id      text NOT NULL REFERENCES documents (doc_id),
  shard_idx   int NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (doc_id, shard_idx)
);

-- ============================================================================
-- outbox — the transactional outbox. Every stage writes here in the same
-- transaction as its ledger state change; a relay publishes afterwards. See
-- "Transactional outbox — REQUIRED".
-- ============================================================================

CREATE TABLE IF NOT EXISTS outbox (
  id            bigserial PRIMARY KEY,   -- monotonic; gives ordering for free
  doc_id        text NOT NULL,
  topic         text NOT NULL,
  payload       jsonb NOT NULL,
  headers       jsonb,
  created_at    timestamptz NOT NULL DEFAULT now(),
  -- Retained for compatibility; unused since the relay switched to delete-on-
  -- ack. A row's *existence* is its pending state -- the relay deletes it once
  -- the broker has acknowledged the message, so the table is bounded by the
  -- backlog rather than growing forever. Nothing ever read published_at as a
  -- timestamp; every reader only asked "is this pending".
  published_at  timestamptz,
  attempts      int NOT NULL DEFAULT 0   -- >0 on a surviving row = failed delivery
);

-- Bounded by backlog, not history -- now structurally, not just by predicate.
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (id);

-- ============================================================================
-- attempt_log — append-only diagnostic history. Not used in control-flow
-- decisions (the counters on `documents` are), so it can be scanned freely.
-- ============================================================================

CREATE TABLE IF NOT EXISTS attempt_log (
  id                  bigserial PRIMARY KEY,
  doc_id              text NOT NULL,
  stage               text NOT NULL,
  attempt_no          int NOT NULL,
  producer_or_model   text,
  outcome             text,
  error_class         text,
  error_msg           text,
  started_at          timestamptz,
  ended_at            timestamptz
);

CREATE INDEX IF NOT EXISTS attempt_log_doc_id_idx ON attempt_log (doc_id);

-- ============================================================================
-- posted_documents — the sink-stub's table; the assertion target for
-- "exactly one row per document, ever". Keyed on doc_id, at-least-once relay.
-- ============================================================================

CREATE TABLE IF NOT EXISTS posted_documents (
  doc_id     text PRIMARY KEY,
  route      text NOT NULL,
  fields     jsonb,
  posted_at  timestamptz NOT NULL DEFAULT now()
);

-- ============================================================================
-- Grants
-- ============================================================================

GRANT ALL PRIVILEGES ON documents, document_shards, outbox, attempt_log, posted_documents
  TO pipeline_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline_rw;

-- Read-only lane: SELECT on the ledger and shard table, GCS writes confined
-- to experiments/ (enforced at the storage layer, not here). Deliberately NO
-- grant on outbox, attempt_log or posted_documents.
GRANT SELECT ON documents, document_shards TO pipeline_ro;

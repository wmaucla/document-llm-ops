"""The document ledger: state machine guard, transactional outbox, and the
scatter-gather join.

Everything here is deliberately raw SQL against real Postgres — the
`ON CONFLICT` / `SKIP LOCKED` / row-lock semantics *are* the design, not
incidental plumbing (see "Substrate — a relational DB, not a log").
"""

from __future__ import annotations

import datetime

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Json

from docpipeline import config
from docpipeline.core import queries

IN_FLIGHT_STATES = ("text_pending", "text_running", "extract_pending", "extract_running")
# Split because the two stages have wildly different legitimate durations and
# therefore need different stuck-detection thresholds — see
# config.EXTRACT_STUCK_THRESHOLD_SECONDS and sweeper._claim_batch.
TEXT_STATES = ("text_pending", "text_running")
EXTRACT_STATES = ("extract_pending", "extract_running")
TERMINAL_STATES = ("complete", "failed")

# "The invariant that kills most illegal transitions: a _running state may
# only be entered from its own _pending state." Every re-drive targets
# _pending, never _running.
ALLOWED_TRANSITIONS: set[tuple[str, str]] = {
    ("text_pending", "text_running"),
    ("text_running", "extract_pending"),
    ("text_running", "text_pending"),   # sweeper re-drive / fall-through to OCR
    ("text_running", "review"),
    ("text_running", "failed"),
    ("extract_pending", "extract_running"),
    ("extract_running", "complete"),
    ("extract_running", "review"),
    ("extract_running", "extract_pending"),
    ("extract_running", "failed"),
    ("review", "extract_pending"),
    ("failed", "text_pending"),
    ("failed", "extract_pending"),
    # Human override only — operator.accept_review(), never a consumer. Every
    # other edge into `complete` means "a worker ran and the blocking gates
    # passed"; this one means "a person looked and disagreed with the gates",
    # which is a weaker claim. It is deliberately unreachable from pipeline
    # code: nothing in stages/ may transition to `complete` from `review`, and
    # accept_review stamps gate_results.operator_override so a `complete`
    # document always says whether it earned that or was granted it.
    ("review", "complete"),
}


class IllegalTransition(Exception):
    def __init__(self, doc_id: str, to_state: str, from_states: set[str]):
        super().__init__(f"{doc_id}: cannot reach {to_state!r} (needed state in {from_states})")
        self.doc_id = doc_id
        self.to_state = to_state
        self.from_states = from_states


def connect(role: str = "rw", autocommit: bool = False) -> psycopg.Connection:
    dsn = config.PG_DSN_RW if role == "rw" else config.PG_DSN_RO
    conn = psycopg.connect(
        dsn, row_factory=dict_row,
        options=f"-c statement_timeout={config.PG_STATEMENT_TIMEOUT_MS}",
    )
    conn.autocommit = autocommit
    return conn


def transition(cur, doc_id: str, to_state: str, *, from_states: set[str] | None = None, idempotent: bool = True) -> str:
    """Guarded state transition. Raises IllegalTransition unless the row is
    already in a valid predecessor state for `to_state` — or, if
    `idempotent`, already in `to_state` itself (harmless redelivery)."""
    if from_states is None:
        from_states = {frm for (frm, to) in ALLOWED_TRANSITIONS if to == to_state}
    allowed = set(from_states) | ({to_state} if idempotent else set())
    cur.execute(queries.TRANSITION, {"to": to_state, "doc_id": doc_id, "allowed": list(allowed)})
    row = cur.fetchone()
    if row is None:
        raise IllegalTransition(doc_id, to_state, from_states)
    return row["state"]


def enqueue(cur, doc_id: str, topic: str, payload: dict, headers: dict | None = None) -> None:
    cur.execute(  # arch diagram: "Outbox → sink", in the caller's transaction
        "INSERT INTO outbox (doc_id, topic, payload, headers) VALUES (%s, %s, %s, %s)",
        (doc_id, topic, Json(payload), Json(headers) if headers else None),
    )


def enqueue_many(cur, rows: list[tuple[str, str, dict]]) -> None:
    """One multi-row INSERT for fan-out (e.g. N shard messages) — see 'Fan-out
    transactions' in the outbox cost table."""
    if not rows:
        return
    cur.executemany(  # same, batched for fan-out (N shard messages from one split)
        "INSERT INTO outbox (doc_id, topic, payload) VALUES (%s, %s, %s)",
        [(doc_id, topic, Json(payload)) for doc_id, topic, payload in rows],
    )


def route_text_production(has_text_layer: bool, page_count: int) -> tuple[str, int]:
    """Decide which topic a document's text production dispatches to, and its
    shards_total at dispatch time (provisional for ocr.split — authoritative
    only once the split step enumerates real pages). Shared by triage (first
    dispatch) and the sweeper (re-drive) so re-drive can never diverge from
    the original routing decision."""
    if has_text_layer:
        return "text.embedded", 1
    if page_count <= config.SHARD_SIZE_PAGES:
        return "ocr.shard", 1
    return "ocr.split", 1


def build_dispatch_payload(topic: str, doc_id: str, gcs_path: str, page_count: int) -> dict:
    """Same routing-decision-to-payload mapping used by triage's first
    dispatch and the sweeper's re-drive of a shards_total<=1 document."""
    if topic == "ocr.shard":
        # Single-shard fast path: the whole (small) document is shard 0, no
        # physical split needed — it reads the source object directly.
        return {
            "doc_id": doc_id,
            "shard_idx": 0,
            "shards_total": 1,
            "page_start": 0,
            "page_end": page_count,
            "shard_gcs_path": gcs_path,
        }
    return {"doc_id": doc_id, "gcs_path": gcs_path, "page_count": page_count}


def insert_initial_document(
    cur,
    doc_id: str,
    gcs_path: str,
    *,
    state: str,
    page_count: int | None = None,
    has_text_layer: bool | None = None,
    priority: int = 0,
    shards_total: int = 1,
    last_error: str | None = None,
    doc_type: str = "invoice",
) -> bool:
    """Triage is the only writer of the initial row (see '1 · State
    machine'). Returns False if the row already exists (checksum dedupe)."""
    cur.execute(
        queries.INSERT_INITIAL_DOCUMENT,
        (doc_id, gcs_path, state, page_count, has_text_layer, priority, shards_total, last_error, doc_type),
    )
    return cur.fetchone() is not None


def record_shard_and_maybe_join(cur, doc_id: str, shard_idx: int, completed_payload: dict) -> bool:
    """The scatter-gather join. Returns True iff *this* caller is the winner
    (fires ocr.completed). Uses UPDATE ... RETURNING under the parent row
    lock, never SELECT count(*) — see 'Detecting completion'."""
    cur.execute(queries.CLAIM_SHARD, (doc_id, shard_idx))
    if cur.fetchone() is None:
        return False  # duplicate delivery; unique index makes this a no-op

    cur.execute(queries.INCREMENT_SHARDS_DONE, (doc_id,))
    row = cur.fetchone()
    won = row["shards_done"] == row["shards_total"]
    if won:
        transition(cur, doc_id, "extract_pending", from_states={"text_running"})
        enqueue(cur, doc_id, "ocr.completed", completed_payload)
    return won


def missing_shards(cur, doc_id: str, shards_total: int) -> list[int]:
    cur.execute("SELECT shard_idx FROM document_shards WHERE doc_id = %s", (doc_id,))
    present = {r["shard_idx"] for r in cur.fetchall()}
    return [i for i in range(shards_total) if i not in present]


def commit_extraction_result(
    cur,
    doc_id: str,
    extraction_result: dict,
    gate_results: dict,
    final_state: str,
    outbox_payload: dict | None = None,
) -> bool:
    """First-writer-wins (see '4 · Retry divergence'). Returns True iff this
    attempt's result was the one persisted."""
    cur.execute(queries.COMMIT_EXTRACTION_RESULT,
                (Json(extraction_result), Json(gate_results), final_state, doc_id))
    won = cur.fetchone() is not None
    if won and outbox_payload is not None:
        enqueue(cur, doc_id, "document.extracted", outbox_payload)
    return won


def route_without_writing(cur, doc_id: str, gate_results: dict, final_state: str) -> bool:
    """For non-terminal routes that don't carry an extraction_result yet
    (e.g. completeness gate failure -> review before extraction ever ran).
    Does not touch the first-writer-wins guard."""
    cur.execute(queries.ROUTE_WITHOUT_WRITING, (Json(gate_results), final_state, doc_id))
    return cur.fetchone() is not None


def log_attempt(
    cur,
    doc_id: str,
    stage: str,
    attempt_no: int,
    *,
    producer_or_model: str | None = None,
    outcome: str | None = None,
    error_class: str | None = None,
    error_msg: str | None = None,
    started_at: datetime.datetime | None = None,
    ended_at: datetime.datetime | None = None,
) -> None:
    cur.execute(
        queries.LOG_ATTEMPT,
        (doc_id, stage, attempt_no, producer_or_model, outcome, error_class, error_msg, started_at, ended_at),
    )


_ATTEMPT_COLUMNS = ("text_attempts", "extract_attempts", "repair_attempts")


def increment_attempts(cur, doc_id: str, column: str) -> int:
    # The only interpolated identifier in this module. A column name cannot be a
    # bound parameter, so it is allowlisted instead -- and with an explicit
    # raise, not an assert, because asserts vanish under `python -O` and would
    # take the guard with them. Every caller passes a literal; nothing
    # document-derived reaches here.
    if column not in _ATTEMPT_COLUMNS:
        raise ValueError(f"not an attempt column: {column!r}")
    cur.execute(
        f"UPDATE documents SET {column} = {column} + 1 WHERE doc_id = %s RETURNING {column}",
        (doc_id,),
    )
    return cur.fetchone()[column]


def set_last_error(cur, doc_id: str, message: str) -> None:
    """Records *why* a document ended up where it is.

    Exists because the sweeper's give-up path used to transition a document
    straight to `failed` without ever writing this column, so every
    attempt-capped document carried an empty `last_error` — which then showed
    up repeatedly in debugging evidence trails explaining nothing, and was once
    mistaken for a signal about *how* the document died.
    """
    cur.execute("UPDATE documents SET last_error = %s WHERE doc_id = %s", (message, doc_id))


def set_gate_results(cur, doc_id: str, gate_results: dict) -> None:
    """Overwrite gate_results without touching state or extraction_result.
    Used by operator.accept_review to stamp an override onto a document whose
    extraction already ran — the normal write path (record_extraction) sets all
    three together, which is wrong here since nothing re-extracted."""
    cur.execute("UPDATE documents SET gate_results = %s WHERE doc_id = %s", (Json(gate_results), doc_id))


def reset_repair_attempts(cur, doc_id: str) -> None:
    """repair_attempts is an inner-loop counter that must reset on each new
    extract_attempt — see '2 · Attempt accounting'."""
    cur.execute("UPDATE documents SET repair_attempts = 0 WHERE doc_id = %s", (doc_id,))


def stamp_build_info(cur, doc_id: str, build_sha: str, prompt_version: str) -> None:
    """Records what code/prompt last touched a document — this is what gates
    DLQ replay (see 'DLQ replay — daily, gated, never fully automatic')."""
    cur.execute(
        "UPDATE documents SET build_sha = %s, prompt_version = %s WHERE doc_id = %s",
        (build_sha, prompt_version, doc_id),
    )


def get_feature_flag(cur, key: str, default: bool = True) -> bool:
    """The auto-post kill switch, and any future flag — a table row instead
    of a LaunchDarkly SDK call, so it can be flipped without a redeploy."""
    cur.execute("SELECT value FROM feature_flags WHERE key = %s", (key,))
    row = cur.fetchone()
    return row["value"] if row is not None else default


def set_feature_flag(cur, key: str, value: bool) -> None:
    cur.execute(
        "INSERT INTO feature_flags (key, value) VALUES (%s, %s) "
        "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        (key, value),
    )


def get_document(cur, doc_id: str) -> dict | None:
    cur.execute("SELECT * FROM documents WHERE doc_id = %s", (doc_id,))
    return cur.fetchone()

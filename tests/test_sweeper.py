"""Reconciler ① — 'Sweeper recovers lost work'."""

from docpipeline.core import ledger
from docpipeline.reconciliation import sweeper
from tests.conftest import insert_document


def _backdate(cur, doc_id: str, seconds: int = 3600) -> None:
    cur.execute(
        "UPDATE documents SET state_updated_at = now() - (%s || ' seconds')::interval WHERE doc_id = %s",
        (seconds, doc_id),
    )


def test_sweeper_republishes_stuck_text_pending(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending", has_text_layer=True, page_count=1)
        _backdate(cur, doc_id)
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=10)
    assert found.get("text_pending", 0) >= 1

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
        cur.execute("SELECT topic FROM outbox WHERE doc_id = %s", (doc_id,))
        topics = {r["topic"] for r in cur.fetchall()}
    assert doc["state"] == "text_pending"  # re-drive targets *_pending
    assert doc["text_attempts"] == 0  # lost publish, not a failed attempt — free to re-drive
    assert "text.embedded" in topics


def test_sweeper_republishes_only_missing_shards(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", has_text_layer=False,
                         page_count=3, shards_total=3)
        ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})  # shard 0 already done
        _backdate(cur, doc_id)
    conn.commit()

    sweeper.sweep_once(conn, batch_cap=10)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT payload FROM outbox WHERE doc_id = %s AND topic = 'ocr.shard' ORDER BY id",
            (doc_id,),
        )
        republished_idxs = sorted(r["payload"]["shard_idx"] for r in cur.fetchall())
    assert republished_idxs == [1, 2]  # not shard 0 — that work is not re-done


def test_sweeper_dlqs_after_attempt_cap(conn, doc_id):
    # 'failed' is only legally reachable from a *_running* state — a document
    # stuck in *_pending* was never picked up, so it re-drives for free
    # instead (see test_sweeper_republishes_stuck_text_pending).
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", has_text_layer=True, page_count=1)
        cur.execute("UPDATE documents SET text_attempts = 999 WHERE doc_id = %s", (doc_id,))
        _backdate(cur, doc_id)
    conn.commit()

    sweeper.sweep_once(conn, batch_cap=10)

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert doc["state"] == "failed"


def test_sweeper_records_why_it_gave_up(conn, doc_id):
    """A `failed` document used to carry an empty last_error, which made the
    column useless exactly when it mattered most."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", has_text_layer=True, page_count=1)
        cur.execute("UPDATE documents SET text_attempts = 999 WHERE doc_id = %s", (doc_id,))
        _backdate(cur, doc_id)
    conn.commit()

    sweeper.sweep_once(conn, batch_cap=10)

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert doc["state"] == "failed"
    assert doc["last_error"], "a failed document must say why"
    assert "attempt cap" in doc["last_error"]
    assert "text_running" in doc["last_error"]


def test_sweeper_leaves_in_progress_extraction_alone(conn, doc_id, monkeypatch):
    """The bug: STUCK_THRESHOLD_SECONDS (30s) was shorter than a single real
    LLM call, so the sweeper redrove documents that were being processed
    successfully — burning their attempt budget and duplicating the work."""
    from docpipeline import config
    monkeypatch.setattr(config, "EXTRACT_STUCK_THRESHOLD_SECONDS", 1500)

    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_running", has_text_layer=True, page_count=1)
        _backdate(cur, doc_id, seconds=200)  # mid-LLM-call, not stuck
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=10)

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert found.get("extract_running", 0) == 0
    assert doc["state"] == "extract_running", "an in-progress extraction must not be redriven"
    assert doc["extract_attempts"] == 0, "and must not burn an attempt"


def test_sweeper_still_catches_genuinely_stuck_extraction(conn, doc_id, monkeypatch):
    """The threshold moved; it didn't disappear."""
    from docpipeline import config
    monkeypatch.setattr(config, "EXTRACT_STUCK_THRESHOLD_SECONDS", 1500)

    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_running", has_text_layer=True, page_count=1)
        _backdate(cur, doc_id, seconds=3600)  # well past the budget
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=10)

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert found.get("extract_running", 0) == 1
    assert doc["state"] == "extract_pending"


def test_text_stage_keeps_its_own_tight_threshold(conn, doc_id, monkeypatch):
    """Raising extraction's threshold must not slow down text-stage recovery —
    that stage is fast in every mode."""
    from docpipeline import config
    monkeypatch.setattr(config, "EXTRACT_STUCK_THRESHOLD_SECONDS", 1500)

    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending", has_text_layer=True, page_count=1)
        _backdate(cur, doc_id, seconds=200)  # past text's 30s, under extraction's 1500s
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=10)
    assert found.get("text_pending", 0) == 1


def test_sweeper_respects_batch_cap(conn):
    doc_ids = [f"test-batchcap-{i}" for i in range(8)]
    with conn.cursor() as cur:
        for d in doc_ids:
            insert_document(cur, d, state="text_pending", has_text_layer=True, page_count=1)
            _backdate(cur, d)
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=3)
    assert sum(found.values()) == 3  # drains in bounded batches, not all at once

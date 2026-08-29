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


def test_sweeper_respects_batch_cap(conn):
    doc_ids = [f"test-batchcap-{i}" for i in range(8)]
    with conn.cursor() as cur:
        for d in doc_ids:
            insert_document(cur, d, state="text_pending", has_text_layer=True, page_count=1)
            _backdate(cur, d)
    conn.commit()

    found = sweeper.sweep_once(conn, batch_cap=3)
    assert sum(found.values()) == 3  # drains in bounded batches, not all at once

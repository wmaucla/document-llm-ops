"""The scatter-gather join — 'Detecting completion'.

The lost-join hazard only shows up under genuine concurrency: two shards
finishing in truly overlapping transactions. A single-threaded test can't
exercise the row-lock serialization this depends on, so these use real
threads against real Postgres.
"""

from __future__ import annotations

import threading

from docpipeline.core import ledger
from tests.conftest import insert_document


def test_join_fires_exactly_once(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", shards_total=3, page_count=3)
    conn.commit()

    with conn.cursor() as cur:
        won = ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})
    conn.commit()
    assert won is False

    with conn.cursor() as cur:
        won = ledger.record_shard_and_maybe_join(cur, doc_id, 1, {"doc_id": doc_id})
    conn.commit()
    assert won is False

    with conn.cursor() as cur:
        won = ledger.record_shard_and_maybe_join(cur, doc_id, 2, {"doc_id": doc_id})
    conn.commit()
    assert won is True  # the last shard fires it


def test_duplicate_shard_delivery_does_not_double_increment(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", shards_total=2, page_count=2)
        ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})
    conn.commit()

    # redeliver shard 0 — must be a no-op, not a second increment
    with conn.cursor() as cur:
        won = ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})
    conn.commit()
    assert won is False

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert doc["shards_done"] == 1


def test_join_survives_concurrent_final_shards(doc_id):
    """The exact hazard the design doc calls out: under READ COMMITTED, a
    bare SELECT count(*) would let two concurrently-finishing shards each see
    'one short' and neither fire — a lost join, worse than a duplicate. The
    UPDATE ... RETURNING row lock is what prevents that."""
    setup_conn = ledger.connect(role="rw")
    with setup_conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", shards_total=3, page_count=3)
        ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})
    setup_conn.commit()
    setup_conn.close()

    results: dict[int, bool] = {}
    barrier = threading.Barrier(2)

    def worker(shard_idx: int) -> None:
        c = ledger.connect(role="rw")
        barrier.wait()
        with c.cursor() as cur:
            results[shard_idx] = ledger.record_shard_and_maybe_join(cur, doc_id, shard_idx, {"doc_id": doc_id})
        c.commit()
        c.close()

    t1 = threading.Thread(target=worker, args=(1,))
    t2 = threading.Thread(target=worker, args=(2,))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert set(results) == {1, 2}
    assert sum(results.values()) == 1, f"expected exactly one winner, got {results}"

    verify_conn = ledger.connect(role="rw")
    try:
        with verify_conn.cursor() as cur:
            doc = ledger.get_document(cur, doc_id)
    finally:
        verify_conn.rollback()
        verify_conn.close()
    assert doc["state"] == "extract_pending"
    assert doc["shards_done"] == 3


def test_winner_publishes_ocr_completed(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_running", shards_total=1, page_count=1)
        won = ledger.record_shard_and_maybe_join(cur, doc_id, 0, {"doc_id": doc_id})
        assert won is True
        cur.execute("SELECT topic, payload FROM outbox WHERE doc_id = %s", (doc_id,))
        rows = cur.fetchall()
    assert any(r["topic"] == "ocr.completed" for r in rows)

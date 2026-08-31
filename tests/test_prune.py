"""Retention safety for attempt_log.

The outbox is no longer pruned at all — the relay deletes each row on delivery
ack, so the table is bounded by the backlog. See tests/test_relay_delivery.py
for the property that matters there: an unacknowledged row must survive."""

from __future__ import annotations

import pytest

from docpipeline.core import ledger
from docpipeline.reconciliation import prune


@pytest.fixture(autouse=True)
def _isolated_outbox():
    """These tests need real isolation, not the usual rollback.

    The `conn` fixture isolates by rolling back at teardown, which works because
    tests write only through it. `prune.run()` opens its own connection and
    commits — that is the behaviour under test — so its deletes survive the
    rollback and leak into whatever runs next. Truncate on both sides instead.
    """
    def _truncate():
        c = ledger.connect(role="rw")
        try:
            with c.cursor() as cur:
                cur.execute("TRUNCATE outbox, attempt_log")
            c.commit()
        finally:
            c.close()

    _truncate()
    yield
    _truncate()


def _attempt(cur, doc_id: str, *, days_ago: int | None) -> int:
    """Insert one attempt_log row. days_ago=None leaves both timestamps NULL,
    which is the case the id-watermark approach exists for."""
    ts = None if days_ago is None else f"now() - interval '{days_ago} days'"
    cur.execute(
        f"INSERT INTO attempt_log (doc_id, stage, attempt_no, ended_at) "
        f"VALUES (%s, 'extraction', 1, {ts or 'NULL'}) RETURNING id",
        (doc_id,),
    )
    return cur.fetchone()["id"]


def test_prune_removes_old_attempt_log_rows(conn, doc_id):
    with conn.cursor() as cur:
        old_id = _attempt(cur, doc_id, days_ago=90)
        fresh_id = _attempt(cur, doc_id, days_ago=0)
    conn.commit()

    result = prune.run(attempt_log_days=30)
    assert result["attempt_log_removed"] >= 1

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM attempt_log WHERE id = ANY(%s)", ([old_id, fresh_id],))
        surviving = {r["id"] for r in cur.fetchall()}
    assert old_id not in surviving
    assert fresh_id in surviving


def test_undated_rows_are_swept_by_position_not_left_forever(conn, doc_id):
    """started_at/ended_at are optional, so a time predicate would skip undated
    rows permanently. The id watermark sweeps them by insert position."""
    with conn.cursor() as cur:
        undated_id = _attempt(cur, doc_id, days_ago=None)   # inserted first = older
        old_id = _attempt(cur, doc_id, days_ago=90)         # datable, sets the watermark
    conn.commit()

    prune.run(attempt_log_days=30)

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM attempt_log WHERE id = ANY(%s)", ([undated_id, old_id],))
        surviving = {r["id"] for r in cur.fetchall()}
    assert old_id not in surviving
    assert undated_id not in surviving, "an undated row below the watermark must be swept too"


def test_dry_run_reports_without_deleting(conn, doc_id):
    with conn.cursor() as cur:
        old_id = _attempt(cur, doc_id, days_ago=90)
    conn.commit()

    result = prune.run(attempt_log_days=30, dry_run=True)
    assert result["dry_run"] is True
    assert result["attempt_log_removed"] >= 1

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM attempt_log WHERE id = %s", (old_id,))
        assert cur.fetchone() is not None, "dry run must not delete"

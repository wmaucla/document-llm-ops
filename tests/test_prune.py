"""Retention safety. The dangerous property here is not "does it delete" but
"does it ever delete something undelivered" — an unpublished outbox row is a
message that has not reached Kafka, and deleting one is exactly the loss the
outbox pattern exists to prevent."""

from __future__ import annotations

import datetime

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


def _outbox_row(cur, doc_id: str, *, published_days_ago: int | None) -> int:
    """Insert one outbox row. published_days_ago=None leaves it pending."""
    published = (
        None if published_days_ago is None
        else datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=published_days_ago)
    )
    cur.execute(
        "INSERT INTO outbox (doc_id, topic, payload, published_at) "
        "VALUES (%s, %s, %s, %s) RETURNING id",
        (doc_id, "document.extracted", ledger.Json({"doc_id": doc_id}), published),
    )
    return cur.fetchone()["id"]


def test_prune_never_deletes_an_unpublished_row_however_old(conn, doc_id):
    """The load-bearing safety property. A pending row can be arbitrarily old —
    broker down, delivery failing, retries backing off — and age must never be
    grounds for deleting it."""
    with conn.cursor() as cur:
        pending_id = _outbox_row(cur, doc_id, published_days_ago=None)
        # Backdate its creation so a created_at-based predicate WOULD catch it.
        cur.execute("UPDATE outbox SET created_at = now() - interval '999 days' WHERE id = %s",
                    (pending_id,))
    conn.commit()

    prune.run(outbox_days=1, attempt_log_days=1)

    with conn.cursor() as cur:
        cur.execute("SELECT published_at FROM outbox WHERE id = %s", (pending_id,))
        row = cur.fetchone()
    assert row is not None, "prune deleted an UNPUBLISHED outbox row — that is message loss"
    assert row["published_at"] is None


def test_prune_removes_published_rows_past_the_window(conn, doc_id):
    with conn.cursor() as cur:
        old_id = _outbox_row(cur, doc_id, published_days_ago=30)
        fresh_id = _outbox_row(cur, doc_id, published_days_ago=0)
    conn.commit()

    result = prune.run(outbox_days=7, attempt_log_days=9999)
    assert result["outbox_removed"] >= 1

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM outbox WHERE id = ANY(%s)", ([old_id, fresh_id],))
        surviving = {r["id"] for r in cur.fetchall()}
    assert old_id not in surviving, "a published row past the window should be gone"
    assert fresh_id in surviving, "a recently published row is still within the window"


def test_dry_run_reports_without_deleting(conn, doc_id):
    with conn.cursor() as cur:
        old_id = _outbox_row(cur, doc_id, published_days_ago=30)
    conn.commit()

    result = prune.run(outbox_days=7, attempt_log_days=9999, dry_run=True)
    assert result["dry_run"] is True
    assert result["outbox_removed"] >= 1  # counted, not deleted

    with conn.cursor() as cur:
        cur.execute("SELECT id FROM outbox WHERE id = %s", (old_id,))
        assert cur.fetchone() is not None, "dry run must not delete"

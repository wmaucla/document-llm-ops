"""The outbox's core promise: a row is marked published only once the broker
actually has the message.

`produce()` is asynchronous, so the only evidence of delivery is `flush()`'s
return value (messages still queued) plus the per-message delivery callback.
Ignoring either turns the transactional outbox into at-most-once delivery —
silently, and in the one component whose entire job is not losing messages.
"""

from __future__ import annotations

import pytest

from docpipeline.core import ledger, outbox
from tests.conftest import insert_document


class FakeProducer:
    """Stands in for confluent_kafka.Producer. `unflushed` is what flush()
    reports as still queued; `delivery_error` fires the callback with an error.
    """

    def __init__(self, unflushed: int = 0, delivery_error: str | None = None):
        self.unflushed = unflushed
        self.delivery_error = delivery_error
        self.produced: list[str] = []
        self._callbacks: list = []

    def produce(self, topic, key=None, value=None, headers=None, on_delivery=None):
        self.produced.append(topic)
        if on_delivery is not None:
            self._callbacks.append(on_delivery)

    def poll(self, _timeout):
        return 0

    def flush(self, _timeout):
        for cb in self._callbacks:
            cb(self.delivery_error, _FakeMsg())
        self._callbacks.clear()
        return self.unflushed


class _FakeMsg:
    def topic(self):
        return "ocr.completed"


def _pending_row_count(cur, doc_id: str) -> int:
    # Scoped to one doc_id on purpose: the failure tests deliberately leave
    # their rows unpublished, so a global count is order-dependent.
    cur.execute(
        "SELECT count(*) AS n FROM outbox WHERE published_at IS NULL AND doc_id = %s", (doc_id,)
    )
    return cur.fetchone()["n"]


def test_successful_delivery_marks_published(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        ledger.enqueue(cur, doc_id, "ocr.completed", {"doc_id": doc_id})
    conn.commit()

    n = outbox.relay_once(conn, FakeProducer(), batch_cap=10)
    assert n == 1
    with conn.cursor() as cur:
        assert _pending_row_count(cur, doc_id) == 0


def test_unflushed_messages_do_not_mark_published(conn, doc_id):
    """The bug: flush() timing out left messages undelivered while the UPDATE
    ran anyway, losing them permanently."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        ledger.enqueue(cur, doc_id, "ocr.completed", {"doc_id": doc_id})
    conn.commit()

    with pytest.raises(outbox.DeliveryFailed):
        outbox.relay_once(conn, FakeProducer(unflushed=1), batch_cap=10)

    with conn.cursor() as cur:
        # Still pending, so the next tick retries it. At-least-once, never
        # at-most-once.
        assert _pending_row_count(cur, doc_id) == 1


def test_delivery_error_does_not_mark_published(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        ledger.enqueue(cur, doc_id, "ocr.completed", {"doc_id": doc_id})
    conn.commit()

    with pytest.raises(outbox.DeliveryFailed):
        outbox.relay_once(conn, FakeProducer(delivery_error="broker down"), batch_cap=10)

    with conn.cursor() as cur:
        assert _pending_row_count(cur, doc_id) == 1

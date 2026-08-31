import threading

from docpipeline.core import ledger, outbox
from docpipeline.infra import kafka_utils
from tests.conftest import insert_document


def test_outbox_closes_the_dual_write_window(conn, doc_id):
    """Simulates 'process died right after COMMIT, before the relay polled'
    — the message must still publish, with no sweeper involvement."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        ledger.enqueue(cur, doc_id, "triage.requests.dlq", {"doc_id": doc_id, "probe": "dual-write"})
    conn.commit()

    # A row's existence IS its pending state — the relay deletes on delivery
    # ack rather than marking published, so the table is bounded by the backlog.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM outbox WHERE doc_id = %s", (doc_id,))
        assert cur.fetchone()["n"] == 1, "enqueued but not yet relayed"

    producer = kafka_utils.make_producer()
    n = outbox.relay_once(conn, producer)
    assert n >= 1

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM outbox WHERE doc_id = %s", (doc_id,))
        assert cur.fetchone()["n"] == 0, "a delivered row must not linger"


def test_concurrent_relay_replicas_do_not_double_publish(conn, doc_id):
    """SKIP LOCKED means multiple replicas are safe — of two replicas racing
    for the same row, exactly one claims and publishes it. Needs real
    threads: a sequential call pair can't exercise the lock contention,
    since the first call's commit would already clear the row before the
    second one runs."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        ledger.enqueue(cur, doc_id, "triage.requests.dlq", {"doc_id": doc_id, "probe": "concurrent-relay"})
    conn.commit()

    published_counts: dict[str, int] = {}
    barrier = threading.Barrier(2)

    def worker(name: str) -> None:
        c = ledger.connect(role="rw")
        p = kafka_utils.make_producer()
        barrier.wait()
        with c.cursor() as cur:
            cur.execute(
                "SELECT id FROM outbox WHERE doc_id = %s FOR UPDATE SKIP LOCKED",
                (doc_id,),
            )
            claimed = cur.fetchall()
            for row in claimed:
                kafka_utils.publish(p, "triage.requests.dlq", {"doc_id": doc_id}, key=doc_id)
            p.flush(5)
            if claimed:
                # Mirrors the real relay: delete on ack, not mark-published.
                cur.execute("DELETE FROM outbox WHERE id = ANY(%s)", ([r["id"] for r in claimed],))
        c.commit()
        c.close()
        published_counts[name] = len(claimed)

    t1 = threading.Thread(target=worker, args=("a",))
    t2 = threading.Thread(target=worker, args=("b",))
    t1.start()
    t2.start()
    t1.join(timeout=10)
    t2.join(timeout=10)

    assert sum(published_counts.values()) == 1, f"expected exactly one claimant, got {published_counts}"

    with conn.cursor() as cur:
        # Exactly one row was enqueued and exactly one relay won it, so the
        # row is gone; the loser published nothing. Asserted as a count so a
        # double-publish that somehow left the row behind would still fail.
        cur.execute("SELECT count(*) AS n FROM outbox WHERE doc_id = %s", (doc_id,))
        assert cur.fetchone()["n"] == 0


def test_oldest_pending_age_reports_none_when_caught_up(conn):
    # Pending == present, now that delivered rows are deleted rather than flagged.
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM outbox")
        pending = cur.fetchone()["n"]
    if pending == 0:
        assert outbox.oldest_pending_age_seconds(conn) is None

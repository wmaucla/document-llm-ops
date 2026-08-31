"""The outbox relay — transport, never a router (see 'The relay').

Reads rows already carrying topic+payload and publishes them; zero routing
decisions live here. Implements the doc's own relay pseudocode literally:
SELECT ... FOR UPDATE SKIP LOCKED, publish, then mark published, all in one
transaction. Multiple replicas are safe because of SKIP LOCKED.
"""

from __future__ import annotations

import argparse
import logging
import time

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.infra import kafka_utils

log = logging.getLogger(__name__)


class DeliveryFailed(Exception):
    """Raised instead of marking rows published when delivery isn't confirmed."""


def relay_once(conn, producer, batch_cap: int = config.RELAY_BATCH_CAP) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, doc_id, topic, payload, headers FROM outbox
             WHERE published_at IS NULL
             ORDER BY id
             LIMIT %s
             FOR UPDATE SKIP LOCKED
            """,
            (batch_cap,),
        )
        rows = cur.fetchall()
        if not rows:
            conn.commit()
            return 0

        # The whole point of the outbox is that a row is marked published only
        # once the broker actually has it. `produce()` is asynchronous and
        # `flush()` returns how many messages are *still queued* after its
        # timeout — discarding that return value (as this did) means a slow or
        # partitioned broker gets the UPDATE anyway and those messages are
        # gone, silently, from the one component whose entire job is not
        # losing them.
        failures: list[str] = []

        def _on_delivery(err, msg) -> None:
            if err is not None:
                failures.append(f"{msg.topic()}: {err}")

        for row in rows:
            kafka_utils.publish(
                producer, row["topic"], row["payload"], row["headers"],
                key=row["doc_id"], on_delivery=_on_delivery,
            )

        remaining = producer.flush(config.RELAY_FLUSH_TIMEOUT_SECONDS)
        if remaining or failures:
            # Roll back rather than mark posted: the rows stay unpublished and
            # the next tick retries them. Messages that *did* land are
            # redelivered, which is fine — every consumer in this pipeline is
            # idempotent by construction (ON CONFLICT DO NOTHING on shards,
            # first-writer-wins on extraction, idempotent transitions), so
            # at-least-once is the contract. At-most-once is not.
            conn.rollback()
            raise DeliveryFailed(
                f"{remaining} message(s) unflushed after {config.RELAY_FLUSH_TIMEOUT_SECONDS}s, "
                f"{len(failures)} delivery error(s): {failures[:3]}"
            )

        ids = [r["id"] for r in rows]
        cur.execute(
            "UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE id = ANY(%s)",
            (ids,),
        )
        conn.commit()
    return len(rows)


def oldest_pending_age_seconds(conn) -> float | None:
    """The single most important relay metric — see 'outbox_oldest_pending_age'.
    A dead relay stalls the whole pipeline while every other dashboard stays green."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extract(epoch from (now() - min(created_at))) AS age "
            "FROM outbox WHERE published_at IS NULL"
        )
        row = cur.fetchone()
        return row["age"] if row and row["age"] is not None else None


def run_forever(poll_seconds: float = config.RELAY_POLL_SECONDS) -> None:
    conn = ledger.connect(role="rw", autocommit=False)
    producer = kafka_utils.make_producer()
    log.info("outbox relay started, polling every %ss", poll_seconds)
    while True:
        try:
            n = relay_once(conn, producer)
            if n:
                log.info("relayed %d messages", n)
        except Exception:
            log.exception("relay tick failed")
            conn.rollback()
        time.sleep(poll_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.once:
        _conn = ledger.connect(role="rw", autocommit=False)
        _producer = kafka_utils.make_producer()
        print(relay_once(_conn, _producer))
    else:
        run_forever()

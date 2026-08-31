"""sink-stub — the downstream contract's local stand-in.

The assertion target for 'exactly one row per document, ever'. The relay is
at-least-once, so this must be idempotent on doc_id (see 'The sink must be
idempotent').
"""

from __future__ import annotations

import logging

from docpipeline.core import ledger
from docpipeline.infra import kafka_utils

log = logging.getLogger(__name__)

CONSUMER_GROUP = "sink-stub"


def handle_document_extracted(cur, payload: dict) -> bool:
    cur.execute(
        """
        INSERT INTO posted_documents (doc_id, route, fields)
        VALUES (%(doc_id)s, %(route)s, %(fields)s)
        ON CONFLICT (doc_id) DO NOTHING
        RETURNING doc_id
        """,
        {
            "doc_id": payload["doc_id"],
            "route": payload.get("route", "auto_post"),
            "fields": ledger.Json(payload.get("fields", {})),
        },
    )
    return cur.fetchone() is not None


def run_forever() -> None:
    conn = ledger.connect(role="rw")
    consumer = kafka_utils.make_consumer(CONSUMER_GROUP, ["document.extracted"])
    log.info("sink-stub consumer started")
    while True:
        payload, msg = kafka_utils.poll_json(consumer)
        if payload is None:
            continue
        try:
            with conn.cursor() as cur:
                inserted = handle_document_extracted(cur, payload)
            conn.commit()
            log.info("sink-stub %s -> %s", payload["doc_id"], "posted" if inserted else "duplicate_ignored")
            consumer.commit(msg)
        except Exception:
            conn.rollback()
            log.exception("sink-stub failed on %s (no offset commit; will redeliver)", payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

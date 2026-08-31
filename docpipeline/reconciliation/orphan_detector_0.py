"""Reconciler ② — the orphan detector.

In production this covers dropped GCS notifications. Locally, fake-gcs-server
has no bucket-notification wiring at all, so this loop *is* the primary
ingest path (see 'Locally, the orphan detector is the ingest path') — a
happy accident that means the component most likely to rot from disuse in
production is exercised by every single local run.

It does not create a ledger row itself — only triage does that (see '1 ·
State machine' / 'One writer, one code path'). It just makes sure every
object under inbox/ eventually gets a triage.requests message.
"""

from __future__ import annotations

import logging
import time

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.reconciliation import queries
from docpipeline.infra import gcs

log = logging.getLogger(__name__)

INBOX_PREFIX = "inbox/"


def find_and_enqueue_orphans(cur, prefix: str = INBOX_PREFIX) -> int:
    candidates: list[tuple[str, str]] = []
    for blob in gcs.ensure_bucket().list_blobs(prefix=prefix):
        if not blob.crc32c:
            continue
        candidates.append((gcs.crc32c_to_doc_id(blob.crc32c), f"gs://{config.GCS_BUCKET}/{blob.name}"))
    if not candidates:
        return 0

    doc_ids = [c[0] for c in candidates]
    cur.execute("SELECT doc_id FROM documents WHERE doc_id = ANY(%s)", (doc_ids,))
    known = {r["doc_id"] for r in cur.fetchall()}
    orphans = [(doc_id, path) for doc_id, path in candidates if doc_id not in known]
    if not orphans:
        return 0

    cur.execute(
        queries.ALREADY_PENDING_TRIAGE,
        ([o[0] for o in orphans],),
    )
    already_pending = {r["doc_id"] for r in cur.fetchall()}
    to_enqueue = [
        (doc_id, "triage.requests", {"gcs_path": path})
        for doc_id, path in orphans
        if doc_id not in already_pending
    ]
    ledger.enqueue_many(cur, to_enqueue)
    return len(to_enqueue)


def run_forever(interval_seconds: int = config.ORPHAN_DETECTOR_INTERVAL_SECONDS) -> None:
    conn = ledger.connect(role="rw")
    gcs.ensure_bucket()
    log.info("orphan detector started, interval=%ss", interval_seconds)
    while True:
        try:
            with conn.cursor() as cur:
                n = find_and_enqueue_orphans(cur)
            conn.commit()
            if n:
                log.info("enqueued %d orphaned object(s)", n)
        except Exception:
            conn.rollback()
            log.exception("orphan detector tick failed")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

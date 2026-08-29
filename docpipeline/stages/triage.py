"""STAGE 0 · TRIAGE — the only writer of the initial ledger row.

doc_id from the GCS-provided checksum, MIME/encryption/corruption checks, a
hard page ceiling, and a has_text_layer routing hint — then ledger-first,
always: write the row, THEN enqueue the outbox row in the same transaction.
Triage stays pure classification: it never does a real parse (that's
pdf_worker's job), only cheap page-tree reads.
"""

from __future__ import annotations

import logging

from docpipeline import config
from docpipeline.core import gates, ledger
from docpipeline.infra import gcs, kafka_utils
from docpipeline.stages import pdf_utils

log = logging.getLogger(__name__)

CONSUMER_GROUP = "triage"


def handle_gcs_path(cur, gcs_path: str) -> str:
    """Returns the terminal classification for logging/tests: one of
    'dispatched', 'duplicate', 'zero_byte', 'unsupported_mime', 'encrypted',
    'corrupt', 'page_ceiling', 'not_found'."""
    try:
        info = gcs.object_info(gcs_path)
    except FileNotFoundError:
        log.warning("triage: object vanished before read: %s", gcs_path)
        return "not_found"

    if info.size == 0:
        ledger.insert_initial_document(
            cur, info.doc_id, gcs_path, state="failed", last_error="zero_byte_object"
        )
        return "zero_byte"

    content_type = info.content_type or "application/octet-stream"
    if content_type not in pdf_utils.ALLOWED_MIME_TYPES:
        ledger.insert_initial_document(
            cur, info.doc_id, gcs_path, state="failed", last_error=f"unsupported_mime:{content_type}"
        )
        return "unsupported_mime"

    data = gcs.download_bytes(gcs_path)

    try:
        page_count = pdf_utils.read_page_count(data, content_type)
    except pdf_utils.EncryptedPdfError:
        ledger.insert_initial_document(
            cur, info.doc_id, gcs_path, state="review", last_error="encrypted_pdf"
        )
        return "encrypted"
    except pdf_utils.CorruptPdfError as exc:
        ledger.insert_initial_document(
            cur, info.doc_id, gcs_path, state="review", last_error=f"corrupt_pdf:{exc}"
        )
        return "corrupt"

    if page_count > config.HARD_PAGE_CEILING:
        ledger.insert_initial_document(
            cur, info.doc_id, gcs_path, state="review",
            last_error=f"page_ceiling_exceeded:{page_count}", page_count=page_count,
        )
        return "page_ceiling"

    text_layer = pdf_utils.has_text_layer(data, content_type)
    doc_type = gates.classify_doc_type(pdf_utils.sample_text(data, content_type))
    topic, shards_total = ledger.route_text_production(text_layer, page_count)

    inserted = ledger.insert_initial_document(
        cur,
        info.doc_id,
        gcs_path,
        state="text_pending",
        page_count=page_count,
        has_text_layer=text_layer,
        shards_total=shards_total,
        doc_type=doc_type,
    )
    if not inserted:
        return "duplicate"  # checksum dedupe — second upload is check-then-skip

    payload = ledger.build_dispatch_payload(topic, info.doc_id, gcs_path, page_count)
    ledger.enqueue(cur, info.doc_id, topic, payload)
    return "dispatched"


def run_forever() -> None:
    conn = ledger.connect(role="rw")
    consumer = kafka_utils.make_consumer(CONSUMER_GROUP, ["triage.requests"])
    log.info("triage consumer started")
    while True:
        payload, msg = kafka_utils.poll_json(consumer)
        if payload is None:
            continue
        try:
            with conn.cursor() as cur:
                result = handle_gcs_path(cur, payload["gcs_path"])
            conn.commit()
            log.info("triage %s -> %s", payload["gcs_path"], result)
            consumer.commit(msg)
        except Exception:
            conn.rollback()
            log.exception("triage failed on %s (no offset commit; will redeliver)", payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

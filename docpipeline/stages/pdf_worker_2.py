"""pdf-worker: text.embedded + ocr.split, one deployment (shared library,
shared memory profile — see 'Topics vs deployments').

text.embedded is tier-0 (pypdf, $0, no OCR). ocr.split is the physical-split
step ('Physical split, not logical') — it enumerates real pages and writes
the *authoritative* shards_total, since triage's page_count is only a
routing hint and malformed PDFs lie.
"""

from __future__ import annotations

import logging
import math

from docpipeline import config
from docpipeline.core import artifact, ledger
from docpipeline.infra import gcs, kafka_utils
from docpipeline.text import pdf_utils

log = logging.getLogger(__name__)

CONSUMER_GROUP = "pdf-worker"
TOPICS = ["text.embedded", "ocr.split"]


def handle_text_embedded(cur, doc_id: str, gcs_path: str) -> str:
    ledger.transition(cur, doc_id, "text_running", from_states={"text_pending"})
    data = gcs.download_bytes(gcs_path)
    pages_text = pdf_utils.extract_embedded_text(data)
    ok, reason = pdf_utils.text_sanity(pages_text)

    if not ok:
        # Fall through to OCR — same shape as every other gate here: attempt
        # the free path, verify exactly, fall through on failure.
        page_count = len(pages_text)
        topic, shards_total = ledger.route_text_production(False, page_count)
        cur.execute(
            "UPDATE documents SET has_text_layer = false, shards_total = %s WHERE doc_id = %s",
            (shards_total, doc_id),
        )
        payload = ledger.build_dispatch_payload(topic, doc_id, gcs_path, page_count)
        ledger.enqueue(cur, doc_id, topic, payload)
        ledger.transition(cur, doc_id, "text_pending", from_states={"text_running"})
        return f"fallthrough_to_ocr:{reason}"

    pages = [{"page_no": i, "text": t} for i, t in enumerate(pages_text)]
    artifact.write_assembled(doc_id, producer="pypdf-text", producer_version="v1", pages=pages)
    ledger.transition(cur, doc_id, "extract_pending", from_states={"text_running"})
    ledger.enqueue(cur, doc_id, "ocr.completed", {"doc_id": doc_id})
    return "tier0_complete"


def begin_split(cur, doc_id: str) -> None:
    ledger.transition(cur, doc_id, "text_running", from_states={"text_pending"})


def do_split(doc_id: str, gcs_path: str) -> tuple[int, int]:
    """GCS writes happen before any DB transaction — never do IO inside a
    transaction (FM2). Returns (authoritative shards_total, real page_count)."""
    data = gcs.download_bytes(gcs_path)
    page_count = pdf_utils.read_page_count(data, "application/pdf")  # real enumeration
    shard_size = config.SHARD_SIZE_PAGES
    shards_total = max(1, math.ceil(page_count / shard_size))
    for idx in range(shards_total):
        start = idx * shard_size
        end = min(start + shard_size, page_count)
        sub_pdf = pdf_utils.split_pages(data, start, end)
        gcs.upload_bytes(gcs.path_for(artifact.split_shard_path(doc_id, idx)), sub_pdf, "application/pdf")
    return shards_total, page_count


def commit_split_result(cur, doc_id: str, shards_total: int, page_count: int) -> None:
    cur.execute(
        "UPDATE documents SET shards_total = %s, page_count = %s WHERE doc_id = %s",
        (shards_total, page_count, doc_id),
    )
    shard_size = config.SHARD_SIZE_PAGES
    rows = []
    for idx in range(shards_total):
        start = idx * shard_size
        end = min(start + shard_size, page_count)
        rows.append((doc_id, "ocr.shard", {
            "doc_id": doc_id, "shard_idx": idx, "shards_total": shards_total,
            "page_start": start, "page_end": end,
            "shard_gcs_path": artifact.split_shard_path(doc_id, idx),
        }))
    ledger.enqueue_many(cur, rows)


def run_forever() -> None:
    conn = ledger.connect(role="rw")
    consumer = kafka_utils.make_consumer(CONSUMER_GROUP, TOPICS)
    log.info("pdf-worker started on %s", TOPICS)
    while True:
        payload, msg = kafka_utils.poll_json(consumer)
        if payload is None:
            continue
        doc_id = payload["doc_id"]
        topic = msg.topic()
        try:
            if topic == "text.embedded":
                with conn.cursor() as cur:
                    result = handle_text_embedded(cur, doc_id, payload["gcs_path"])
                conn.commit()
            else:  # ocr.split
                with conn.cursor() as cur:
                    begin_split(cur, doc_id)
                conn.commit()

                shards_total, page_count = do_split(doc_id, payload["gcs_path"])

                with conn.cursor() as cur:
                    commit_split_result(cur, doc_id, shards_total, page_count)
                conn.commit()
                result = f"split_into_{shards_total}_shards"
            log.info("pdf-worker %s %s -> %s", topic, doc_id, result)
            consumer.commit(msg)
        except ledger.IllegalTransition:
            # Stale/duplicate redelivery of a message whose document already
            # moved on — retrying can never succeed, so commit past it
            # instead of redelivering forever (see extraction.py's same fix).
            conn.rollback()
            log.info("pdf-worker skipping stale redelivery for %s %s", topic, payload)
            consumer.commit(msg)
        except Exception:
            conn.rollback()
            log.exception("pdf-worker failed on %s %s (no offset commit; will redeliver)", topic, payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

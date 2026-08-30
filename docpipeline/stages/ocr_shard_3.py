"""ocr-shard: rasterise + OCR one page range, then the scatter-gather join.

Kept CPU-bound and dumb on purpose: rasterise -> OCR -> discard the bitmap ->
next page (memory peak is one page, ~26 MB @ 300 DPI in prod, far less at the
local 150 DPI). The winning shard publishes only `ocr.completed { doc_id }`
and does *not* reassemble — different failure modes must not share a retry
boundary (see 'The winner publishes; it does not assemble').
"""

from __future__ import annotations

import io
import logging

from pdf2image import convert_from_bytes

from docpipeline import config
from docpipeline.core import artifact, ledger
from docpipeline.infra import gcs, kafka_utils
from docpipeline.text import ocr_engine

log = logging.getLogger(__name__)

CONSUMER_GROUP = "ocr-shard"


def handle_shard(conn, payload: dict) -> bool:
    doc_id = payload["doc_id"]
    shard_idx = payload["shard_idx"]
    page_start = payload["page_start"]
    page_end = payload["page_end"]
    shard_gcs_path = payload["shard_gcs_path"]

    # Consumers must advance state on receipt — idempotent, harmless if a
    # later shard message for the same doc arrives after this is already true.
    with conn.cursor() as cur:
        ledger.transition(cur, doc_id, "text_running", from_states={"text_pending"})
    conn.commit()

    data = gcs.download_bytes(shard_gcs_path)
    engine = ocr_engine.get_engine()

    if data[:4] == b"%PDF":
        n_local_pages = page_end - page_start
        page_images = []
        for image in convert_from_bytes(data, dpi=config.OCR_DPI, first_page=1, last_page=max(1, n_local_pages)):
            buf = io.BytesIO()
            image.save(buf, format="PNG")
            page_images.append(buf.getvalue())
    else:
        # 'Non-PDF inputs': a single image is already one rasterised page.
        page_images = [data]

    pages = []
    for i, image_bytes in enumerate(page_images):
        page_no = page_start + i
        text, confidence = engine.ocr_page(doc_id, page_no, image_bytes)
        pages.append({"page_no": page_no, "text": text, "confidence": confidence})
    artifact.write_shard_output(doc_id, shard_idx, pages)

    with conn.cursor() as cur:
        won = ledger.record_shard_and_maybe_join(cur, doc_id, shard_idx, {"doc_id": doc_id})
    conn.commit()
    return won


def run_forever() -> None:
    conn = ledger.connect(role="rw")
    consumer = kafka_utils.make_consumer(CONSUMER_GROUP, ["ocr.shard"])
    log.info("ocr-shard consumer started (engine=%s, dpi=%s)", config.OCR_ENGINE, config.OCR_DPI)
    while True:
        payload, msg = kafka_utils.poll_json(consumer)
        if payload is None:
            continue
        try:
            won = handle_shard(conn, payload)
            log.info(
                "ocr-shard doc=%s shard=%s/%s won_join=%s",
                payload["doc_id"], payload["shard_idx"], payload["shards_total"], won,
            )
            consumer.commit(msg)
        except ledger.IllegalTransition:
            # Stale/duplicate redelivery of a message whose document already
            # moved on — retrying can never succeed, so commit past it
            # instead of redelivering forever (see extraction.py's same fix).
            conn.rollback()
            log.info("ocr-shard skipping stale redelivery for %s", payload)
            consumer.commit(msg)
        except Exception:
            conn.rollback()
            log.exception("ocr-shard failed on %s (no offset commit; will redeliver)", payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

"""Reconciler ① — the stuck-state sweeper.

Detecting lost work requires an independent source of truth (the ledger)
because Kafka cannot see work that vanished. This is deliberately before any
OCR in the local build order — it is what makes every later step safe to
interrupt.

Two-phase, per 'FM3 · The sweeper holding locks while publishing': a short
claim transaction (SKIP LOCKED, reset to *_pending, commit), then outbox
enqueues happen in separate short transactions outside that lock.
"""

from __future__ import annotations

import argparse
import logging
import time
from collections import Counter

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.reconciliation import queries

log = logging.getLogger(__name__)


def _claim_batch(cur, batch_cap: int) -> list[dict]:
    # Per-stage thresholds, not one global one. A real extraction call takes
    # 150-450s and the document sits in extract_running for all of it, so the
    # 30s threshold that suits text production would redrive documents that
    # are being processed successfully right now — burning their attempt
    # budget, duplicating work onto the single shared Ollama pod, and growing
    # consumer lag while the original worker goes on to succeed. See
    # config.EXTRACT_STUCK_THRESHOLD_SECONDS.
    cur.execute(
        queries.CLAIM_STUCK_BATCH,
        {
            "text_states": list(ledger.TEXT_STATES),
            "text_threshold": config.STUCK_THRESHOLD_SECONDS,
            "extract_states": list(ledger.EXTRACT_STATES),
            "extract_threshold": config.EXTRACT_STUCK_THRESHOLD_SECONDS,
            "cap": batch_cap,
        },
    )
    rows = cur.fetchall()
    claimed = []
    for row in rows:
        is_text_stage = row["state"].startswith("text_")
        is_running = row["state"].endswith("_running")

        # `failed` is only ever legally entered from a *_running* state (see
        # '1 · State machine' legal transitions) — a *_pending* document was
        # never picked up at all (a lost publish, not a failed attempt), so
        # re-driving it is free and uncapped; only a *_running* document that
        # keeps dying actually burns its attempt budget.
        if is_running:
            attempts = row["text_attempts"] if is_text_stage else row["extract_attempts"]
            max_attempts = config.MAX_TEXT_ATTEMPTS if is_text_stage else config.MAX_EXTRACT_ATTEMPTS
            if attempts >= max_attempts:
                ledger.transition(cur, row["doc_id"], "failed")
                # Without this the column stays empty and a `failed` document
                # carries no reason at all — see ledger.set_last_error.
                ledger.set_last_error(
                    cur, row["doc_id"],
                    f"sweeper: exceeded attempt cap in {row['state']} ({attempts}/{max_attempts})",
                )
                log.warning("doc %s exceeded attempt cap in %s -> failed (DLQ)", row["doc_id"], row["state"])
                continue
            ledger.increment_attempts(cur, row["doc_id"], "text_attempts" if is_text_stage else "extract_attempts")

        pending_state = row["state"] if row["state"].endswith("_pending") else row["state"].replace("_running", "_pending")
        ledger.transition(cur, row["doc_id"], pending_state)
        claimed.append(row)
    return claimed


def redrive_document(cur, row: dict) -> None:
    """Republish whatever message a document's current stage needs. Public
    because `docpipeline.operator`'s break-glass re-drive reuses this exact
    function rather than reimplementing it — see 'Break-glass reuses the
    sweeper, it does not reimplement it'. `row` needs the same columns
    `_claim_batch`'s query selects: doc_id, state, gcs_path, page_count,
    has_text_layer, shards_total."""
    doc_id = row["doc_id"]
    state = row["state"]

    if state in ("text_pending", "text_running") and row["shards_total"] <= 1:
        topic, _ = ledger.route_text_production(row["has_text_layer"], row["page_count"])
        payload = ledger.build_dispatch_payload(topic, doc_id, row["gcs_path"], row["page_count"])
        ledger.enqueue(cur, doc_id, topic, payload)
        return

    if state == "text_running" and row["shards_total"] > 1:
        # Shard-aware: republish only missing shards, never re-do completed work.
        missing = ledger.missing_shards(cur, doc_id, row["shards_total"])
        shard_size = config.SHARD_SIZE_PAGES
        for idx in missing:
            page_start = idx * shard_size
            page_end = min(page_start + shard_size, row["page_count"])
            ledger.enqueue(cur, doc_id, "ocr.shard", {
                "doc_id": doc_id,
                "shard_idx": idx,
                "shards_total": row["shards_total"],
                "page_start": page_start,
                "page_end": page_end,
                "shard_gcs_path": f"gs://{config.GCS_BUCKET}/shards/{doc_id}/{idx:04d}.pdf",
            })
        return

    if state == "text_pending" and row["shards_total"] > 1:
        # Split step itself never ran / was lost; re-drive to ocr.split from scratch.
        ledger.enqueue(cur, doc_id, "ocr.split", {
            "doc_id": doc_id, "gcs_path": row["gcs_path"], "page_count": row["page_count"],
        })
        return

    if state in ("extract_pending", "extract_running"):
        # Assembly + funnel are idempotent (check-then-skip); redeliver doc_id only.
        ledger.enqueue(cur, doc_id, "ocr.completed", {"doc_id": doc_id})
        return

    log.warning("sweeper: no re-drive rule for %s in state %s", doc_id, state)


def sweep_once(conn, batch_cap: int = config.SWEEPER_BATCH_CAP) -> Counter:
    with conn.cursor() as cur:
        claimed = _claim_batch(cur, batch_cap)
    conn.commit()

    for row in claimed:
        with conn.cursor() as cur:
            redrive_document(cur, row)
        conn.commit()

    found = Counter(row["state"] for row in claimed)
    # The best single health signal: reconciler_stuck_docs_found{state}.
    for state, count in found.items():
        log.info("reconciler_stuck_docs_found{state=%s} %d", state, count)
    return found


def run_forever(interval_seconds: int = config.SWEEPER_CADENCE_SECONDS) -> None:
    conn = ledger.connect(role="rw")
    log.info("sweeper started, cadence=%ss batch_cap=%s", interval_seconds, config.SWEEPER_BATCH_CAP)
    while True:
        try:
            sweep_once(conn)
        except Exception:
            conn.rollback()
            log.exception("sweep tick failed")
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.once:
        print(dict(sweep_once(ledger.connect(role="rw"))))
    else:
        run_forever()

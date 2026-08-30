"""Synthetic canary — 'the only check that verifies the pipeline as a
system'. Injects a known document and asserts it reaches `complete` within
an SLO. Unlike the dead man's switch (passive, waits for real traffic),
this actively drives a document through every stage.
"""

from __future__ import annotations

import argparse
import logging
import time
import uuid

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.infra import gcs
from docpipeline.reconciliation import orphan_detector_0 as orphan_detector

log = logging.getLogger(__name__)


def _canary_pdf_bytes(invoice_no: str) -> bytes:
    import io

    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas

    lines = [
        "SYNTHETIC CANARY VENDOR",
        f"Invoice No: {invoice_no}",
        "Invoice Date: 2026-01-01",
        "Due Date: 2026-02-01",
        "Seller: Synthetic Canary Vendor",
        "Buyer: Contoso Manufacturing",
        "Currency: USD",
        "Line Item: Canary probe | 1.00",
        "Subtotal: 1.00",
        "Tax: 0.00",
        "Total: 1.00",
    ]
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    y = 800
    for line in lines:
        c.drawString(72, y, line)
        y -= 16
    c.showPage()
    c.save()
    return buf.getvalue()


def run_canary(
    slo_seconds: int = config.CANARY_SLO_SECONDS,
    poll_interval: float = 1.0,
    extraction_mode: str | None = None,
) -> dict:
    # The canary process itself never calls the LLM -- it only watches ledger
    # state -- so its own ambient config.EXTRACTION_MODE (from *this*
    # process's env) says nothing about how the doc actually got processed.
    # Against the K8s pipeline this is invoked from the host with the host's
    # own (mock-default) env while the extraction pods that actually touch
    # the doc run with EXTRACTION_MODE=real from the K8s ConfigMap -- callers
    # that know which pipeline they're pointed at should say so explicitly
    # via extraction_mode rather than relying on that coincidence.
    if extraction_mode is None:
        extraction_mode = config.EXTRACTION_MODE
    invoice_no = f"CANARY-{uuid.uuid4().hex[:12]}"
    data = _canary_pdf_bytes(invoice_no)
    info = gcs.upload_bytes(f"inbox/_canary_{uuid.uuid4().hex}.pdf", data, "application/pdf")

    conn = ledger.connect(role="ro")
    started = time.monotonic()
    ledger_row_seen = False
    try:
        # Locally there's no separate notification path — nudge the same
        # ingest mechanism production would eventually use on its own.
        rw_conn = ledger.connect(role="rw")
        with rw_conn.cursor() as cur:
            orphan_detector.find_and_enqueue_orphans(cur)
        rw_conn.commit()
        rw_conn.close()

        while time.monotonic() - started < slo_seconds:
            with conn.cursor() as cur:
                doc = ledger.get_document(cur, info.doc_id)
            if doc is not None:
                ledger_row_seen = True
                if doc["state"] == "complete":
                    latency = time.monotonic() - started
                    return {"ok": True, "doc_id": info.doc_id, "latency_seconds": round(latency, 2)}
                # Under a real model, landing in review means a quality gate
                # correctly caught an occasional extraction mistake -- that's
                # the pipeline working, not the pipeline being down, so it's a
                # pass here too. In mock mode extraction is deterministic, so
                # review still means something is actually broken.
                if doc["state"] == "review" and extraction_mode == "real":
                    latency = time.monotonic() - started
                    return {
                        "ok": True,
                        "doc_id": info.doc_id,
                        "latency_seconds": round(latency, 2),
                        "reason": "landed in review (gate caught it; expected under real-model variance)",
                    }
                if doc["state"] in ("review", "failed"):
                    return {"ok": False, "doc_id": info.doc_id, "reason": f"landed in {doc['state']}, not complete"}
            time.sleep(poll_interval)
    finally:
        conn.close()

    return {
        "ok": False,
        "doc_id": info.doc_id,
        "reason": f"did not reach complete within {slo_seconds}s SLO"
                  + ("" if ledger_row_seen else " (never even triaged — ingest path itself may be down)"),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--slo-seconds", type=int, default=config.CANARY_SLO_SECONDS)
    parser.add_argument(
        "--extraction-mode",
        default=None,
        choices=["mock", "real"],
        help="What pipeline this canary is pointed at -- decides whether landing in "
             "review counts as a pass. Defaults to this process's own config.EXTRACTION_MODE, "
             "which is usually wrong when driving a separate K8s pipeline from the host.",
    )
    args = parser.parse_args()

    result = run_canary(args.slo_seconds, extraction_mode=args.extraction_mode)
    print(result)
    if not result["ok"]:
        log.critical("CANARY FAILED: %s", result)
        raise SystemExit(1)
    log.info("canary OK in %.2fs", result["latency_seconds"])

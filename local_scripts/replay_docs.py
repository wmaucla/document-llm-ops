#!/usr/bin/env python3
"""Injects N fresh synthetic documents into an already-running pipeline --
for exercising a live cluster (KEDA scaling, the extraction liveness probe,
sweeper redrive) repeatedly, without redeploying anything.

Each document gets a unique invoice_no and upload path (same trick
canary.py's synthetic_invoice_pdf_bytes() already uses) -- doc_id is derived
from a content checksum, so literally re-uploading the same fixture bytes
would just dedupe into a no-op instead of exercising anything new.

Fires and returns immediately; it doesn't wait for the documents to settle.
Check on them with `make summary` (host) / `kubectl exec deploy/docpipeline-triage
-- python local_scripts/summarize.py` (in-cluster), or `make canary` for a
single tracked round-trip.

  make replay-docs COUNT=5
  kubectl exec deploy/docpipeline-triage -- python local_scripts/replay_docs.py --count 5
"""
from __future__ import annotations

import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline.core import ledger
from docpipeline.infra import gcs
from docpipeline.reconciliation import canary
from docpipeline.reconciliation import orphan_detector_0 as orphan_detector


def inject(count: int) -> list[str]:
    doc_ids = []
    for _ in range(count):
        invoice_no = f"REPLAY-{uuid.uuid4().hex[:12]}"
        data = canary.synthetic_invoice_pdf_bytes(invoice_no)
        info = gcs.upload_bytes(f"inbox/_replay_{uuid.uuid4().hex}.pdf", data, "application/pdf")
        doc_ids.append(info.doc_id)

    # Locally there's no bucket-notification wiring -- nudge the same ingest
    # path production would eventually use on its own (see orphan_detector_0's
    # own docstring). Without this, the new objects just wait for the next
    # 10s poll, which is fine too, just slower to see results.
    conn = ledger.connect(role="rw")
    with conn.cursor() as cur:
        orphan_detector.find_and_enqueue_orphans(cur)
    conn.commit()
    conn.close()
    return doc_ids


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=3)
    args = parser.parse_args()
    doc_ids = inject(args.count)
    print(f"injected {len(doc_ids)} document(s):")
    for d in doc_ids:
        print(f"  {d}")

"""Fixture generator — 'Fixture set — generate them, don't hunt for them'.

Builds every fixture programmatically,
uploads it to inbox/ in fake-gcs-server, and — for documents with no real
text layer — registers ground-truth OCR text keyed by (doc_id, page_no) so
the mock OCR engine can answer deterministically once triage/pdf-worker/
ocr-shard route it through the real split/shard/join machinery.

Writes fixtures/generated/manifest.json: {name: {doc_id, gcs_path, ...}}.
Run: `uv run python3 fixtures/generate_fixtures.py` (needs the infra up —
`make fixtures` does this, or `make e2e`/`make e2e-k8s` as part of the
full run).
"""

from __future__ import annotations

import io
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pypdf import PdfWriter  # noqa: E402
from reportlab.lib.pagesizes import A4  # noqa: E402
from reportlab.pdfgen import canvas  # noqa: E402

from fixtures import content as fixture_content  # noqa: E402
from docpipeline.infra import gcs  # noqa: E402
from docpipeline.text import ocr_engine  # noqa: E402

MANIFEST_PATH = Path(__file__).resolve().parent / "generated" / "manifest.json"

# Caps how many of the 14 fixtures actually get uploaded — set by
# make e2e-k8s specifically, so the real-LLM path (one Ollama pod, effectively
# serial inference regardless of extraction replica count) doesn't have to
# drain all 14 real-mode-bound documents to prove itself. make e2e (host,
# mock) always uses the full 14 as the actual correctness proof.
FIXTURE_LIMIT = int(os.environ.get("FIXTURE_LIMIT", "0")) or None


def pdf_with_text_pages(pages_of_lines: list[list[str]]) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    _, height = A4
    for lines in pages_of_lines:
        y = height - 72
        for line in lines:
            c.drawString(72, y, line)
            y -= 16
        c.showPage()
    c.save()
    return buf.getvalue()


def blank_pdf(num_pages: int) -> bytes:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    for _ in range(num_pages):
        c.showPage()
    c.save()
    return buf.getvalue()


def encrypt(data: bytes, password: str = "secret") -> bytes:
    from pypdf import PdfReader

    reader = PdfReader(io.BytesIO(data))
    writer = PdfWriter()
    for page in reader.pages:
        writer.add_page(page)
    writer.encrypt(password)
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


def join_lines(lines: list[str]) -> str:
    return "\n".join(lines)


def build() -> dict:
    manifest: dict = {}

    def upload(name: str, data: bytes, content_type: str, ocr_pages_for: list[str] | None = None) -> None:
        if FIXTURE_LIMIT is not None and len(manifest) >= FIXTURE_LIMIT:
            print(f"  {name:28s} skipped (FIXTURE_LIMIT={FIXTURE_LIMIT})")
            return
        info = gcs.upload_bytes(f"inbox/{name}.bin" if content_type == "application/octet-stream" else f"inbox/{name}.pdf", data, content_type)
        if ocr_pages_for:
            for page_no, text in enumerate(ocr_pages_for):
                ocr_engine.register_page_text(info.doc_id, page_no, text)
        manifest[name] = {
            "doc_id": info.doc_id,
            "gcs_path": info.gcs_path,
            "size": info.size,
            "content_type": content_type,
        }
        print(f"  {name:28s} doc_id={info.doc_id} ({info.size} bytes)")

    print("digital PDF, clean text layer (tier-0, zero OCR):")
    upload("digital_clean", pdf_with_text_pages([fixture_content.CLEAN_INVOICE_LINES]), "application/pdf")

    print("digital PDF, garbage text layer (falls through to OCR):")
    # Page 0 has just enough for triage's loose sample check to say
    # has_text_layer=True; pages 1-4 are empty, dragging the *full-document*
    # density (pdf_worker's text_sanity gate) below the floor.
    garbage_bytes = pdf_with_text_pages([["Digits: 1234567890"], [], [], [], []])
    ocr_pages = [join_lines(fixture_content.GARBAGE_TEXT_LAYER_OCR_LINES)] + [""] * 4
    upload("digital_garbage_text_layer", garbage_bytes, "application/pdf", ocr_pages_for=ocr_pages)

    print("1-page scan (single-shard fast path):")
    one_page_lines = [
        "GLOBEX TRADING CO", "Invoice No: INV-10001", "Invoice Date: 2026-06-01",
        "Due Date: 2026-07-01", "Seller: Globex Trading Co", "Buyer: Contoso Manufacturing",
        "Currency: USD", "Line Item: Consulting | 800.00", "Subtotal: 800.00",
        "Tax: 0.00", "Total: 800.00",
    ]
    upload("one_page_scan", blank_pdf(1), "application/pdf", ocr_pages_for=[join_lines(one_page_lines)])

    print("3-page scan (forces split + 3 shards + the join):")
    page0 = ["INITECH CORP", "Invoice No: INV-30003", "Invoice Date: 2026-06-10",
             "Due Date: 2026-07-10", "Seller: Initech Corp", "Buyer: Contoso Manufacturing", "Currency: USD"]
    page1 = ["Line Item: Widget A | 1000.00", "Line Item: Widget B | 500.00"]
    page2 = ["Subtotal: 1500.00", "Tax: 0.00", "Total: 1500.00"]
    upload("three_page_scan", blank_pdf(3), "application/pdf",
           ocr_pages_for=[join_lines(page0), join_lines(page1), join_lines(page2)])

    print("25-page scan (exceeds the local page ceiling of 20 -> review):")
    upload("twenty_five_page_scan", blank_pdf(25), "application/pdf")

    print("encrypted PDF (-> review, must not crash the pod):")
    upload("encrypted_pdf", encrypt(pdf_with_text_pages([fixture_content.CLEAN_INVOICE_LINES])), "application/pdf")

    print("corrupt PDF (-> review, classified not propagated):")
    valid = pdf_with_text_pages([fixture_content.CLEAN_INVOICE_LINES])
    upload("corrupt_pdf", valid[: len(valid) // 2], "application/pdf")

    print("zero-byte object (-> failed, terminal, no retry):")
    upload("zero_byte", b"", "application/octet-stream")

    print("footer prompt-injection (grounding must pass, arithmetic must fail):")
    upload("injected_footer", pdf_with_text_pages([fixture_content.INJECTED_FOOTER_LINES]), "application/pdf")

    print("invoice with no line items (arithmetic -> inconclusive, must not auto-post):")
    upload("no_line_items", pdf_with_text_pages([fixture_content.NO_LINE_ITEMS_INVOICE_LINES]), "application/pdf")

    print("credit memo, negative total:")
    upload("credit_memo", pdf_with_text_pages([fixture_content.CREDIT_MEMO_LINES]), "application/pdf")

    print("rescanned duplicate (different bytes, same vendor+invoice_no -> business dedupe):")
    upload("rescanned_duplicate", pdf_with_text_pages([fixture_content.RESCANNED_DUPLICATE_LINES]), "application/pdf")

    print("EU locale amounts/dates (documented limitation — see README):")
    eu_lines = [
        "EUROPA GMBH", "Invoice No: INV-EU001", "Invoice Date: 2026-04-15",
        "Seller: Europa GmbH", "Buyer: Contoso Manufacturing", "Currency: EUR",
        "Total: 4.297,00 EUR",
    ]
    upload("eu_locale", pdf_with_text_pages([eu_lines]), "application/pdf")

    print("buyer/seller both present (role-assignment gap fixture — register")
    print("mock_llm 'swapped_roles' behaviour on this doc_id in your test):")
    upload("role_swap_candidate", pdf_with_text_pages([fixture_content.ROLE_SWAP_CANDIDATE_LINES]), "application/pdf")

    return manifest


def main() -> None:
    gcs.ensure_bucket()
    print(f"generating fixtures into gs://{gcs.config.GCS_BUCKET}/inbox/ ...")
    manifest = build()
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2))
    print(f"\nwrote {MANIFEST_PATH} ({len(manifest)} fixtures)")


if __name__ == "__main__":
    main()

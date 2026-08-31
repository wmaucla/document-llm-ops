"""'Business dedupe' driven through the real triage -> pdf-worker ->
extraction path (in-process, not via Kafka) for two documents with different
bytes but the same (vendor, invoice_no) — checksum dedupe cannot catch this,
only the unique index can."""

import uuid

from docpipeline import fixture_content
from docpipeline.core import ledger
from docpipeline.infra import gcs
from docpipeline.stages import extraction_4 as extraction, pdf_worker_2 as pdf_worker, triage_1 as triage
from fixtures.generate_fixtures import pdf_with_text_pages


def _unique_invoice_lines(suffix: str, extra: list[str] | None = None) -> list[str]:
    """A fresh, never-before-seen invoice_no per test run — this test's DB is
    the shared local dev instance, not a per-test sandbox, so reusing a fixed
    invoice_no would collide with whatever the fixture generator or a prior
    test run already committed as 'complete'."""
    lines = [
        "ACME INDUSTRIAL SUPPLY",
        f"Invoice No: INV-TEST-{suffix}",
        "Invoice Date: 2026-04-15",
        "Due Date: 2026-05-15",
        "Seller: Acme Industrial Supply",
        "Buyer: Contoso Manufacturing",
        "Currency: USD",
        "Line Item: Steel Brackets | 2500.00",
        "Line Item: Shipping | 150.00",
        "Line Item: Installation | 1647.00",
        "Subtotal: 4297.00",
        "Tax: 0.00",
        "Total: 4297.00",
    ]
    return lines + (extra or [])


def _upload(lines: list[str]):
    data = pdf_with_text_pages([lines])
    return gcs.upload_bytes(f"inbox/test-dedupe-{uuid.uuid4().hex}.pdf", data, "application/pdf")


def _run_to_extraction(conn, gcs_path: str, expected_doc_id: str) -> str:
    with conn.cursor() as cur:
        triage.handle_gcs_path(cur, gcs_path)
    conn.commit()
    with conn.cursor() as cur:
        pdf_worker.handle_text_embedded(cur, expected_doc_id, gcs_path)
    conn.commit()
    return extraction.handle_ocr_completed(conn, expected_doc_id)


def test_rescanned_duplicate_caught_by_business_dedupe_not_checksum(conn):
    suffix = uuid.uuid4().hex[:10]

    info1 = _upload(_unique_invoice_lines(suffix))
    result1 = _run_to_extraction(conn, info1.gcs_path, info1.doc_id)
    assert result1 == "complete"

    info2 = _upload(_unique_invoice_lines(suffix, extra=["Rescanned copy - same invoice, different bytes"]))
    assert info2.doc_id != info1.doc_id  # different bytes, different checksum

    result2 = _run_to_extraction(conn, info2.gcs_path, info2.doc_id)
    assert result2 != "complete"

    with conn.cursor() as cur:
        doc2 = ledger.get_document(cur, info2.doc_id)
    assert doc2["state"] == "review"
    assert doc2["gate_results"]["business_dedupe"]["outcome"] == "fail"
    assert doc2["gate_results"]["business_dedupe"]["detail"]["duplicate_of"] == info1.doc_id


def test_exact_duplicate_upload_is_checksum_deduped(conn):
    info = _upload(fixture_content.NO_LINE_ITEMS_INVOICE_LINES)

    with conn.cursor() as cur:
        first = triage.handle_gcs_path(cur, info.gcs_path)
    conn.commit()
    with conn.cursor() as cur:
        second = triage.handle_gcs_path(cur, info.gcs_path)  # re-upload, identical bytes
    conn.commit()

    assert first == "dispatched"
    assert second == "duplicate"
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM documents WHERE doc_id = %s", (info.doc_id,))
        assert cur.fetchone()["n"] == 1

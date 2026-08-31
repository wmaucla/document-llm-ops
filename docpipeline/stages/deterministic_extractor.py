"""The deterministic extraction backend.

Programmable per (doc_id, funnel_version, attempt_no) so every row in 'The
mock LLM is a real component' table is constructible by hand: grounding
failures, swapped roles, schema-invalid JSON, refusal, context overflow,
transient errors, and divergence between attempts.

Unregistered documents get a deterministic default extraction pulled
straight out of the fixture's own labelled text (`Invoice No: ...`, `Total:
$...`) — this is what makes the happy path work without registering every
single fixture by hand.
"""

from __future__ import annotations

import re

from docpipeline.stages.extractor import ExtractionError


_FIELD_PATTERNS = {
    "invoice_no": r"Invoice No:\s*([A-Za-z0-9\-]+)",
    "invoice_date": r"Invoice Date:\s*(\d{4}-\d{2}-\d{2})",
    "due_date": r"Due Date:\s*(\d{4}-\d{2}-\d{2})",
    "seller": r"Seller:\s*(.+)",
    "buyer": r"Buyer:\s*(.+)",
    "currency": r"Currency:\s*([A-Z]{3})",
    "iban": r"IBAN:\s*([A-Za-z0-9]+)",
}


def _money_to_cents(text: str) -> int:
    cleaned = text.strip().lstrip("$").strip()
    negative = cleaned.startswith("(") or cleaned.startswith("-")
    cleaned = cleaned.strip("()-").replace(",", "")
    cents = round(float(cleaned) * 100)
    return -cents if negative else cents


def default_extract(source_text: str) -> dict:
    fields: dict = {"doc_type": "invoice"}
    for key, pattern in _FIELD_PATTERNS.items():
        m = re.search(pattern, source_text)
        if m:
            fields[key] = m.group(1).strip()

    for label, key in (("Total", "total_cents"), ("Subtotal", "subtotal_cents"), ("Tax", "tax_cents")):
        m = re.search(rf"{label}:\s*\$?\(?-?[\d,]+\.\d{{2}}\)?", source_text)
        if m:
            fields[key] = _money_to_cents(m.group(0).split(":", 1)[1])

    line_items = [
        {"description": desc.strip(), "amount_cents": _money_to_cents(amount)}
        for desc, amount in re.findall(r"Line Item:\s*(.+?)\s*\|\s*(\$?-?[\d,]+\.\d{2})", source_text)
    ]
    if line_items:
        fields["line_items"] = line_items

    lowered = source_text.lower()
    if "credit memo" in lowered:
        fields["doc_type"] = "credit_memo"
    elif "receipt" in lowered:
        fields["doc_type"] = "receipt"
    return fields


class DeterministicExtractor:
    """Class-level registry: it must be reachable from whichever process
    registers a behaviour (fixtures/tests) and whichever process later
    consumes it (the extraction consumer) — see docpipeline.text.ocr_engine for
    the same pattern applied to OCR, file-backed for cross-process use.
    In-process (tests driving the consumer's functions directly) just uses
    the class dict directly, which is enough for v1."""

    _behaviors: dict[tuple, dict] = {}

    @classmethod
    def set_behavior(cls, doc_id: str, behavior: str, *, attempt_no: int | None = None, **kwargs) -> None:
        cls._behaviors[(doc_id, attempt_no)] = {"behavior": behavior, **kwargs}

    @classmethod
    def clear(cls) -> None:
        cls._behaviors.clear()

    @classmethod
    def _lookup(cls, doc_id: str, attempt_no: int) -> dict | None:
        for key in ((doc_id, attempt_no), (doc_id, None)):
            if key in cls._behaviors:
                return cls._behaviors[key]
        return None

    @classmethod
    def extract(cls, doc_id: str, tier: str, source_text: str, attempt_no: int) -> dict:
        spec = cls._lookup(doc_id, attempt_no) or {"behavior": "correct"}
        behavior = spec["behavior"]

        if behavior == "context_overflow":
            raise ExtractionError("context_overflow")
        if behavior == "refusal":
            raise ExtractionError("refusal")
        if behavior in ("429", "529", "timeout"):
            raise ExtractionError("transient", behavior)
        if behavior == "schema_invalid":
            return {"doc_type": "invoice", "total_cents": "not-a-number"}
        if behavior == "swapped_roles":
            fields = default_extract(source_text)
            fields["seller"], fields["buyer"] = fields.get("buyer"), fields.get("seller")
            return fields
        if behavior == "not_grounded":
            fields = default_extract(source_text)
            fields["total_cents"] = (fields.get("total_cents") or 0) + 999_999
            return fields
        if behavior == "injected_total":
            # Simulates a model swayed by an injected footer: the injected
            # value *is* in the text, so grounding passes; arithmetic must not.
            fields = default_extract(source_text)
            fields["total_cents"] = spec.get("total_cents", 1)
            return fields
        if behavior == "diverge_on_redelivery" and attempt_no and attempt_no >= 2:
            fields = default_extract(source_text)
            fields["total_cents"] = (fields.get("total_cents") or 0) + 100
            return fields
        return default_extract(source_text)

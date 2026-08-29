"""Extracted-field schema — Tier 1 of the extraction funnel (free, enforced
by the model layer before application code sees the response)."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ValidationError


class LineItem(BaseModel):
    description: str
    amount_cents: int


class ExtractedFields(BaseModel):
    doc_type: Literal["invoice", "receipt", "credit_memo"] = "invoice"
    invoice_no: str | None = None
    invoice_date: str | None = None
    due_date: str | None = None
    seller: str | None = None
    buyer: str | None = None
    currency: str = "USD"
    total_cents: int | None = None
    subtotal_cents: int | None = None
    tax_cents: int | None = None
    iban: str | None = None
    line_items: list[LineItem] | None = None


def validate_schema(raw: dict) -> tuple[str, dict | None, ExtractedFields | None]:
    """Returns (outcome, error_detail, model). outcome is 'pass' or 'fail'."""
    try:
        model = ExtractedFields.model_validate(raw)
        return "pass", None, model
    except ValidationError as exc:
        return "fail", {"errors": exc.errors(include_url=False)}, None

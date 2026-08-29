"""Quality gates — cheapest first, all deterministic (see 'Quality gates').

Four outcomes: pass / fail / inconclusive / not_applicable. `applies_to`
predicates only ever look at attributes known *before* extraction (here:
`doc.doc_type`, set at triage time from a keyword heuristic) — never the
model's own output, which is what makes arithmetic un-evadable by a model
that simply omits line items.
"""

from __future__ import annotations

import dataclasses
import datetime

from docpipeline import config

# gate_set_version 1 — see '3 · Gate outcome storage'.
GATE_SET_VERSION = 1
BLOCKING_GATES = {"grounding", "arithmetic", "business_dedupe"}
ON_INCONCLUSIVE = {
    "grounding": "block",
    "arithmetic": "block",
    "iban_mod97": "allow",
    "business_dedupe": "block",
}


@dataclasses.dataclass
class GateResult:
    outcome: str  # pass | fail | inconclusive | not_applicable
    detail: dict | None = None

    def to_json(self) -> dict:
        d: dict = {"outcome": self.outcome}
        if self.detail:
            d["detail"] = self.detail
        return d


def _money_reprs(cents: int) -> set[str]:
    dollars = cents / 100
    return {f"{dollars:.2f}", f"{dollars:,.2f}", f"{abs(dollars):.2f}", f"({abs(dollars):,.2f})"}


def grounding(fields: dict, source_text: str) -> GateResult:
    """Every extracted value appears in the source text. Catches
    hallucination directly — but NOT role assignment (see 'role-assignment
    gap'): a swapped seller/buyer both genuinely appear in the text."""
    text_lower = source_text.lower()
    missing = []
    for key in ("invoice_no", "seller"):
        val = fields.get(key)
        if val and str(val).lower() not in text_lower:
            missing.append(key)
    total_cents = fields.get("total_cents")
    if total_cents is not None and not any(r in source_text for r in _money_reprs(total_cents)):
        missing.append("total_cents")
    if missing:
        return GateResult("fail", {"missing_fields": missing})
    return GateResult("pass")


def arithmetic(doc: dict, fields: dict) -> GateResult:
    """applies_to doc_type == invoice — known before extraction, so a model
    cannot switch this off by omitting line_items (see 'applies_to must not
    be evadable')."""
    if doc.get("doc_type") != "invoice":
        return GateResult("not_applicable")
    line_items = fields.get("line_items")
    if not line_items:
        return GateResult("inconclusive", {"reason": "no_line_items"})
    computed_subtotal = sum(li["amount_cents"] for li in line_items)
    declared_subtotal = fields.get("subtotal_cents")
    tax = fields.get("tax_cents") or 0
    total = fields.get("total_cents")
    if declared_subtotal is not None and declared_subtotal != computed_subtotal:
        return GateResult("fail", {"reason": "subtotal_mismatch", "computed": computed_subtotal, "declared": declared_subtotal})
    if total is not None and total != computed_subtotal + tax:
        return GateResult("fail", {"reason": "total_mismatch", "computed": computed_subtotal + tax, "declared": total})
    return GateResult("pass")


def iban_mod97(fields: dict) -> GateResult:
    iban = fields.get("iban")
    if not iban:
        return GateResult("not_applicable")
    cleaned = iban.replace(" ", "").upper()
    try:
        rearranged = cleaned[4:] + cleaned[:4]
        numeric = "".join(str(int(c, 36)) for c in rearranged)
        ok = int(numeric) % 97 == 1
    except (ValueError, IndexError):
        return GateResult("fail", {"reason": "malformed_iban"})
    return GateResult("pass") if ok else GateResult("fail", {"reason": "checksum_mismatch"})


def completeness(actual_page_numbers: set[int], expected_page_count: int) -> GateResult:
    """assert the union of page_no values equals {0 .. page_count-1} exactly
    — no holes, no duplicates. Catches a shard that recorded success but
    wrote only some of its pages, which a shard *count* cannot."""
    expected = set(range(expected_page_count))
    if actual_page_numbers != expected:
        return GateResult("fail", {
            "missing_pages": sorted(expected - actual_page_numbers),
            "unexpected_pages": sorted(actual_page_numbers - expected),
        })
    return GateResult("pass")


def plausibility(doc: dict, fields: dict) -> GateResult:
    reasons = []
    total = fields.get("total_cents")
    if total is None:
        reasons.append("total_missing")
    elif doc.get("doc_type") == "credit_memo":
        if not (0 < abs(total) <= config.PLAUSIBLE_TOTAL_CEILING_CENTS):
            reasons.append("total_out_of_range")
    elif not (0 < total <= config.PLAUSIBLE_TOTAL_CEILING_CENTS):
        reasons.append("total_out_of_range")

    issue, due = fields.get("invoice_date"), fields.get("due_date")
    if issue and due:
        try:
            d_issue = datetime.date.fromisoformat(issue)
            d_due = datetime.date.fromisoformat(due)
            if not (d_issue <= d_due <= d_issue + datetime.timedelta(days=365)):
                reasons.append("due_date_out_of_range")
        except ValueError:
            reasons.append("unparseable_dates")

    return GateResult("fail", {"reasons": reasons}) if reasons else GateResult("pass")


def business_dedupe(cur, doc_id: str, vendor: str | None, invoice_no: str | None) -> GateResult:
    """Content-checksum dedupe (the PK) does not catch a rescanned/re-emailed
    duplicate — different bytes, same invoice. Only this catches it."""
    if not vendor or not invoice_no:
        return GateResult("not_applicable")
    cur.execute(
        "SELECT doc_id FROM documents WHERE vendor = %s AND invoice_no = %s AND doc_id != %s AND state = 'complete'",
        (vendor, invoice_no, doc_id),
    )
    row = cur.fetchone()
    return GateResult("fail", {"duplicate_of": row["doc_id"]}) if row else GateResult("pass")


def run_all(cur, doc: dict, fields: dict, source_text: str) -> dict[str, GateResult]:
    """Tier-2 gates run against a schema-valid extraction. Grounding runs
    first as the cheapest broad check; arithmetic is what actually defeats
    prompt injection (grounding alone does not — see 'Prompt injection')."""
    return {
        "grounding": grounding(fields, source_text),
        "arithmetic": arithmetic(doc, fields),
        "iban_mod97": iban_mod97(fields),
        "plausibility": plausibility(doc, fields),
        "business_dedupe": business_dedupe(cur, doc["doc_id"], fields.get("seller"), fields.get("invoice_no")),
    }


def classify_doc_type(sample_text: str) -> str:
    """Triage-time, keyword-based — deliberately cheap and deliberately
    *not* derived from the extraction model's own output (see 'applies_to
    must not be evadable by the thing being checked')."""
    lowered = sample_text.lower()
    if "credit memo" in lowered:
        return "credit_memo"
    if "receipt" in lowered:
        return "receipt"
    return "invoice"

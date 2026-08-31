"""Operator and R&D entry points — 'the whole design is keeping two lanes
separate'.

**Read-only lane** (`replay_documents`): connects as `pipeline_ro`, which has
no `INSERT` grant on `outbox` and no Kafka producer credentials — the same
grant absence `tests/test_read_only_role.py` proves directly. That single
missing grant is what makes this lane *structurally* incapable of affecting
production, not merely a function that happens not to call `enqueue`.

**Break-glass lane** (`force_redrive`, `bulk_redrive`, `set_kill_switch`):
connects as `pipeline_rw`, requires a non-empty reason, writes an audit row,
and enforces a blast-radius cap on bulk actions. `force_redrive` calls
`sweeper.redrive_document` — the same function the sweeper itself calls —
rather than a second write path, per 'Fewer distinct write paths means fewer
places for a write bug to live'.
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import artifact, gates, ledger, models
from docpipeline.reconciliation import sweeper
from docpipeline.stages import mock_llm

log = logging.getLogger(__name__)


class BreakGlassError(Exception):
    pass


class BlastRadiusExceeded(BreakGlassError):
    def __init__(self, matched: int, cap: int):
        super().__init__(f"predicate matched {matched} documents, cap is {cap} — pass approved=True to override")
        self.matched = matched
        self.cap = cap


def _require_reason(reason: str) -> None:
    if not reason or not reason.strip():
        raise BreakGlassError("a reason string is required for any break-glass action")


def _audit(cur, *, doc_id: str | None, action: str, reason: str, actor: str, detail: dict | None = None) -> None:
    cur.execute(
        "INSERT INTO break_glass_audit (doc_id, action, reason, actor, detail) VALUES (%s, %s, %s, %s, %s)",
        (doc_id, action, reason, actor, ledger.Json(detail) if detail else None),
    )


def _redrive_target_state(doc_id: str, row: dict) -> str:
    """review always re-drives to extract_pending (only state the design's
    own legal-transition table allows from review). failed can go to either
    text_pending or extract_pending — decided here by whether text
    production ever finished (the assembled artifact exists)."""
    current_state = row["state"]
    if current_state == "review":
        return "extract_pending"
    if current_state == "failed":
        if row["page_count"] is None:
            # Triage itself rejected this object (zero-byte, unsupported
            # MIME) before it ever reached text_pending — has_text_layer/
            # page_count were never set, so there is no state to route on.
            # This isn't a failed *attempt*, it's an unusable upload.
            raise BreakGlassError(
                f"{doc_id} failed at triage before classification (e.g. zero-byte or unreadable "
                "upload) — nothing to retry; it needs a fresh upload, not a re-drive"
            )
        return "extract_pending" if artifact.read_assembled(doc_id) is not None else "text_pending"
    raise BreakGlassError(f"{doc_id} is in {current_state!r}; force-redrive only applies to review/failed")


def force_redrive(doc_id: str, reason: str, actor: str, conn=None) -> dict:
    """'Force re-drive one doc' — precisely what the stuck-state sweeper
    already does, just invoked for a document the sweeper would not select
    on its own (review/failed, not merely stuck-and-in-flight)."""
    _require_reason(reason)
    owns_conn = conn is None
    conn = conn or ledger.connect(role="rw")
    try:
        with conn.cursor() as cur:
            row = ledger.get_document(cur, doc_id)
            if row is None:
                raise BreakGlassError(f"no such document: {doc_id}")
            target_state = _redrive_target_state(doc_id, row)

            from_state = row["state"]
            ledger.transition(cur, doc_id, target_state)
            row = dict(row)
            row["state"] = target_state
            sweeper.redrive_document(cur, row)  # the same function the sweeper itself calls

            _audit(cur, doc_id=doc_id, action="force_redrive", reason=reason, actor=actor,
                   detail={"from_state": from_state, "to_state": target_state})
        conn.commit()
        log.warning("break-glass force_redrive doc=%s -> %s (actor=%s, reason=%s)", doc_id, target_state, actor, reason)
        return {"doc_id": doc_id, "redriven_to": target_state}
    finally:
        if owns_conn:
            conn.close()


def accept_review(doc_id: str, reason: str, actor: str, conn=None) -> dict:
    """'Accept a document the gates rejected' — the human-judgement override.

    This is the ONLY path from `review` to `complete`, and the only edge into
    `complete` that does not mean "a worker ran and the blocking gates passed".
    It exists because a person who reviews a document and finds the extraction
    correct previously had nowhere to record that: re-driving it would just fail
    the same gates again, so the document sat in `review` forever.

    Two deliberate constraints. It stamps `gate_results.operator_override`, so a
    `complete` document always states whether it earned that or was granted it —
    without this, an override is indistinguishable downstream from a genuine
    pass, and `complete` quietly stops meaning what it says. And it enqueues
    `document.extracted` exactly as the normal path does, so an accepted
    document really does post: an override that silently withheld the side
    effect would leave `complete` documents that never reach
    `posted_documents`, which is the kind of quiet divergence this pipeline
    exists to avoid.
    """
    _require_reason(reason)
    owns_conn = conn is None
    conn = conn or ledger.connect(role="rw")
    try:
        with conn.cursor() as cur:
            row = ledger.get_document(cur, doc_id)
            if row is None:
                raise BreakGlassError(f"no such document: {doc_id}")
            if row["state"] != "review":
                raise BreakGlassError(
                    f"{doc_id} is in {row['state']!r}, not 'review' — accept_review only "
                    "overrides a gate decision. Use force_redrive for anything else."
                )

            gate_results = dict(row["gate_results"] or {})
            gate_results["operator_override"] = {
                "outcome": "accepted",
                "detail": {"actor": actor, "reason": reason, "was": "review"},
            }
            ledger.set_gate_results(cur, doc_id, gate_results)
            ledger.transition(cur, doc_id, "complete", from_states={"review"})
            ledger.enqueue(cur, doc_id, "document.extracted",
                           {"doc_id": doc_id, "operator_override": True})
            _audit(cur, doc_id=doc_id, action="accept_review", reason=reason, actor=actor,
                   detail={"from_state": "review", "to_state": "complete"})
        conn.commit()
        log.warning("break-glass accept_review doc=%s -> complete (actor=%s, reason=%s)",
                    doc_id, actor, reason)
        return {"doc_id": doc_id, "state": "complete", "operator_override": True}
    finally:
        if owns_conn:
            conn.close()


def bulk_redrive(predicate_sql: str, reason: str, actor: str, *,
                  cap: int = config.BREAK_GLASS_BLAST_RADIUS_CAP, approved: bool = False) -> dict:
    """'Bulk re-drive after a fix'. `predicate_sql` is a raw SQL WHERE clause
    — acceptable here because this is operator tooling invoked deliberately
    by a human with database access already, never a user-facing input path.

    Blast-radius cap is mandatory for the same reason `batch_cap` is on the
    sweeper: a mistyped predicate must not re-drive every document. Above
    the cap, `approved=True` stands in for the design's 'Argo suspend step
    for a second approver'.
    """
    _require_reason(reason)
    conn = ledger.connect(role="rw")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT doc_id FROM documents WHERE {predicate_sql}")  # noqa: S608 — trusted operator input
            matched = [r["doc_id"] for r in cur.fetchall()]

        if len(matched) > cap and not approved:
            raise BlastRadiusExceeded(len(matched), cap)

        redriven, failed = [], []
        for doc_id in matched:
            try:
                force_redrive(doc_id, reason, actor, conn=conn)
                redriven.append(doc_id)
            except BreakGlassError as exc:
                failed.append({"doc_id": doc_id, "error": str(exc)})

        with conn.cursor() as cur:
            _audit(cur, doc_id=None, action="bulk_redrive", reason=reason, actor=actor,
                   detail={"predicate": predicate_sql, "matched": len(matched),
                           "redriven": len(redriven), "failed": failed})
        conn.commit()
        return {"matched": len(matched), "redriven": redriven, "failed": failed}
    finally:
        conn.close()


def set_kill_switch(enabled: bool, reason: str, actor: str) -> None:
    """The auto-post kill switch. Degrades, does not stop: processing keeps
    running normally — see extraction.handle_ocr_completed's own check of
    this flag — only the auto-post decision is affected."""
    _require_reason(reason)
    conn = ledger.connect(role="rw")
    try:
        with conn.cursor() as cur:
            ledger.set_feature_flag(cur, "auto_post_enabled", enabled)
            _audit(cur, doc_id=None, action="kill_switch_toggle", reason=reason, actor=actor,
                   detail={"auto_post_enabled": enabled})
        conn.commit()
        log.warning("break-glass kill switch: auto_post_enabled=%s (actor=%s, reason=%s)", enabled, actor, reason)
    finally:
        conn.close()


def replay_documents(doc_ids: list[str], tier: str = "cheap") -> list[dict]:
    """Read-only lane: a bake-off/single-document-probe/prompt-experiment
    vehicle. Connects as pipeline_ro — SELECT on the ledger and GCS reads
    only. Runs the *same* gates production uses (the free referee) and
    returns scored results; writes nothing to outbox or the ledger.

    Because assembled OCR text lives at a deterministic, never-deleted path,
    this costs only the LLM call per document — no re-OCR, no re-splitting.
    """
    conn = ledger.connect(role="ro")
    results = []
    try:
        with conn.cursor() as cur:
            for doc_id in doc_ids:
                doc = ledger.get_document(cur, doc_id)
                if doc is None:
                    results.append({"doc_id": doc_id, "path": "replay", "error": "not_found"})
                    continue
                # Read-only: never call ensure_assembled — it can *write* a
                # reassembled artifact to ocr/, which is out of scope for
                # this lane (GCS writes confined to experiments/). If the
                # artifact isn't there yet, this document just isn't
                # replayable yet.
                assembled = artifact.read_assembled(doc_id)
                if assembled is None:
                    results.append({"doc_id": doc_id, "path": "replay", "error": "not_yet_assembled"})
                    continue
                source_text = "\n".join(p["text"] for p in sorted(assembled["pages"], key=lambda p: p["page_no"]))
                try:
                    raw = mock_llm.MockLLM.extract(doc_id, tier, source_text, attempt_no=0)
                except mock_llm.ExtractionError as exc:
                    results.append({"doc_id": doc_id, "path": "replay", "tier": tier, "error": exc.kind})
                    continue
                outcome, detail, model = models.validate_schema(raw)
                fields = model.model_dump(exclude_none=True) if model else {}
                gate_results = {name: g.to_json() for name, g in gates.run_all(cur, doc, fields, source_text).items()} \
                    if model else {"schema": {"outcome": outcome, "detail": detail}}
                results.append({
                    "doc_id": doc_id, "path": "replay", "tier": tier,
                    "fields": fields, "gates": gate_results,
                })
    finally:
        conn.close()
    return results


def _cli() -> None:
    parser = argparse.ArgumentParser(description="Break-glass / replay operator CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("redrive")
    p.add_argument("doc_id")
    p.add_argument("--reason", required=True)
    p.add_argument("--actor", required=True)

    p = sub.add_parser("bulk-redrive")
    p.add_argument("predicate_sql")
    p.add_argument("--reason", required=True)
    p.add_argument("--actor", required=True)
    p.add_argument("--approved", action="store_true")

    p = sub.add_parser("kill-switch")
    p.add_argument("state", choices=["on", "off"])
    p.add_argument("--reason", required=True)
    p.add_argument("--actor", required=True)

    p = sub.add_parser("replay")
    p.add_argument("doc_ids", nargs="+")
    p.add_argument("--tier", default="cheap")

    args = parser.parse_args()
    if args.cmd == "redrive":
        print(force_redrive(args.doc_id, args.reason, args.actor))
    elif args.cmd == "bulk-redrive":
        print(bulk_redrive(args.predicate_sql, args.reason, args.actor, approved=args.approved))
    elif args.cmd == "kill-switch":
        set_kill_switch(args.state == "on", args.reason, args.actor)
    elif args.cmd == "replay":
        for r in replay_documents(args.doc_ids, tier=args.tier):
            print(r)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    _cli()

"""STAGE 2 · EXTRACTION FUNNEL.

Assembly is a lazily-materialised precondition of extraction (no extra topic,
no extra deployment): ensure the canonical artifact exists, then run the
funnel — layout cache (stub, always a miss in v1) -> cheap tier -> strong
tier, gated at every step, first-writer-wins on commit.

Assembly failure is its own non-escalating failure class: a transient GCS
error retries the read and never reaches the model tiers; a genuinely
missing shard output is a completeness failure, routed straight to review.
"""

from __future__ import annotations

import logging

import psycopg
from google.api_core.exceptions import NotFound

from docpipeline import config
from docpipeline.core import artifact, gates, ledger, models
from docpipeline.infra import kafka_utils
from docpipeline.stages import llm_client, mock_llm

log = logging.getLogger(__name__)

CONSUMER_GROUP = "extraction"


def _call_model(doc_id: str, tier: str, source_text: str, attempt_no: int, repair_hint: str | None = None) -> dict:
    """Dispatches to the mock (steps 0-7, deterministic) or the real LiteLLM
    gateway (step 8) — same exception contract either way, so the tier/repair
    loop above doesn't need to know which one is running."""
    if config.EXTRACTION_MODE == "real":
        return llm_client.extract(tier, source_text, repair_hint=repair_hint)
    return mock_llm.MockLLM.extract(doc_id, tier, source_text, attempt_no)


class MissingShardOutput(Exception):
    def __init__(self, doc_id: str, shard_idx: int):
        super().__init__(f"{doc_id}: shard {shard_idx} output object missing")
        self.doc_id = doc_id
        self.shard_idx = shard_idx


def ensure_assembled(doc_id: str, shards_total: int) -> dict:
    """Check-then-skip. For a tier-0 (pypdf) document the artifact already
    exists and this is a no-op; for sharded OCR it performs the actual
    reassembly — see 'Assembly is a lazily-materialised precondition'."""
    existing = artifact.read_assembled(doc_id)
    if existing is not None:
        return existing

    pages = []
    for idx in range(shards_total):
        try:
            shard = artifact.read_shard_output(doc_id, idx)
        except NotFound as exc:
            raise MissingShardOutput(doc_id, idx) from exc
        pages.extend(shard["pages"])
    pages.sort(key=lambda p: p["page_no"])
    artifact.write_assembled(doc_id, producer=f"ocr-{config.OCR_ENGINE}", producer_version="v1", pages=pages)
    return artifact.read_assembled(doc_id)


def run_funnel(cur, doc: dict, source_text: str, attempt_no: int) -> tuple[dict | None, dict]:
    """Returns (fields, gate_results). fields is None if the funnel was
    exhausted without a clean pass (route to review)."""
    doc_id = doc["doc_id"]
    gate_results: dict = {"layout_cache": gates.GateResult("not_applicable", {"reason": "v1_stub_always_miss"}).to_json()}

    for tier in ("cheap", "strong"):
        raw = None
        schema_result_json = None
        repair_hint = None
        for _repair in range(config.MAX_REPAIR_ATTEMPTS + 1):
            try:
                raw = _call_model(doc_id, tier, source_text, attempt_no, repair_hint=repair_hint)
            except mock_llm.ExtractionError as exc:
                if exc.kind == "refusal":
                    gate_results["schema"] = {"outcome": "fail", "detail": {"reason": "refusal"}}
                    return None, gate_results  # no retry, straight to review
                if exc.kind == "context_overflow":
                    # deterministic — never retry this tier; escalate once
                    gate_results["schema"] = {"outcome": "fail", "detail": {"reason": "context_overflow"}}
                    raw = None
                    break
                if exc.kind == "unparseable":
                    # the real model's equivalent of a schema-invalid reply —
                    # a repair rung, not a transport retry.
                    schema_result_json = {"outcome": "fail", "detail": {"reason": "unparseable", "raw": exc.args[0]}}
                    ledger.increment_attempts(cur, doc_id, "repair_attempts")
                    repair_hint = f"response was not valid JSON: {exc.args[0]}"
                    raw = None
                    continue
                # transient (429/529/timeout): bounded inline retry, same tier
                ledger.log_attempt(cur, doc_id, "extraction", attempt_no, producer_or_model=tier,
                                    outcome="retry", error_class="transient", error_msg=exc.kind)
                continue

            outcome, detail, model = models.validate_schema(raw)
            schema_result_json = {"outcome": outcome, **({"detail": detail} if detail else {})}
            if outcome == "pass":
                break
            ledger.increment_attempts(cur, doc_id, "repair_attempts")
            repair_hint = f"schema errors: {detail}"
            raw = None  # schema repair rung: ask again (same tier, next loop iteration)

        if schema_result_json:
            gate_results["schema"] = schema_result_json
        if raw is None:
            continue  # escalate to the next tier

        fields = model.model_dump(exclude_none=True)
        tier_gates = gates.run_all(cur, doc, fields, source_text)
        gate_results.update({name: g.to_json() for name, g in tier_gates.items()})
        gate_results["tier_used"] = tier

        blocking_fail = any(g.outcome == "fail" for name, g in tier_gates.items() if name in gates.BLOCKING_GATES)
        blocking_inconclusive = any(
            g.outcome == "inconclusive" and gates.ON_INCONCLUSIVE.get(name) == "block"
            for name, g in tier_gates.items()
        )
        if not blocking_fail and not blocking_inconclusive:
            return fields, gate_results  # eligible for auto-post

    return None, gate_results  # funnel exhausted -> review


def handle_ocr_completed(conn: psycopg.Connection, doc_id: str) -> str:
    with conn.cursor() as cur:
        ledger.transition(cur, doc_id, "extract_running", from_states={"extract_pending"})
    conn.commit()

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
        if doc is not None:
            ledger.stamp_build_info(cur, doc_id, config.BUILD_SHA, config.PROMPT_VERSION)
    conn.commit()
    if doc is None:
        return "unknown_doc"

    try:
        assembled = ensure_assembled(doc_id, doc["shards_total"])
    except MissingShardOutput as exc:
        gate_results = {"completeness": gates.GateResult(
            "fail", {"reason": "shard_output_missing", "shard_idx": exc.shard_idx}
        ).to_json()}
        with conn.cursor() as cur:
            ledger.route_without_writing(cur, doc_id, gate_results, "review")
        conn.commit()
        return "review:shard_output_missing"
    # Any other exception (transient GCS failure) propagates: no offset
    # commit, Kafka redelivers, the read is retried — model tiers are never
    # reached, so no escalation happens on an infrastructure problem.

    page_numbers = {p["page_no"] for p in assembled["pages"]}
    completeness = gates.completeness(page_numbers, doc["page_count"])
    if completeness.outcome != "pass":
        with conn.cursor() as cur:
            ledger.route_without_writing(cur, doc_id, {"completeness": completeness.to_json()}, "review")
        conn.commit()
        return "review:incomplete"

    source_text = "\n".join(p["text"] for p in sorted(assembled["pages"], key=lambda p: p["page_no"]))

    with conn.cursor() as cur:
        attempt_no = ledger.increment_attempts(cur, doc_id, "extract_attempts")
        ledger.reset_repair_attempts(cur, doc_id)
    conn.commit()

    with conn.cursor() as cur:
        fields, gate_results = run_funnel(cur, doc, source_text, attempt_no)
    conn.commit()

    if fields is None:
        with conn.cursor() as cur:
            ledger.commit_extraction_result(cur, doc_id, {}, gate_results, "review")
        conn.commit()
        return "review:gates_exhausted"

    with conn.cursor() as cur:
        auto_post_enabled = ledger.get_feature_flag(cur, "auto_post_enabled", default=True)

    if not auto_post_enabled:
        # 'The kill switch degrades, it does not stop': processing (OCR,
        # extraction, gates) keeps running exactly as normal — only the
        # auto-post decision is overridden. vendor/invoice_no are
        # deliberately left unset here (only set on the actual posting path
        # below), since the business_dedupe unique index has no state
        # filter and would otherwise let a held-back review wrongly block a
        # different, legitimate document from later posting.
        gate_results["kill_switch"] = {"outcome": "fail", "detail": {"reason": "auto_post_disabled"}}
        with conn.cursor() as cur:
            ledger.commit_extraction_result(cur, doc_id, fields, gate_results, "review")
        conn.commit()
        return "review:kill_switch"

    payload = {
        "doc_id": doc_id,
        "event": "document.extracted",
        "route": "auto_post",
        "producer": assembled["producer"],
        "funnel_version": config.FUNNEL_VERSION,
        "fields": fields,
        "gates": gate_results,
    }
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE documents SET vendor = %s, invoice_no = %s WHERE doc_id = %s",
                (fields.get("seller"), fields.get("invoice_no"), doc_id),
            )
            won = ledger.commit_extraction_result(cur, doc_id, fields, gate_results, "complete", payload)
        conn.commit()
    except psycopg.errors.UniqueViolation:
        conn.rollback()
        with conn.cursor() as cur:
            gate_results["business_dedupe"] = gates.GateResult("fail", {"reason": "unique_violation_at_commit"}).to_json()
            ledger.route_without_writing(cur, doc_id, gate_results, "review")
        conn.commit()
        return "review:duplicate"

    if not won:
        with conn.cursor() as cur:
            current = ledger.get_document(cur, doc_id)
        if current and current["extraction_result"] != fields:
            log.warning("extract_divergence_detected doc=%s", doc_id)
        return "discarded:not_first_writer"

    return "complete"


def run_forever() -> None:
    conn = ledger.connect(role="rw")
    consumer = kafka_utils.make_consumer(CONSUMER_GROUP, ["ocr.completed"])
    log.info("extraction consumer started")
    while True:
        payload, msg = kafka_utils.poll_json(consumer)
        if payload is None:
            continue
        try:
            result = handle_ocr_completed(conn, payload["doc_id"])
            log.info("extraction %s -> %s", payload["doc_id"], result)
            consumer.commit(msg)
        except ledger.IllegalTransition:
            # A duplicate/stale redelivery of ocr.completed whose document has
            # already moved past extract_pending (e.g. a race with the
            # sweeper's own redrive) — not a failed attempt, and retrying it
            # can never succeed. Commit past it rather than redeliver forever;
            # confirmed live as a poison-message loop that starved every
            # other message in the consumer group of processing time.
            conn.rollback()
            log.info("extraction skipping stale redelivery for %s", payload)
            consumer.commit(msg)
        except Exception:
            conn.rollback()
            log.exception("extraction failed on %s (no offset commit; will redeliver)", payload)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    run_forever()

"""Step 9b — operator lanes. 'Break-glass demands a reason', 'Blast-radius
cap holds', and 'Kill switch degrades, doesn't stop'."""

import pytest

from docpipeline.core import ledger
from docpipeline.reconciliation import operator
from tests.conftest import insert_document


def _to_review(cur, doc_id: str) -> None:
    insert_document(cur, doc_id, state="text_pending")
    ledger.transition(cur, doc_id, "text_running")
    ledger.transition(cur, doc_id, "review")


def _to_failed_never_assembled(cur, doc_id: str) -> None:
    insert_document(cur, doc_id, state="text_pending")
    ledger.transition(cur, doc_id, "text_running")
    ledger.transition(cur, doc_id, "failed")


def test_force_redrive_requires_a_reason(conn, doc_id):
    with conn.cursor() as cur:
        _to_review(cur, doc_id)
    conn.commit()

    with pytest.raises(operator.BreakGlassError):
        operator.force_redrive(doc_id, "", "wmaucla")
    with pytest.raises(operator.BreakGlassError):
        operator.force_redrive(doc_id, "   ", "wmaucla")


def test_force_redrive_from_review_targets_extract_pending(conn, doc_id):
    with conn.cursor() as cur:
        _to_review(cur, doc_id)
    conn.commit()

    result = operator.force_redrive(doc_id, "human approved re-extraction", "wmaucla", conn=conn)
    assert result["redriven_to"] == "extract_pending"

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
        cur.execute(
            "SELECT reason, actor, action FROM break_glass_audit WHERE doc_id = %s", (doc_id,)
        )
        audit = cur.fetchone()
    assert doc["state"] == "extract_pending"
    assert audit["action"] == "force_redrive"
    assert audit["reason"] == "human approved re-extraction"
    assert audit["actor"] == "wmaucla"


def test_force_redrive_from_failed_without_assembly_targets_text_pending(conn, doc_id):
    with conn.cursor() as cur:
        _to_failed_never_assembled(cur, doc_id)
    conn.commit()

    result = operator.force_redrive(doc_id, "fixed the upstream bug", "wmaucla", conn=conn)
    assert result["redriven_to"] == "text_pending"


def test_force_redrive_rejects_in_flight_states(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
    conn.commit()

    with pytest.raises(operator.BreakGlassError):
        operator.force_redrive(doc_id, "should not be allowed", "wmaucla", conn=conn)


def test_bulk_redrive_enforces_blast_radius_cap(conn):
    doc_ids = [f"test-blast-{i}" for i in range(10)]
    with conn.cursor() as cur:
        for d in doc_ids:
            _to_failed_never_assembled(cur, d)
    conn.commit()

    with pytest.raises(operator.BlastRadiusExceeded) as exc_info:
        operator.bulk_redrive("doc_id LIKE 'test-blast-%'", "batch fix", "wmaucla", cap=5)
    assert exc_info.value.matched == 10
    assert exc_info.value.cap == 5

    # unchanged — the cap must block before touching anything
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) AS n FROM documents WHERE doc_id LIKE 'test-blast-%' AND state = 'failed'")
        assert cur.fetchone()["n"] == 10


def test_bulk_redrive_approved_overrides_the_cap(conn):
    doc_ids = [f"test-blastok-{i}" for i in range(10)]
    with conn.cursor() as cur:
        for d in doc_ids:
            _to_failed_never_assembled(cur, d)
    conn.commit()

    result = operator.bulk_redrive(
        "doc_id LIKE 'test-blastok-%'", "batch fix, reviewed", "wmaucla", cap=5, approved=True
    )
    assert result["matched"] == 10
    assert len(result["redriven"]) == 10


def test_kill_switch_requires_a_reason():
    with pytest.raises(operator.BreakGlassError):
        operator.set_kill_switch(False, "", "wmaucla")


def test_kill_switch_toggles_the_feature_flag(conn):
    operator.set_kill_switch(False, "suspected bad prompt version", "wmaucla")
    with conn.cursor() as cur:
        assert ledger.get_feature_flag(cur, "auto_post_enabled") is False

    operator.set_kill_switch(True, "incident resolved", "wmaucla")
    with conn.cursor() as cur:
        assert ledger.get_feature_flag(cur, "auto_post_enabled") is True


def test_replay_never_triggers_assembly_as_a_side_effect(conn, doc_id):
    """The read-only lane's replay function never calls enqueue/commit — but
    the real guarantee is the missing grant (test_read_only_role.py). This
    confirms replay() itself doesn't even attempt a GCS write to assemble a
    missing artifact (out of scope for pipeline_ro — see 'GCS writes
    confined to a experiments/ prefix'); it just reports the document isn't
    replayable yet."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_pending", shards_total=2, has_text_layer=False)
    conn.commit()

    results = operator.replay_documents([doc_id])
    assert results == [{"doc_id": doc_id, "path": "replay", "error": "not_yet_assembled"}]


def test_accept_review_requires_a_reason(conn, doc_id):
    with conn.cursor() as cur:
        _to_review(cur, doc_id)
    conn.commit()

    with pytest.raises(operator.BreakGlassError):
        operator.accept_review(doc_id, "", "wmaucla")


def test_accept_review_marks_complete_and_records_the_override(conn, doc_id):
    """The only path from review to complete. It must leave evidence: a
    `complete` document has to say whether it earned that or was granted it."""
    with conn.cursor() as cur:
        _to_review(cur, doc_id)
    conn.commit()

    result = operator.accept_review(doc_id, "totals verified by hand", "wmaucla", conn=conn)
    assert result["state"] == "complete"

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
        cur.execute("SELECT action, reason FROM break_glass_audit WHERE doc_id = %s", (doc_id,))
        audit = cur.fetchone()
        cur.execute("SELECT topic FROM outbox WHERE doc_id = %s ORDER BY id DESC LIMIT 1", (doc_id,))
        outbox_row = cur.fetchone()

    assert doc["state"] == "complete"
    override = (doc["gate_results"] or {}).get("operator_override")
    assert override is not None, "an override must be distinguishable from a genuine gate pass"
    assert override["detail"]["actor"] == "wmaucla"
    assert audit["action"] == "accept_review"
    # It must actually post, or `complete` diverges from posted_documents.
    assert outbox_row["topic"] == "document.extracted"


def test_accept_review_refuses_anything_not_in_review(conn, doc_id):
    """Guards the narrow meaning of this lane: it overrides a *gate* decision.
    A failed document has a different problem and a different remedy."""
    with conn.cursor() as cur:
        _to_review(cur, doc_id)
        ledger.transition(cur, doc_id, "extract_pending")
    conn.commit()

    with pytest.raises(operator.BreakGlassError, match="not 'review'"):
        operator.accept_review(doc_id, "should not work", "wmaucla", conn=conn)

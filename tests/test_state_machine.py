"""'Illegal state transitions rejected' — the invariant that kills most
illegal transitions: a _running state may only be entered from its own
_pending state, and `complete` has no outbound transitions."""

import pytest

from docpipeline.core import ledger
from tests.conftest import insert_document


def test_skipping_text_production_is_rejected(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
    conn.commit()
    with conn.cursor() as cur, pytest.raises(ledger.IllegalTransition):
        ledger.transition(cur, doc_id, "extract_pending", from_states={"text_running"})


def test_running_state_only_enterable_from_its_own_pending(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_pending")
    conn.commit()
    with conn.cursor() as cur, pytest.raises(ledger.IllegalTransition):
        ledger.transition(cur, doc_id, "text_running", from_states={"text_pending"})


def test_complete_has_no_outbound_transitions(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_running")
        ledger.transition(cur, doc_id, "complete")
    conn.commit()
    with conn.cursor() as cur, pytest.raises(ledger.IllegalTransition):
        ledger.transition(cur, doc_id, "extract_pending", idempotent=False)


def test_valid_transition_succeeds(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        result = ledger.transition(cur, doc_id, "text_running", from_states={"text_pending"})
    assert result == "text_running"


def test_redrive_transition_is_idempotent(conn, doc_id):
    """Every re-drive targets *_pending, never *_running — and re-driving an
    already-pending document must be a harmless no-op, not an error."""
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
        result = ledger.transition(cur, doc_id, "text_pending")
    assert result == "text_pending"

"""Dead man's switch — 'alerting on absence'. Every error-rate alert is
blind to total silence; this checks completion rate directly."""

import pytest

from docpipeline.core import ledger
from docpipeline.reconciliation import deadmans_switch
from tests.conftest import insert_document


@pytest.fixture(autouse=True)
def _isolated_documents_table(conn):
    """check_liveness is a genuinely system-wide, wall-clock-windowed query
    by design — it has to be, that's what a dead man's switch checks. That
    makes it untestable against the shared session state other test files
    leave behind (their 'complete' rows are still inside any generous
    window). Truncate locally so these three tests see only their own data."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE documents, document_shards CASCADE")
    conn.commit()


def test_healthy_when_nothing_ingested():
    """A genuinely idle system (no ingest, nothing in flight) is not a
    failure — see canary.py for the check that actively drives traffic."""
    result = deadmans_switch.check_liveness(window_seconds=60)
    assert result["healthy"] is True


def test_unhealthy_when_ingested_but_nothing_completed(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="text_pending")
    conn.commit()

    result = deadmans_switch.check_liveness(window_seconds=60)
    assert result["healthy"] is False
    assert result["completed_in_window"] == 0
    assert result["ingested_in_window"] >= 1


def test_healthy_when_completions_are_happening(conn, doc_id):
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_running")
        ledger.transition(cur, doc_id, "complete")
    conn.commit()

    result = deadmans_switch.check_liveness(window_seconds=60)
    assert result["healthy"] is True
    assert result["completed_in_window"] >= 1

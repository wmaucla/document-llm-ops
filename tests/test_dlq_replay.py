"""Reconciler ③ — 'DLQ replay gating' — replay WHERE build_sha/prompt_version
changed since the attempt that landed the document in failed."""

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.reconciliation import dlq_replay
from tests.conftest import insert_document


def _failed_doc(cur, doc_id: str, build_sha: str, prompt_version: str) -> None:
    insert_document(cur, doc_id, state="text_pending")
    ledger.transition(cur, doc_id, "text_running")
    ledger.transition(cur, doc_id, "failed")
    ledger.stamp_build_info(cur, doc_id, build_sha, prompt_version)


def test_unchanged_build_is_a_noop(conn, doc_id):
    """Replaying against unchanged code reproduces the failure at cost,
    forever — so nothing happens when nothing changed."""
    with conn.cursor() as cur:
        _failed_doc(cur, doc_id, config.BUILD_SHA, config.PROMPT_VERSION)
    conn.commit()

    result = dlq_replay.run(dry_run=True)
    assert doc_id not in result["would_replay"]


def test_changed_build_sha_is_replayed(conn, doc_id):
    with conn.cursor() as cur:
        _failed_doc(cur, doc_id, "old-sha-123", config.PROMPT_VERSION)
    conn.commit()

    dry = dlq_replay.run(dry_run=True)
    assert doc_id in dry["would_replay"]

    result = dlq_replay.run()
    assert doc_id in result["replayed"]

    with conn.cursor() as cur:
        doc = ledger.get_document(cur, doc_id)
    assert doc["state"] != "failed"


def test_changed_prompt_version_alone_is_also_replayed(conn, doc_id):
    """build_sha alone is insufficient — a prompt change can fix a document
    without a code change."""
    with conn.cursor() as cur:
        _failed_doc(cur, doc_id, config.BUILD_SHA, "invoice-extract@v99")
    conn.commit()

    dry = dlq_replay.run(dry_run=True)
    assert doc_id in dry["would_replay"]


def test_second_replay_after_same_version_failure_is_a_noop(conn, doc_id):
    """Anything that fails again on the new version stays in the DLQ."""
    with conn.cursor() as cur:
        _failed_doc(cur, doc_id, "old-sha-123", config.PROMPT_VERSION)
    conn.commit()

    dlq_replay.run()  # first replay: build_sha differs, re-drives it

    # simulate it failing again, now stamped with the *current* build
    with conn.cursor() as cur:
        ledger.transition(cur, doc_id, "text_running")
        ledger.transition(cur, doc_id, "failed")
        ledger.stamp_build_info(cur, doc_id, config.BUILD_SHA, config.PROMPT_VERSION)
    conn.commit()

    dry = dlq_replay.run(dry_run=True)
    assert doc_id not in dry["would_replay"]

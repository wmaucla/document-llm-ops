"""These tests run against real, live infra (Postgres/Redpanda/fake-gcs-server
via docker-compose) — not mocks. The ON CONFLICT / SKIP LOCKED / row-lock
semantics under test are exactly the thing a mock would paper over.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline import config  # noqa: E402
from docpipeline.core import ledger  # noqa: E402
from docpipeline.stages import deterministic_extractor  # noqa: E402


@pytest.fixture(scope="session", autouse=True)
def _force_mock_extraction_mode():
    """The suite's determinism depends on the mock model — the K8s ConfigMap
    now defaults EXTRACTION_MODE=real in-cluster, and a stray real value in
    the shell environment (e.g. left over from a `kubectl port-forward`
    session) must not silently make test_extraction_funnel.py's assertions
    flaky. test_real_llm_integration.py overrides this per-test via its own
    monkeypatch, which still wins over this session-level default."""
    original = config.EXTRACTION_MODE
    config.EXTRACTION_MODE = "mock"
    yield
    config.EXTRACTION_MODE = original


def _truncate_ledger() -> None:
    # pipeline_rw doesn't own the outbox/attempt_log sequences, so RESTART
    # IDENTITY (used by the superuser-run reset task) isn't available here.
    c = ledger.connect(role="rw", autocommit=True)
    with c.cursor() as cur:
        cur.execute(
            "TRUNCATE documents, document_shards, outbox, attempt_log, posted_documents, "
            "break_glass_audit CASCADE"
        )
        cur.execute("UPDATE feature_flags SET value = true WHERE key = 'auto_post_enabled'")
    c.close()


@pytest.fixture(scope="session", autouse=True)
def _clean_ledger_before_session():
    """This suite runs against the shared local dev Postgres, not a per-run
    sandbox — without the before-truncate, a previous pytest invocation's (or
    a manual fixtures/run_local.py smoke test's) leftover 'complete' rows
    collide with this session's business_dedupe checks on the next run.
    Without the after-truncate, every test-inserted 'test-*' document (blast
    tests, batch-cap tests, ...) sits forever in whatever non-terminal state
    that specific unit test left it in — harmless in isolation, but confirmed
    live to make `summarize.py` (now a real pass/fail gate — see AGENT.md's
    bug register) wrongly report a completely unrelated `make e2e-k8s`
    or `make summary` run as stuck, when it's actually just this suite's own
    debris from an unrelated earlier `make test`."""
    _truncate_ledger()
    yield
    _truncate_ledger()


@pytest.fixture
def conn():
    c = ledger.connect(role="rw")
    yield c
    c.rollback()
    c.close()


@pytest.fixture
def doc_id():
    return f"test-{uuid.uuid4().hex[:16]}"


@pytest.fixture(autouse=True)
def _clear_deterministic_extractor():
    yield
    deterministic_extractor.DeterministicExtractor.clear()


def insert_document(cur, doc_id: str, **overrides) -> None:
    fields = dict(
        gcs_path=f"gs://test-bucket/{doc_id}.pdf",
        state="text_pending",
        page_count=1,
        has_text_layer=True,
        shards_total=1,
    )
    fields.update(overrides)
    gcs_path = fields.pop("gcs_path")
    state = fields.pop("state")
    ledger.insert_initial_document(cur, doc_id, gcs_path, state=state, **fields)

"""Step 8 integration check — talks to the real LiteLLM gateway (the sibling
mlops-llm-repo's `litellm` Deployment) and a real small model on Ollama.

Skipped by default: a single call to a CPU-only llama3.2:1b under load took
~170s in verification, so this can't be part of the fast default suite
without making every `pytest tests/` run minutes long. Run explicitly with:

    kubectl port-forward svc/litellm 4000:4000 &
    RUN_REAL_LLM_TESTS=1 uv run pytest tests/test_real_llm_integration.py -v -s --timeout=300
"""

import os

import pytest

from docpipeline.stages import extraction_4 as extraction, llm_client
from tests.conftest import insert_document

pytestmark = pytest.mark.skipif(
    not os.environ.get("RUN_REAL_LLM_TESTS"),
    reason="slow (real model call, ~1-3 min/tier under load) and needs `kubectl port-forward svc/litellm 4000:4000` — set RUN_REAL_LLM_TESTS=1 to run",
)

CLEAN_TEXT = (
    "Invoice No: INV-REAL-1\nSeller: Acme\nBuyer: Contoso\n"
    "Line Item: Widget | 100.00\nSubtotal: 100.00\nTax: 0.00\nTotal: 100.00\n"
)


def test_llm_client_returns_parseable_json():
    result = llm_client.extract("cheap", CLEAN_TEXT)
    assert isinstance(result, dict)
    assert "doc_type" in result


def test_real_mode_reaches_a_terminal_state(conn, doc_id, monkeypatch):
    """Doesn't assert 'complete' — a 1B model's output is genuinely
    unreliable (see this file's module docstring and the README's 'What's
    verified' real-mode note) — only that the funnel completes the loop and
    lands somewhere terminal-ish without hanging or crashing."""
    from docpipeline import config
    from docpipeline.core import artifact

    monkeypatch.setattr(config, "EXTRACTION_MODE", "real")
    artifact.write_assembled(doc_id, "pypdf-text", "v1", [{"page_no": 0, "text": CLEAN_TEXT}])
    with conn.cursor() as cur:
        insert_document(cur, doc_id, state="extract_pending", page_count=1, has_text_layer=True)
    conn.commit()

    result = extraction.handle_ocr_completed(conn, doc_id)
    assert result in ("complete", "review:gates_exhausted", "review:kill_switch")

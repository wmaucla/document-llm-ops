"""Synthetic canary — 'the only check that verifies the pipeline as a
system'. The happy path needs real consumers running end to end (proved
live — see README's 'What's verified'), and this suite can't assume it's the
only thing connected to the shared infra (K8s deployment pods may legitimately
be live against the same Postgres/GCS/Kafka). So the failure path is tested
hermetically: the ingest nudge and the ledger lookup are mocked at the
boundary, rather than relying on nothing else touching the bucket.
"""

from unittest.mock import patch

from docpipeline import config
from docpipeline.reconciliation import canary


def test_canary_fails_loudly_when_nothing_processes_it():
    with patch("docpipeline.reconciliation.canary.orphan_detector.find_and_enqueue_orphans", return_value=0), \
         patch("docpipeline.reconciliation.canary.ledger.get_document", return_value=None):
        result = canary.run_canary(slo_seconds=2, poll_interval=0.5)

    assert result["ok"] is False
    assert "never even triaged" in result["reason"]


def test_canary_review_passes_under_real_mode_via_config_default(monkeypatch):
    monkeypatch.setattr(config, "EXTRACTION_MODE", "real")
    with patch("docpipeline.reconciliation.canary.orphan_detector.find_and_enqueue_orphans", return_value=0), \
         patch("docpipeline.reconciliation.canary.ledger.get_document", return_value={"state": "review"}):
        result = canary.run_canary(slo_seconds=2, poll_interval=0.5)

    assert result["ok"] is True
    assert "review" in result["reason"]


def test_canary_review_still_fails_under_mock_mode_via_config_default(monkeypatch):
    monkeypatch.setattr(config, "EXTRACTION_MODE", "mock")
    with patch("docpipeline.reconciliation.canary.orphan_detector.find_and_enqueue_orphans", return_value=0), \
         patch("docpipeline.reconciliation.canary.ledger.get_document", return_value={"state": "review"}):
        result = canary.run_canary(slo_seconds=2, poll_interval=0.5)

    assert result["ok"] is False
    assert "landed in review" in result["reason"]


def test_canary_explicit_extraction_mode_overrides_ambient_config(monkeypatch):
    # Confirmed-live bug: the canary process runs on the host under its own
    # (mock-default) env while a separate K8s pipeline actually processes the
    # doc with EXTRACTION_MODE=real -- an explicit extraction_mode= must win
    # over whatever config.EXTRACTION_MODE happens to be in *this* process,
    # or every ansible-driven e2e-k8s canary run wrongly fails on `review`.
    monkeypatch.setattr(config, "EXTRACTION_MODE", "mock")
    with patch("docpipeline.reconciliation.canary.orphan_detector.find_and_enqueue_orphans", return_value=0), \
         patch("docpipeline.reconciliation.canary.ledger.get_document", return_value={"state": "review"}):
        result = canary.run_canary(slo_seconds=2, poll_interval=0.5, extraction_mode="real")

    assert result["ok"] is True
    assert "review" in result["reason"]

"""Synthetic canary — 'the only check that verifies the pipeline as a
system'. The happy path needs real consumers running end to end (proved
live — see README's 'What's verified'), and this suite can't assume it's the
only thing connected to the shared infra (K8s deployment pods may legitimately
be live against the same Postgres/GCS/Kafka). So the failure path is tested
hermetically: the ingest nudge and the ledger lookup are mocked at the
boundary, rather than relying on nothing else touching the bucket.
"""

from unittest.mock import patch

from docpipeline.reconciliation import canary


def test_canary_fails_loudly_when_nothing_processes_it():
    with patch("docpipeline.reconciliation.canary.orphan_detector.find_and_enqueue_orphans", return_value=0), \
         patch("docpipeline.reconciliation.canary.ledger.get_document", return_value=None):
        result = canary.run_canary(slo_seconds=2, poll_interval=0.5)

    assert result["ok"] is False
    assert "never even triaged" in result["reason"]

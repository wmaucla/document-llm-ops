# fixtures/

`generate_fixtures.py` builds and uploads every fixture in the design doc's fixture table
programmatically (never hand-authored PDFs) — `FIXTURE_LIMIT` env var caps how many upload (the
full 14 for `make e2e`'s host loop, 3 for `make e2e-k8s`'s real-LLM path, since all inference
serializes on one Ollama pod regardless of extraction replica count).

For fixtures with no real text layer, it also registers ground-truth OCR text keyed by
`(doc_id, page_no)` via [`../docpipeline/text/ocr_engine.py`](../docpipeline/text/README.md), so
`MockOcrEngine` can answer deterministically once triage/pdf-worker/ocr-shard route the document
through the real split/shard/join machinery — the OCR mechanism is exercised for real, only the
OCR *engine* is mocked.

`generated/` (gitignored) holds the run's manifest (`{name: {doc_id, gcs_path, ...}}`) and the
mock-OCR ground-truth registry.

**Ordering gotcha:** fixture generation must happen *before* the K8s image is built for
`make e2e-k8s`, not after — the mock-OCR registry is baked into the image at build time. If you
regenerate fixtures after `make image`, rebuild the image again before redeploying.

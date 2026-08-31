# docpipeline/text/

Text/OCR helpers shared across *multiple* stages — not a stage's own logic, which is why this
isn't part of `stages/`. Moved out deliberately: both files were living next to the 5 numbered
sequential-step files, making them look like "just another stage" when they're cross-cutting
support code instead.

| File | What it is | Shared by |
|---|---|---|
| `pdf_utils.py` | Text production — tier-0 pypdf extraction, physical page-range splitting, MIME/encryption/corruption checks | `stages/triage_1.py`, `stages/pdf_worker_2.py` |
| `ocr_engine.py` | The OCR engine abstraction — `MockOcrEngine` (default, deterministic, keyed by `(doc_id, page_no)`) and `TesseractOcrEngine` (opt-in via `OCR_ENGINE=tesseract`) | `stages/ocr_shard_3.py`, and `fixtures/generate_fixtures.py` (registers ground-truth OCR text for fixtures) |

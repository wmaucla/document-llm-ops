"""OCR engine abstraction — 'The OCR engine locally — two tiers, and mostly
a mock'.

Tier A (default): a deterministic mock keyed by the checksum of the rendered
page image — same idea as the doc's `otiai10/ocrserver`-fronting stub, just
in-process instead of a second container, since the point is pathological
inputs constructed by hand, not a real HTTP boundary.

Tier B: real Tesseract via pytesseract, for realism checks only.
"""

from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

from docpipeline import config
from docpipeline.infra import gcs

# Top-level fixtures/, not docpipeline/fixtures/ -- this used to resolve one
# directory too high. Read and write agreed, so host mode never noticed, but
# `make reset`'s registry clear targets the top-level file and was a no-op.
DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parent.parent.parent / "fixtures" / "generated" / "mock_ocr_registry.json"


def _resolve(location: str | Path | None) -> str:
    """Where the registry lives: a gs:// URI or a local path.

    Local is fine only where the fixture generator and the OCR workers share a
    filesystem. In k8s they are different pods, so values.yaml points this at
    GCS -- with a local path there, ocr-shard reads a stale copy baked into the
    image and every OCR document extracts from "unregistered page" (AGENT.md
    bug #9).
    """
    if location is not None:
        return str(location)
    return config.MOCK_OCR_REGISTRY_URI or str(DEFAULT_REGISTRY_PATH)


def _read_registry(location: str) -> dict:
    if location.startswith("gs://"):
        return json.loads(gcs.download_bytes(location)) if gcs.exists(location) else {}
    path = Path(location)
    return json.loads(path.read_text()) if path.exists() else {}


def _write_registry(location: str, registry: dict) -> None:
    data = json.dumps(registry).encode()
    if location.startswith("gs://"):
        gcs.upload_bytes(gcs.path_for(location), data, "application/json")
        return
    path = Path(location)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def image_checksum(image_bytes: bytes) -> str:
    return hashlib.sha256(image_bytes).hexdigest()


class OcrEngine:
    def ocr_page(self, doc_id: str, page_no: int, image_bytes: bytes) -> tuple[str, float]:
        raise NotImplementedError


class MockOcrEngine(OcrEngine):
    """Keyed primarily by (doc_id, page_no) rather than the rendered image's
    checksum — an image-checksum-keyed stub would key on the
    input object's checksum, but a PDF-embedded placeholder page re-rasterises
    to different bytes than whatever the fixture generator rendered at
    generation time (recompression, DPI, colour space), so doc_id/page_no is
    the robust local substitute. Falls back to an image checksum for ad hoc
    registration when the caller doesn't have a doc_id yet."""

    def __init__(self, location: str | Path | None = None):
        self.location = _resolve(location)
        self._cache: dict | None = None

    def _load(self) -> dict:
        # Cached for this engine's lifetime -- get_engine() is called once per
        # shard message, so this is one fetch per message rather than per page.
        # Deliberately not process-global: fixtures can be regenerated under a
        # long-lived consumer, and a stale cache there is silent wrong text.
        if self._cache is None:
            self._cache = _read_registry(self.location)
        return self._cache

    def ocr_page(self, doc_id: str, page_no: int, image_bytes: bytes) -> tuple[str, float]:
        registry = self._load()
        entry = registry.get(f"{doc_id}:{page_no}") or registry.get(image_checksum(image_bytes))
        if entry:
            return entry["text"], entry.get("confidence", 0.95)
        return f"[mock-ocr:{doc_id[:12]}:{page_no}] unregistered page", 0.5


def register_mock_ocr_page(doc_id: str, page_no: int, text: str, confidence: float = 0.95,
                            location: str | Path | None = None) -> None:
    loc = _resolve(location)
    registry = _read_registry(loc)
    registry[f"{doc_id}:{page_no}"] = {"text": text, "confidence": confidence}
    _write_registry(loc, registry)


class TesseractOcrEngine(OcrEngine):
    def ocr_page(self, doc_id: str, page_no: int, image_bytes: bytes) -> tuple[str, float]:
        import pytesseract
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes))
        text = pytesseract.image_to_string(img)
        return text, 0.85  # pytesseract has no cheap overall-confidence figure


def get_engine() -> OcrEngine:
    if config.OCR_ENGINE == "tesseract":
        return TesseractOcrEngine()
    return MockOcrEngine()

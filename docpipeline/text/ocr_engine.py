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

DEFAULT_REGISTRY_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "generated" / "mock_ocr_registry.json"


def image_checksum(image_bytes: bytes) -> str:
    return hashlib.sha256(image_bytes).hexdigest()


class OcrEngine:
    def ocr_page(self, doc_id: str, page_no: int, image_bytes: bytes) -> tuple[str, float]:
        raise NotImplementedError


class MockOcrEngine(OcrEngine):
    """Keyed primarily by (doc_id, page_no) rather than the rendered image's
    checksum — the design doc's `otiai10/ocrserver`-fronting stub keys on the
    input object's checksum, but a PDF-embedded placeholder page re-rasterises
    to different bytes than whatever the fixture generator rendered at
    generation time (recompression, DPI, colour space), so doc_id/page_no is
    the robust local substitute. Falls back to an image checksum for ad hoc
    registration when the caller doesn't have a doc_id yet."""

    def __init__(self, registry_path: Path = DEFAULT_REGISTRY_PATH):
        self.registry_path = registry_path

    def _load(self) -> dict:
        if self.registry_path.exists():
            return json.loads(self.registry_path.read_text())
        return {}

    def ocr_page(self, doc_id: str, page_no: int, image_bytes: bytes) -> tuple[str, float]:
        registry = self._load()
        entry = registry.get(f"{doc_id}:{page_no}") or registry.get(image_checksum(image_bytes))
        if entry:
            return entry["text"], entry.get("confidence", 0.95)
        return f"[mock-ocr:{doc_id[:12]}:{page_no}] unregistered page", 0.5


def _load_registry(registry_path: Path) -> dict:
    if registry_path.exists():
        return json.loads(registry_path.read_text())
    return {}


def _save_registry(registry_path: Path, registry: dict) -> None:
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(json.dumps(registry))


def register_mock_ocr_page(doc_id: str, page_no: int, text: str, confidence: float = 0.95,
                            registry_path: Path = DEFAULT_REGISTRY_PATH) -> None:
    registry = _load_registry(registry_path)
    registry[f"{doc_id}:{page_no}"] = {"text": text, "confidence": confidence}
    _save_registry(registry_path, registry)


def register_mock_ocr_image(image_bytes: bytes, text: str, confidence: float = 0.95,
                             registry_path: Path = DEFAULT_REGISTRY_PATH) -> None:
    registry = _load_registry(registry_path)
    registry[image_checksum(image_bytes)] = {"text": text, "confidence": confidence}
    _save_registry(registry_path, registry)


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

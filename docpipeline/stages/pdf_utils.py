"""Shared PDF helpers used by triage and pdf_worker.

Kept separate from both so the "triage stays pure classification, never does
a real parse" rule (see 'Why not collapse further') is visible in the
module boundary, not just a comment: triage only ever calls the cheap
`read_page_count`/`sample_text` helpers here, never `split_pages`.
"""

from __future__ import annotations

import io

from pypdf import PdfReader, PdfWriter

ALLOWED_MIME_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
}

MIN_TOKEN_DENSITY_PER_PAGE = 15  # chars/page over the WHOLE doc; the deep gate (text_sanity)
TRIAGE_MIN_SAMPLE_CHARS = 5      # cheap routing heuristic only, over a 3-page sample


class EncryptedPdfError(Exception):
    pass


class CorruptPdfError(Exception):
    pass


def _reader(data: bytes) -> PdfReader:
    try:
        reader = PdfReader(io.BytesIO(data))
        if reader.is_encrypted:
            raise EncryptedPdfError()
        _ = len(reader.pages)  # forces parsing the page tree
        return reader
    except EncryptedPdfError:
        raise
    except Exception as exc:  # pypdf raises a variety of exception types on garbage input
        raise CorruptPdfError(str(exc)) from exc


def read_page_count(data: bytes, content_type: str) -> int:
    if content_type == "application/pdf":
        return len(_reader(data).pages)
    return 1  # single image = one page (multi-page TIFF not supported in v1)


def sample_text(data: bytes, content_type: str, max_pages: int = 3) -> str:
    """Cheap sample used by triage's has_text_layer heuristic. The deeper
    check (text-sanity gate) lives in pdf_worker, run only on the tier-0
    path."""
    if content_type != "application/pdf":
        return ""
    reader = _reader(data)
    pages = reader.pages[:max_pages]
    return "\n".join(p.extract_text() or "" for p in pages)


def has_text_layer(data: bytes, content_type: str) -> bool:
    """Triage's cheap routing hint — deliberately loose. It only decides
    which topic to dispatch to; the deep check (text_sanity, run by
    pdf_worker over the *whole* document) is what actually gates the tier-0
    path, and a document can pass this and still fail that one (a garbage or
    partial text layer — see 'Guard against the trap')."""
    text = sample_text(data, content_type)
    if not text:
        return False
    has_digit = any(c.isdigit() for c in text)
    return len(text.strip()) >= TRIAGE_MIN_SAMPLE_CHARS and has_digit


def text_sanity(pages_text: list[str]) -> tuple[bool, str]:
    """The gate tier-0 falls through on. Same shape as every other gate here:
    attempt the free path, verify exactly, fall through on failure."""
    if not pages_text:
        return False, "no_pages"
    total_chars = sum(len(t.strip()) for t in pages_text)
    density = total_chars / len(pages_text)
    has_digit = any(c.isdigit() for t in pages_text for c in t)
    if density < MIN_TOKEN_DENSITY_PER_PAGE:
        return False, f"token_density_{density:.1f}_below_floor"
    if not has_digit:
        return False, "no_digits_present"
    return True, "ok"


def split_pages(data: bytes, page_start: int, page_end: int) -> bytes:
    """Physical split — a real sub-PDF per shard, not a byte-range slice (see
    'Physical split, not logical'). page_end is exclusive."""
    reader = PdfReader(io.BytesIO(data))
    writer = PdfWriter()
    for i in range(page_start, page_end):
        writer.add_page(reader.pages[i])
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def extract_embedded_text(data: bytes) -> list[str]:
    reader = PdfReader(io.BytesIO(data))
    return [p.extract_text() or "" for p in reader.pages]

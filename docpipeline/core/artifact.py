"""Deterministic GCS paths and the canonical text-production artifact.

Every producer — pypdf tier-0 or sharded OCR — writes the same schema to the
same deterministic path (see 'Text production — the producer contract').
Only `page_no` and `text` are required per page; everything else is
producer-optional.
"""

from __future__ import annotations

import json

from docpipeline import config
from docpipeline.infra import gcs


def assembled_path(doc_id: str) -> str:
    return f"gs://{config.GCS_BUCKET}/ocr/{doc_id}.json"


def split_shard_path(doc_id: str, shard_idx: int) -> str:
    return f"gs://{config.GCS_BUCKET}/shards/{doc_id}/{shard_idx:04d}.pdf"


def shard_output_path(doc_id: str, shard_idx: int) -> str:
    return f"gs://{config.GCS_BUCKET}/shards_out/{doc_id}/{shard_idx:04d}.json"


def write_assembled(doc_id: str, producer: str, producer_version: str, pages: list[dict]) -> None:
    doc = {
        "doc_id": doc_id,
        "producer": producer,
        "producer_version": producer_version,
        "pages": pages,
    }
    gcs.upload_bytes(gcs.path_for(assembled_path(doc_id)), json.dumps(doc).encode(), "application/json")


def read_assembled(doc_id: str) -> dict | None:
    if not gcs.exists(assembled_path(doc_id)):
        return None
    return json.loads(gcs.download_bytes(assembled_path(doc_id)))


def write_shard_output(doc_id: str, shard_idx: int, pages: list[dict]) -> None:
    payload = {"shard_idx": shard_idx, "pages": pages}
    gcs.upload_bytes(gcs.path_for(shard_output_path(doc_id, shard_idx)), json.dumps(payload).encode(), "application/json")


def read_shard_output(doc_id: str, shard_idx: int) -> dict:
    return json.loads(gcs.download_bytes(shard_output_path(doc_id, shard_idx)))

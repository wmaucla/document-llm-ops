"""Storage wrapper over fake-gcs-server.

Locally this points at fake-gcs-server via STORAGE_EMULATOR_HOST — the
google-cloud-storage client picks that env var up itself, so there is no
application-code branch for "local vs prod" here, matching the design doc's
"Tool substitutions" table.

doc_id is derived from the GCS-provided crc32c checksum, never a SHA-256 of
the object (see "Content-checksum keying is load-bearing" and the caveat
about md5Hash being absent on composite objects).
"""

from __future__ import annotations

import base64
import dataclasses

from google.api_core.exceptions import NotFound
from google.cloud import storage

from docpipeline import config

_client: storage.Client | None = None


def client() -> storage.Client:
    global _client
    if _client is None:
        _client = storage.Client(project=config.GOOGLE_CLOUD_PROJECT)
    return _client


def ensure_bucket(name: str = config.GCS_BUCKET) -> storage.Bucket:
    c = client()
    bucket = c.bucket(name)
    if not bucket.exists(timeout=config.GCS_TIMEOUT_SECONDS):
        bucket = c.create_bucket(name, timeout=config.GCS_TIMEOUT_SECONDS)
    return bucket


def crc32c_to_doc_id(crc32c_b64: str) -> str:
    raw = base64.b64decode(crc32c_b64)
    return f"crc32c-{raw.hex()}"


@dataclasses.dataclass
class ObjectInfo:
    doc_id: str
    gcs_path: str  # gs://bucket/name
    size: int
    content_type: str | None


def upload_bytes(path: str, data: bytes, content_type: str = "application/octet-stream") -> ObjectInfo:
    """path is bucket-relative, e.g. 'inbox/foo.pdf'."""
    bucket = ensure_bucket()
    blob = bucket.blob(path)
    blob.upload_from_string(data, content_type=content_type, timeout=config.GCS_TIMEOUT_SECONDS)
    blob.reload(timeout=config.GCS_TIMEOUT_SECONDS)  # populate crc32c after upload
    if not blob.crc32c:
        raise RuntimeError(
            f"fake-gcs-server did not return crc32c for {path} — doc_id derivation "
            "depends on this; see the design doc's day-1 verification note."
        )
    return ObjectInfo(
        doc_id=crc32c_to_doc_id(blob.crc32c),
        gcs_path=f"gs://{config.GCS_BUCKET}/{path}",
        size=blob.size or len(data),
        content_type=content_type,
    )


def download_bytes(gcs_path: str) -> bytes:
    bucket_name, blob_name = parse_gcs_path(gcs_path)
    bucket = client().bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.download_as_bytes(timeout=config.GCS_TIMEOUT_SECONDS)


def exists(gcs_path: str) -> bool:
    bucket_name, blob_name = parse_gcs_path(gcs_path)
    return client().bucket(bucket_name).blob(blob_name).exists(timeout=config.GCS_TIMEOUT_SECONDS)


def object_info(gcs_path: str) -> ObjectInfo:
    bucket_name, blob_name = parse_gcs_path(gcs_path)
    blob = client().bucket(bucket_name).blob(blob_name)
    try:
        blob.reload(timeout=config.GCS_TIMEOUT_SECONDS)
    except NotFound as exc:
        raise FileNotFoundError(gcs_path) from exc
    return ObjectInfo(
        doc_id=crc32c_to_doc_id(blob.crc32c),
        gcs_path=gcs_path,
        size=blob.size,
        content_type=blob.content_type,
    )


def list_paths(prefix: str) -> list[str]:
    bucket = ensure_bucket()
    return [f"gs://{config.GCS_BUCKET}/{b.name}" for b in bucket.list_blobs(prefix=prefix, timeout=config.GCS_TIMEOUT_SECONDS)]


def delete_prefix(prefix: str) -> None:
    bucket = ensure_bucket()
    for blob in bucket.list_blobs(prefix=prefix, timeout=config.GCS_TIMEOUT_SECONDS):
        blob.delete(timeout=config.GCS_TIMEOUT_SECONDS)


def parse_gcs_path(gcs_path: str) -> tuple[str, str]:
    assert gcs_path.startswith("gs://"), gcs_path
    rest = gcs_path[len("gs://") :]
    bucket_name, _, blob_name = rest.partition("/")
    return bucket_name, blob_name


def path_for(gcs_path: str) -> str:
    """Bucket-relative path from a gs:// URI."""
    return parse_gcs_path(gcs_path)[1]

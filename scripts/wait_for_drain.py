#!/usr/bin/env python3
"""Polls the ledger until every document has reached a terminal state
(complete/review/failed) or a timeout elapses. Used by `make e2e` so it
doesn't have to guess a fixed sleep duration.

The expected count comes from fixtures/generated/manifest.json when present
— NOT from `SELECT count(*) FROM documents` at start-of-poll. The orphan
detector (the local ingest path) only scans every
ORPHAN_DETECTOR_INTERVAL_SECONDS; snapshotting "whatever's in the table so
far" as the target lets this script declare victory after only the first
few objects have even been triaged, while the rest are still waiting to be
discovered. Falls back to requiring the observed total to be stable across
two consecutive polls when there's no manifest to read.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline.core import ledger

TERMINAL = ("complete", "review", "failed")
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "generated" / "manifest.json"


def expected_count() -> int | None:
    if MANIFEST_PATH.exists():
        return len(json.loads(MANIFEST_PATH.read_text()))
    return None


def main(timeout_seconds: int = 120, poll_seconds: float = 2.0) -> int:
    conn = ledger.connect(role="ro")
    started = time.monotonic()
    target = expected_count()
    last_total = None

    while time.monotonic() - started < timeout_seconds:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) AS n FROM documents")
            total = cur.fetchone()["n"]
            cur.execute("SELECT count(*) AS n FROM documents WHERE state = ANY(%s)", (list(TERMINAL),))
            done = cur.fetchone()["n"]

        effective_target = target if target is not None else total
        stable = target is not None or total == last_total
        print(f"{done}/{total} terminal (target={effective_target}, stable={stable}) "
              f"({time.monotonic() - started:.0f}s elapsed)")

        if total == 0:
            pass  # orphan detector hasn't discovered anything yet — keep waiting
        elif total >= effective_target and stable and done == total:
            return 0

        last_total = total
        time.sleep(poll_seconds)

    print(f"TIMEOUT after {timeout_seconds}s — {done}/{total} terminal (target={effective_target})")
    return 1


if __name__ == "__main__":
    timeout = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    sys.exit(main(timeout))

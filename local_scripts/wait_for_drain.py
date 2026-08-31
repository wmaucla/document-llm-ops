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
discovered.

Without a manifest the fallback is a *time-based* quiet period, not a count
of polls. In k8s there is never a manifest -- it is written by the one-shot
fixtures Job into its own container, so the pod running this has no copy --
so this fallback is the only thing guarding every in-cluster drain. Two
consecutive polls used to be enough, which at 2s each is a 4s window against
a 10s ingest interval: a document could be discovered *after* the check
declared victory. Confirmed live 2026-08-31, a replay drain reported "7/7
settled" while an 8th document had not yet been ingested. The total must now
hold steady for longer than one full ingest cycle before it counts as drained.
"""
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from docpipeline import config
from docpipeline.core import ledger

TERMINAL = ("complete", "review", "failed")
MANIFEST_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "generated" / "manifest.json"
# Two full ingest cycles: one to catch a document already in flight, one of
# genuine quiet. Derived rather than a literal so it tracks the poll interval.
QUIET_SECONDS = config.ORPHAN_DETECTOR_INTERVAL_SECONDS * 2


def expected_count() -> int | None:
    if MANIFEST_PATH.exists():
        return len(json.loads(MANIFEST_PATH.read_text()))
    return None


def main(timeout_seconds: int = 120, poll_seconds: float = 2.0) -> int:
    conn = ledger.connect(role="ro")
    started = time.monotonic()
    target = expected_count()
    last_total = None
    last_change_at = time.monotonic()
    total = done = 0
    effective_target: int | None = target

    # See AGENT.md's "Connection-leak discipline". This polls for up to 900s
    # inside a long-lived pod via `kubectl exec`; an exception partway through
    # would otherwise strand an open connection for the pod's lifetime.
    try:
        while time.monotonic() - started < timeout_seconds:
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) AS n FROM documents")
                total = cur.fetchone()["n"]
                cur.execute("SELECT count(*) AS n FROM documents WHERE state = ANY(%s)", (list(TERMINAL),))
                done = cur.fetchone()["n"]

            if total != last_total:
                last_change_at = time.monotonic()
            quiet_for = time.monotonic() - last_change_at

            effective_target = target if target is not None else total
            # A manifest is authoritative about how many documents to expect;
            # without one, only a quiet period longer than an ingest cycle
            # tells us no more are still on their way in.
            stable = target is not None or quiet_for >= QUIET_SECONDS
            print(f"{done}/{total} terminal (target={effective_target}, "
                  f"quiet={quiet_for:.0f}s/{QUIET_SECONDS}s, stable={stable}) "
                  f"({time.monotonic() - started:.0f}s elapsed)")

            if total == 0:
                pass  # orphan detector hasn't discovered anything yet — keep waiting
            elif total >= effective_target and stable and done == total:
                return 0

            last_total = total
            time.sleep(poll_seconds)
    finally:
        conn.close()

    print(f"TIMEOUT after {timeout_seconds}s — {done}/{total} terminal (target={effective_target})")
    return 1


if __name__ == "__main__":
    timeout = int(sys.argv[1]) if len(sys.argv) > 1 else 120
    sys.exit(main(timeout))

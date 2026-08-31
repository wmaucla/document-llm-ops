"""Reconciler ③ — DLQ replay, daily, gated, never fully automatic.

A DLQ message already exhausted the ladder. Replaying against unchanged
code reproduces the failure at cost, forever — so this only re-drives
documents where `build_sha` or `prompt_version` has changed since the
attempt that landed them in `failed`. Anything that fails again on the new
version stays in the DLQ and needs a human (this script never re-drives the
same doc_id twice with the same recorded build_sha/prompt_version pairing).
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import ledger
from docpipeline.reconciliation import queries
from docpipeline.reconciliation import operator

log = logging.getLogger(__name__)


def find_replayable(cur) -> list[dict]:
    cur.execute(
queries.FIND_REPLAYABLE,
        (config.BUILD_SHA, config.PROMPT_VERSION),
    )
    return cur.fetchall()


def run(actor: str = "dlq-replay-cron", dry_run: bool = False) -> dict:
    conn = ledger.connect(role="rw")
    try:
        with conn.cursor() as cur:
            candidates = find_replayable(cur)
        conn.commit()

        if dry_run:
            return {"would_replay": [r["doc_id"] for r in candidates]}

        reason = (
            f"dlq_replay: build_sha/prompt_version changed since last attempt "
            f"(now build_sha={config.BUILD_SHA}, prompt_version={config.PROMPT_VERSION})"
        )
        replayed, failed = [], []
        for row in candidates:
            try:
                operator.force_redrive(row["doc_id"], reason, actor, conn=conn)
                replayed.append(row["doc_id"])
            except operator.BreakGlassError as exc:
                failed.append({"doc_id": row["doc_id"], "error": str(exc)})
                log.warning("dlq_replay: could not redrive %s: %s", row["doc_id"], exc)

        log.info("dlq_replay: %d replayed, %d skipped (of %d candidates)", len(replayed), len(failed), len(candidates))
        return {"replayed": replayed, "failed": failed}
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--actor", default="dlq-replay-cron")
    args = parser.parse_args()
    print(run(actor=args.actor, dry_run=args.dry_run))

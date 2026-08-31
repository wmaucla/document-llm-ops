"""Retention for `attempt_log`, the one table that still grows without bound.

`outbox` used to need this too. It no longer does: the relay deletes each row
once the broker acknowledges it (see `outbox.relay_once`), so that table is
bounded by the backlog rather than by history and needs no retention job,
partitioning, or archival at all.

`attempt_log` is different in kind — it is genuine append-only diagnostic
history with no duplicate anywhere else, so it cannot simply be dropped on
success. It also hides its growth: nothing queries it in the hot path, so no
latency ever degrades. The symptom is disk exhaustion, or autovacuum falling
behind and the bloat compounding.

Two properties here are load-bearing:

1. **Deleted by id watermark, not timestamp.** `started_at`/`ended_at` are both
   optional on this table, so a time predicate would skip every row written
   without them, forever. `id` is bigserial and therefore monotonic in insert
   order, so a watermark taken from the newest *datable* old row also sweeps
   the undated rows interleaved among them, which are old by position.
2. **Batch, and commit between batches.** A single DELETE over tens of millions
   of rows is one enormous transaction: huge WAL, a long-held lock, and it
   blocks autovacuum for its whole duration — causing the bloat it was meant to
   prevent.

At real volume the better answer is declarative partitioning by time and
`DROP TABLE` on the old partition: O(1), no WAL churn, no dead tuples. That is
a schema migration rather than a job, and is left as a documented next step —
see AGENT.md "Retention and growth".
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import ledger

log = logging.getLogger(__name__)

BATCH = 10_000


def _prune_attempt_log(conn, days: int, dry_run: bool) -> int:
    """Deletes by id watermark rather than by timestamp.

    `started_at`/`ended_at` are both optional on this table, so a plain time
    predicate would skip every row written without them — forever. `id` is
    bigserial and therefore monotonic in insert order, so a watermark taken
    from the newest *datable* old row also sweeps the undated rows interleaved
    among them, which are old by position.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max(id) AS watermark FROM attempt_log "
            "WHERE coalesce(ended_at, started_at) < now() - (%s || ' days')::interval",
            (days,),
        )
        watermark = cur.fetchone()["watermark"]
        if watermark is None:
            return 0
        cur.execute("SELECT count(*) AS n FROM attempt_log WHERE id <= %s", (watermark,))
        eligible = cur.fetchone()["n"]
    if dry_run or not eligible:
        return eligible

    removed = 0
    while True:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM attempt_log WHERE id IN "
                "(SELECT id FROM attempt_log WHERE id <= %s ORDER BY id LIMIT %s)",
                (watermark, BATCH),
            )
            n = cur.rowcount
        conn.commit()
        removed += n
        if n < BATCH:
            return removed


def run(attempt_log_days: int | None = None, dry_run: bool = False) -> dict:
    attempt_log_days = (config.ATTEMPT_LOG_RETENTION_DAYS
                        if attempt_log_days is None else attempt_log_days)
    conn = ledger.connect(role="rw")
    try:
        attempts_n = _prune_attempt_log(conn, attempt_log_days, dry_run)
    finally:
        conn.close()  # see AGENT.md "Connection-leak discipline"
    return {
        "dry_run": dry_run,
        "attempt_log_removed": attempts_n,
        "attempt_log_retention_days": attempt_log_days,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="report how many rows WOULD be removed, delete nothing")
    ap.add_argument("--attempt-log-days", type=int, default=None)
    args = ap.parse_args()

    result = run(args.attempt_log_days, args.dry_run)
    verb = "would remove" if result["dry_run"] else "removed"
    print(f"prune: {verb} attempt_log={result['attempt_log_removed']} "
          f"(>{result['attempt_log_retention_days']}d). "
          "outbox needs no retention — the relay deletes on ack.")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    raise SystemExit(main())

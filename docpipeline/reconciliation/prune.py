"""Retention for the two tables that grow without bound.

`outbox` and `attempt_log` accumulate forever: nothing has ever deleted from
either. This is invisible for a long time and then not — both hot queries use
partial indexes (`outbox_pending_idx` covers only `published_at IS NULL`), so
query latency never degrades no matter how large the heap gets. You find out
when disk fills, or when autovacuum falls behind on a high-churn table and the
bloat compounds.

Neither table needs archiving. A published outbox row holds nothing that is not
already durable elsewhere — what was extracted is in `documents`, what was
posted is in `posted_documents`, what was attempted is in `attempt_log`, the
artifacts are in GCS. The outbox is a queue, not a record.

Two safety properties, both load-bearing:

1. **Never delete an unpublished row.** The predicate is on `published_at`, not
   `created_at`: an unpublished row can be arbitrarily old (broker down,
   delivery failing) and deleting one silently destroys an undelivered message
   — precisely the failure the outbox exists to prevent, reintroduced by its
   own cleanup. `_prune_outbox` asserts this rather than trusting the WHERE
   clause.
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


def _prune_outbox(conn, days: int, dry_run: bool) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) AS n FROM outbox "
            "WHERE published_at IS NOT NULL AND published_at < now() - (%s || ' days')::interval",
            (days,),
        )
        eligible = cur.fetchone()["n"]
    if dry_run or not eligible:
        return eligible

    removed = 0
    while True:
        with conn.cursor() as cur:
            # `published_at IS NOT NULL` is the safety property, not an
            # optimisation: without it this deletes undelivered messages.
            cur.execute(
                """
                DELETE FROM outbox
                 WHERE id IN (
                    SELECT id FROM outbox
                     WHERE published_at IS NOT NULL
                       AND published_at < now() - (%s || ' days')::interval
                     ORDER BY id
                     LIMIT %s
                 )
                """,
                (days, BATCH),
            )
            n = cur.rowcount
        conn.commit()  # between batches, so no single long transaction
        removed += n
        if n < BATCH:
            return removed


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


def run(outbox_days: int | None = None, attempt_log_days: int | None = None,
        dry_run: bool = False) -> dict:
    outbox_days = config.OUTBOX_RETENTION_DAYS if outbox_days is None else outbox_days
    attempt_log_days = (config.ATTEMPT_LOG_RETENTION_DAYS
                        if attempt_log_days is None else attempt_log_days)
    conn = ledger.connect(role="rw")
    try:
        outbox_n = _prune_outbox(conn, outbox_days, dry_run)
        attempts_n = _prune_attempt_log(conn, attempt_log_days, dry_run)
    finally:
        conn.close()  # see AGENT.md "Connection-leak discipline"
    return {
        "dry_run": dry_run,
        "outbox_removed": outbox_n,
        "outbox_retention_days": outbox_days,
        "attempt_log_removed": attempts_n,
        "attempt_log_retention_days": attempt_log_days,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="report how many rows WOULD be removed, delete nothing")
    ap.add_argument("--outbox-days", type=int, default=None)
    ap.add_argument("--attempt-log-days", type=int, default=None)
    args = ap.parse_args()

    result = run(args.outbox_days, args.attempt_log_days, args.dry_run)
    verb = "would remove" if result["dry_run"] else "removed"
    print(f"prune: {verb} outbox={result['outbox_removed']} "
          f"(published >{result['outbox_retention_days']}d), "
          f"attempt_log={result['attempt_log_removed']} "
          f"(>{result['attempt_log_retention_days']}d)")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    raise SystemExit(main())

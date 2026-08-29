"""Dead man's switch — alerting on absence.

Every error-rate alert is blind to total silence: if a consumer group is
scaled to zero, a topic renamed, or a subscription deleted, error rate stays
zero and every dashboard is green while nothing is processed. This checks
`documents_completed_total`'s rate directly rather than inferring health
from the absence of errors.
"""

from __future__ import annotations

import argparse
import logging

from docpipeline import config
from docpipeline.core import ledger

log = logging.getLogger(__name__)


def check_liveness(window_seconds: int = config.DEADMANS_SWITCH_WINDOW_SECONDS) -> dict:
    conn = ledger.connect(role="ro")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) AS n FROM documents "
                "WHERE state = 'complete' AND state_updated_at > now() - (%s || ' seconds')::interval",
                (window_seconds,),
            )
            completed = cur.fetchone()["n"]

            cur.execute(
                "SELECT count(*) AS n FROM documents WHERE created_at > now() - (%s || ' seconds')::interval",
                (window_seconds,),
            )
            ingested = cur.fetchone()["n"]

            cur.execute(
                "SELECT count(*) AS n FROM documents WHERE state = ANY(%s)",
                (list(ledger.IN_FLIGHT_STATES),),
            )
            in_flight = cur.fetchone()["n"]
    finally:
        conn.close()

    # Silence is only meaningful if there was something to complete. A
    # genuinely idle system (no ingest, nothing in flight) isn't a failure —
    # that's what the synthetic canary (canary.py) exists to catch instead.
    had_work = ingested > 0 or in_flight > 0
    healthy = completed > 0 or not had_work

    return {
        "healthy": healthy,
        "completed_in_window": completed,
        "ingested_in_window": ingested,
        "in_flight": in_flight,
        "window_seconds": window_seconds,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--window-seconds", type=int, default=config.DEADMANS_SWITCH_WINDOW_SECONDS)
    args = parser.parse_args()

    result = check_liveness(args.window_seconds)
    print(result)
    if not result["healthy"]:
        log.critical(
            "DEAD MAN'S SWITCH: %d documents in flight or ingested in the last %ds, "
            "zero completions — page on-call",
            result["ingested_in_window"] + result["in_flight"], result["window_seconds"],
        )
        raise SystemExit(1)

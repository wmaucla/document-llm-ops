"""'Enforcement is structural, not policy' — the read-only operator lane's
safety is a database grant, not a code path. This asserts the grant is
absent, rather than trusting that no code path happens to call INSERT."""

import psycopg
import pytest

from docpipeline.core import ledger


def test_read_only_role_cannot_insert_outbox():
    conn = ledger.connect(role="ro")
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        with conn.cursor() as cur:
            cur.execute("INSERT INTO outbox (doc_id, topic, payload) VALUES ('x', 'y', '{}'::jsonb)")
    conn.rollback()
    conn.close()


def test_read_only_role_can_select_the_ledger():
    conn = ledger.connect(role="ro")
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM documents")
        cur.fetchone()
    conn.rollback()
    conn.close()

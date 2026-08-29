-- Step 9b: operator lanes — the break-glass audit trail and the auto-post
-- kill switch. See "Operator and R&D entry points" in the design doc.

CREATE TABLE IF NOT EXISTS break_glass_audit (
  id            bigserial PRIMARY KEY,
  doc_id        text,                -- NULL for cluster-wide actions (e.g. the kill switch)
  action        text NOT NULL,       -- 'force_redrive' | 'bulk_redrive' | 'kill_switch_toggle'
  reason        text NOT NULL,       -- REQUIRED — see 'Break-glass demands a reason'
  actor         text NOT NULL,
  detail        jsonb,
  requested_at  timestamptz NOT NULL DEFAULT now()
);

-- The auto-post kill switch — a LaunchDarkly flag in production, a table
-- row locally so it can be flipped without a redeploy. "The kill switch
-- degrades, it does not stop": processing keeps running; only the
-- auto-post decision reads this.
CREATE TABLE IF NOT EXISTS feature_flags (
  key    text PRIMARY KEY,
  value  boolean NOT NULL
);
INSERT INTO feature_flags (key, value) VALUES ('auto_post_enabled', true)
  ON CONFLICT (key) DO NOTHING;

GRANT ALL PRIVILEGES ON break_glass_audit, feature_flags TO pipeline_rw;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pipeline_rw;

-- Read-only lane can see the flag (it may want to report on it) but the
-- break-glass write path is pipeline_rw only, same as everything else that
-- writes to the ledger.
GRANT SELECT ON feature_flags TO pipeline_ro;

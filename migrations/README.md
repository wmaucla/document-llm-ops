# migrations/

Applied in filename order (`001_init.sql`, `002_operator_lanes.sql`, ...) — host mode via
ansible's own `psql` invocation, `make e2e-k8s` via an in-cluster Job connecting as the bootstrap
admin (`k8s/templates/jobs.yaml`; the app's own `pipeline_rw`/`pipeline_ro` roles can't apply
migrations that create those same roles).

| File | What it creates |
|---|---|
| `001_init.sql` | The ledger schema, `pipeline_rw`/`pipeline_ro` roles and grants (see [AGENT.md](../AGENT.md)'s "Two roles, enforced by grants, not by convention") |
| `002_operator_lanes.sql` | `break_glass_audit`, `feature_flags` (the auto-post kill switch) |

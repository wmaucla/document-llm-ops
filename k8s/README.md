# k8s/

A Helm chart — used purely for templating (no `helm install`/`helm upgrade` anywhere; ArgoCD is
the only thing that ever touches the live cluster, rendering this chart client-side via `helm
template` when it syncs). `make e2e-k8s` only; host-mode `make e2e` uses `docker-compose.yml`
directly and shares no infrastructure with this.

```
Chart.yaml                  chart metadata
values.yaml                 the actual config — services, resources, KEDA targets, env
templates/
  configmap.yaml             docpipeline-config — wave -1
  infra.yaml                 Postgres/Redis/Redpanda/fake-gcs-server — wave -1
  jobs.yaml                  migrate/topics/fixtures one-off Jobs — wave 0
  deployment.yaml             the 8 app-tier Deployments (+ optional metrics Service) — wave 1
  keda.yaml                  2 ScaledObjects (ocr-shard, extraction) — wave 2
  monitoring.yaml             Prometheus + Grafana — wave -1
```

**Sync-wave ordering is load-bearing, not decorative.** `argocd.argoproj.io/sync-wave` annotations
are what enforce "infra up → schema/topics/fixtures exist → app tier starts → KEDA ScaledObjects
reference existing Deployments" — ArgoCD blocks the next wave until the current one reports
Healthy (a Job counts Healthy once it's Succeeded). This replaced an earlier ansible-orchestrated
version of the same ordering; if you need it back, it's in git history.

**Confirmed-live gotcha:** KEDA's `ScaledObject`s originally had no wave annotation (defaulting to
0, alongside the setup Jobs) and their `scaleTargetRef` failed admission validation because the
target Deployments (wave 1) didn't exist yet — that failure degraded the whole sync and silently
blocked every later wave, app tier included. Moving them to wave 2 fixed it. If you add a new
resource here, check its wave against what it actually depends on existing first.

**Everything here is one ArgoCD Application** (`../argocd/application.yaml`, `path: k8s`) — no
raw-kubectl exception for infra, monitoring, or anything else. See
[AGENT.md](../AGENT.md#handoff-where-this-stands-right-now) for the full story, including why
`k8s-infra/` briefly existed as a separate raw-`kubectl apply` directory before being folded back
into this chart.

**Per-service metrics port:** `values.yaml`'s `services:` entries take an optional `metricsPort` —
setting one adds both a container port and a dedicated `<name>-metrics` Service (a bare Deployment
only gives per-pod IPs, not something Prometheus can target by stable DNS). Currently only
`triage` sets one, for its `triage_results_total` counter.

# argocd/

The `Application` objects themselves — pointer/meta config, not the manifests being deployed.
Structurally separate from `../k8s/` on purpose: an `Application` can't manage the object that
declares it (self-referential), so it can't live inside the directory it points at. These are two
of the only raw `kubectl apply` calls in the whole deploy path — irreducible, since ArgoCD can't
sync the object that tells it what to sync.

| File | Application | Source | Sync policy |
|---|---|---|---|
| `application.yaml` | `docpipeline` | `--local ../k8s` (working tree, no push needed) | Manual only — a deploy is a decision |
| `keda-application.yaml` | `keda` | kedacore's real upstream Helm chart (third-party, not this repo) | Manual only, `ServerSideApply=true` (the `scaledjobs.keda.sh` CRD is too large for client-side apply's annotation limit) |

Both apply via `kubectl app sync <name> --core` — no `argocd-server` login or port-forward, talks
to the Kubernetes API directly. See [AGENT.md](../AGENT.md)'s "ArgoCD: both apps, no exceptions
but two" section for the full reasoning.

# document-llm-ops — single entrypoint. Almost every target is a thin alias for
# `ansible-playbook ansible/site.yml --tags <name>`; each is commented below.
#
#   make e2e       fast loop — host processes, deterministic extraction, ~15s
#   make e2e-k8s   full loop — K8s Deployments via ArgoCD, KEDA, real model.
#                  DESTROYS and rebuilds the whole cluster.
#   make verify-loop  e2e-k8s, then prove the cluster keeps taking new work

ANSIBLE := ansible-playbook -i ansible/inventory.ini ansible/site.yml
COUNT ?= 3

.PHONY: install up down init-db topics fixtures reset run-local test test-real-llm \
        cluster-rebuild image keda-install deploy undeploy k8s-status canary dlq-replay \
        deadmans-switch terminal-report prune summary summary-k8s replay-docs e2e e2e-k8s verify-loop

# uv sync the project. No manual venv — uv run handles the rest.
install:
	uv sync --extra dev

# docker compose up: Postgres/Redpanda/fake-gcs-server. Host mode only;
# e2e-k8s runs its own copies of these in-cluster (k8s/templates/infra.yaml).
up:
	$(ANSIBLE) --tags up

# docker compose down. Note `make test` needs these running.
down:
	$(ANSIBLE) --tags down

# Apply migrations/*.sql to the host database.
init-db:
	$(ANSIBLE) --tags init-db

# Truncate the ledger, clear GCS, recreate Redpanda. Host mode only —
# e2e-k8s gets empty state for free from `minikube delete`.
reset:
	$(ANSIBLE) --tags reset

# Create every Kafka topic. Idempotent.
topics:
	$(ANSIBLE) --tags topics

# Generate + upload all 14 fixtures. e2e-k8s uploads only 4 (see
# k8s/templates/jobs.yaml) because they contend for one Ollama pod.
fixtures:
	$(ANSIBLE) --tags fixtures

# All consumers as host processes, deterministic extraction. Backgrounded by
# ansible elsewhere; run directly here so Ctrl-C actually kills the group.
run-local:
	set -a && . ./.env && set +a && uv run python3 local_scripts/run_local.py

# The pytest suite against real Postgres + fake-GCS from docker-compose.
# conftest forces the deterministic backend, so it never calls a model.
test:
	set -a && . ./.env && set +a && uv run pytest tests/ -v --timeout=60 -k "not real_llm"

# Opt-in, slow, needs a port-forward. Excluded from `make test` by -k.
test-real-llm:
	@echo "needs: kubectl port-forward svc/litellm 4000:4000 &  (separately, left running)"
	set -a && . ./.env && set +a && RUN_REAL_LLM_TESTS=1 uv run pytest tests/test_real_llm_integration.py -v -s --timeout=300

# DESTRUCTIVE: minikube delete + start, then rebuild the sibling repo's whole
# stack via its own terraform. Runs automatically as part of e2e-k8s.
cluster-rebuild:
	$(ANSIBLE) --tags cluster-rebuild

# Build the docpipeline image into minikube's docker daemon (never pulled).
image:
	$(ANSIBLE) --tags image

# Apply the KEDA ArgoCD Application, then sync it from kedacore's chart.
keda-install:
	$(ANSIBLE) --tags keda-install

# Apply the docpipeline Application, then `argocd app sync --local ./k8s`.
# Reads the working tree, so don't edit k8s/ while this runs.
deploy:
	$(ANSIBLE) --tags deploy

# `argocd app delete --cascade` — removes exactly what deploy created.
undeploy:
	$(ANSIBLE) --tags undeploy

# Pods, ScaledObjects and Application health at a glance.
k8s-status:
	kubectl get pods -l 'app in (docpipeline-triage,docpipeline-pdf-worker,docpipeline-ocr-shard,docpipeline-extraction,docpipeline-sink-stub,docpipeline-outbox-relay,docpipeline-sweeper,docpipeline-orphan-detector,docpipeline-gpu-watchdog)'
	kubectl get scaledobject
	kubectl get application docpipeline -n argocd

# Inject one synthetic document and track it end to end against its SLO.
canary:
	$(ANSIBLE) --tags canary

# Re-drive `failed` documents whose build_sha/prompt_version has moved.
# Deliberately not scheduled: re-running the same code would fail identically.
dlq-replay:
	$(ANSIBLE) --tags dlq-replay

# Check for total silence. Exits 1 if unhealthy. Also runs every 15 min as a
# CronJob — a switch that only fires when someone remembers it is not one.
deadmans-switch:
	$(ANSIBLE) --tags deadmans-switch

# Same report the docpipeline-terminal-report CronJob runs on a schedule
# (k8s/templates/cronjobs.yaml); this is the on-demand version.
terminal-report:
	$(ANSIBLE) --tags terminal-report

# Same retention pass the docpipeline-prune CronJob runs nightly.
prune:
	$(ANSIBLE) --tags prune

# Per-document state report against the HOST database.
summary:
	$(ANSIBLE) --tags summary

# In-cluster equivalent of `make summary` — checks the k8s cluster's own
# Postgres via kubectl exec, not the host docker-compose one `make summary`
# points at (the two share no infrastructure). Standalone, safe to run
# any time after `make e2e-k8s` or `make replay-docs`.
summary-k8s:
	$(ANSIBLE) --tags summary-k8s

# Assumes the cluster from a prior e2e-k8s is already up -- doesn't deploy or
# rebuild anything, just injects COUNT fresh synthetic documents and returns.
# Each gets a unique invoice_no/upload path (same trick canary.py uses), so
# re-running this never dedupes into a no-op the way literally re-uploading
# the same fixture bytes would (doc_id is a content checksum).
replay-docs:
	$(ANSIBLE) --tags replay -e replay_count=$(COUNT)

# Fast loop: reset -> fixtures -> host consumers -> drain -> test. ~15s.
e2e:
	$(ANSIBLE) --tags reset,e2e

# No `reset` tag here (unlike e2e): those tasks target docker-compose's
# host infra, which e2e-k8s no longer uses at all. cluster-rebuild's
# `minikube delete` + fresh in-cluster postgres/redpanda/fake-gcs-server
# pods (see k8s/templates/infra.yaml) already guarantee empty state every
# run — a stronger reset than truncate/flush ever was.
e2e-k8s:
	$(ANSIBLE) --tags cluster-rebuild,image,keda-install,deploy,e2e-k8s

# The "human verification loop": e2e-k8s (full rebuild) -> summary-k8s
# (confirm it settled) -> replay-docs (inject fresh work against the now-
# running cluster, no redeploy) -> wait -> summary-k8s again. The point of
# the middle steps isn't just firing replay-docs -- it's proving the replayed
# documents actually reach a terminal state too, i.e. that the cluster keeps
# taking new work after the initial deploy, not just once.
#
# verify-extras runs last: dlq-replay and the dead man's switch are the only
# reconciliation lanes with no other live coverage (everything else is
# exercised by the e2e path itself, and these two were pytest-only until
# 2026-08-31). The switch must run *after* the drain -- it fails on total
# silence, so on an idle cluster it would report unhealthy correctly but
# uselessly. make verify-loop COUNT=10 to replay more than the default 3.
verify-loop:
	$(ANSIBLE) --tags cluster-rebuild,image,keda-install,deploy,e2e-k8s
	$(ANSIBLE) --tags summary-k8s
	$(ANSIBLE) --tags replay -e replay_count=$(COUNT)
	$(ANSIBLE) --tags replay-wait
	$(ANSIBLE) --tags summary-k8s
	$(ANSIBLE) --tags verify-extras

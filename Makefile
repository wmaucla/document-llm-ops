# document-llm-ops — single entrypoint. Every target is a thin alias
# for `ansible-playbook ansible/site.yml --tags <name>` (see site.yml).
#
#   make e2e       fast loop — host-based consumers, mock LLM (steps 0-7)
#   make e2e-k8s   full loop — K8s Deployments via ArgoCD, KEDA, real LLM,
#                  destroys + rebuilds the whole cluster (steps 8-9)

ANSIBLE := ansible-playbook -i ansible/inventory.ini ansible/site.yml
COUNT ?= 3

.PHONY: help install up down init-db topics fixtures reset run-local test test-real-llm \
        cluster-rebuild image keda-install deploy undeploy k8s-status canary dlq-replay \
        deadmans-switch summary summary-k8s replay-docs e2e e2e-k8s verify-loop

help:
	@echo "make install       uv sync the project (no manual venv — uv run handles the rest)"
	@echo "make up            docker compose up (Postgres/Redpanda/fake-gcs-server/Redis)"
	@echo "make down          docker compose down"
	@echo "make init-db       apply migrations/*.sql"
	@echo "make reset         truncate ledger, clear GCS, wipe Redpanda, flush Redis"
	@echo "make topics        create every Kafka topic"
	@echo "make fixtures      generate + upload the 14 fixtures"
	@echo "make run-local     run all consumers as host processes (mock LLM, fast)"
	@echo "make test          run the pytest suite (mock mode, ~3s)"
	@echo "make test-real-llm run the opt-in real-LLM integration test (needs port-forward, slow)"
	@echo "make cluster-rebuild  DESTRUCTIVE: minikube delete + start, then rebuild the sibling"
	@echo "                   mlops-llm-repo's entire stack (ArgoCD/Ollama/LiteLLM/Langfuse) via"
	@echo "                   its own terraform apply. Runs automatically as part of e2e-k8s."
	@echo "make image         build the docpipeline image into minikube's docker daemon"
	@echo "make keda-install  apply the KEDA ArgoCD Application, then 'argocd app sync keda'"
	@echo "make deploy        apply the ArgoCD Application, then 'argocd app sync --local ./k8s'"
	@echo "make undeploy      remove everything make deploy created"
	@echo "make k8s-status    show docpipeline pods + ScaledObjects"
	@echo "make canary        inject + track one synthetic document end to end"
	@echo "make dlq-replay    re-drive failed docs whose build_sha/prompt_version changed"
	@echo "make deadmans-switch  check for total silence (exits 1 if unhealthy)"
	@echo "make summary       per-document state report against the HOST (docker-compose) Postgres"
	@echo "make summary-k8s   the in-cluster equivalent -- checks the k8s cluster's own Postgres,"
	@echo "                   not the host one. Standalone, safe to run any time."
	@echo "make replay-docs   inject COUNT (default 3) fresh docs into an already-running cluster,"
	@echo "                   no redeploy -- make replay-docs COUNT=10 to override"
	@echo "make e2e           fast end-to-end run: reset -> fixtures -> host consumers -> test"
	@echo "make e2e-k8s       full end-to-end run: DESTROYS + rebuilds the whole minikube cluster,"
	@echo "                   then image -> ArgoCD deploy (in-cluster infra, migrate/topics/fixtures"
	@echo "                   Jobs, app tier, KEDA, monitoring, all one sync) -> canary (~15-20 min)"
	@echo "make verify-loop   the human verification loop: e2e-k8s -> summary-k8s -> replay-docs ->"
	@echo "                   summary-k8s again, proving the cluster keeps taking new work after the"
	@echo "                   initial deploy, not just once -- then dlq-replay + dead man's switch,"
	@echo "                   the two reconciliation lanes nothing else covers live."
	@echo "                   COUNT=N to replay more than 3."

install:
	uv sync --extra dev

up:
	$(ANSIBLE) --tags up

down:
	$(ANSIBLE) --tags down

init-db:
	$(ANSIBLE) --tags init-db

reset:
	$(ANSIBLE) --tags reset

topics:
	$(ANSIBLE) --tags topics

fixtures:
	$(ANSIBLE) --tags fixtures

run-local:
	set -a && . ./.env && set +a && uv run python3 local_scripts/run_local.py

test:
	set -a && . ./.env && set +a && uv run pytest tests/ -v --timeout=60 -k "not real_llm"

test-real-llm:
	@echo "needs: kubectl port-forward svc/litellm 4000:4000 &  (separately, left running)"
	set -a && . ./.env && set +a && RUN_REAL_LLM_TESTS=1 uv run pytest tests/test_real_llm_integration.py -v -s --timeout=300

cluster-rebuild:
	$(ANSIBLE) --tags cluster-rebuild

image:
	$(ANSIBLE) --tags image

keda-install:
	$(ANSIBLE) --tags keda-install

deploy:
	$(ANSIBLE) --tags deploy

undeploy:
	$(ANSIBLE) --tags undeploy

k8s-status:
	kubectl get pods -l 'app in (docpipeline-triage,docpipeline-pdf-worker,docpipeline-ocr-shard,docpipeline-extraction,docpipeline-sink-stub,docpipeline-outbox-relay,docpipeline-sweeper,docpipeline-orphan-detector)'
	kubectl get scaledobject
	kubectl get application docpipeline -n argocd

canary:
	$(ANSIBLE) --tags canary

dlq-replay:
	$(ANSIBLE) --tags dlq-replay

deadmans-switch:
	$(ANSIBLE) --tags deadmans-switch

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

e2e:
	$(ANSIBLE) --tags reset,e2e

# No `reset` tag here (unlike e2e): those tasks target docker-compose's
# host infra, which e2e-k8s no longer uses at all. cluster-rebuild's
# `minikube delete` + fresh in-cluster postgres/redis/redpanda/fake-gcs-server
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

# ansible/

The sole orchestration layer — replaces bash scripts and raw `kubectl`/`helm` calls. Every
`Makefile` target is a thin `ansible-playbook site.yml --tags <name>` alias; there's no
bash-script orchestration left.

- `site.yml` — every task, in every tag combination
- `inventory.ini` — `localhost` only; everything here runs against local Docker/minikube

**File order is execution order, regardless of `--tags` order.** `ansible-playbook site.yml --tags
a,b,c` runs matching tasks in the order they appear in the file, not the order you listed the
tags. This has bitten twice (test/summary tasks running before fixtures existed; reset running
after fixtures instead of before) — see [AGENT.md](../AGENT.md)'s "Ansible task ordering" section
before repositioning any tagged block, and check its new position against every tag combination
that might select it, not just the one you're testing.

`make e2e` (host mode, docker-compose) and `make e2e-k8s` (in-cluster, ArgoCD-driven) are
genuinely separate paths through this one file now — they share no infrastructure or ledger. See
[AGENT.md](../AGENT.md) for the GPU-registration race self-heal, the PID-capture gotcha, and every
other confirmed-live fix baked into this playbook.

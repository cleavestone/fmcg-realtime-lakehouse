# ADR-008: Docker Compose for orchestration

**Status:** Accepted (2026-10-03)

## Context
The platform has around 15 services that need to start in order, with one-shot init steps, restarts, persistent state, and a single command to bring everything up on a laptop.

## Decision
Use Docker Compose (v2.20+ for `include:`):
- A thin root `docker-compose.yml` declares the network (`fmcg-net`) and named volumes, and includes one file per domain from `compose/`.
- Each service belongs to a profile (`source`, `sim`, `kafka`, `cdc`, `lake`, `streaming`, `serving`), so phases can be brought up incrementally.
- Ordering uses only `depends_on` conditions (`service_healthy`, `service_completed_successfully`). Every long-running service has a healthcheck.
- One-shot init jobs are idempotent and use `restart: "no"`. Long-running services use `restart: unless-stopped`.
- Every image tag, credential and memory limit comes from `.env`. Log rotation is set on every service.
- A Makefile wraps every common command.

## Consequences
- `make demo` is the whole deployment story; `make nuke` is a full reset.
- No scheduler: periodic dbt runs are a shell loop in a container.
- Single host, no autoscaling or rolling deploys.

## Alternatives / production evolution
- **Kubernetes** (Strimzi for Kafka, Spark Operator, Helm charts for Trino) for multi-node deployment.
- **Airflow or Dagster** for dbt scheduling, backfills, alerting and lineage-aware orchestration.

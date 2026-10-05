# fmcg-realtime-lakehouse: every common command lives here.
# Usage: make help

SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE  := docker compose
PROFILES := source sim kafka cdc lake streaming serving
ALL_PROFILES := $(foreach p,$(PROFILES),--profile $(p))

P    ?=
S    ?=
NAME ?=

# Placeholder for targets implemented in a later phase.
define not_yet
	@echo "make $@: not implemented until Phase $(1)" >&2; exit 1
endef

.PHONY: help env up demo down nuke ps logs build seed simulate stop-sim scenario \
        kafka-topics kafka-smoke register-connector connector-status connector-restart cdc-counts cdc-tail spark-smoke \
        bronze-check spark-shell spark-sql silver-facts-rebuild silver-dims-rebuild scd2-check \
        test-host test-streaming \
        psql dbt-build reconcile test lint

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

env: ## Create .env from .env.example if missing
	@test -f .env || { cp .env.example .env && echo "created .env from .env.example"; }

# --- Stack lifecycle --------------------------------------------------------

up: env ## Start one profile: make up P=<profile>
	@test -n "$(P)" || { echo "usage: make up P=<$(subst $() ,|,$(PROFILES))>" >&2; exit 1; }
	$(COMPOSE) --profile $(P) up -d

demo: env ## Start every profile (ordering via healthchecks + depends_on)
	$(COMPOSE) $(ALL_PROFILES) up -d

down: ## Stop everything, keep volumes
	$(COMPOSE) $(ALL_PROFILES) down --remove-orphans

nuke: ## Stop everything and delete all volumes (full reset)
	$(COMPOSE) $(ALL_PROFILES) down -v --remove-orphans

ps: ## Show service status
	$(COMPOSE) $(ALL_PROFILES) ps -a

logs: ## Follow logs: make logs S=<service> (all services if omitted)
	$(COMPOSE) $(ALL_PROFILES) logs -f --tail=200 $(S)

build: env ## Build all custom images
	$(COMPOSE) $(ALL_PROFILES) build

# --- Source and simulator ---------------------------------------------------

seed: env ## Run the deterministic seed (skips if already populated)
	$(COMPOSE) --profile source run --rm seed

psql: ## Open psql on the source database
	$(COMPOSE) --profile source exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

simulate: env ## Start the live simulator and follow its logs (Ctrl+C stops following, not the simulator)
	$(COMPOSE) --profile sim up -d simulator
	$(COMPOSE) --profile sim logs -f --tail=20 simulator

stop-sim: ## Stop the simulator (graceful: finishes the in-flight event)
	$(COMPOSE) --profile sim stop simulator

scenario: env ## Run a deterministic scenario: make scenario NAME=<scd2_price_test|scd2_burst_test|order_lifecycle_test|delete_test>
	@test -n "$(NAME)" || { echo "usage: make scenario NAME=<name>" >&2; exit 1; }
	$(COMPOSE) --profile sim run --rm simulator scenario $(NAME)

# --- Kafka --------------------------------------------------------------------

kafka-topics: ## List Kafka topics
	$(COMPOSE) --profile kafka exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

kafka-smoke: ## Broker smoke test: create, produce, consume and delete a test topic
	$(COMPOSE) --profile kafka exec -T kafka bash < infra/kafka/smoke-test.sh

# --- Lake -----------------------------------------------------------------------

spark-smoke: env ## Lake smoke test: write a Delta table to MinIO, register it in HMS, read it back
	$(COMPOSE) --profile lake --profile streaming run --rm spark-bronze python -m streaming.smoke_test

TOOLS := $(COMPOSE) --profile tools run --rm spark-tools

bronze-check: ## Bronze rows per table/op, quarantine count, duplicate-offset check
	$(TOOLS) python -m streaming.tools.bronze_check

spark-shell: ## Interactive PySpark shell with the lake config (Delta, HMS, MinIO)
	$(TOOLS) pyspark

Q ?=
spark-sql: ## Run SQL against the lake: make spark-sql Q="SELECT * FROM silver.orders LIMIT 5"
	@test -n "$(value Q)" || { echo 'usage: make spark-sql Q="<sql>"' >&2; exit 1; }
	$(TOOLS) python -m streaming.tools.spark_sql "$(value Q)"

# --- CDC ----------------------------------------------------------------------

register-connector: env ## Create/update the Debezium connector (idempotent) and wait for RUNNING
	$(COMPOSE) --profile cdc run --rm connector-register

connector-status: ## Show the Debezium connector and task state
	@curl -fsS http://localhost:8083/connectors/fmcg-postgres/status; echo

connector-restart: ## Restart the connector's failed task (it resumes from the slot, nothing lost)
	@curl -fsS -X POST "http://localhost:8083/connectors/fmcg-postgres/restart?includeTasks=true&onlyFailed=true" && echo "restart requested"

cdc-counts: ## Kafka messages per CDC topic vs Postgres row counts
	@bash infra/debezium/cdc-counts.sh

T ?= orders
N ?= 5
cdc-tail: ## Last N CDC messages for a table: make cdc-tail T=orders N=5
	@$(COMPOSE) --profile cdc exec -T kafka bash -s -- $(T) $(N) < infra/debezium/cdc-tail.sh

# --- Lakehouse and gold -------------------------------------------------------

silver-facts-rebuild: ## Rebuild Silver facts from Bronze (drops tables + checkpoints, replays)
	$(COMPOSE) --profile streaming stop spark-silver-facts
	$(TOOLS) python -m streaming.tools.reset_tables silver orders order_items inventory regions
	$(COMPOSE) --profile streaming up -d spark-silver-facts

silver-dims-rebuild: ## Rebuild SCD2 dimensions from Bronze (drops tables + checkpoints, replays)
	$(COMPOSE) --profile streaming stop spark-silver-dims
	$(TOOLS) python -m streaming.tools.reset_tables silver dim_store dim_product dim_sales_rep
	$(COMPOSE) --profile streaming up -d spark-silver-dims

scd2-check: ## SCD2 invariants on every dimension (one current, contiguous, no overlaps)
	$(TOOLS) python -m streaming.tools.scd2_check

reconcile: ## Silver facts vs Postgres, row by row (stop the simulator first)
	$(TOOLS) python -m streaming.tools.reconcile

dbt-build: ## Run dbt build against Trino
	$(call not_yet,8)

# --- Quality ------------------------------------------------------------------

test: test-host test-streaming ## Run all tests (host + Spark container)

test-host: ## Host tests (simulator) with uv
	uv run --frozen pytest

test-streaming: env ## Spark unit tests inside the Spark test image (Java is not needed on the host)
	$(COMPOSE) --profile test run --rm --build spark-test

lint: ## Run all pre-commit hooks on every file
	uv run --frozen pre-commit run --all-files

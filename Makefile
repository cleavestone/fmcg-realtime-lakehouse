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
        kafka-topics kafka-smoke register-connector connector-status cdc-counts cdc-tail spark-smoke \
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

# --- CDC ----------------------------------------------------------------------

register-connector: env ## Create/update the Debezium connector (idempotent) and wait for RUNNING
	$(COMPOSE) --profile cdc run --rm connector-register

connector-status: ## Show the Debezium connector and task state
	@curl -fsS http://localhost:8083/connectors/fmcg-postgres/status; echo

cdc-counts: ## Kafka messages per CDC topic vs Postgres row counts
	@bash infra/debezium/cdc-counts.sh

T ?= orders
N ?= 5
cdc-tail: ## Last N CDC messages for a table: make cdc-tail T=orders N=5
	@$(COMPOSE) --profile cdc exec -T kafka bash -s -- $(T) $(N) < infra/debezium/cdc-tail.sh

# --- Lakehouse and gold -------------------------------------------------------

reconcile: ## Compare Silver facts against Postgres (counts + checksums)
	$(call not_yet,6)

dbt-build: ## Run dbt build against Trino
	$(call not_yet,8)

# --- Quality ------------------------------------------------------------------

test: ## Run pytest (exit code 5 = no tests collected yet, treated as success)
	uv run --frozen pytest || [ $$? -eq 5 ]

lint: ## Run all pre-commit hooks on every file
	uv run --frozen pre-commit run --all-files

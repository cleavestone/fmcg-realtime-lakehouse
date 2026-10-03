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

.PHONY: help env up demo down nuke ps logs build seed simulate scenario \
        register-connector psql dbt-build reconcile test lint

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

simulate: ## Start the simulator and follow its logs
	$(call not_yet,1b)

scenario: ## Run a deterministic scenario: make scenario NAME=<name>
	$(call not_yet,1b)

# --- CDC ----------------------------------------------------------------------

register-connector: ## Create/update the Debezium connector (idempotent)
	$(call not_yet,3)

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

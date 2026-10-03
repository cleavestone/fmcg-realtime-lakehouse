# ADR-006: dbt on Trino for the Gold layer

**Status:** Accepted (2026-10-03)

## Context
Gold is analytical modelling (star schema, aggregates), which is best expressed in SQL with tests and documentation, and is maintainable by analytics engineers who don't write Spark.

## Decision
Build Gold with dbt-core and the dbt-trino adapter, writing Delta tables through Trino's Delta Lake connector. Models are layered staging → intermediate → marts. Tests cover keys, relationships, accepted values, and a custom test that SCD2 validity windows never overlap. A one-shot `dbt` service builds on startup; `dbt-scheduler` rebuilds every `DBT_INTERVAL_SECONDS`.

## Consequences
- SQL-first models with lineage (`dbt docs`) and tests in CI.
- Trino is the single SQL entry point for both dbt and Power BI.
- Gold freshness is bounded by the scheduler interval (default 5 minutes), which is fine for dashboards.

## Alternatives
- **Gold in Spark:** one engine, but loses dbt's testing, docs and accessibility.
- **dbt-spark (Thrift server):** adds a Spark Thrift service. Trino is already needed for serving.

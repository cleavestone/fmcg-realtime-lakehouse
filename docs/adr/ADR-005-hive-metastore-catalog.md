# ADR-005: Hive Metastore as the shared catalog

**Status:** Accepted (2026-10-03)

## Context
Spark writes the Bronze and Silver tables; Trino (and dbt through it) reads them and writes Gold. Both engines need a shared view of table names, locations and schemas.

## Decision
Run a standalone Hive Metastore (HMS) backed by its own Postgres database (`hms-db`). Spark uses it via `spark.sql.catalogImplementation=hive` plus the metastore URI. Trino's Delta Lake connector uses `hive.metastore=thrift`.

**Version risk:** Spark 3.5 has a built-in Hive 2.3.9 metastore client. HMS 3.1.x is wire-compatible with that client for the operations Delta needs. HMS 4.x has reported incompatibilities. Plan: pin HMS 3.1.3 and validate it in the Phase 4 smoke test before building anything on it.

## Consequences
- A table written by Spark is immediately queryable from Trino.
- One more stateful service with its own database.
- HMS is a legacy-shaped API: no fine-grained access control or lineage.

## Alternatives
- **Path-based tables + Trino `register_table`:** no metastore, but manual registration and fragile.
- **Unity Catalog OSS / Apache Polaris (Iceberg REST):** the modern direction and noted in the production roadmap, but less mature with Delta + Trino + Spark 3.5 together.
- **AWS Glue:** not local.

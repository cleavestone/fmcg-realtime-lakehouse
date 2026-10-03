# ADR-005: Hive Metastore as the shared catalog

**Status:** Accepted (2026-10-03)

## Context
Spark writes the Bronze and Silver tables; Trino (and dbt through it) reads them and writes Gold. Both engines need a shared view of table names, locations and schemas.

## Decision
Run a standalone Hive Metastore (HMS) backed by its own Postgres database (`hms-db`). Spark uses it via `spark.sql.catalogImplementation=hive` plus the metastore URI. Trino's Delta Lake connector uses `hive.metastore=thrift`.

**Version:** Spark 3.5 has a built-in Hive 2.3.9 metastore client. HMS 3.1.x is wire-compatible with that client for the operations Delta needs, while HMS 4.x has reported incompatibilities. HMS is pinned at `apache/hive:3.1.3` and validated by the Phase 4 smoke test.

**Image adjustments** (`infra/hive-metastore/Dockerfile`, a thin layer over the official image):
- The bundled Postgres JDBC driver (9.4, from 2016) can't authenticate against Postgres 16 (scram-sha-256), so it is replaced with 42.7.13, pinned by SHA-256.
- Hadoop's `hadoop-aws-3.1.0` and `aws-java-sdk-bundle-1.11.271` ship in the image but aren't on Hive's classpath. They're linked in, because the metastore creates database and table directories on S3.
- The stock entrypoint runs `schematool -initSchema` on every start, so the second start fails. A wrapper runs `schematool -info` first and skips init when the schema exists.
- Secrets stay out of `hive-site.xml`: JDBC settings arrive as `-Djavax.jdo.option.*` in `SERVICE_OPTS`, and S3 credentials come from `AWS_*` env vars (`EnvironmentVariableCredentialsProvider`).

## Consequences
- A table written by Spark is immediately queryable from Trino.
- One more stateful service with its own database.
- HMS is a legacy-shaped API: no fine-grained access control or lineage.

## Alternatives
- **Path-based tables + Trino `register_table`:** no metastore, but manual registration and fragile.
- **Unity Catalog OSS / Apache Polaris (Iceberg REST):** the modern direction and noted in the production roadmap, but less mature with Delta + Trino + Spark 3.5 together.
- **AWS Glue:** not local.

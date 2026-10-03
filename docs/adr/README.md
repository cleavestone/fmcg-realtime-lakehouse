# Architecture Decision Records

Format: context, decision, consequences, alternatives considered.

| ADR | Decision |
|---|---|
| [001](ADR-001-kafka-kraft.md) | Kafka in KRaft mode, no ZooKeeper |
| [002](ADR-002-debezium-unwrap-json.md) | Debezium unwrap SMT, JSON without schemas, deletes as `__deleted` |
| [003](ADR-003-medallion-silver-reads-bronze.md) | Medallion layers; Silver streams from Bronze Delta, not Kafka |
| [004](ADR-004-scd2-in-spark.md) | SCD Type 2 in Spark, not dbt snapshots |
| [005](ADR-005-hive-metastore-catalog.md) | Hive Metastore as the shared catalog |
| [006](ADR-006-dbt-on-trino.md) | dbt on Trino for the Gold layer |
| [007](ADR-007-idempotency.md) | Idempotency via checkpoints + LSN-guarded MERGE |
| [008](ADR-008-docker-compose-orchestration.md) | Docker Compose for orchestration |
| [009](ADR-009-uv-packaging.md) | uv workspace for Python packaging |
| [010](ADR-010-power-bi.md) | Power BI for reporting, outside the Compose stack |

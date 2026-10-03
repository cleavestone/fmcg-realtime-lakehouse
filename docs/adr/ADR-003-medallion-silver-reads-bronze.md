# ADR-003: Medallion architecture; Silver reads Bronze, not Kafka

**Status:** Accepted (2026-10-03)

## Context
Silver jobs could consume Kafka directly or consume the Bronze Delta table. Kafka retention is finite, so if Silver logic changes or has a bug, re-deriving Silver from Kafka works only as long as the data is still retained.

## Decision
Use Bronze / Silver / Gold layers. Only the Bronze job reads Kafka. Silver jobs read Bronze Delta tables with `spark.readStream.format("delta")`.

## Consequences
- Bronze is the permanent, replayable change log. Silver can be rebuilt at any time by deleting its tables and checkpoints and restarting the job.
- Kafka retention can be short (days) without risking data loss.
- One extra hop adds roughly one trigger interval of latency to Silver. It stays within the target of "visible in Silver in about a minute".
- Bronze storage grows without bound, so production would need retention or compaction (`VACUUM`, archival tiers).

## Alternatives
- **Silver directly from Kafka:** lower latency, but replay depends on Kafka retention and every Silver job re-implements parsing.
- **Kafka with infinite retention / tiered storage:** possible, but it moves the system of record into the broker.

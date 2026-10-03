# ADR-001: Kafka in KRaft mode

**Status:** Accepted (2026-10-03)

## Context
Kafka needs a metadata quorum. Historically that was ZooKeeper, a separate distributed system to run, secure and monitor. KRaft (Kafka Raft) moves metadata management into Kafka itself. It has been production-ready since 3.3, and ZooKeeper support was removed in Kafka 4.0.

## Decision
Run a single Kafka node in KRaft combined mode (broker + controller) using the official `apache/kafka` image, pinned to the 3.9.x line. Broker-side topic auto-creation is disabled; topics are created explicitly by Kafka Connect (`topic.creation.*`) or by tooling.

3.9 rather than 4.x keeps the broker on the same major version as the Kafka client libraries shipped inside the Debezium 2.7 Connect image.

## Consequences
- One fewer service and no ZooKeeper failure modes; faster startup.
- Disabling auto-create means a typo in a topic name fails loudly instead of silently creating a topic.
- Single node, replication factor 1: no fault tolerance. That is acceptable for a laptop and documented as a production gap.

## Alternatives
- **ZooKeeper-based Kafka:** deprecated, extra moving part.
- **Redpanda:** Kafka-compatible and lighter, but the goal is to demonstrate the standard Apache stack.
- **Kafka 4.x:** viable later; revisit when Debezium 3.x is adopted.

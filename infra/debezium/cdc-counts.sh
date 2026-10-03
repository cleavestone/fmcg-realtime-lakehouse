#!/usr/bin/env bash
# Messages per CDC topic next to Postgres row counts. Right after the initial snapshot
# (simulator not running) they match exactly; afterwards topics also hold updates/deletes.
set -euo pipefail

TABLES="regions stores products sales_reps orders order_items inventory"
COMPOSE="docker compose --profile cdc"

offsets=$($COMPOSE exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic 'fmcg\.public\..*' --time -1)

printf "%-14s %12s %12s\n" "table" "kafka_msgs" "pg_rows"
for t in $TABLES; do
  msgs=$(echo "$offsets" | awk -F: -v topic="fmcg.public.$t" '$1 == topic {s += $3} END {print s + 0}')
  rows=$($COMPOSE exec -T postgres sh -c "psql -tA -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -c 'SELECT count(*) FROM $t'")
  printf "%-14s %12s %12s\n" "$t" "$msgs" "$rows"
done

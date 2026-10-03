#!/usr/bin/env bash
# Broker smoke test: create a topic, produce 5 messages, consume them back, delete the topic.
# Runs inside the kafka container: docker compose exec kafka bash < infra/kafka/smoke-test.sh
set -euo pipefail

BIN=/opt/kafka/bin
BS=localhost:9092
TOPIC=smoke-test

echo "== create topic '$TOPIC'"
$BIN/kafka-topics.sh --bootstrap-server $BS --create --if-not-exists --topic $TOPIC \
  --partitions 1 --replication-factor 1

echo "== produce 5 messages"
for i in 1 2 3 4 5; do echo "hello-$i"; done | \
  $BIN/kafka-console-producer.sh --bootstrap-server $BS --topic $TOPIC

echo "== consume from the beginning (offset: message)"
$BIN/kafka-console-consumer.sh --bootstrap-server $BS --topic $TOPIC --from-beginning \
  --max-messages 5 --timeout-ms 15000 --property print.offset=true 2>/dev/null

echo "== describe"
$BIN/kafka-topics.sh --bootstrap-server $BS --describe --topic $TOPIC

echo "== auto-create is off: producing to a missing topic must fail"
if echo "x" | timeout 20 $BIN/kafka-console-producer.sh --bootstrap-server $BS \
     --topic does-not-exist --producer-property max.block.ms=5000 2>/dev/null; then
  :
fi
if $BIN/kafka-topics.sh --bootstrap-server $BS --list | grep -qx does-not-exist; then
  echo "FAIL: topic 'does-not-exist' was auto-created" >&2; exit 1
fi
echo "OK: 'does-not-exist' was not created"

echo "== delete topic '$TOPIC'"
$BIN/kafka-topics.sh --bootstrap-server $BS --delete --topic $TOPIC
echo "SMOKE TEST PASSED"

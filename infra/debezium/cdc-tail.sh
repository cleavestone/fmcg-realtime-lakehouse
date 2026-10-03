#!/usr/bin/env bash
# Print the last N messages of fmcg.public.<table> as "key | value".
# Runs inside the kafka container: bash -s -- <table> [n]
set -euo pipefail

TABLE="${1:?usage: cdc-tail.sh <table> [n]}"
N="${2:-5}"
BIN=/opt/kafka/bin
TOPIC="fmcg.public.${TABLE}"

end=$($BIN/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic "$TOPIC" --time -1 \
  | awk -F: '{print $3}')
start=$(( end > N ? end - N : 0 ))
echo "== $TOPIC offsets $start..$((end - 1))"
$BIN/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --partition 0 --offset "$start" --max-messages $(( end - start )) --timeout-ms 10000 \
  --property print.key=true --property print.offset=true --property key.separator=" | " \
  2>/dev/null

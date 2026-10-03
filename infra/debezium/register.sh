#!/bin/sh
# Create or update the Debezium connector (PUT is idempotent), then wait until the
# connector and its task are RUNNING. Exit 0 on success, 1 on failure or timeout.
set -eu

CONNECT_URL="${CONNECT_URL:-http://kafka-connect:8083}"
NAME="${CONNECTOR_NAME:-fmcg-postgres}"
CONFIG="${CONNECTOR_CONFIG:-/config/fmcg-postgres.json}"
TIMEOUT_S="${REGISTER_TIMEOUT_S:-120}"

echo "register: PUT ${CONNECT_URL}/connectors/${NAME}/config"
curl -fsS -X PUT -H "Content-Type: application/json" --data @"${CONFIG}" \
  "${CONNECT_URL}/connectors/${NAME}/config" > /dev/null

elapsed=0
while [ "$elapsed" -lt "$TIMEOUT_S" ]; do
  status=$(curl -fsS "${CONNECT_URL}/connectors/${NAME}/status" || true)
  running=$(echo "$status" | grep -o '"state":"RUNNING"' | wc -l)
  if echo "$status" | grep -q '"state":"FAILED"'; then
    echo "register: connector FAILED" >&2
    echo "$status" >&2
    exit 1
  fi
  # One state for the connector plus one for its single task.
  if [ "$running" -ge 2 ]; then
    echo "register: ${NAME} and its task are RUNNING"
    exit 0
  fi
  sleep 2
  elapsed=$((elapsed + 2))
done

echo "register: timed out after ${TIMEOUT_S}s; last status: ${status:-none}" >&2
exit 1

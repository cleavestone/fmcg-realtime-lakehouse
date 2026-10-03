#!/bin/sh
# One-shot MinIO setup (idempotent): buckets + a least-privilege service user for
# Spark / Hive Metastore / Trino. Only this job ever uses the MinIO root credentials.
set -eu

: "${MINIO_ROOT_USER:?}" "${MINIO_ROOT_PASSWORD:?}" "${LAKE_ACCESS_KEY:?}" "${LAKE_SECRET_KEY:?}"

export MC_CONFIG_DIR=/tmp/.mc
# Bitnami's MinIO runs a temporary server during first-boot setup and then restarts, so
# the healthcheck can pass a moment before the final server accepts connections: retry.
tries=0
until mc alias set lake http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" > /dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 30 ]; then
    echo "minio-init: MinIO not reachable after 60s" >&2
    exit 1
  fi
  sleep 2
done

for bucket in lakehouse checkpoints; do
  mc mb --ignore-existing "lake/$bucket"
done

cat > /tmp/lakehouse-rw.json <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:*"],
      "Resource": [
        "arn:aws:s3:::lakehouse", "arn:aws:s3:::lakehouse/*",
        "arn:aws:s3:::checkpoints", "arn:aws:s3:::checkpoints/*"
      ]
    }
  ]
}
JSON

# Re-running updates the policy and resets the user's secret to the value in .env.
mc admin policy create lake lakehouse-rw /tmp/lakehouse-rw.json > /dev/null
mc admin user add lake "$LAKE_ACCESS_KEY" "$LAKE_SECRET_KEY" > /dev/null
mc admin policy attach lake lakehouse-rw --user "$LAKE_ACCESS_KEY" > /dev/null 2>&1 || true

echo "minio-init: buckets $(mc ls lake | awk '{print $NF}' | tr '\n' ' ')"
echo "minio-init: user '$LAKE_ACCESS_KEY' has policy: $(mc admin user info lake "$LAKE_ACCESS_KEY" --json | grep -o '"policyName":"[^"]*"')"

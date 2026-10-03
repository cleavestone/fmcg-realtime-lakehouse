# syntax=docker/dockerfile:1
# Thin layer over apache/hive:3.1.3 for a standalone metastore on Postgres + S3A:
#  - replace the bundled 2016 Postgres JDBC driver (9.4, no scram-sha-256) with 42.7.x
#  - put Hadoop's S3A jars (already in the image) on Hive's classpath
#  - idempotent schema init (the stock entrypoint re-runs initSchema on every start)
ARG HIVE_IMAGE
FROM ${HIVE_IMAGE}

USER root
RUN rm -f /opt/hive/lib/postgresql-9.4.1208.jre7.jar \
 && ln -s /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-3.1.0.jar /opt/hive/lib/ \
 && ln -s /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/
ADD --chmod=644 \
    --checksum=sha256:6e0e4cc2d8cae902084f8a2b18728b073a6fd9d1f87c9d8bff8f298c18185b93 \
    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
    /opt/hive/lib/postgresql-42.7.13.jar
COPY --chmod=755 infra/hive-metastore/entrypoint.sh /hms-entrypoint.sh

USER hive
ENTRYPOINT ["/hms-entrypoint.sh"]

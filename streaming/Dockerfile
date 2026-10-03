# syntax=docker/dockerfile:1
# Spark streaming image: Python 3.11 + Java 17 + uv-installed pyspark/delta-spark + pinned JARs.
# Build context: repo root (uv workspace). One image for every Spark job; the job is the command.
ARG PYTHON_IMAGE
ARG UV_IMAGE

FROM ${UV_IMAGE} AS uv

FROM ${PYTHON_IMAGE}

# Java 17 for Spark 3.5 (bookworm still ships it; trixie does not). procps: Spark scripts use ps.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

COPY --from=uv /uv /uvx /bin/

ENV JAVA_HOME=/opt/java \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PYTHONUNBUFFERED=1 \
    SPARK_CONF_DIR=/opt/spark/conf \
    PATH=/app/.venv/bin:$PATH

WORKDIR /app

# 1) Python dependencies only (cached until uv.lock or a pyproject changes).
COPY pyproject.toml uv.lock ./
COPY simulator/pyproject.toml simulator/pyproject.toml
COPY streaming/pyproject.toml streaming/pyproject.toml
COPY dbt/pyproject.toml dbt/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package streaming --no-install-workspace

# 2) Pinned JARs from Maven Central into pyspark's jars dir, each verified against its SHA-1.
ARG MAVEN_REPO=https://repo1.maven.org/maven2
COPY streaming/jars.txt /tmp/jars.txt
RUN set -eu; \
    jars_dir="$(/app/.venv/bin/python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    grep -Ev '^\s*(#|$)' /tmp/jars.txt | while IFS=: read -r group artifact version; do \
      path="$(echo "$group" | tr . /)/${artifact}/${version}/${artifact}-${version}.jar"; \
      curl -fsSL -o "${jars_dir}/${artifact}-${version}.jar" "${MAVEN_REPO}/${path}"; \
      echo "$(curl -fsSL "${MAVEN_REPO}/${path}.sha1" | cut -c1-40)  ${jars_dir}/${artifact}-${version}.jar" | sha1sum -c -; \
    done

# 3) The streaming package and Spark config.
COPY streaming/conf /opt/spark/conf
COPY streaming/src streaming/src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --package streaming --no-editable

RUN useradd --system --uid 10001 --create-home --home-dir /home/spark spark
USER spark

CMD ["python", "-m", "streaming.smoke_test"]

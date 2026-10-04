"""Job settings from the environment (set in compose/streaming.yml from .env)."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap_servers: str
    cdc_topic_pattern: str
    lakehouse_uri: str
    checkpoint_uri: str
    bronze_trigger_seconds: int
    bronze_max_offsets_per_trigger: int


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        kafka_bootstrap_servers=env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        cdc_topic_pattern=env("CDC_TOPIC_PATTERN", r"fmcg\.public\..*"),
        lakehouse_uri=env("LAKEHOUSE_URI", "s3a://lakehouse"),
        checkpoint_uri=env("CHECKPOINT_URI", "s3a://checkpoints"),
        bronze_trigger_seconds=int(env("BRONZE_TRIGGER_SECONDS", "30")),
        bronze_max_offsets_per_trigger=int(env("BRONZE_MAX_OFFSETS_PER_TRIGGER", "50000")),
    )

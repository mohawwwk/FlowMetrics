import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class StreamingConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "ecommerce-transactions"))
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("CHECKPOINT_DIR", "data/checkpoint"))
    processed_dir: str = field(
        default_factory=lambda: os.getenv("PROCESSED_DIR", "data/processed"))
    starting_offsets: str = field(default_factory=lambda: os.getenv("KAFKA_STARTING_OFFSETS", "earliest"))
    trigger_interval_seconds: int = field(
        default_factory=lambda: int(os.getenv("TRIGGER_INTERVAL_SECONDS", "10")))
    watermark_seconds: int = field(
        default_factory=lambda: int(os.getenv("WATERMARK_SECONDS", "60")))
    tumbling_window_seconds: int = field(
        default_factory=lambda: int(os.getenv("TUMBLING_WINDOW_SECONDS", "60")))
    sliding_window_seconds: int = field(
        default_factory=lambda: int(os.getenv("SLIDING_WINDOW_SECONDS", "300")))
    sliding_slide_seconds: int = field(
        default_factory=lambda: int(os.getenv("SLIDING_SLIDE_SECONDS", "60")))
    app_name: str = field(default_factory=lambda: os.getenv("SPARK_APP_NAME", "EcommerceStreaming"))
    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))

    @classmethod
    def from_overrides(
        cls,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        checkpoint_dir: Optional[str] = None,
        processed_dir: Optional[str] = None,
        starting_offsets: Optional[str] = None,
        trigger_interval_seconds: Optional[int] = None,
        watermark_seconds: Optional[int] = None,
    ) -> "StreamingConfig":
        base = cls()
        if bootstrap_servers is not None:
            base.bootstrap_servers = bootstrap_servers
        if topic is not None:
            base.topic = topic
        if checkpoint_dir is not None:
            base.checkpoint_dir = checkpoint_dir
        if processed_dir is not None:
            base.processed_dir = processed_dir
        if starting_offsets is not None:
            base.starting_offsets = starting_offsets
        if trigger_interval_seconds is not None:
            base.trigger_interval_seconds = trigger_interval_seconds
        if watermark_seconds is not None:
            base.watermark_seconds = watermark_seconds
        return base

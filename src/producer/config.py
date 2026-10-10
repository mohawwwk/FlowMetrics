import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass
class ProducerConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC", "ecommerce-transactions"))
    csv_file_path: str = field(default_factory=lambda: os.getenv("CSV_FILE_PATH", "data/raw/online_retail.csv"))
    stream_delay_seconds: float = field(
        default_factory=lambda: float(os.getenv("STREAM_DELAY_SECONDS", "0.1"))
    )
    max_messages: int = field(default_factory=lambda: int(os.getenv("MAX_MESSAGES", "0")))
    client_id: str = field(default_factory=lambda: os.getenv("CLIENT_ID", "ecommerce-producer"))

    @property
    def is_unlimited(self) -> bool:
        return self.max_messages <= 0

    @classmethod
    def from_overrides(
        cls,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        csv_file_path: Optional[str] = None,
        stream_delay_seconds: Optional[float] = None,
        max_messages: Optional[int] = None,
        client_id: Optional[str] = None,
    ) -> "ProducerConfig":
        base = cls()
        if bootstrap_servers is not None:
            base.bootstrap_servers = bootstrap_servers
        if topic is not None:
            base.topic = topic
        if csv_file_path is not None:
            base.csv_file_path = csv_file_path
        if stream_delay_seconds is not None:
            base.stream_delay_seconds = stream_delay_seconds
        if max_messages is not None:
            base.max_messages = max_messages
        if client_id is not None:
            base.client_id = client_id
        return base

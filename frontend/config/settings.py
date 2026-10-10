import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

# Load root .env if present
ROOT_DIR = Path(__file__).resolve().parents[2]
load_dotenv(ROOT_DIR / ".env")


@dataclass
class FrontendConfig:
    project_name: str = "ApexAnalytics"
    project_title: str = "Real-Time Big Data Financial & E-Commerce Platform"
    project_version: str = "v1.0.0 (Production)"
    
    # Database
    db_path: str = field(
        default_factory=lambda: os.getenv("SQLITE_DB_PATH", str(ROOT_DIR / "data" / "ecommerce.db"))
    )
    
    # Kafka
    kafka_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    kafka_topic: str = field(
        default_factory=lambda: os.getenv("KAFKA_TOPIC", "ecommerce-transactions")
    )
    
    # Spark & ML Paths
    checkpoint_dir: str = field(
        default_factory=lambda: os.getenv("CHECKPOINT_DIR", str(ROOT_DIR / "data" / "checkpoint"))
    )
    parquet_path: str = field(
        default_factory=lambda: os.getenv("CLEANED_PARQUET_PATH", str(ROOT_DIR / "data" / "processed" / "cleaned"))
    )
    model_dir: str = field(
        default_factory=lambda: os.getenv("MODEL_OUTPUT_DIR", str(ROOT_DIR / "models" / "revenue_model"))
    )
    raw_csv_path: str = field(
        default_factory=lambda: os.getenv("CSV_FILE_PATH", str(ROOT_DIR / "data" / "raw" / "online_retail.csv"))
    )
    
    # Grafana
    grafana_url: str = field(
        default_factory=lambda: os.getenv("GRAFANA_URL", "http://localhost:3000")
    )
    
    # Refresh intervals
    default_refresh_seconds: int = 3
    
    def get_absolute_db_path(self) -> Path:
        p = Path(self.db_path)
        if not p.is_absolute():
            return ROOT_DIR / p
        return p


config = FrontendConfig()

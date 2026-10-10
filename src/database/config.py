import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseConfig:
    db_path: str = field(
        default_factory=lambda: os.getenv("SQLITE_DB_PATH", "data/ecommerce.db")
    )
    pool_size: int = field(
        default_factory=lambda: int(os.getenv("SQLITE_POOL_SIZE", "5"))
    )
    timeout_seconds: float = field(
        default_factory=lambda: float(os.getenv("SQLITE_TIMEOUT_SECONDS", "30.0"))
    )
    cleaned_parquet_path: str = field(
        default_factory=lambda: os.getenv("CLEANED_PARQUET_PATH", "data/processed/cleaned")
    )
    model_output_dir: str = field(
        default_factory=lambda: os.getenv("MODEL_OUTPUT_DIR", "models/revenue_model")
    )

    def get_absolute_db_path(self) -> Path:
        return Path(self.db_path).resolve()

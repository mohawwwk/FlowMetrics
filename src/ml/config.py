import os
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class MLConfig:
    cleaned_parquet_path: str = field(
        default_factory=lambda: os.getenv("CLEANED_PARQUET_PATH", "data/processed/cleaned")
    )
    model_output_dir: str = field(
        default_factory=lambda: os.getenv("MODEL_OUTPUT_DIR", "models/revenue_model")
    )
    test_split_ratio: float = field(
        default_factory=lambda: float(os.getenv("TEST_SPLIT_RATIO", "0.2"))
    )
    random_seed: int = field(
        default_factory=lambda: int(os.getenv("RANDOM_SEED", "42"))
    )
    features_col: str = "features"
    label_col: str = "Revenue"
    prediction_col: str = "prediction"

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("WatermarkManager")


class WatermarkManager:
    """Manages high-watermark timestamps for incremental ETL state tracking."""

    def __init__(self, state_file: str = "data/watermark_state.json"):
        self.state_file = Path(state_file)
        self.state_file.parent.mkdir(parents=True, exist_ok=True)

    def get_last_watermark(self) -> Optional[str]:
        """Reads the high-watermark ISO timestamp from state storage."""
        if not self.state_file.exists():
            return None
        try:
            state = json.loads(self.state_file.read_text(encoding="utf-8"))
            return state.get("high_watermark")
        except Exception as e:
            logger.warning(f"Could not read watermark state ({e}), defaulting to full load.")
            return None

    def update_watermark(self, new_watermark: str) -> None:
        """Persists the latest high-watermark timestamp."""
        payload = {
            "high_watermark": new_watermark,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }
        self.state_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        logger.info(f"High-watermark updated to: {new_watermark}")

    def filter_incremental_records(self, df: DataFrame, timestamp_col: str = "api_last_updated") -> DataFrame:
        """Filters incoming dataset to include only records strictly newer than watermark."""
        last_wm = self.get_last_watermark()
        if not last_wm:
            logger.info("No prior watermark found. Proceeding with full initial load.")
            return df

        logger.info(f"Applying incremental filter: {timestamp_col} > '{last_wm}'")
        incremental_df = df.filter(F.col(timestamp_col) > F.lit(last_wm))
        return incremental_df

    def compute_and_save_latest_watermark(self, df: DataFrame, timestamp_col: str = "api_last_updated") -> Optional[str]:
        """Finds max timestamp in current batch and updates state."""
        max_val = df.select(F.max(timestamp_col).alias("max_ts")).collect()[0]["max_ts"]
        if max_val:
            max_ts_str = max_val.isoformat() if hasattr(max_val, "isoformat") else str(max_val)
            self.update_watermark(max_ts_str)
            return max_ts_str
        return None

import logging
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.transformation.spark_session import PySparkManager
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, TimestampType
)
from src.transformation.spark_session import PySparkManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DataCleaner:
    """Transformation pipeline to clean raw crypto market Parquet data using PySpark."""

    def __init__(self):
        self.spark = PySparkManager.get_spark_session()

    def clean_market_data(self, input_path: str, output_path: str) -> Optional[DataFrame]:
        """Reads raw market parquet file, enforces schema, cleans, and writes processed dataset."""
        logger.info(f"Reading raw Parquet data from: {input_path}")
        
        try:
            raw_df = self.spark.read.parquet(input_path)

            cleaned_df = (
                raw_df
                # 1. Select key financial columns
                .select(
                    F.col("id").alias("coin_id"),
                    F.col("symbol"),
                    F.col("name"),
                    F.col("current_price").cast(DoubleType()),
                    F.col("market_cap").cast(LongType()),
                    F.col("market_cap_rank").cast(LongType()),
                    F.col("total_volume").cast(LongType()),
                    F.col("high_24h").cast(DoubleType()),
                    F.col("low_24h").cast(DoubleType()),
                    F.col("price_change_percentage_24h").cast(DoubleType()),
                    F.to_timestamp(F.col("last_updated")).alias("api_last_updated")
                )
                # 2. Filter out records missing vital prices
                .filter(F.col("current_price").isNotNull() & F.col("coin_id").isNotNull())
                # 3. Add derived metrics
                .withColumn("price_spread_24h", F.round(F.col("high_24h") - F.col("low_24h"), 2))
                .withColumn("ingestion_timestamp", F.current_timestamp())
            )

            logger.info(f"Successfully processed {cleaned_df.count()} records.")
            
            # Write transformed data back to Parquet
            logger.info(f"Writing cleaned data to: {output_path}")
            cleaned_df.write.mode("overwrite").parquet(output_path)
            
            return cleaned_df

        except Exception as e:
            logger.error(f"Error during PySpark data cleaning: {e}")
            raise


if __name__ == "__main__":
    cleaner = DataCleaner()
    # Locate latest raw file dynamically
    raw_files = list(Path("data/raw").glob("**/*.parquet"))
    
    if raw_files:
        latest_raw = str(raw_files[-1])
        output_dir = "data/processed/crypto_markets_cleaned.parquet"
        
        cleaned_data = cleaner.clean_market_data(latest_raw, output_dir)
        if cleaned_data is not None:
            cleaned_data.show(5, truncate=False)
    else:
        logger.warning("No raw Parquet files found under data/raw to clean.")
    
    PySparkManager.stop_session()
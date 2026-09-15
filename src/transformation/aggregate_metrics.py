import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from src.transformation.spark_session import PySparkManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class MetricsAggregator:
    """Aggregates cleaned market data into financial analytical summaries using PySpark."""

    def __init__(self):
        self.spark = PySparkManager.get_spark_session()

    def compute_market_summary(self, input_path: str, output_path: str) -> Optional[DataFrame]:
        """Calculates market tier groupings, share of total volume, and relative price metrics."""
        logger.info(f"Reading cleaned data from: {input_path}")
        
        try:
            df = self.spark.read.parquet(input_path)

            # Window specification across entire market snapshot
            total_market_window = Window.partitionBy()

            summary_df = (
                df
                # 1. Segment coins by Market Cap tier
                .withColumn(
                    "market_tier",
                    F.when(F.col("market_cap_rank") <= 5, "Mega-Cap")
                     .when(F.col("market_cap_rank") <= 20, "Large-Cap")
                     .otherwise("Mid/Small-Cap")
                )
                # 2. Compute percentage share of total 24h trading volume
                .withColumn("total_market_volume", F.sum("total_volume").over(total_market_window))
                .withColumn(
                    "volume_market_share_pct",
                    F.round((F.col("total_volume") / F.col("total_market_volume")) * 100, 2)
                )
                # 3. Compute intraday price volatility percentage
                .withColumn(
                    "volatility_pct",
                    F.round(((F.col("high_24h") - F.col("low_24h")) / F.col("low_24h")) * 100, 2)
                )
                # 4. Select relevant analytics dimensions and facts
                .select(
                    "coin_id",
                    "symbol",
                    "market_tier",
                    "current_price",
                    "price_spread_24h",
                    "volatility_pct",
                    "volume_market_share_pct",
                    "market_cap_rank",
                    "ingestion_timestamp"
                )
                .orderBy(F.col("market_cap_rank").asc())
            )

            logger.info(f"Writing aggregated analytics data to: {output_path}")
            summary_df.write.mode("overwrite").parquet(output_path)
            
            return summary_df

        except Exception as e:
            logger.error(f"Error aggregating market metrics: {e}")
            raise


if __name__ == "__main__":
    aggregator = MetricsAggregator()
    input_dir = "data/processed/crypto_markets_cleaned.parquet"
    output_dir = "data/analytics/crypto_market_summary.parquet"

    if Path(input_dir).exists():
        summary = aggregator.compute_market_summary(input_dir, output_dir)
        if summary is not None:
            summary.show(10, truncate=False)
    else:
        logger.warning(f"Cleaned dataset not found at {input_dir}. Run clean_data.py first.")

    PySparkManager.stop_session()
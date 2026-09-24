import logging
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from src.transformation.spark_session import PySparkManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("MetricsAggregator")


class MetricsAggregator:
    """Computes market aggregations, applies broadcast dimension joins, and writes partitioned Parquet."""

    def __init__(self):
        self.spark = PySparkManager.get_spark_session()

    def _get_currency_dimension_table(self) -> DataFrame:
        """Small reference dimension table suitable for broadcast joins."""
        schema = StructType([
            StructField("currency_code", StringType(), False),
            StructField("fx_rate_to_eur", DoubleType(), False)
        ])
        data = [("usd", 0.92), ("eur", 1.00), ("gbp", 1.17)]
        return self.spark.createDataFrame(data, schema)

    def compute_market_summary(self, input_path: str, output_path: str) -> DataFrame:
        """Processes cleaned records into optimized analytical summaries with partition pruning."""
        logger.info(f"Reading cleaned data from: {input_path}")
        df = self.spark.read.parquet(input_path)

        # 1. Volatility and Tier Categorization
        window_all = Window.partitionBy()
        summary_df = (
            df
            .withColumn("volatility_pct", F.round((F.col("price_spread_24h") / F.col("current_price")) * 100.0, 2))
            .withColumn(
                "market_tier",
                F.when(F.col("market_cap_rank") <= 5, "Mega-Cap")
                .when((F.col("market_cap_rank") > 5) & (F.col("market_cap_rank") <= 20), "Large-Cap")
                .otherwise("Mid/Small-Cap")
            )
            .withColumn("total_market_volume", F.sum("total_volume").over(window_all))
            .withColumn(
                "volume_market_share_pct",
                F.round((F.col("total_volume") / F.col("total_market_volume")) * 100.0, 2)
            )
            .withColumn("trade_date", F.to_date(F.col("ingestion_timestamp")))
            .withColumn("currency_code", F.lit("usd"))
        )

        # 2. Broadcast Join with reference dimension
        fx_dim_df = self._get_currency_dimension_table()
        enriched_df = summary_df.join(
            F.broadcast(fx_dim_df),
            on="currency_code",
            how="left"
        ).withColumn(
            "current_price_eur",
            F.round(F.col("current_price") * F.col("fx_rate_to_eur"), 2)
        )

        # 3. Partitioned Storage Write
        logger.info(f"Writing partitioned analytics dataset to: {output_path}")
        (
            enriched_df
            .write
            .mode("overwrite")
            .partitionBy("trade_date", "market_tier")
            .parquet(output_path)
        )

        return enriched_df


if __name__ == "__main__":
    aggregator = MetricsAggregator()
    aggregator.compute_market_summary(
        "data/processed/crypto_markets_cleaned.parquet",
        "data/analytics/crypto_market_summary.parquet"
    )

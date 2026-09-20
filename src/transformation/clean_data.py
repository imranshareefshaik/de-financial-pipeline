import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, IntegerType, TimestampType
from src.transformation.spark_session import PySparkManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataCleaner")


class DataCleaner:
    """Cleans and casts raw cryptocurrency market data using PySpark."""

    def __init__(self):
        self.spark = PySparkManager.get_spark_session()

    def clean_market_data(self, input_path: str, output_path: str) -> DataFrame:
        """Reads raw parquet data, cleans types, handles nulls, and saves output."""
        logger.info(f"Reading raw Parquet data from: {input_path}")
        raw_df = self.spark.read.parquet(input_path)

        cleaned_df = (
            raw_df
            .filter(F.col("id").isNotNull())
            .withColumnRenamed("id", "coin_id")
            .withColumn("current_price", F.col("current_price").cast(DoubleType()))
            .withColumn("market_cap", F.col("market_cap").cast(LongType()))
            .withColumn("market_cap_rank", F.col("market_cap_rank").cast(IntegerType()))
            .withColumn("total_volume", F.col("total_volume").cast(LongType()))
            .withColumn("high_24h", F.col("high_24h").cast(DoubleType()))
            .withColumn("low_24h", F.col("low_24h").cast(DoubleType()))
            .withColumn("price_change_percentage_24h", F.col("price_change_percentage_24h").cast(DoubleType()))
            .withColumn("api_last_updated", F.to_timestamp(F.col("last_updated")))
            .withColumn("price_spread_24h", F.round(F.col("high_24h") - F.col("low_24h"), 4))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .drop("last_updated", "roi", "image")
        )

        record_count = cleaned_df.count()
        logger.info(f"Successfully processed {record_count} records.")

        logger.info(f"Writing cleaned data to: {output_path}")
        cleaned_df.write.mode("overwrite").parquet(output_path)
        return cleaned_df


if __name__ == "__main__":
    cleaner = DataCleaner()
    cleaner.clean_market_data("data/raw/2026/09/20/crypto_markets.parquet", "data/processed/crypto_markets_cleaned.parquet")

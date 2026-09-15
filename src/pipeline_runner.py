import logging
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingestion.api_client import APIClient
from src.ingestion.storage import LocalStorageHandler
from src.transformation.clean_data import DataCleaner
from src.transformation import aggregate_metrics
from src.transformation.db_loader import DatabaseLoader
from src.transformation.spark_session import PySparkManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("PipelineRunner")


def run_pipeline() -> None:
    """Executes the end-to-end financial data pipeline."""
    start_time = time.time()
    logger.info("Starting financial data pipeline execution...")

    try:
        # Step 1: Ingestion
        logger.info("Stage 1/4: Ingesting raw crypto market data from API...")
        client = APIClient()
        raw_data = client.get_market_data(per_page=20)
        
        storage = LocalStorageHandler()
        raw_file_path = storage.save(raw_data, filename="crypto_markets.parquet")
        logger.info(f"Stage 1 complete. Saved raw data to: {raw_file_path}")

        # Step 2: Data Cleaning via PySpark
        logger.info("Stage 2/4: Cleaning and casting raw data with PySpark...")
        cleaner = DataCleaner()
        cleaned_dir = "data/processed/crypto_markets_cleaned.parquet"
        cleaner.clean_market_data(raw_file_path, cleaned_dir)
        logger.info("Stage 2 complete. Cleaned dataset generated.")

        # Step 3: Aggregations & Volatility Metrics
        logger.info("Stage 3/4: Computing market metrics and tier aggregations...")
        aggregator_class = getattr(aggregate_metrics, "MarketAggregator")
        aggregator = aggregator_class()
        analytics_dir = "data/analytics/crypto_market_summary.parquet"
        aggregator.compute_market_summary(cleaned_dir, analytics_dir)
        logger.info("Stage 3 complete. Analytics dataset generated.")

        # Step 4: Loading into Database Warehouse
        logger.info("Stage 4/4: Loading curated analytics data into SQL database...")
        loader = DatabaseLoader()
        rows_loaded = loader.load_analytics_data(analytics_dir)
        logger.info(f"Stage 4 complete. Successfully loaded {rows_loaded} rows.")

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Pipeline executed successfully in {elapsed}s.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
    finally:
        # Ensure PySpark cluster resources are released cleanly
        PySparkManager.stop_session()


if __name__ == "__main__":
    run_pipeline()
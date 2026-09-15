import logging
import sys
import time
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ingestion.api_client import APIClient
from src.ingestion.storage import LocalStorageHandler
from src.transformation.clean_data import DataCleaner
from src.transformation.aggregate_metrics import MetricsAggregator
from src.transformation.db_loader import DatabaseLoader
from src.transformation.spark_session import PySparkManager
from src.utils.config_loader import load_config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("PipelineRunner")


def run_pipeline(config_path: str = "config/pipeline_config.yaml") -> None:
    """Executes the financial data pipeline driven by YAML configuration."""
    start_time = time.time()
    cfg = load_config(config_path)
    logger.info(f"Loaded pipeline configuration for environment: {cfg['app']['environment']}")

    try:
        # Stage 1: Ingestion
        logger.info("Stage 1/4: Ingesting raw crypto market data...")
        client = APIClient(base_url=cfg["ingestion"]["base_url"])
        raw_data = client.get_market_data(
            vs_currency=cfg["ingestion"]["vs_currency"],
            per_page=cfg["ingestion"]["per_page"]
        )

        storage = LocalStorageHandler(base_path=cfg["ingestion"]["raw_storage_path"])
        raw_file_path = storage.save(raw_data, filename="crypto_markets.parquet")
        logger.info(f"Stage 1 complete. Saved raw data to: {raw_file_path}")

        # Stage 2: Data Cleaning via PySpark
        logger.info("Stage 2/4: Cleaning raw Parquet data...")
        cleaner = DataCleaner()
        cleaned_path = cfg["transformation"]["processed_storage_path"]
        cleaner.clean_market_data(raw_file_path, cleaned_path)
        logger.info(f"Stage 2 complete. Cleaned data at: {cleaned_path}")

        # Stage 3: Aggregations & Volatility Metrics
        logger.info("Stage 3/4: Computing market analytics and tier aggregations...")
        aggregator = MetricsAggregator()
        analytics_path = cfg["transformation"]["analytics_storage_path"]
        aggregator.compute_market_summary(cleaned_path, analytics_path)
        logger.info(f"Stage 3 complete. Analytics data at: {analytics_path}")

        # Stage 4: Loading into Database Warehouse
        logger.info("Stage 4/4: Loading curated analytics into warehouse...")
        db_path = Path(__file__).resolve().parents[1] / cfg["warehouse"]["db_path"]
        db_url = f"sqlite:///{db_path}"
        loader = DatabaseLoader(db_url=db_url)
        rows_loaded = loader.load_analytics_data(
            analytics_path,
            table_name=cfg["warehouse"]["table_name"]
        )
        logger.info(f"Stage 4 complete. Successfully loaded {rows_loaded} rows into table '{cfg['warehouse']['table_name']}'.")

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Pipeline executed successfully in {elapsed}s.")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)
        raise
    finally:
        PySparkManager.stop_session()


if __name__ == "__main__":
    run_pipeline()

import argparse
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
from src.quality.validator import DataQualityValidator, DataQualityError
from src.quality.drift_detector import StatisticalDriftDetector, DataDriftException
from src.warehouse.analytics_views import WarehouseViewManager
from src.utils.config_loader import load_config
from src.utils.audit_logger import PipelineAuditLogger
from src.utils.notifier import PipelineNotifier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s"
)
logger = logging.getLogger("PipelineRunner")


def run_pipeline(
    config_path: str = "config/pipeline_config.yaml",
    limit_override: int = None,
    skip_ingestion: bool = False
) -> None:
    """Executes financial data pipeline with audit logging, quality gates, drift checks, and alerting."""
    start_time = time.time()
    cfg = load_config(config_path)
    per_page = limit_override or cfg["ingestion"]["per_page"]
    logger.info(f"Loaded pipeline configuration (limit={per_page}, skip_ingestion={skip_ingestion})")

    storage = LocalStorageHandler(base_path=cfg["ingestion"]["raw_storage_path"])
    audit_logger = PipelineAuditLogger()
    notifier = PipelineNotifier()
    rows_loaded = 0
    current_stage = "Initialization"

    try:
        # Stage 1: Ingestion
        current_stage = "Stage 1: Ingestion"
        if not skip_ingestion:
            logger.info("Stage 1/7: Ingesting raw crypto market data from API...")
            client = APIClient(base_url=cfg["ingestion"]["base_url"])
            raw_data = client.get_market_data(
                vs_currency=cfg["ingestion"]["vs_currency"],
                per_page=per_page
            )
            raw_file_path = storage.save(raw_data, filename="crypto_markets.parquet")
            logger.info(f"Stage 1 complete. Saved raw data to: {raw_file_path}")
        else:
            logger.info("Stage 1/7: Skipped ingestion. Locating most recent raw Parquet file...")
            raw_files = sorted(list(Path(cfg["ingestion"]["raw_storage_path"]).glob("**/*.parquet")))
            if not raw_files:
                raise FileNotFoundError("No existing raw Parquet files found to backfill.")
            raw_file_path = str(raw_files[-1])
            logger.info(f"Using raw file: {raw_file_path}")

        # Stage 2: Data Cleaning via PySpark
        current_stage = "Stage 2: Data Cleaning"
        logger.info("Stage 2/7: Cleaning raw Parquet data...")
        cleaner = DataCleaner()
        cleaned_path = cfg["transformation"]["processed_storage_path"]
        cleaner.clean_market_data(raw_file_path, cleaned_path)
        logger.info(f"Stage 2 complete. Cleaned data at: {cleaned_path}")

        # Stage 3: Data Quality Gate
        current_stage = "Stage 3: Data Quality Gate"
        logger.info("Stage 3/7: Running Data Quality audit...")
        spark = PySparkManager.get_spark_session()
        persisted_df = spark.read.parquet(cleaned_path)
        
        validator = DataQualityValidator(persisted_df, dataset_name="CleanedMarketData")
        validator.run_all(
            primary_key="coin_id",
            required_cols=["coin_id", "symbol"],
            positive_cols=["current_price"]
        )
        logger.info("Stage 3 complete. Data quality verified.")

        # Stage 4: Aggregations & Volatility Metrics
        current_stage = "Stage 4: Analytics Aggregations"
        logger.info("Stage 4/7: Computing market analytics and tier aggregations...")
        aggregator = MetricsAggregator()
        analytics_path = cfg["transformation"]["analytics_storage_path"]
        aggregator.compute_market_summary(cleaned_path, analytics_path)
        logger.info(f"Stage 4 complete. Analytics data at: {analytics_path}")

        # Stage 5: Statistical Data Drift Detection
        current_stage = "Stage 5: Drift Detection"
        logger.info("Stage 5/7: Inspecting metrics for statistical data drift...")
        analytics_df = spark.read.parquet(analytics_path)
        drift_detector = StatisticalDriftDetector(analytics_df, max_price_drift_pct=75.0)
        # Using a conservative baseline based on top cryptocurrency historical averages
        baseline_stats = {"avg_price": 8000.0, "avg_volatility": 4.5}
        drift_detector.check_drift_against_baseline(baseline_stats)
        logger.info("Stage 5 complete. No critical drift detected.")

        # Stage 6: Loading into Database Warehouse
        current_stage = "Stage 6: Warehouse Loading"
        logger.info("Stage 6/7: Loading curated analytics into warehouse...")
        db_path = Path(__file__).resolve().parents[1] / cfg["warehouse"]["db_path"]
        db_url = f"sqlite:///{db_path}"
        loader = DatabaseLoader(db_url=db_url)
        rows_loaded = loader.load_analytics_data(
            analytics_path,
            table_name=cfg["warehouse"]["table_name"]
        )
        logger.info(f"Stage 6 complete. Loaded {rows_loaded} rows into table '{cfg['warehouse']['table_name']}'.")

        # Stage 7: Refresh Analytical Views
        current_stage = "Stage 7: View Synchronization"
        logger.info("Stage 7/7: Refreshing SQL analytics views and data marts...")
        view_manager = WarehouseViewManager(db_url=db_url)
        view_manager.create_views()
        logger.info("Stage 7 complete. Analytical views synchronized.")

        elapsed = round(time.time() - start_time, 2)
        logger.info(f"Pipeline executed successfully in {elapsed}s.")

        audit_logger.log_run(
            pipeline_name=cfg["app"]["name"],
            status="SUCCESS",
            records_ingested=rows_loaded,
            duration_seconds=elapsed
        )
        notifier.notify_success({
            "pipeline_name": cfg["app"]["name"],
            "records_ingested": rows_loaded,
            "duration_seconds": elapsed,
            "environment": cfg["app"].get("environment", "local")
        })

    except Exception as e:
        elapsed = round(time.time() - start_time, 2)
        audit_logger.log_run(
            pipeline_name=cfg["app"]["name"],
            status="FAILED",
            records_ingested=rows_loaded,
            duration_seconds=elapsed,
            error_message=str(e)
        )
        notifier.notify_failure(
            error_message=str(e),
            stage=current_stage,
            run_metrics={"duration_seconds": elapsed}
        )
        logger.error(f"Pipeline execution failed at '{current_stage}': {e}", exc_info=True)
        raise
    finally:
        PySparkManager.stop_session()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-to-End Financial Data Engineering Pipeline")
    parser.add_argument("--config", type=str, default="config/pipeline_config.yaml", help="Path to pipeline YAML config")
    parser.add_argument("--limit", type=int, default=None, help="Override ingestion asset count")
    parser.add_argument("--skip-ingestion", action="store_true", help="Bypass API call and process latest raw Parquet partition")

    args = parser.parse_args()
    run_pipeline(
        config_path=args.config,
        limit_override=args.limit,
        skip_ingestion=args.skip_ingestion
    )

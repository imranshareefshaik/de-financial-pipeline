import logging
from typing import Optional
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class PySparkManager:
    """Singleton-style manager for PySpark session setup and lifecycle management."""

    _instance: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(cls, app_name: str = "DE-Financial-Pipeline") -> SparkSession:
        """Create or return an existing PySpark Session with optimized local configs."""
        if cls._instance is None:
            logger.info("Initializing new PySpark Session...")
            try:
                cls._instance = (
                    SparkSession.builder
                    .appName(app_name)
                    .master("local[*]")  # Utilize all available CPU cores locally
                    .config("spark.driver.memory", "2g")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                    .config("spark.sql.shuffle.partitions", "4")
                    .getOrCreate()
                )
                logger.info(f"PySpark Session '{app_name}' initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to start PySpark Session: {e}")
                raise RuntimeError("PySpark initialization failed") from e

        return cls._instance

    @classmethod
    def stop_session(cls) -> None:
        """Safely stop active PySpark session."""
        if cls._instance is not None:
            logger.info("Stopping active PySpark Session...")
            cls._instance.stop()
            cls._instance = None
            logger.info("PySpark Session stopped.")


if __name__ == "__main__":
    # Test local execution
    spark = PySparkManager.get_spark_session()
    print(f"Active Spark Version: {spark.version}")
    PySparkManager.stop_session()
import os
import sys
import logging
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession

# Bind Spark workers to current virtual environment python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

hadoop_home = str(Path.home() / "hadoop")
if Path(hadoop_home).exists():
    os.environ["HADOOP_HOME"] = hadoop_home
    hadoop_bin = str(Path(hadoop_home) / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{hadoop_bin};{os.environ.get('PATH', '')}"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger(__name__)


class PySparkManager:
    _instance: Optional[SparkSession] = None

    @classmethod
    def get_spark_session(cls, app_name: str = "DE-Financial-Pipeline") -> SparkSession:
        if cls._instance is None:
            logger.info("Initializing PySpark Session with AQE & Query Tuning...")
            try:
                cls._instance = (
                    SparkSession.builder
                    .appName(app_name)
                    .master("local[*]")
                    .config("spark.driver.memory", "2g")
                    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
                    .config("spark.sql.shuffle.partitions", "4")
                    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
                    # Adaptive Query Execution (AQE) Optimizations
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB auto-broadcast
                    .getOrCreate()
                )
                logger.info(f"PySpark Session '{app_name}' initialized successfully.")
            except Exception as e:
                logger.error(f"Failed to start PySpark Session: {e}")
                raise RuntimeError("PySpark initialization failed") from e

        return cls._instance

    @classmethod
    def stop_session(cls) -> None:
        if cls._instance is not None:
            logger.info("Stopping active PySpark Session...")
            cls._instance.stop()
            cls._instance = None
            logger.info("PySpark Session stopped.")

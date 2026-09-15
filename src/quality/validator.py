import logging
from typing import List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("DataQualityValidator")


class DataQualityError(Exception):
    """Raised when critical data quality checks fail."""
    pass


class DataQualityValidator:
    """Enforces schema rules, completeness, and value sanity on PySpark DataFrames."""

    def __init__(self, df: DataFrame, dataset_name: str = "dataset"):
        self.df = df
        self.dataset_name = dataset_name
        self.check_results: List[Dict[str, Any]] = []

    def check_non_empty(self, min_rows: int = 1) -> "DataQualityValidator":
        """Verify the dataset contains at least min_rows records."""
        count = self.df.count()
        passed = count >= min_rows
        self.check_results.append({
            "check": "min_rows",
            "passed": passed,
            "details": f"Expected >= {min_rows}, found {count}"
        })
        if not passed:
            raise DataQualityError(f"[{self.dataset_name}] Empty or insufficient records: {count}")
        return self

    def check_not_null(self, columns: List[str]) -> "DataQualityValidator":
        """Ensure critical identifier columns contain zero null values."""
        for col in columns:
            null_count = self.df.filter(F.col(col).isNull()).count()
            passed = null_count == 0
            self.check_results.append({
                "check": f"not_null_{col}",
                "passed": passed,
                "details": f"Found {null_count} nulls"
            })
            if not passed:
                raise DataQualityError(f"[{self.dataset_name}] Null values found in mandatory column '{col}': {null_count}")
        return self

    def check_positive_values(self, columns: List[str]) -> "DataQualityValidator":
        """Ensure numerical values (prices, market caps) are greater than zero."""
        for col in columns:
            invalid_count = self.df.filter(F.col(col) <= 0).count()
            passed = invalid_count == 0
            self.check_results.append({
                "check": f"positive_values_{col}",
                "passed": passed,
                "details": f"Found {invalid_count} non-positive values"
            })
            if not passed:
                raise DataQualityError(f"[{self.dataset_name}] Non-positive values found in column '{col}': {invalid_count}")
        return self

    def check_uniqueness(self, primary_key: str) -> "DataQualityValidator":
        """Ensure primary key values are unique."""
        total_count = self.df.count()
        unique_count = self.df.select(primary_key).distinct().count()
        passed = total_count == unique_count
        self.check_results.append({
            "check": f"uniqueness_{primary_key}",
            "passed": passed,
            "details": f"Total: {total_count}, Unique: {unique_count}"
        })
        if not passed:
            raise DataQualityError(f"[{self.dataset_name}] Duplicate primary keys found in '{primary_key}'")
        return self

    def run_all(self, primary_key: str, required_cols: List[str], positive_cols: List[str]) -> bool:
        """Executes full suite of quality gates."""
        logger.info(f"Running data quality audit on '{self.dataset_name}'...")
        (
            self.check_non_empty(min_rows=1)
            .check_not_null(required_cols)
            .check_positive_values(positive_cols)
            .check_uniqueness(primary_key)
        )
        logger.info(f"All data quality gates PASSED for '{self.dataset_name}'.")
        return True


if __name__ == "__main__":
    from src.transformation.spark_session import PySparkManager

    spark = PySparkManager.get_spark_session()
    path = "data/processed/crypto_markets_cleaned.parquet"
    
    try:
        df = spark.read.parquet(path)
        validator = DataQualityValidator(df, dataset_name="CleanedCryptoData")
        validator.run_all(
            primary_key="coin_id",
            required_cols=["coin_id", "symbol"],
            positive_cols=["current_price"]
        )
    finally:
        PySparkManager.stop_session()
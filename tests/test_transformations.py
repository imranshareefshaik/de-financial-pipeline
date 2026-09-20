import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from src.transformation.spark_session import PySparkManager
from src.transformation.aggregate_metrics import MetricsAggregator
from src.quality.validator import DataQualityValidator, DataQualityError


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provides a shared PySpark session for unit tests."""
    return PySparkManager.get_spark_session(app_name="DE-Test-Session")


def test_data_quality_validator_passes(spark):
    """Verify validator passes with healthy schema and clean rows."""
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("symbol", StringType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("market_cap", LongType(), True)
    ])
    
    data = [
        ("bitcoin", "btc", 60000.0, 1200000000),
        ("ethereum", "eth", 3000.0, 400000000)
    ]
    df = spark.createDataFrame(data, schema)
    
    validator = DataQualityValidator(df, dataset_name="TestDataset")
    result = validator.run_all(
        primary_key="coin_id",
        required_cols=["coin_id", "symbol"],
        positive_cols=["current_price"]
    )
    assert result is True


def test_data_quality_validator_fails_on_null_primary_key(spark):
    """Verify validator raises an exception when primary key has nulls."""
    schema = StructType([
        StructField("coin_id", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("current_price", DoubleType(), False)
    ])
    
    data = [
        (None, "btc", 60000.0),
        ("ethereum", "eth", 3000.0)
    ]
    df = spark.createDataFrame(data, schema)
    
    validator = DataQualityValidator(df, dataset_name="TestDatasetNulls")
    with pytest.raises(DataQualityError):
        validator.check_not_null(["coin_id"])


def test_data_quality_validator_fails_on_negative_price(spark):
    """Verify validator catches illegal negative values."""
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("current_price", DoubleType(), False)
    ])
    
    data = [("doge", -0.15)]
    df = spark.createDataFrame(data, schema)
    
    validator = DataQualityValidator(df, dataset_name="TestDatasetNegative")
    with pytest.raises(DataQualityError):
        validator.check_positive_values(["current_price"])
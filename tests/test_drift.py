import pytest
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.transformation.spark_session import PySparkManager
from src.quality.drift_detector import StatisticalDriftDetector, DataDriftException


@pytest.fixture(scope="session")
def spark():
    return PySparkManager.get_spark_session()


def test_drift_detector_healthy(spark):
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("volatility_pct", DoubleType(), False)
    ])
    data = [("btc", 60000.0, 2.5), ("eth", 3000.0, 3.1)]
    df = spark.createDataFrame(data, schema)

    detector = StatisticalDriftDetector(df, max_price_drift_pct=25.0)
    baseline = {"avg_price": 31000.0, "avg_volatility": 2.8}
    assert detector.check_drift_against_baseline(baseline) is True


def test_drift_detector_flags_anomaly(spark):
    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("volatility_pct", DoubleType(), False)
    ])
    # Extreme price collapse
    data = [("btc", 100.0, 2.5), ("eth", 50.0, 3.1)]
    df = spark.createDataFrame(data, schema)

    detector = StatisticalDriftDetector(df, max_price_drift_pct=20.0)
    baseline = {"avg_price": 30000.0, "avg_volatility": 2.5}
    with pytest.raises(DataDriftException):
        detector.check_drift_against_baseline(baseline)

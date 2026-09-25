import pytest
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from src.transformation.spark_session import PySparkManager
from src.transformation.watermark_manager import WatermarkManager


@pytest.fixture(scope="session")
def spark():
    return PySparkManager.get_spark_session()


def test_watermark_state_lifecycle(tmp_path):
    state_file = tmp_path / "watermark_state.json"
    wm = WatermarkManager(state_file=str(state_file))

    assert wm.get_last_watermark() is None

    sample_ts = "2026-09-25T12:00:00"
    wm.update_watermark(sample_ts)
    assert wm.get_last_watermark() == sample_ts


def test_filter_incremental_records(spark, tmp_path):
    state_file = tmp_path / "watermark_state.json"
    wm = WatermarkManager(state_file=str(state_file))

    schema = StructType([
        StructField("coin_id", StringType(), False),
        StructField("api_last_updated", StringType(), False)
    ])

    data = [
        ("btc", "2026-09-25 10:00:00"),
        ("eth", "2026-09-25 14:00:00")
    ]
    df = spark.createDataFrame(data, schema)

    wm.update_watermark("2026-09-25 12:00:00")
    filtered = wm.filter_incremental_records(df, timestamp_col="api_last_updated")

    assert filtered.count() == 1
    assert filtered.collect()[0]["coin_id"] == "eth"

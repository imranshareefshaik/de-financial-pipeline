import json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from src.transformation.spark_session import PySparkManager
from src.utils.lineage_tracker import LineageTracker

def test_lineage_recording(tmp_path):
    catalog_file = tmp_path / "test_lineage.json"
    tracker = LineageTracker(catalog_path=str(catalog_file))

    schema_snapshot = {"coin_id": "StringType()", "current_price": "DoubleType()"}
    tracker.record_lineage(
        stage_name="TestStage",
        inputs=["raw/data.parquet"],
        outputs=["processed/data.parquet"],
        schema_snapshot=schema_snapshot,
        record_count=100
    )

    assert catalog_file.exists()
    catalog = json.loads(catalog_file.read_text(encoding="utf-8"))
    assert len(catalog) == 1
    assert catalog[0]["stage"] == "TestStage"
    assert catalog[0]["record_count"] == 100

def test_schema_drift_detection(tmp_path):
    catalog_file = tmp_path / "test_lineage.json"
    tracker = LineageTracker(catalog_path=str(catalog_file))
    spark = PySparkManager.get_spark_session()

    # Step 1: Initial schema baseline
    schema_v1 = StructType([
        StructField("coin_id", StringType(), False),
        StructField("current_price", DoubleType(), False)
    ])
    df_v1 = spark.createDataFrame([("btc", 60000.0)], schema_v1)
    tracker.record_lineage(
        stage_name="CleaningStage",
        inputs=["raw.parquet"],
        outputs=["clean.parquet"],
        schema_snapshot={f.name: str(f.dataType) for f in schema_v1.fields},
        record_count=1
    )

    # Step 2: Schema v2 with an added column 'market_cap_rank'
    schema_v2 = StructType([
        StructField("coin_id", StringType(), False),
        StructField("current_price", DoubleType(), False),
        StructField("market_cap_rank", IntegerType(), True)
    ])
    df_v2 = spark.createDataFrame([("btc", 60000.0, 1)], schema_v2)

    diff = tracker.detect_schema_drift("CleaningStage", df_v2)
    assert "market_cap_rank" in diff["added_columns"]
    assert len(diff["removed_columns"]) == 0

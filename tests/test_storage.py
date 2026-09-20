import json
import pytest
from src.ingestion.storage import LocalStorageHandler


@pytest.fixture
def storage(tmp_path):
    return LocalStorageHandler(base_path=str(tmp_path))


def test_normalize_dict(storage):
    data = {"symbol": "BTC", "price": 50000}
    assert storage._normalize_data(data) == [{"symbol": "BTC", "price": 50000}]


def test_normalize_valid_json_string(storage):
    data_str = json.dumps([{"symbol": "BTC", "price": 50000}])
    assert storage._normalize_data(data_str) == [{"symbol": "BTC", "price": 50000}]


def test_normalize_invalid_string(storage):
    with pytest.raises(ValueError, match="Input JSON string is invalid"):
        storage._normalize_data("market closed")


def test_save_creates_parquet_file(storage, tmp_path):
    data = [{"symbol": "BTC", "price": 50000}]
    file_path = storage.save(data, filename="test_quotes.parquet")

    assert file_path.endswith(".parquet")
    assert tmp_path.exists()
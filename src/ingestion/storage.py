import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Union

try:
    import pandas as pd  # type: ignore[import-not-found]
except ModuleNotFoundError:
    pd = None


class LocalStorageHandler:
    """Persist JSON/dictionary data as Parquet files in date-based partitions."""

    def __init__(self, base_path: str = "data/raw"):
        self.base_path = Path(base_path)

    def _get_partition_dir(self) -> Path:
        today = date.today()
        return self.base_path / str(today.year) / f"{today.month:02d}" / f"{today.day:02d}"

    def _normalize_data(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        if isinstance(data, str):
            try:
                loaded = json.loads(data)
            except json.JSONDecodeError as exc:
                raise ValueError("Input JSON string is invalid") from exc
            data = loaded

        if isinstance(data, dict):
            return [data]

        if isinstance(data, list):
            if not all(isinstance(item, dict) for item in data):
                raise TypeError("Data must be a dictionary or a list of dictionaries.")
            return data

        raise TypeError("Data must be a dictionary, JSON string, or list of dictionaries.")

    def save(self, data: Union[str, Dict[str, Any], List[Dict[str, Any]]], filename: str = "data.parquet") -> str:
        records = self._normalize_data(data)

        partition_dir = self._get_partition_dir()
        partition_dir.mkdir(parents=True, exist_ok=True)

        if not filename.endswith(".parquet"):
            filename = f"{filename}.parquet"

        file_path = partition_dir / filename

        if pd is None:
            raise ModuleNotFoundError("pandas is required to save parquet files.")

        df = pd.DataFrame(records)
        df.to_parquet(file_path, index=False, engine="pyarrow")
        return str(file_path)
if __name__ == "__main__":
    import logging
    from src.ingestion.api_client import APIClient

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    # Fetch live market data
    client = APIClient()
    market_data = client.get_market_data(per_page=10)

    # Save to partitioned raw directory
    storage = LocalStorageHandler()
    saved_path = storage.save(market_data, filename="crypto_markets.parquet")
    print(f"Data successfully saved to: {saved_path}")
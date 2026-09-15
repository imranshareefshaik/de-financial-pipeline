import os
import logging
from pathlib import Path
from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DatabaseLoader:
    """Loads transformed parquet datasets into a relational database table."""

    def __init__(self, db_url: Optional[str] = None):
        # Defaults to local SQLite database if PostgreSQL connection string is not provided
        self.db_url = db_url or os.getenv(
            "DATABASE_URL", 
            f"sqlite:///{Path(__file__).resolve().parents[2]}/data/financial_warehouse.db"
        )
        self.engine = create_engine(self.db_url)

    def load_analytics_data(self, parquet_path: str, table_name: str = "crypto_market_summary") -> int:
        """Reads analytics parquet file and writes records to database table."""
        logger.info(f"Reading analytics dataset from: {parquet_path}")
        
        if not Path(parquet_path).exists():
            raise FileNotFoundError(f"Parquet source file not found at {parquet_path}")

        # Read parquet directly using pandas/pyarrow
        df = pd.read_parquet(parquet_path)
        
        logger.info(f"Writing {len(df)} records to database table '{table_name}'...")
        # Write to DB table (replace or append)
        df.to_sql(table_name, con=self.engine, if_exists="replace", index=False)
        
        logger.info(f"Successfully loaded {len(df)} rows into '{table_name}'.")
        return len(df)

    def verify_table(self, table_name: str = "crypto_market_summary") -> None:
        """Query and display table records to verify ingestion."""
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT coin_id, current_price, market_tier, volatility_pct FROM {table_name} LIMIT 5"))
            rows = result.fetchall()
            logger.info("Sample verification rows:")
            for row in rows:
                print(row)


if __name__ == "__main__":
    loader = DatabaseLoader()
    input_file = "data/analytics/crypto_market_summary.parquet"
    
    rows_loaded = loader.load_analytics_data(input_file)
    loader.verify_table()
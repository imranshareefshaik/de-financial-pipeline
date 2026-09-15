import logging
from pathlib import Path

from sqlalchemy import create_engine, text

from src.utils.config_loader import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("WarehouseViews")


class WarehouseViewManager:
    """Manages analytical SQL views and aggregate data marts."""

    def __init__(self, db_url: str | None = None):
        if not db_url:
            cfg = load_config()
            db_path = Path(__file__).resolve().parents[2] / cfg["warehouse"]["db_path"]
            db_url = f"sqlite:///{db_path}"
        self.engine = create_engine(db_url)

    def create_views(self) -> None:
        """Create analytical SQL views over curated tables."""
        views = {
            "v_market_leaders": """
                CREATE VIEW IF NOT EXISTS v_market_leaders AS
                SELECT
                    coin_id,
                    symbol,
                    current_price,
                    volume_market_share_pct,
                    market_cap_rank
                FROM crypto_market_summary
                WHERE market_tier = 'Mega-Cap'
                ORDER BY market_cap_rank ASC;
            """,
            "v_high_volatility_alerts": """
                CREATE VIEW IF NOT EXISTS v_high_volatility_alerts AS
                SELECT
                    coin_id,
                    symbol,
                    current_price,
                    price_spread_24h,
                    volatility_pct,
                    market_tier
                FROM crypto_market_summary
                WHERE volatility_pct >= 4.0
                ORDER BY volatility_pct DESC;
            """,
            "v_tier_summary": """
                CREATE VIEW IF NOT EXISTS v_tier_summary AS
                SELECT
                    market_tier,
                    COUNT(coin_id) AS total_assets,
                    ROUND(AVG(current_price), 2) AS avg_price,
                    ROUND(AVG(volatility_pct), 2) AS avg_volatility_pct,
                    ROUND(SUM(volume_market_share_pct), 2) AS cumulative_volume_share
                FROM crypto_market_summary
                GROUP BY market_tier
                ORDER BY total_assets DESC;
            """
        }

        with self.engine.begin() as conn:
            for view_name, query in views.items():
                logger.info(f"Creating SQL View '{view_name}'...")
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name};"))
                conn.execute(text(query))
        logger.info("All analytical SQL views created successfully.")

    def query_summary_mart(self) -> None:
        """Fetch and display the tier summary view."""
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT * FROM v_tier_summary;"))
            rows = result.fetchall()
            print("\n" + "=" * 50)
            print("MARKET TIER SUMMARY DATA MART")
            print("=" * 50)
            for row in rows:
                print(row)
            print("=" * 50 + "\n")


if __name__ == "__main__":
    manager = WarehouseViewManager()
    manager.create_views()
    manager.query_summary_mart()

import logging
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("DriftDetector")


class DataDriftException(Exception):
    """Raised when incoming data deviates abnormally from historical baselines."""
    pass


class StatisticalDriftDetector:
    """Detects distribution drift and value spikes between batch runs."""

    def __init__(self, current_df: DataFrame, max_price_drift_pct: float = 50.0):
        self.current_df = current_df
        self.max_price_drift_pct = max_price_drift_pct
        self.anomalies: List[Dict[str, Any]] = []

    def compute_summary_stats(self) -> Dict[str, float]:
        """Calculates current batch key metrics."""
        stats = self.current_df.select(
            F.avg("current_price").alias("avg_price"),
            F.stddev("current_price").alias("std_price"),
            F.avg("volatility_pct").alias("avg_volatility")
        ).collect()[0]

        return {
            "avg_price": float(stats["avg_price"] or 0.0),
            "std_price": float(stats["std_price"] or 0.0),
            "avg_volatility": float(stats["avg_volatility"] or 0.0)
        }

    def check_drift_against_baseline(self, baseline_stats: Dict[str, float]) -> bool:
        """Compares current batch stats against historical baseline."""
        current_stats = self.compute_summary_stats()
        logger.info(f"Current Batch Stats: {current_stats}")
        logger.info(f"Baseline Historical Stats: {baseline_stats}")

        baseline_price = baseline_stats.get("avg_price", 0.0)
        if baseline_price > 0:
            drift_pct = abs(current_stats["avg_price"] - baseline_price) / baseline_price * 100.0
            logger.info(f"Average Price Drift: {drift_pct:.2f}% (Threshold: {self.max_price_drift_pct}%)")

            if drift_pct > self.max_price_drift_pct:
                anomaly = {
                    "metric": "avg_price",
                    "current": current_stats["avg_price"],
                    "baseline": baseline_price,
                    "drift_pct": round(drift_pct, 2)
                }
                self.anomalies.append(anomaly)
                raise DataDriftException(
                    f"Significant price drift detected: {drift_pct:.2f}% shift exceeds {self.max_price_drift_pct}% limit"
                )

        logger.info("Statistical drift audit PASSED. No critical divergence found.")
        return True

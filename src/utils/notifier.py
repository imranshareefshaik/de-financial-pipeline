import os
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("PipelineNotifier")


class PipelineNotifier:
    """Dispatches pipeline status alerts and quality incident reports via Webhook or local log."""

    def __init__(self, webhook_url: Optional[str] = None):
        self.webhook_url = webhook_url or os.getenv("PIPELINE_WEBHOOK_URL")

    def notify_success(self, run_metrics: Dict[str, Any]) -> bool:
        """Sends a success notification with execution summary."""
        payload = {
            "status": "SUCCESS",
            "title": "Data Pipeline Completed Successfully",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "metrics": {
                "Pipeline": run_metrics.get("pipeline_name", "DE-Financial-Pipeline"),
                "Records Ingested": run_metrics.get("records_ingested", 0),
                "Duration": f"{run_metrics.get('duration_seconds', 0.0)}s",
                "Environment": run_metrics.get("environment", "local")
            }
        }
        return self._dispatch(payload)

    def notify_failure(self, error_message: str, stage: str, run_metrics: Optional[Dict[str, Any]] = None) -> bool:
        """Sends a high-priority incident notification on pipeline failure."""
        payload = {
            "status": "CRITICAL_FAILURE",
            "title": "Pipeline Alert: Execution Halted",
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "incident_details": {
                "Failed Stage": stage,
                "Error": error_message,
                "Duration Before Failure": f"{(run_metrics or {}).get('duration_seconds', 0.0)}s"
            }
        }
        return self._dispatch(payload)

    def _dispatch(self, payload: Dict[str, Any]) -> bool:
        """Sends payload to webhook if configured, else saves to data/alerts.json."""
        if self.webhook_url:
            try:
                response = requests.post(
                    self.webhook_url,
                    json={"text": json.dumps(payload, indent=2)},
                    timeout=5
                )
                response.raise_for_status()
                logger.info("Webhook notification sent successfully.")
                return True
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to deliver webhook alert: {e}")

        alert_file = Path("data/alerts.json")
        alert_file.parent.mkdir(parents=True, exist_ok=True)

        alerts = []
        if alert_file.exists():
            try:
                alerts = json.loads(alert_file.read_text(encoding="utf-8"))
            except Exception:
                alerts = []

        alerts.append(payload)
        alert_file.write_text(json.dumps(alerts, indent=2), encoding="utf-8")
        logger.info(f"Notification logged locally to {alert_file} (Status: {payload['status']})")
        return True


if __name__ == "__main__":
    notifier = PipelineNotifier()
    notifier.notify_success({
        "pipeline_name": "DE-Financial-Pipeline",
        "records_ingested": 10,
        "duration_seconds": 15.4,
        "environment": "local"
    })

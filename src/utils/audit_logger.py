import logging
from datetime import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from src.utils.config_loader import load_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("AuditLogger")


class PipelineAuditLogger:
    """Records pipeline run metrics and audit events into a metadata database."""

    def __init__(self, db_url: str = None):
        if not db_url:
            cfg = load_config()
            db_path = Path(__file__).resolve().parents[2] / cfg["warehouse"]["db_path"]
            db_url = f"sqlite:///{db_path}"
        self.engine = create_engine(db_url)
        self._init_audit_table()

    def _init_audit_table(self) -> None:
        """Ensures the audit log table exists."""
        query = """
        CREATE TABLE IF NOT EXISTS pipeline_execution_logs (
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_name TEXT NOT NULL,
            execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,
            records_ingested INTEGER DEFAULT 0,
            duration_seconds REAL DEFAULT 0.0,
            error_message TEXT
        );
        """
        with self.engine.begin() as conn:
            conn.execute(text(query))

    def log_run(
        self,
        pipeline_name: str,
        status: str,
        records_ingested: int = 0,
        duration_seconds: float = 0.0,
        error_message: str = None
    ) -> None:
        """Inserts an execution event record."""
        query = """
        INSERT INTO pipeline_execution_logs (pipeline_name, status, records_ingested, duration_seconds, error_message)
        VALUES (:pipeline_name, :status, :records_ingested, :duration_seconds, :error_message);
        """
        with self.engine.begin() as conn:
            conn.execute(text(query), {
                "pipeline_name": pipeline_name,
                "status": status,
                "records_ingested": records_ingested,
                "duration_seconds": round(duration_seconds, 2),
                "error_message": error_message
            })
        logger.info(f"Audit record saved: [{status}] {records_ingested} records in {duration_seconds:.2f}s")


if __name__ == "__main__":
    audit = PipelineAuditLogger()
    audit.log_run("TestRun", "SUCCESS", records_ingested=25, duration_seconds=12.4)

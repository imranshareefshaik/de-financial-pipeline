import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from pyspark.sql import DataFrame

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
logger = logging.getLogger("LineageTracker")


class LineageTracker:
    """Tracks dataset provenance and monitors schema evolution across pipeline runs."""

    def __init__(self, catalog_path: str = "data/lineage_catalog.json"):
        self.catalog_path = Path(catalog_path)
        self.catalog_path.parent.mkdir(parents=True, exist_ok=True)

    def record_lineage(
        self,
        stage_name: str,
        inputs: List[str],
        outputs: List[str],
        schema_snapshot: Dict[str, str],
        record_count: int
    ) -> Dict[str, Any]:
        """Appends a transformation lineage node and schema state to the catalog."""
        lineage_node = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "stage": stage_name,
            "inputs": inputs,
            "outputs": outputs,
            "record_count": record_count,
            "schema": schema_snapshot
        }

        catalog = self._load_catalog()
        catalog.append(lineage_node)
        self.catalog_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
        logger.info(f"Lineage recorded for [{stage_name}]: {len(inputs)} input(s) -> {len(outputs)} output(s)")
        return lineage_node

    def detect_schema_drift(self, stage_name: str, current_df: DataFrame) -> Dict[str, List[str]]:
        """Compares current DataFrame schema against the last recorded schema for this stage."""
        catalog = self._load_catalog()
        previous_entries = [entry for entry in catalog if entry.get("stage") == stage_name]

        current_schema = {field.name: str(field.dataType) for field in current_df.schema.fields}

        if not previous_entries:
            logger.info(f"No baseline schema found for stage '{stage_name}'. Establishing initial snapshot.")
            return {"added_columns": [], "removed_columns": [], "type_changes": []}

        last_schema = previous_entries[-1]["schema"]
        
        added = [col for col in current_schema if col not in last_schema]
        removed = [col for col in last_schema if col not in current_schema]
        type_changes = [
            f"{col}: {last_schema[col]} -> {current_schema[col]}"
            for col in current_schema
            if col in last_schema and current_schema[col] != last_schema[col]
        ]

        if added or removed or type_changes:
            logger.warning(
                f"Schema evolution detected in '{stage_name}': "
                f"Added={added}, Removed={removed}, TypeChanges={type_changes}"
            )
        else:
            logger.info(f"Schema for '{stage_name}' matches latest baseline.")

        return {
            "added_columns": added,
            "removed_columns": removed,
            "type_changes": type_changes
        }

    def _load_catalog(self) -> List[Dict[str, Any]]:
        if self.catalog_path.exists():
            try:
                return json.loads(self.catalog_path.read_text(encoding="utf-8"))
            except Exception:
                return []
        return []

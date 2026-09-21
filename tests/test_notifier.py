import json
from pathlib import Path
from src.utils.notifier import PipelineNotifier

def test_notifier_local_fallback(tmp_path, monkeypatch):
    test_alert_file = tmp_path / "alerts.json"
    notifier = PipelineNotifier(webhook_url=None)

    # Divert alert sink to pytest temporary path
    monkeypatch.setattr("src.utils.notifier.Path", lambda p: test_alert_file)

    success = notifier.notify_success({
        "pipeline_name": "Test-Pipeline",
        "records_ingested": 20,
        "duration_seconds": 5.2
    })

    assert success is True
    assert test_alert_file.exists()

    data = json.loads(test_alert_file.read_text(encoding="utf-8"))
    assert data[-1]["status"] == "SUCCESS"
    assert data[-1]["metrics"]["Records Ingested"] == 20

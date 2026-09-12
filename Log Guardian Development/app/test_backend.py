"""Quick verification script for Log Guardian backend and AI explanations."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.data_provider import db
from backend.ai_service import ai_service
from backend.main import app
from fastapi.testclient import TestClient

client = TestClient(app)

def test_endpoints():
    print("Testing /api/health...")
    r = client.get("/api/health")
    assert r.status_code == 200, f"Health failed: {r.text}"
    health = r.json()
    assert health["databricks_mode"] in {"live", "fallback"}
    print("Health response:", health)

    print("\nTesting /api/system/data-source...")
    r = client.get("/api/system/data-source")
    assert r.status_code == 200
    source = r.json()
    assert source["active_data_source"] in {"databricks", "fallback"}
    print("Active data source:", source["active_data_source"])

    print("\nTesting /api/dashboard/full data source...")
    r = client.get("/api/dashboard/full")
    assert r.status_code == 200
    dashboard = r.json()
    assert dashboard["data_source"] in {"databricks", "fallback"}
    print("Dashboard data source:", dashboard["data_source"])

    print("\nTesting /api/estate/kpis...")
    r = client.get("/api/estate/kpis")
    assert r.status_code == 200
    kpis = r.json()
    assert kpis["total_log_lines"] > 0
    assert kpis["total_anomalies"] >= 0
    print(f"Estate KPIs OK: {kpis['total_log_lines']} logs, {kpis['total_anomalies']} anomalies")

    print("\nTesting /api/services...")
    r = client.get("/api/services")
    assert r.status_code == 200
    services = r.json()
    assert len(services) == 12
    print(f"Services OK: {len(services)} services loaded")

    print("\nTesting /api/ai/explain (service_health for oslo_service.periodic_task)...")
    r = client.post("/api/ai/explain", json={
        "service_name": "oslo_service.periodic_task",
        "explanation_type": "service_health"
    })
    assert r.status_code == 200
    exp = r.json()["explanation"]
    assert "what_happened" in exp
    assert "why_did_this_happen" in exp
    assert "impact" in exp
    assert "what_to_check_next" in exp
    print("Critical service explanation OK:")
    print("  Headline:", exp.get("headline"))
    print("  What:", exp["what_happened"][:80], "...")

    print("\nTesting /api/ai/explain (anomaly for nova.compute.manager)...")
    r = client.post("/api/ai/explain", json={
        "service_name": "nova.compute.manager",
        "explanation_type": "anomaly"
    })
    assert r.status_code == 200
    exp = r.json()["explanation"]
    assert "what_happened" in exp
    print("Anomaly explanation OK:")
    print("  Headline:", exp.get("headline"))
    print("  What:", exp["what_happened"][:80], "...")

    print("\nAll verification tests passed successfully!")

if __name__ == "__main__":
    test_endpoints()

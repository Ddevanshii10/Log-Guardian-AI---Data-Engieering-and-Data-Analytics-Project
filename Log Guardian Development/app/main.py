"""Log Guardian — FastAPI Backend & Web Server.

Serves REST APIs for:
- Databricks Gold Layer analytics & complete Dashboard telemetry
- Machine Learning insights and anomaly summaries
- Context-grounded Gemini AI "Why?" diagnostic explanations
- Static Web UI hosting
"""

import os
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path

from backend.data_provider import db
from backend.ai_service import ai_service

app = FastAPI(
    title="Log Guardian API",
    description="Automated Log Analytics, Data Engineering & AI Intelligence Platform",
    version="2.0.0"
)

# Enable CORS for local development and web frontends
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ExplainRequest(BaseModel):
    service_name: str
    explanation_type: str = "service_health"  # "service_health", "anomaly", "chart_metric"
    context_data: Optional[Dict[str, Any]] = None

@app.get("/api/health")
def get_health():
    """System health check and connection status."""
    databricks_connected = db.is_databricks_available()
    return {
        "status": "healthy",
        "service": "Log Guardian Core API",
        "databricks_connected": databricks_connected,
        "databricks_mode": "live" if databricks_connected else "fallback",
        "gemini_configured": bool(ai_service.api_key),
        "gemini_model": ai_service.model_name
    }

@app.get("/api/system/data-source")
def get_data_source():
    """Report whether the live warehouse is reachable right now."""
    return db.data_source_status()

@app.get("/api/dashboard/full")
def get_full_dashboard():
    """Retrieve all datasets for the primary analytics dashboard in one fast payload."""
    return db.get_full_dashboard_data()

@app.get("/api/estate/kpis")
def get_estate_kpis():
    """Retrieve overall estate metrics matching Superset Gold views."""
    return db.get_estate_kpis()

@app.get("/api/services")
def list_services():
    """Retrieve service scorecards and health statuses."""
    return db.get_services()

@app.get("/api/services/{service_name}")
def get_service_detail(service_name: str):
    """Retrieve detailed telemetry for a single service."""
    svc = db.get_service_by_name(service_name)
    if not svc:
        raise HTTPException(status_code=404, detail=f"Service '{service_name}' not found.")
    return svc

@app.get("/api/anomalies")
def list_anomalies():
    """Retrieve machine learning anomaly patterns and summary."""
    return {
        "total_anomalies": db.get_estate_kpis()["total_anomalies"],
        "patterns": db.get_anomaly_patterns()
    }

@app.get("/api/models")
def list_models():
    """Retrieve ML model cards and evaluation metrics."""
    return db.get_model_cards()

@app.post("/api/ai/explain")
def explain_telemetry(req: ExplainRequest):
    """Generate a data-grounded 4-part AI explanation for any service or anomaly pattern."""
    facts = req.context_data
    if not facts:
        if req.explanation_type == "service_health":
            facts = db.get_service_by_name(req.service_name)
            if not facts:
                raise HTTPException(status_code=404, detail=f"Service '{req.service_name}' not found.")
        elif req.explanation_type == "anomaly":
            patterns = db.get_anomaly_patterns()
            match = next((p for p in patterns if p["service"].lower() == req.service_name.lower()), None)
            facts = match if match else {"service": req.service_name, "anomaly_occurrences": 0}
        elif req.explanation_type == "chart_metric":
            facts = {
                "metric_topic": req.service_name,
                "estate": db.get_estate_kpis(),
                "top_service": db.get_services()[0]
            }
        else:
            facts = {"service": req.service_name, "estate": db.get_estate_kpis()}

    explanation = ai_service.explain(req.service_name, req.explanation_type, facts)
    return {
        "service_name": req.service_name,
        "explanation_type": req.explanation_type,
        "facts_evaluated": facts,
        "explanation": explanation
    }

# Mount Frontend static directory
frontend_dir = Path(__file__).resolve().parent.parent / "frontend"
if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    def serve_frontend():
        return FileResponse(frontend_dir / "index.html")

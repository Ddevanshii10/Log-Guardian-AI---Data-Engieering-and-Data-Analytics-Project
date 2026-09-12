# Log Guardian

## Databricks Live Data Integration

The FastAPI backend connects to Databricks SQL Warehouse when all three variables below are present in the backend environment or `.env` file. The token is never stored in source code or printed in logs.

```env
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_TOKEN=your-personal-access-token
# Optional when Gold is not in the default project catalog:
DATABRICKS_CATALOG=log-analytics
```

Install the backend dependencies from `app/requirements.txt`:

```powershell
python -m pip install -r "Log Guardian Development/app/requirements.txt"
```

The configured warehouse must be running and the `gold` schema must contain the dashboard views created by `dashboard_views.sql`, including `vw_dash_service_overview`, `vw_dash_service_latency`, `vw_dash_log_level`, `vw_dash_http_status`, `vw_dash_hourly_trend`, `vw_dash_hourly_latency`, and `vw_dash_topic`.

Run the API from the workspace root:

```powershell
python -m uvicorn backend.main:app --reload
```

Verify the warehouse directly:

```powershell
python "Log Guardian Development/app/test_databricks.py"
```

Verify the API and fallback behavior:

```powershell
python "Log Guardian Development/app/test_backend.py"
```

`GET /api/health` returns `databricks_mode: "live"` after a successful warehouse query and `databricks_mode: "fallback"` when the warehouse is not configured or reachable. `GET /api/system/data-source` returns the same status explicitly. Dashboard requests query current Gold data on each request; a failed dataset keeps its existing static fallback so the dashboard remains usable.
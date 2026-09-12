-- =====================================================================================
-- Log Guardian — Superset dashboard views
-- =====================================================================================
-- Eight NULL-free views over `log-analytics`.silver.silver_logs.
--
-- The rule that keeps them NULL-free without faking anything:
--   * volume / error / anomaly metrics  -> all log lines
--   * latency metrics                   -> HTTP requests only (status_code IS NOT NULL)
--
-- Latency and status_code only exist on wsgi.server request lines (the parser's regex
-- groups for `status:` and `time:` are optional), so scoping latency to that subset means
-- response_time is always present and nothing has to be imputed. Zero-filling would claim
-- an instant response for services that never served a measurable request.
--
-- Run top to bottom, then run the verification query at the bottom. Every count must be 0.
-- Create one Superset dataset per view: SQL Lab -> SELECT * FROM <view> -> Save as dataset.
-- =====================================================================================


-- 1. Main service scorecard -----------------------------------------------------------
-- Replaces service_performance, service_health, service_risk_dashboard, anomaly_summary.
-- risk_score can no longer vanish: the latency term is COALESCEd, and latency_coverage
-- tells the reader whether the score includes a latency contribution.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_service_overview AS
WITH base AS (
  SELECT
    service,
    COUNT(*)                                                    AS log_lines,
    SUM(CASE WHEN log_level = 'ERROR'    THEN 1 ELSE 0 END)     AS error_count,
    SUM(CASE WHEN log_level = 'WARNING'  THEN 1 ELSE 0 END)     AS warning_count,
    SUM(CASE WHEN log_level = 'CRITICAL' THEN 1 ELSE 0 END)     AS critical_count,
    SUM(CASE WHEN is_anomaly = 1         THEN 1 ELSE 0 END)     AS anomaly_count,
    COUNT(status_code)                                          AS http_requests,
    COUNT(response_time)                                        AS latency_samples,
    AVG(response_time)                                          AS avg_response_time_raw
  FROM `log-analytics`.silver.silver_logs
  GROUP BY service
)
SELECT
  service,
  log_lines,
  http_requests,
  error_count,
  warning_count,
  critical_count,
  anomaly_count,
  ROUND(100.0 * error_count   / log_lines, 2)                   AS error_rate,
  ROUND(100.0 * warning_count / log_lines, 2)                   AS warning_rate,
  ROUND(100.0 * anomaly_count / log_lines, 2)                   AS anomaly_rate,
  CASE
    WHEN 100.0 * error_count / log_lines <  1 THEN 'Healthy'
    WHEN 100.0 * error_count / log_lines <  5 THEN 'Degraded'
    ELSE 'Critical'
  END                                                           AS health_status,
  ROUND(
      (100.0 * error_count   / log_lines) * 0.5
    + (100.0 * anomaly_count / log_lines) * 0.3
    + COALESCE(avg_response_time_raw * 20, 0) * 0.2
  , 2)                                                          AS risk_score,
  CASE WHEN latency_samples > 0 THEN 'measured' ELSE 'not measured' END AS latency_coverage
FROM base;


-- 2. Latency per service, HTTP-scoped -------------------------------------------------
-- Only services that served requests appear, so there is nothing to be NULL.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_service_latency AS
SELECT
  service,
  COUNT(*)                                              AS request_count,
  ROUND(AVG(response_time), 4)                          AS avg_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.50), 4)      AS p50_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.95), 4)      AS p95_response_time,
  ROUND(MAX(response_time), 4)                          AS max_response_time,
  ROUND(MIN(response_time), 4)                          AS min_response_time,
  SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END)   AS server_errors,
  SUM(CASE WHEN status_code >= 400
            AND status_code <  500 THEN 1 ELSE 0 END)   AS client_errors,
  SUM(CASE WHEN status_code  = 200 THEN 1 ELSE 0 END)   AS success_count
FROM `log-analytics`.silver.silver_logs
WHERE status_code   IS NOT NULL
  AND response_time IS NOT NULL
GROUP BY service;


-- 3. HTTP status distribution ---------------------------------------------------------
-- The NULL status bucket disappears by construction, and pct_of_requests now sums to 100
-- because the denominator is total requests rather than total log lines.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_http_status AS
SELECT
  status_code,
  CASE
    WHEN status_code >= 500 THEN '5xx server error'
    WHEN status_code >= 400 THEN '4xx client error'
    WHEN status_code >= 300 THEN '3xx redirect'
    WHEN status_code >= 200 THEN '2xx success'
    ELSE 'other'
  END                                                   AS status_class,
  COUNT(*)                                              AS request_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)    AS pct_of_requests,
  ROUND(AVG(response_time), 4)                          AS avg_response_time
FROM `log-analytics`.silver.silver_logs
WHERE status_code IS NOT NULL
GROUP BY status_code;


-- 4. Daily volume and severity trend -------------------------------------------------
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_daily_trend AS
SELECT
  event_date,
  COUNT(*)                                                  AS log_lines,
  SUM(CASE WHEN log_level = 'ERROR'    THEN 1 ELSE 0 END)   AS errors,
  SUM(CASE WHEN log_level = 'WARNING'  THEN 1 ELSE 0 END)   AS warnings,
  SUM(CASE WHEN log_level = 'CRITICAL' THEN 1 ELSE 0 END)   AS critical,
  SUM(CASE WHEN is_anomaly = 1         THEN 1 ELSE 0 END)   AS anomalies,
  COUNT(status_code)                                        AS http_requests
FROM `log-analytics`.silver.silver_logs
WHERE event_date IS NOT NULL
GROUP BY event_date;


-- 5. Hourly volume trend -------------------------------------------------------------
-- Use this instead of the daily view if COUNT(DISTINCT event_date) is small.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_hourly_trend AS
SELECT
  DATE_TRUNC('HOUR', event_timestamp)                       AS event_hour_ts,
  COUNT(*)                                                  AS log_lines,
  SUM(CASE WHEN log_level = 'ERROR'    THEN 1 ELSE 0 END)   AS errors,
  SUM(CASE WHEN log_level = 'WARNING'  THEN 1 ELSE 0 END)   AS warnings,
  SUM(CASE WHEN is_anomaly = 1         THEN 1 ELSE 0 END)   AS anomalies,
  COUNT(status_code)                                        AS http_requests
FROM `log-analytics`.silver.silver_logs
WHERE event_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('HOUR', event_timestamp);


-- 6. Hourly latency trend, HTTP-scoped ------------------------------------------------
-- Only hours that contain requests appear, so the line has no gaps.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_hourly_latency AS
SELECT
  DATE_TRUNC('HOUR', event_timestamp)                   AS event_hour_ts,
  COUNT(*)                                              AS request_count,
  ROUND(AVG(response_time), 4)                          AS avg_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.95), 4)      AS p95_response_time
FROM `log-analytics`.silver.silver_logs
WHERE status_code    IS NOT NULL
  AND response_time  IS NOT NULL
  AND event_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('HOUR', event_timestamp);


-- 7. Log level and Kafka topic breakdowns --------------------------------------------
-- severity_score is carried through so charts can sort INFO -> CRITICAL rather than
-- alphabetically.
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_log_level AS
SELECT
  log_level,
  COUNT(*)                                              AS log_lines,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)    AS pct_of_logs,
  MAX(severity_score)                                   AS severity_score
FROM `log-analytics`.silver.silver_logs
GROUP BY log_level;

CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_topic AS
SELECT
  kafka_topic,
  COUNT(*)                                                 AS log_lines,
  SUM(CASE WHEN log_level = 'ERROR'   THEN 1 ELSE 0 END)   AS error_count,
  SUM(CASE WHEN log_level = 'WARNING' THEN 1 ELSE 0 END)   AS warning_count,
  SUM(CASE WHEN is_anomaly = 1        THEN 1 ELSE 0 END)   AS anomaly_count,
  COUNT(status_code)                                       AS http_requests,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)       AS pct_of_logs
FROM `log-analytics`.silver.silver_logs
GROUP BY kafka_topic;


-- =====================================================================================
-- VERIFICATION — every bad_rows value must be 0 before you build charts
-- =====================================================================================
SELECT 'service_overview' AS view_name, COUNT(*) AS bad_rows
FROM `log-analytics`.gold.vw_dash_service_overview
WHERE service IS NULL OR log_lines IS NULL OR error_rate IS NULL
   OR risk_score IS NULL OR health_status IS NULL
UNION ALL SELECT 'service_latency', COUNT(*)
FROM `log-analytics`.gold.vw_dash_service_latency
WHERE avg_response_time IS NULL OR p95_response_time IS NULL OR request_count IS NULL
UNION ALL SELECT 'http_status', COUNT(*)
FROM `log-analytics`.gold.vw_dash_http_status
WHERE status_code IS NULL OR pct_of_requests IS NULL OR avg_response_time IS NULL
UNION ALL SELECT 'daily_trend', COUNT(*)
FROM `log-analytics`.gold.vw_dash_daily_trend
WHERE event_date IS NULL OR log_lines IS NULL
UNION ALL SELECT 'hourly_trend', COUNT(*)
FROM `log-analytics`.gold.vw_dash_hourly_trend
WHERE event_hour_ts IS NULL OR log_lines IS NULL
UNION ALL SELECT 'hourly_latency', COUNT(*)
FROM `log-analytics`.gold.vw_dash_hourly_latency
WHERE event_hour_ts IS NULL OR avg_response_time IS NULL
UNION ALL SELECT 'log_level', COUNT(*)
FROM `log-analytics`.gold.vw_dash_log_level
WHERE log_level IS NULL OR log_lines IS NULL
UNION ALL SELECT 'topic', COUNT(*)
FROM `log-analytics`.gold.vw_dash_topic
WHERE kafka_topic IS NULL OR log_lines IS NULL;


-- =====================================================================================
-- Useful one-off checks
-- =====================================================================================
-- Daily or hourly trend axis? Fewer than ~5 distinct dates -> use the hourly views.
-- SELECT COUNT(DISTINCT event_date) AS distinct_dates FROM `log-analytics`.silver.silver_logs;

-- Which services have no measured latency, and confirm they still have log lines
-- (proving the NULL was never "no traffic"):
-- SELECT service, log_lines, http_requests, latency_coverage
-- FROM `log-analytics`.gold.vw_dash_service_overview
-- ORDER BY latency_coverage, log_lines DESC;

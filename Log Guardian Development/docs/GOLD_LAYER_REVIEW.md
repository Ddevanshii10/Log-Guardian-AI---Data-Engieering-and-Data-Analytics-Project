# Log Guardian — Gold Layer Review & Superset Dashboard Plan

**Prepared 2026-08-24 · target completion 2026-08-27 (3 days)**

This document reviews all ten tables in `log-analytics.gold`: what each one is for, how it was
built, what its columns mean, where NULLs can appear, and whether it belongs on the Superset
dashboard. It ends with eight NULL-free dashboard datasets, the chart list, and a day-by-day plan.

Everything here comes from reading the code in the repo snapshot (`bin/log_parser.py`,
`notebooks/03`, `04`, `05`, `06`, `08`). **No queries were run against Databricks**, so row counts
and percentages are marked as things for you to fill in — the structure and logic, however, are
read directly from your code and are exact.

---

## 1. The two facts that decide everything

Almost every design question about this dashboard resolves to two properties of the parser. Both
are deliberate, and both are invisible unless you read `bin/log_parser.py`.

### Fact 1 — latency and status code only exist on HTTP request lines

```python
# bin/log_parser.py, HTTP_REST_PATTERN
(?:status:\s*(?P<status_code>\d+))?     # optional group
(?:time:\s*(?P<response_time>[\d.]+))?  # optional group
```

Only `nova.osapi_compute.wsgi.server` request lines carry `status:` and `time:`. Every other
OpenStack line — periodic tasks, compute-manager INFO, scheduler messages, tracebacks — has no
HTTP status and no latency, so the parser emits `None` and Silver stores NULL.

This is correct behaviour, not a defect. A "periodic task ran" log line has no response time to
report, and inventing one would be fabrication.

The consequence for Gold: `F.avg("response_time")` silently skips NULLs, so `avg_response_time`
only comes out NULL when **every row in that group was a non-HTTP line**. So:

> **A NULL `avg_response_time` means "this service / day / topic logged no HTTP requests at all."**

It does *not* mean "no traffic" — `total_requests` is `F.count("*")` over all log lines and is
greater than zero on exactly those rows. And it certainly does not mean zero seconds, which is why
zero-filling is off the table.

### Fact 2 — `instance_id` and the HTTP fields are mutually exclusive

```python
http_match = HTTP_REST_PATTERN.match(rest)
if http_match:
    ...                       # sets status_code, response_time; instance_id stays None
else:
    instance_match = INSTANCE_PATTERN.search(rest)
    if instance_match:
        instance_id = ...     # sets instance_id; status_code/response_time stay None
```

The two branches are `if`/`else`, so a row can have HTTP fields **or** an `instance_id`, never
both. And `is_anomaly` is produced in notebook 03 by joining the label file on `instance_id`:

```python
silver_df.join(anomaly_df.withColumn("is_anomaly", F.lit(1)), on="instance_id", how="left")
```

Therefore **every anomaly row has `response_time = NULL` and `status_code = NULL`, by
construction**. Anomalies are instance-lifecycle events; they are structurally incapable of
carrying a latency.

This matters for the dashboard: any chart that puts anomaly counts next to response times implies
a relationship the data cannot express. `anomaly_summary.avg_response_time` in particular is *not*
"the latency of anomalous events" — it is the service's overall latency, sitting next to an
unrelated anomaly count. Putting those two columns in one chart would be actively misleading, so
the plan in section 4 separates them.

---

## 2. How the layers connect

```
OpenStack raw logs
        ↓  bin/log_parser.py — regex header + optional HTTP/instance branches
Kafka topics
        ↓  notebook 01
Bronze  /Volumes/log-analytics/bronze/key_volume/bronze_delta_v2   (raw parsed events + Kafka metadata)
        ↓  notebook 03 — dedupe on event_id, timestamps, severity_score,
        ↓                response_category, join anomaly labels
Silver  `log-analytics`.silver.silver_logs                          (one row per log event)
        ↓  notebook 04 — eight business aggregates
        ↓  notebook 05 — one ML feature table
Gold    `log-analytics`.gold.*                                      (10 tables)
```

Silver is the grain that matters: **one row per log event**. Every Gold table in notebook 04 is a
`groupBy` over Silver, so each one is defined entirely by its grouping key and its aggregates.
That is the frame for the reviews below.

Silver's columns, for reference when writing dashboard SQL:

| group | columns |
|---|---|
| identity | `event_id`, `dataset_source`, `log_file`, `process_id` |
| time | `timestamp`, `event_timestamp`, `event_date`, `event_hour`, `event_month`, `event_year`, `day_of_week` |
| source | `service`, `log_level`, `message` |
| request context | `request_id`, `user_id`, `project_id`, `instance_id` |
| HTTP (nullable — Fact 1) | `client_ip`, `http_method`, `http_path`, `status_code`, `response_time` |
| derived | `severity_score`, `response_category`, `is_anomaly` |
| Kafka | `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `ingestion_timestamp` |

---

## 3. The ten Gold tables, one by one

### 3.1 `service_performance`

**Why it exists.** The per-service scorecard — the base table that answers "which service is
misbehaving?" Everything else about services is derived from it.

**How it was built.** Notebook 04, cell 6: `silver_df.groupBy("service").agg(...)`, then three
rate columns via `withColumn`. Cell 7 recomputes `error_rate` identically — harmless, but
redundant; delete it.

**Grain.** One row per `service`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `service` | grouping key | e.g. `nova.osapi_compute.wsgi.server` | none |
| `total_requests` | `count("*")` | **log lines, not requests** — misnamed | none |
| `avg_response_time` | `avg(response_time)` | mean latency over HTTP lines only | **NULL if no HTTP lines** |
| `max_response_time` | `max(response_time)` | slowest request | **NULL, same rows** |
| `min_response_time` | `min(response_time)` | fastest request | **NULL, same rows** |
| `avg_status_code` | `avg(status_code)` | meaningless — mean of 200 and 404 is 302 | **NULL, same rows** |
| `error_count` | `sum(when(log_level=='ERROR',1).otherwise(0))` | ERROR lines | none |
| `warning_count` | same pattern for `WARNING` | WARNING lines | none |
| `critical_count` | same pattern for `CRITICAL` | CRITICAL lines | none |
| `success_count` | `sum(when(status_code==200,1).otherwise(0))` | HTTP 200s | none (`.otherwise(0)`) |
| `anomaly_count` | `sum("is_anomaly")` | labelled anomaly events | none in practice (0-filled in Silver) |
| `error_rate` | `error_count/total_requests*100` | % of log lines that are errors | none |
| `warning_rate` | `warning_count/total_requests*100` | % warnings | none |
| `anomaly_rate` | `anomaly_count/total_requests*100` | % anomalies | none |

**Verdict.** Superseded on the dashboard by `vw_dash_service_overview` (section 4), which fixes
the naming and drops `avg_status_code`. Keep the table — it is the honest base aggregate.

Note the rates all divide by `count("*")`, so `error_rate` is "errors per log line", not "errors
per request". That is a defensible definition, but label the chart accordingly.

### 3.2 `service_health`

**Why it exists.** Turns the numeric scorecard into a status a human can act on.

**How it was built.** Notebook 04 cell 9: `service_performance` plus one `withColumn`.

**Grain.** One row per `service`. All of `service_performance`'s columns, plus:

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `health_status` | `error_rate < 1` → `Healthy`; `1 ≤ error_rate < 5` → `Degraded`; else `Critical` | traffic-light status | none — `error_rate` is never NULL |

**Verdict.** `health_status` is genuinely useful and NULL-free; the rest is a duplicate of
`service_performance`. Don't create a separate Superset dataset for this — fold `health_status`
into the service overview view.

### 3.3 `service_risk_dashboard`

**Why it exists.** The flagship table: one number per service that ranks who needs attention.

**How it was built.** Notebook 04 cell 15: `service_health` plus a weighted risk score.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `risk_score` | `round(error_rate*0.5 + anomaly_rate*0.3 + avg_response_time*20*0.2, 2)` | weighted composite risk | **NULL whenever `avg_response_time` is NULL** |

**This is the most consequential problem in the Gold layer.** SQL arithmetic with NULL yields
NULL, so a service with no HTTP requests gets *no risk score at all* — not a low one. On a "top
services by risk" chart those services silently vanish; in a sorted table they clump at one end
depending on Superset's null handling. The flagship metric of the flagship table has a hole in it
that is invisible until someone asks why a service is missing.

The fix is in section 4: let the latency term contribute only when it exists, and carry a
`risk_score_complete` flag so a partial score is never mistaken for a complete one.

The `*20` factor is an implicit unit conversion — it scales seconds so that a 0.5s average
contributes ~2 points against error rates measured in percent. Worth a sentence in your write-up,
since a reviewer will ask.

**Verdict.** Replaced on the dashboard by `vw_dash_service_overview` with the corrected score.

### 3.4 `log_level_summary`

**Why it exists.** The severity mix across the whole platform — the simplest "is today bad?" signal.

**How it was built.** Notebook 04 cell 10: `groupBy("log_level").count()`, then a percentage
against `total_logs` captured before the aggregation.

**Grain.** One row per `log_level`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `log_level` | grouping key | INFO / WARNING / ERROR / CRITICAL | none — required regex group |
| `count` | `count()` | log lines at that level | none |
| `percentage` | `count/total_logs*100` | share of all log lines | none |

**Verdict.** Use it — this table is already clean and NULL-free. Only caveat: `count` is a
reserved-ish word that reads badly in Superset chart labels, so alias it to `log_lines` in the
dashboard view.

### 3.5 `http_status_summary`

**Why it exists.** The HTTP response-code distribution — the classic API health chart.

**How it was built.** Notebook 04 cell 11: `groupBy("status_code")` with a count and average
latency, plus a percentage against all log lines.

**Grain.** One row per `status_code`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `status_code` | grouping key | 200, 404, 500 … | **NULL bucket exists** — all non-HTTP lines group together |
| `total_requests` | `count("*")` | rows in that bucket | none |
| `avg_response_time` | `avg(response_time)` | mean latency for that status | **NULL for the NULL-status bucket** |
| `percentage` | `total_requests/total_logs*100` | share of **all log lines**, not of requests | none, but misleading |

Two problems here, both from Fact 1. The NULL `status_code` bucket is probably the largest row in
the table, and it is not an HTTP status — it is "everything that wasn't an HTTP request". And
`percentage` is computed against every log line, so on a chart titled "HTTP status distribution"
every real status code is understated and the percentages don't sum to 100 in any meaningful way.

**Verdict.** Do not point Superset at this table. Rebuild it HTTP-scoped
(`WHERE status_code IS NOT NULL`) as `vw_dash_http_status`, where the NULL bucket disappears by
construction and percentages become correct.

### 3.6 `daily_summary`

**Why it exists.** The time series — trend over days, which is what makes a dashboard feel alive.

**How it was built.** Notebook 04 cell 12: `groupBy("event_date")`.

**Grain.** One row per `event_date`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `event_date` | grouping key | calendar date | none |
| `total_logs` | `count("*")` | log lines that day | none |
| `errors` | `sum(when(log_level=='ERROR',1)…)` | ERROR lines | none |
| `warnings` | same for `WARNING` | WARNING lines | none |
| `critical` | same for `CRITICAL` | CRITICAL lines | none |
| `anomalies` | `sum(when(is_anomaly==1,1)…)` | anomaly events | none |
| `avg_response_time` | `avg(response_time)` | mean latency that day | **NULL if no HTTP lines that day** |

**Verdict.** Use it for volume and severity trends, where it is entirely NULL-free. Move
`avg_response_time` out to a separate HTTP-scoped daily view so the latency chart has no gaps.
Splitting one table into two datasets is the right call here: the two metrics have different
denominators and genuinely belong on different charts.

Note the OpenStack sample spans a short window, so expect few distinct dates. If you end up with
only two or three, use `event_hour` for the trend axis instead — a three-point line chart looks
like an accident. There is an hourly variant in section 4 for exactly this reason.

### 3.7 `topic_summary`

**Why it exists.** Kafka-side observability — proves the ingestion layer works and shows how
traffic splits across topics. For a data engineering portfolio project this is one of the more
distinctive charts, because it shows the pipeline, not just the application.

**How it was built.** Notebook 04 cell 13: `groupBy("kafka_topic")`.

**Grain.** One row per `kafka_topic`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `kafka_topic` | grouping key | source topic | none |
| `total_logs` | `count("*")` | log lines from that topic | none |
| `avg_response_time` | `avg(response_time)` | mean latency | **NULL if the topic carries no HTTP lines** |
| `error_count` | `sum(when(log_level=='ERROR',1)…)` | ERROR lines | none |
| `warning_count` | same for `WARNING` | WARNING lines | none |
| `anomaly_count` | `sum(when(is_anomaly==1,1)…)` | anomaly events | none |

**Verdict.** Use it for volume and error breakdown; drop `avg_response_time` from the dashboard
view. Since topics tend to map to log sources, a topic that carries no `wsgi.server` lines will
have NULL latency permanently — that is structural, not transient.

### 3.8 `anomaly_summary`

**Why it exists.** Where are the labelled anomalies concentrated?

**How it was built.** Notebook 04 cell 14: `groupBy("service")`.

**Grain.** One row per `service`.

| column | how it's computed | meaning | NULL risk |
|---|---|---|---|
| `service` | grouping key | service name | none |
| `anomaly_count` | `sum(when(is_anomaly==1,1)…)` | labelled anomaly events | none |
| `avg_response_time` | `avg(response_time)` | the service's overall latency — **unrelated to the anomalies** | **NULL if no HTTP lines** |
| `error_count` | `sum(when(log_level=='ERROR',1)…)` | ERROR lines | none |

By Fact 2, anomaly rows can never carry a latency, so `avg_response_time` here is the service's
latency from *different rows entirely*. Keeping the two columns side by side invites the reader to
infer "anomalous services are slow", which this data cannot support.

**Verdict.** Use `service`, `anomaly_count` and `error_count`; **drop `avg_response_time`** from
the dashboard view. This table also overlaps `service_performance` almost entirely — the honest
simplification is to serve anomalies from the single service overview view and skip this table on
the dashboard.

### 3.9 `feature_engineering_dataset`

**Why it exists.** The ML-ready wide table. Notebook 05's markdown explains the philosophy well:
missing values are handled according to each feature's business meaning rather than by a generic
imputation, and `message` is retained for future LLM root-cause work even though classical models
don't use it.

**How it was built.** Notebook 05: Silver, plus time features and `instance_exists`, with sentinel
fills (`http_method`→`NO_HTTP`, `http_path`→`NO_PATH`, `status_code`→`-1`, `response_time`→`-1`),
dropping `event_id`, `dataset_source`, `timestamp`, `event_timestamp`, `event_date`.

**Grain.** One row per log event — same as Silver.

`instance_exists` is the nicest idea in this notebook: it encodes *missingness itself* as a
feature, which is exactly the right instinct given Fact 2.

**Verdict.** **Not for the dashboard.** It is event-grain, wide, and full of `-1` sentinels that
would render as real values in a chart. It exists to feed models.

One bug to fix here (notebook 05, cell 19):

```python
.withColumn("event_hour",    F.hour("event_timestamp"))
.withColumn("event_day",     F.hour("event_timestamp"))   # should be F.dayofmonth
.withColumn("event_month",   F.hour("event_timestamp"))   # should be F.month
.withColumn("event_weekday", F.hour("event_timestamp"))   # should be F.dayofweek
```

All four call `F.hour`, so `event_day`, `event_month` and `event_weekday` are identical copies of
`event_hour` — and because `withColumn` replaces, this also overwrote Silver's correct
`event_month`. Three of your model's features are duplicates of a fourth, and one previously
correct column was silently corrupted. A four-character fix per line.

### 3.10 `ml_feature_dataset`

**Why it exists.** Notebook 05's header names `ml_feature_dataset` as its output, but cell 25
actually writes to `feature_engineering_dataset`, and **no notebook in the repo writes
`ml_feature_dataset` at all**. So this table was created outside the committed notebooks.

**Verdict.** Not for the dashboard, and worth resolving before you submit: either delete it as a
stray, or find the code that made it and commit that code. A reviewer who opens your catalog will
see a table with no lineage. Renaming `feature_engineering_dataset` → `ml_feature_dataset` in
notebook 05 and dropping the other would make the naming match the documentation.

---

## 4. What actually goes on the dashboard

Two of the ten tables are ML-only (`feature_engineering_dataset`, `ml_feature_dataset`). Of the
remaining eight, `service_performance`, `service_health` and `service_risk_dashboard` are the same
table at three stages, and `anomaly_summary` overlaps them. So the eight analytics tables get
re-cut into **eight purpose-built views** — the same information reorganised so each view answers
one question and contains no NULLs.

The rule that makes them NULL-free without faking anything: **volume, error and anomaly metrics
come from all log lines; latency metrics come only from the HTTP-request subset**
(`WHERE status_code IS NOT NULL`), where `response_time` always exists. Nothing is imputed,
nothing is dropped from the volume charts, and the latency numbers actually become *more* correct
than they are today, because they stop being diluted by rows that were never measurable.

These are views in Gold, so cleaning stays in Databricks and Superset only reads — consistent with
how you decided to handle this. Run the DDL in a Databricks notebook or in Superset's SQL Lab
against the Databricks connection, then create one Superset dataset per view.

### 4.1 `vw_dash_service_overview` — the main service table

Replaces `service_performance`, `service_health`, `service_risk_dashboard` and `anomaly_summary`.

```sql
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_service_overview AS
WITH base AS (
  SELECT
    service,
    COUNT(*)                                                         AS log_lines,
    SUM(CASE WHEN log_level = 'ERROR'    THEN 1 ELSE 0 END)          AS error_count,
    SUM(CASE WHEN log_level = 'WARNING'  THEN 1 ELSE 0 END)          AS warning_count,
    SUM(CASE WHEN log_level = 'CRITICAL' THEN 1 ELSE 0 END)          AS critical_count,
    SUM(CASE WHEN is_anomaly = 1         THEN 1 ELSE 0 END)          AS anomaly_count,
    COUNT(status_code)                                               AS http_requests,
    COUNT(response_time)                                             AS latency_samples,
    AVG(response_time)                                               AS avg_response_time_raw
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
  ROUND(100.0 * error_count   / log_lines, 2)                        AS error_rate,
  ROUND(100.0 * warning_count / log_lines, 2)                        AS warning_rate,
  ROUND(100.0 * anomaly_count / log_lines, 2)                        AS anomaly_rate,
  CASE
    WHEN 100.0 * error_count / log_lines <  1 THEN 'Healthy'
    WHEN 100.0 * error_count / log_lines <  5 THEN 'Degraded'
    ELSE 'Critical'
  END                                                                AS health_status,
  -- latency contributes only when measured, so the score can never disappear
  ROUND(
      (100.0 * error_count   / log_lines) * 0.5
    + (100.0 * anomaly_count / log_lines) * 0.3
    + COALESCE(avg_response_time_raw * 20, 0) * 0.2
  , 2)                                                               AS risk_score,
  CASE WHEN latency_samples > 0 THEN 'measured' ELSE 'not measured' END AS latency_coverage
FROM base;
```

Every column is non-NULL: the counters use `CASE … ELSE 0`, `log_lines` is always positive, and
the latency term is `COALESCE`d. `latency_coverage` is a readable string rather than a boolean so
it works directly as a Superset filter or colour dimension. Note this view reproduces
`health_status` and `risk_score` from Silver rather than joining Gold, so it stays consistent even
if notebook 04 hasn't been re-run.

### 4.2 `vw_dash_service_latency` — latency, HTTP-scoped

```sql
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_service_latency AS
SELECT
  service,
  COUNT(*)                                            AS request_count,
  ROUND(AVG(response_time), 4)                        AS avg_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.50), 4)    AS p50_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.95), 4)    AS p95_response_time,
  ROUND(MAX(response_time), 4)                        AS max_response_time,
  ROUND(MIN(response_time), 4)                        AS min_response_time,
  SUM(CASE WHEN status_code >= 500 THEN 1 ELSE 0 END) AS server_errors,
  SUM(CASE WHEN status_code >= 400
            AND status_code <  500 THEN 1 ELSE 0 END) AS client_errors,
  SUM(CASE WHEN status_code  = 200 THEN 1 ELSE 0 END) AS success_count
FROM `log-analytics`.silver.silver_logs
WHERE status_code IS NOT NULL          -- HTTP requests only: latency always exists here
  AND response_time IS NOT NULL
GROUP BY service;
```

Only services that actually served requests appear, so there is nothing to be NULL. The `p95` is
worth having — it is the number an SRE would look at, and it makes the dashboard look considered
rather than generated. Adding `min`/`max` here also lets you retire the NULL-prone
`min_response_time` / `max_response_time` columns from the service table.

### 4.3 `vw_dash_http_status` — status distribution done correctly

```sql
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_http_status AS
SELECT
  status_code,
  CASE
    WHEN status_code >= 500 THEN '5xx server error'
    WHEN status_code >= 400 THEN '4xx client error'
    WHEN status_code >= 300 THEN '3xx redirect'
    WHEN status_code >= 200 THEN '2xx success'
    ELSE 'other'
  END                                                  AS status_class,
  COUNT(*)                                             AS request_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)   AS pct_of_requests,
  ROUND(AVG(response_time), 4)                         AS avg_response_time
FROM `log-analytics`.silver.silver_logs
WHERE status_code IS NOT NULL
GROUP BY status_code;
```

`SUM(COUNT(*)) OVER ()` gives the denominator as total *requests*, so `pct_of_requests` sums to
100 and each code's share is finally correct.

### 4.4 `vw_dash_daily_trend` — volume and severity over time

```sql
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
```

### 4.5 `vw_dash_hourly_trend` — use this if the date range is short

```sql
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
```

Check `SELECT COUNT(DISTINCT event_date) FROM silver_logs` first. Fewer than about five distinct
dates and the daily chart looks broken — use the hourly view as your trend axis instead.

### 4.6 `vw_dash_hourly_latency` — latency over time, HTTP-scoped

```sql
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_hourly_latency AS
SELECT
  DATE_TRUNC('HOUR', event_timestamp)                    AS event_hour_ts,
  COUNT(*)                                              AS request_count,
  ROUND(AVG(response_time), 4)                           AS avg_response_time,
  ROUND(PERCENTILE_APPROX(response_time, 0.95), 4)       AS p95_response_time
FROM `log-analytics`.silver.silver_logs
WHERE status_code IS NOT NULL
  AND response_time IS NOT NULL
  AND event_timestamp IS NOT NULL
GROUP BY DATE_TRUNC('HOUR', event_timestamp);
```

Only hours containing requests appear, so the line has no NULL gaps.

### 4.7 `vw_dash_log_level` and `vw_dash_topic` — the two clean breakdowns

```sql
CREATE OR REPLACE VIEW `log-analytics`.gold.vw_dash_log_level AS
SELECT
  log_level,
  COUNT(*)                                            AS log_lines,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)  AS pct_of_logs,
  MAX(severity_score)                                 AS severity_score
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
```

`severity_score` is included in the log-level view purely so charts can sort INFO → CRITICAL in a
sensible order instead of alphabetically.

### 4.8 Verify all eight are NULL-free before building charts

Run this once. Every number must be zero.

```sql
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
```

---

## 5. The dashboard itself

Eight datasets, eleven charts, one page. In Superset go SQL Lab → run
`SELECT * FROM <view>` → **Save as dataset**, which sidesteps the table-browser problem you hit,
then build charts from the datasets.

| # | chart | type | dataset | columns |
|---|---|---|---|---|
| 1 | Total log lines | Big Number | `vw_dash_daily_trend` | `SUM(log_lines)` |
| 2 | Error rate | Big Number | `vw_dash_service_overview` | `100*SUM(error_count)/SUM(log_lines)` |
| 3 | Anomalies detected | Big Number | `vw_dash_service_overview` | `SUM(anomaly_count)` |
| 4 | p95 latency | Big Number | `vw_dash_service_latency` | `MAX(p95_response_time)` |
| 5 | Top services by risk | Bar, horizontal | `vw_dash_service_overview` | `service` × `risk_score`, colour by `health_status` |
| 6 | Log level mix | Pie or donut | `vw_dash_log_level` | `log_level` × `log_lines` |
| 7 | Volume & errors over time | Mixed time-series | `vw_dash_hourly_trend` | `event_hour_ts` × `log_lines` bar + `errors` line |
| 8 | Latency over time | Line | `vw_dash_hourly_latency` | `event_hour_ts` × `avg_response_time`, `p95_response_time` |
| 9 | HTTP status distribution | Bar | `vw_dash_http_status` | `status_class` × `request_count` |
| 10 | Service latency detail | Table | `vw_dash_service_latency` | `service`, `request_count`, `avg`, `p50`, `p95`, `max` |
| 11 | Kafka topic throughput | Bar | `vw_dash_topic` | `kafka_topic` × `log_lines`, `error_count` |

Chart 5 is the centrepiece — it is the one that would have been broken by the `risk_score` NULL.
Charts 1–4 across the top, 5–6 on the second row, 7–8 on the third, 9–11 at the bottom.

Add a dashboard-level filter on `service` if the service count is manageable. Skip a date-range
filter unless the sample spans enough time to make it meaningful — an empty date filter looks worse
than no filter.

---

## 6. Bugs worth fixing, in priority order

Six issues found by reading the code. The first four are quick and materially improve correctness.

**1. `risk_score` disappears on NULL latency** (notebook 04, cell 15). Fixed by the view in 4.1.
To fix it at source, wrap the latency term: `COALESCE(F.col("avg_response_time") * 20, F.lit(0))`.

**2. `response_category` labels unmeasured rows "Slow"** (notebook 03, cell 11). `NULL < 0.2`
evaluates to NULL, not true, so both branches fall through to `.otherwise("Slow")` and every
non-HTTP line is stamped Slow. This is worse than a NULL — it is a NULL converted into a confident
wrong value, and it flows into the feature set of both models. Fix:

```python
F.when(F.col("response_time").isNull(), "Not Measured")
 .when(F.col("response_time") < 0.2, "Fast")
 .when(F.col("response_time") < 0.5, "Normal")
 .otherwise("Slow")
```

**3. Three duplicated time features** (notebook 05, cell 19). `event_day`, `event_month` and
`event_weekday` all call `F.hour`. Use `F.dayofmonth`, `F.month`, `F.dayofweek`.

**4. `avg_status_code` is meaningless** (notebook 04, cell 6). The mean of 200 and 404 is 302 — a
real status code with an unrelated meaning. Delete the aggregate; `success_count` and the status
view already carry the signal.

**5. Feature importance never displays** (notebook 06, cell 8). `hasattr(model, "featureImportance")`
is missing its trailing `s` — the attribute is `featureImportances` — so the function always takes
the `else` branch. And `top_features` calls `show_feature_importance` while the function is defined
as `show_fearure_importance`, which would raise `NameError`. Worth fixing because working feature
importance is exactly what would have surfaced issue 6.

**6. Target leakage in the service-health model** (notebook 08). The `service_health` target in
cell 10 is a deterministic rule over `log_level`, `response_time`, `severity_score`, `status_code`
and `is_anomaly`; the `feature_cols` list in cell 14 then hands the model `log_level`,
`response_time`, `status_code`, `severity_score` and `response_category`. The model is recovering
an if/else statement, which is why the F1 looks too good. Fact 2 makes it worse: anomaly rows
always have NULL latency and status, which cell 7 fills with `0.0` and `-1`, giving every anomaly a
perfectly separable signature (`status_code = -1`, `response_category = 'Slow'`).

You do not need to retrain to handle this well. The strongest move in three days is to **name it**:
state in the write-up that the service-health target is rule-derived, that its inputs overlap the
feature set, and that the reported F1 therefore measures rule recovery rather than prediction.
Presenting that analysis is a more advanced result than a high score, and it pairs naturally with
the early-warning model you paused for honest reasons. Also note cell 7 fills `response_time` with
`0.0` while notebook 05 uses `-1` for the same column — pick one convention.

---

## 7. Three-day plan

### Day 1 — make Gold correct and dashboard-ready

Start by running the audit notebook so you have real numbers to quote: NULL counts per column,
latency coverage per service, and how many services lose their `risk_score`. Then apply fixes 1–4
above and re-run notebooks 03, 04 and 05 in order so Silver and Gold are consistent. Then create
the eight views from section 4 and run the verification query in 4.8 until every count is zero.
Finish by checking `COUNT(DISTINCT event_date)` so you know whether the trend charts should be
daily or hourly.

The re-run of notebook 03 matters more than it looks: fixing `response_category` changes Silver,
which changes the feature tables, so do it before you touch anything ML-related.

### Day 2 — build the dashboard

Create eight Superset datasets via SQL Lab → Save as dataset. Build the eleven charts from section
5, starting with charts 1–5 since they carry most of the story. Assemble the single dashboard,
arrange the rows as described, add the `service` filter, and set sensible titles and axis labels —
"errors per 1,000 log lines" reads far better than `error_rate`. Take screenshots as you go for the
README.

Budget time for Superset's chart-level quirks rather than data problems; the views are built so
that nothing needs cleaning inside Superset.

### Day 3 — the AI layer and the write-up

For the AI intelligence layer, the highest-value-per-hour version reads
`vw_dash_service_overview` plus the top rows of `vw_dash_service_latency` and produces a short
incident narrative per service: what the numbers say, which evidence supports it, plausible causes,
and a recommended action. Feeding it the views rather than raw Silver keeps the prompt small and
grounded, and `latency_coverage` lets the model say "latency unmeasured for this service" instead
of hallucinating a number — the NULL discipline paying off directly.

Then write the README, which is currently empty and is the first thing anyone will look at. Cover
the architecture diagram, the ten Gold tables and their purpose, the dashboard screenshots, both
models with honest metrics and the leakage caveat, the paused early-warning model with the 223/5
class counts, and the NULL-handling policy — the fact that you traced NULLs to optional regex
groups and scoped latency to HTTP requests rather than zero-filling is one of the strongest
engineering-judgement stories in the project. Say it explicitly.

If time runs short, cut the AI layer to a single worked example for one service and keep the
README complete. An excellent README with one good example beats a half-built feature nobody can
find.


# Databricks notebook source
# MAGIC %md
# MAGIC # Log Guardian — Gold NULL Audit & Root Cause
# MAGIC
# MAGIC The NULLs in Gold are **not** a mystery and **not** a pipeline failure. Reading
# MAGIC `bin/log_parser.py` and `notebooks/03`/`04` explains all of them:
# MAGIC
# MAGIC ```
# MAGIC log_parser.py line 81-83
# MAGIC     (?:status:\s*(?P<status_code>\d+))?     <- optional group
# MAGIC     (?:time:\s*(?P<response_time>[\d.]+))?  <- optional group
# MAGIC ```
# MAGIC
# MAGIC Only `wsgi.server` HTTP request lines carry `status:` and `time:`. Every other OpenStack log
# MAGIC line — periodic tasks, compute-manager INFO, tracebacks — has no latency to report, so the
# MAGIC parser emits `None`, and Silver stores NULL.
# MAGIC
# MAGIC In Gold, `F.avg("response_time")` silently skips NULLs, so `avg_response_time` only comes out
# MAGIC NULL when **every** row in a group was a non-HTTP line.
# MAGIC
# MAGIC **So the meaning of the NULL is: "this group logged no HTTP requests at all."**
# MAGIC
# MAGIC That rules out zero-filling completely — `0.0` would claim an instant response for a service
# MAGIC that never served a measurable request, and would drag every cross-service average down.
# MAGIC It also rules out "no traffic", because `total_requests` counts *log lines*, not requests,
# MAGIC and is greater than zero on exactly those rows.
# MAGIC
# MAGIC This notebook quantifies that, then checks three consequences that look like real bugs.
# MAGIC Sections A–E are read-only. Section G writes views and is the only part that changes anything.

# COMMAND ----------

CATALOG = "`log-analytics`"
SILVER = f"{CATALOG}.silver.silver_logs"
GOLD = f"{CATALOG}.gold"

DASHBOARD_TABLES = [
    "service_risk_dashboard",
    "daily_summary",
    "anomaly_summary",
    "http_status_summary",
    "log_level_summary",
    "topic_summary",
    "service_health",
    "service_performance",
]

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. NULL profile — how much, and exactly where
# MAGIC
# MAGIC One row per (table, column). `distinct_non_null` matters: a column that is 100% NULL is dead
# MAGIC weight, while a patchy one needs a story on the dashboard.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

PROFILE_SCHEMA = StructType([
    StructField("table_name", StringType(), False),
    StructField("column_name", StringType(), False),
    StructField("data_type", StringType(), False),
    StructField("total_rows", LongType(), False),
    StructField("null_count", LongType(), False),
    StructField("null_pct", DoubleType(), True),
    StructField("distinct_non_null", LongType(), False),
])

profile_rows = []

for t in DASHBOARD_TABLES:
    df = spark.table(f"{GOLD}.{t}")
    fields = list(df.schema.fields)
    total = df.count()

    if total == 0:
        print(f"{t}: EMPTY TABLE")
        for f_ in fields:
            profile_rows.append((t, f_.name, f_.dataType.simpleString(), 0, 0, None, 0))
        continue

    # positional aliases so no source column name can break the aggregate
    aggs = []
    for i, f_ in enumerate(fields):
        c = f_.name
        aggs.append(F.sum(F.col(f"`{c}`").isNull().cast("long")).alias(f"n_{i}"))
        aggs.append(F.countDistinct(F.col(f"`{c}`")).alias(f"d_{i}"))

    res = df.agg(*aggs).collect()[0].asDict()

    for i, f_ in enumerate(fields):
        nulls = int(res[f"n_{i}"] or 0)
        profile_rows.append((
            t, f_.name, f_.dataType.simpleString(), int(total), nulls,
            round(100.0 * nulls / total, 2), int(res[f"d_{i}"] or 0),
        ))

    print(f"{t}: {total} rows, {len(fields)} columns")

null_profile = spark.createDataFrame(profile_rows, schema=PROFILE_SCHEMA)
null_profile.createOrReplaceTempView("null_profile")

display(null_profile.filter(F.col("null_count") > 0).orderBy(F.desc("null_pct")))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The decision list: every column that needs a call, worst first.
# MAGIC SELECT
# MAGIC   table_name, column_name, data_type, total_rows, null_count, null_pct, distinct_non_null,
# MAGIC   CASE
# MAGIC     WHEN null_pct = 100 THEN 'entirely null — column carries no information, drop it'
# MAGIC     WHEN null_pct >= 50 THEN 'mostly null — unusable as a headline KPI'
# MAGIC     WHEN null_pct >= 10 THEN 'materially null — needs an explicit no-data treatment'
# MAGIC     ELSE 'sparse — safe to chart with a gap'
# MAGIC   END AS severity
# MAGIC FROM null_profile
# MAGIC WHERE null_count > 0
# MAGIC ORDER BY null_pct DESC, table_name, column_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Root cause confirmation in Silver
# MAGIC
# MAGIC Two things to confirm:
# MAGIC 1. `response_time` and `status_code` are NULL on the **same** rows (same optional regex groups).
# MAGIC 2. Those rows are the non-`wsgi.server` log lines — i.e. NULL tracks the log source, not a failure.

# COMMAND ----------

silver = spark.table(SILVER)
total_silver = silver.count()

coverage = silver.agg(
    F.count("*").alias("rows"),
    F.sum(F.col("response_time").isNull().cast("long")).alias("rt_null"),
    F.sum(F.col("status_code").isNull().cast("long")).alias("sc_null"),
    F.sum((F.col("response_time").isNull() & F.col("status_code").isNull()).cast("long")).alias("both_null"),
    F.sum((F.col("response_time").isNull() & F.col("status_code").isNotNull()).cast("long")).alias("rt_null_only"),
    F.sum((F.col("response_time").isNotNull() & F.col("status_code").isNull()).cast("long")).alias("sc_null_only"),
).collect()[0]

print(f"silver_logs rows        : {coverage['rows']}")
print(f"response_time NULL      : {coverage['rt_null']}  ({100*coverage['rt_null']/total_silver:.1f}%)")
print(f"status_code   NULL      : {coverage['sc_null']}  ({100*coverage['sc_null']/total_silver:.1f}%)")
print(f"both NULL together      : {coverage['both_null']}")
print(f"response_time NULL only : {coverage['rt_null_only']}   <- expect ~0 if they share a source")
print(f"status_code   NULL only : {coverage['sc_null_only']}   <- expect ~0 if they share a source")
print()
print("If the last two are ~0, both columns are populated only by wsgi.server request lines,")
print("confirming NULL = 'not an HTTP request record' rather than a parsing defect.")

# COMMAND ----------

# MAGIC %md
# MAGIC Latency coverage per service. `services with rt_measured = 0` are exactly the ones that end up
# MAGIC with NULL `avg_response_time` in Gold — note their `log_lines` is **not** zero.

# COMMAND ----------

service_coverage = (
    silver.groupBy("service")
    .agg(
        F.count("*").alias("log_lines"),
        F.count("response_time").alias("rt_measured"),   # count() skips NULLs
        F.avg("response_time").alias("avg_response_time"),
    )
    .withColumn("rt_coverage_pct", F.round(100.0 * F.col("rt_measured") / F.col("log_lines"), 2))
    .withColumn(
        "gold_avg_will_be_null",
        F.when(F.col("rt_measured") == 0, "YES — and log_lines > 0, so this is NOT 'no traffic'")
         .otherwise("no"),
    )
    .orderBy("rt_coverage_pct")
)

display(service_coverage)

# COMMAND ----------

# latency coverage by log_level and by topic — shows the pattern is structural
display(
    silver.groupBy("log_level")
    .agg(
        F.count("*").alias("log_lines"),
        F.count("response_time").alias("rt_measured"),
    )
    .withColumn("rt_coverage_pct", F.round(100.0 * F.col("rt_measured") / F.col("log_lines"), 2))
    .orderBy(F.desc("log_lines"))
)

display(
    silver.groupBy("kafka_topic")
    .agg(
        F.count("*").alias("log_lines"),
        F.count("response_time").alias("rt_measured"),
    )
    .withColumn("rt_coverage_pct", F.round(100.0 * F.col("rt_measured") / F.col("log_lines"), 2))
    .orderBy(F.desc("log_lines"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Consequence 1 — `risk_score` silently disappears
# MAGIC
# MAGIC From notebook 04:
# MAGIC
# MAGIC ```python
# MAGIC "risk_score",
# MAGIC F.round(error_rate*0.5 + anomaly_rate*0.3 + avg_response_time*20*0.2, 2)
# MAGIC ```
# MAGIC
# MAGIC SQL arithmetic with NULL yields NULL, so **any** service with no measured latency gets no risk
# MAGIC score at all — not a low score, no score. `service_risk_dashboard` is the flagship dashboard
# MAGIC table, so those services would silently vanish from a "top risk" chart, or sort unpredictably.
# MAGIC This is the most consequential NULL in the project.

# COMMAND ----------

srd = spark.table(f"{GOLD}.service_risk_dashboard")

display(
    srd.select(
        "service", "total_requests", "error_rate", "anomaly_rate",
        "avg_response_time", "risk_score", "health_status",
    )
    .withColumn(
        "diagnosis",
        F.when(F.col("risk_score").isNull() & F.col("avg_response_time").isNull(),
               "risk_score lost to NULL latency — service is invisible on the risk chart")
         .when(F.col("risk_score").isNull(), "risk_score NULL for another reason — investigate")
         .otherwise("ok"),
    )
    .orderBy(F.col("risk_score").asc_nulls_first())
)

impact = srd.agg(
    F.count("*").alias("services"),
    F.sum(F.col("risk_score").isNull().cast("long")).alias("no_risk_score"),
    F.sum(F.col("avg_response_time").isNull().cast("long")).alias("no_latency"),
).collect()[0]
print(f"services: {impact['services']}, without risk_score: {impact['no_risk_score']}, "
      f"without latency: {impact['no_latency']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Consequence 2 — `response_category` mislabels missing latency as "Slow"
# MAGIC
# MAGIC From notebook 03:
# MAGIC
# MAGIC ```python
# MAGIC F.when(F.col("response_time") < 0.2, "Fast")
# MAGIC  .when(F.col("response_time") < 0.5, "Normal")
# MAGIC  .otherwise("Slow")
# MAGIC ```
# MAGIC
# MAGIC `NULL < 0.2` evaluates to NULL, which is not true, so both branches fall through and every
# MAGIC unmeasured row is stamped **"Slow"**. This is not a NULL in Gold — it is worse, a NULL that was
# MAGIC silently converted into a confident wrong value, and it flows into the ML feature set
# MAGIC (`response_category` is in `feature_cols` in notebooks 07 and 08).

# COMMAND ----------

display(
    silver.groupBy("response_category")
    .agg(
        F.count("*").alias("rows"),
        F.sum(F.col("response_time").isNull().cast("long")).alias("rows_with_null_response_time"),
        F.round(F.avg("response_time"), 4).alias("avg_measured_response_time"),
    )
    .withColumn(
        "verdict",
        F.when(F.col("rows_with_null_response_time") > 0,
               "MISLABELLED — unmeasured rows counted as Slow").otherwise("ok"),
    )
    .orderBy(F.desc("rows"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC The corrected version — a fourth category that tells the truth:
# MAGIC
# MAGIC ```python
# MAGIC .withColumn(
# MAGIC     "response_category",
# MAGIC     F.when(F.col("response_time").isNull(), "Not Measured")
# MAGIC      .when(F.col("response_time") < 0.2, "Fast")
# MAGIC      .when(F.col("response_time") < 0.5, "Normal")
# MAGIC      .otherwise("Slow"),
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Consequence 3 — `http_status_summary` has a NULL status bucket, and a misleading name
# MAGIC
# MAGIC `http_status_summary` groups by `status_code`, which is NULL for every non-HTTP line, so there is
# MAGIC a NULL bucket that is probably the largest row in the table — and its `percentage` is computed
# MAGIC against *all* log lines, not against HTTP requests. On a chart titled "HTTP status distribution"
# MAGIC that bucket is meaningless and the percentages understate every real status code.
# MAGIC
# MAGIC Same naming issue in `service_performance`: `total_requests` is `F.count("*")` over all log
# MAGIC lines, so it is really `total_log_lines`. Any "requests" label on the dashboard would be wrong.

# COMMAND ----------

hss = spark.table(f"{GOLD}.http_status_summary")
display(
    hss.withColumn(
        "note",
        F.when(F.col("status_code").isNull(),
               "non-HTTP log lines — exclude from any HTTP status chart").otherwise("real status code"),
    ).orderBy(F.desc("total_requests"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Recommended handling
# MAGIC
# MAGIC Every NULL here has the same origin, so the policy is short. Nothing gets zero-filled.
# MAGIC
# MAGIC | table | column(s) | handling | why |
# MAGIC |---|---|---|---|
# MAGIC | service_performance, service_health, service_risk_dashboard | avg / max / min_response_time | **keep NULL**, add `latency_sample_count` + `latency_observed` flag | no HTTP requests logged; 0.0 would fake an instant response |
# MAGIC | service_risk_dashboard | risk_score | **recompute** so latency contributes only when observed, plus `risk_score_complete` flag | today the whole score vanishes on one NULL term |
# MAGIC | daily_summary, topic_summary, anomaly_summary | avg_response_time | **keep NULL**, chart as a gap | same origin |
# MAGIC | http_status_summary | status_code NULL bucket | **exclude** from HTTP charts; recompute `percentage` over HTTP rows only | not an HTTP status |
# MAGIC | service_performance | avg_status_code | **drop** | the mean of 200 and 404 is 302 — a real code, wrong meaning |
# MAGIC | service_performance | total_requests | **rename** to `total_log_lines` | it counts log lines, not requests |
# MAGIC | silver_logs | response_category | **fix** — add a `Not Measured` branch | currently mislabels unmeasured rows as Slow |
# MAGIC
# MAGIC The counter columns (`error_count`, `warning_count`, `critical_count`, `success_count`,
# MAGIC `anomaly_count`) are built with `F.sum(F.when(...).otherwise(0))`, so they are never NULL and
# MAGIC need no treatment. `error_rate` / `warning_rate` / `anomaly_rate` divide by `count("*")`, which is
# MAGIC always positive, so they are safe too. The NULL problem is entirely a latency problem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. The fix — dashboard-ready views
# MAGIC
# MAGIC Views, not table rewrites: Gold keeps its honest NULLs, Superset reads a clean contract, and
# MAGIC nothing has to be re-run through Bronze/Silver. Point the Superset datasets at these.
# MAGIC
# MAGIC `latency_sample_count` is the important addition — it turns "why is this blank?" into a number
# MAGIC the dashboard can show.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW `log-analytics`.gold.vw_service_risk_dashboard AS
# MAGIC WITH latency AS (
# MAGIC   SELECT
# MAGIC     service,
# MAGIC     COUNT(response_time)                                  AS latency_sample_count,
# MAGIC     COUNT(*)                                              AS log_lines
# MAGIC   FROM `log-analytics`.silver.silver_logs
# MAGIC   GROUP BY service
# MAGIC )
# MAGIC SELECT
# MAGIC   d.service,
# MAGIC   d.total_requests                                        AS total_log_lines,
# MAGIC   COALESCE(l.latency_sample_count, 0)                     AS latency_sample_count,
# MAGIC   (COALESCE(l.latency_sample_count, 0) > 0)               AS latency_observed,
# MAGIC   d.error_count, d.warning_count, d.critical_count, d.success_count, d.anomaly_count,
# MAGIC   d.error_rate, d.warning_rate, d.anomaly_rate,
# MAGIC   d.health_status,
# MAGIC   -- left NULL on purpose: absence of measurement, not a fast service
# MAGIC   d.avg_response_time, d.max_response_time, d.min_response_time,
# MAGIC   -- latency term contributes only when it exists, so the score never vanishes
# MAGIC   ROUND(
# MAGIC     d.error_rate * 0.5
# MAGIC     + d.anomaly_rate * 0.3
# MAGIC     + COALESCE(d.avg_response_time * 20 * 0.2, 0)
# MAGIC   , 2)                                                    AS risk_score,
# MAGIC   -- so a partial score is never mistaken for a complete one
# MAGIC   (d.avg_response_time IS NOT NULL)                       AS risk_score_complete
# MAGIC FROM `log-analytics`.gold.service_risk_dashboard d
# MAGIC LEFT JOIN latency l ON d.service = l.service

# COMMAND ----------

# MAGIC %sql
# MAGIC -- HTTP status distribution over HTTP requests only, with percentages that add to 100
# MAGIC CREATE OR REPLACE VIEW `log-analytics`.gold.vw_http_status_summary AS
# MAGIC SELECT
# MAGIC   status_code,
# MAGIC   CASE
# MAGIC     WHEN status_code >= 500 THEN '5xx server error'
# MAGIC     WHEN status_code >= 400 THEN '4xx client error'
# MAGIC     WHEN status_code >= 300 THEN '3xx redirect'
# MAGIC     WHEN status_code >= 200 THEN '2xx success'
# MAGIC     ELSE 'other'
# MAGIC   END                                                     AS status_class,
# MAGIC   COUNT(*)                                                AS request_count,
# MAGIC   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2)      AS pct_of_requests,
# MAGIC   ROUND(AVG(response_time), 4)                            AS avg_response_time,
# MAGIC   COUNT(response_time)                                    AS latency_sample_count
# MAGIC FROM `log-analytics`.silver.silver_logs
# MAGIC WHERE status_code IS NOT NULL      -- the NULL bucket is not an HTTP status
# MAGIC GROUP BY status_code

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily trend with latency coverage exposed alongside the metric
# MAGIC CREATE OR REPLACE VIEW `log-analytics`.gold.vw_daily_summary AS
# MAGIC SELECT
# MAGIC   d.event_date,
# MAGIC   d.total_logs,
# MAGIC   d.errors, d.warnings, d.critical, d.anomalies,
# MAGIC   d.avg_response_time,                                    -- NULL = nothing measured that day
# MAGIC   COALESCE(s.latency_sample_count, 0)                     AS latency_sample_count,
# MAGIC   (COALESCE(s.latency_sample_count, 0) > 0)               AS latency_observed
# MAGIC FROM `log-analytics`.gold.daily_summary d
# MAGIC LEFT JOIN (
# MAGIC   SELECT event_date, COUNT(response_time) AS latency_sample_count
# MAGIC   FROM `log-analytics`.silver.silver_logs
# MAGIC   GROUP BY event_date
# MAGIC ) s ON d.event_date = s.event_date

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify the fix
# MAGIC
# MAGIC `risk_score` should now be non-NULL for every service, with `risk_score_complete = false`
# MAGIC marking the ones missing a latency contribution.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                        AS services,
# MAGIC   SUM(CASE WHEN risk_score IS NULL THEN 1 ELSE 0 END)             AS still_null_risk_score,
# MAGIC   SUM(CASE WHEN NOT risk_score_complete THEN 1 ELSE 0 END)        AS partial_scores,
# MAGIC   SUM(CASE WHEN NOT latency_observed THEN 1 ELSE 0 END)           AS services_without_latency
# MAGIC FROM `log-analytics`.gold.vw_service_risk_dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT service, total_log_lines, latency_sample_count, latency_observed,
# MAGIC        error_rate, anomaly_rate, avg_response_time, risk_score, risk_score_complete
# MAGIC FROM `log-analytics`.gold.vw_service_risk_dashboard
# MAGIC ORDER BY risk_score DESC

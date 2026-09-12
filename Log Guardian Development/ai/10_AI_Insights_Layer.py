# Databricks notebook source
# MAGIC %md
# MAGIC # AI Insights Layer
# MAGIC
# MAGIC Turns the numbers already computed by the `vw_dash_*` Gold views into written
# MAGIC explanations, and records what each ML model actually is.
# MAGIC
# MAGIC Responsibilities:
# MAGIC
# MAGIC - Read facts from the eight dashboard views (no new arithmetic)
# MAGIC - Compose a deterministic rule-based narrative for every scope
# MAGIC - Optionally upgrade that narrative with a Databricks Foundation Model
# MAGIC - Record which path produced each row
# MAGIC - Build model cards, reading metrics from MLflow rather than hardcoding them
# MAGIC - Write and verify two Gold tables
# MAGIC
# MAGIC Output:
# MAGIC - `gold.ai_insights`
# MAGIC - `gold.ml_model_cards`
# MAGIC
# MAGIC ### The grounding rule
# MAGIC
# MAGIC The model never sees the database. It sees a dictionary of numbers that were
# MAGIC already computed in SQL, and its only job is to turn those numbers into sentences.
# MAGIC Every figure in every narrative is therefore traceable to a view column, and the
# MAGIC exact dictionary is stored beside the narrative in `evidence`.
# MAGIC
# MAGIC ### Why there is a rule-based path at all
# MAGIC
# MAGIC The rule-based narrative runs first and always succeeds. The model output replaces
# MAGIC it only when the endpoint returns usable text. So if Foundation Model APIs are not
# MAGIC enabled in this workspace region, every panel in the UI still has an explanation,
# MAGIC and `generation_mode` records the difference instead of hiding it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = "log-analytics"
GOLD = f"`{CATALOG}`.gold"
SILVER = f"`{CATALOG}`.silver"

INSIGHTS_TABLE = f"{GOLD}.ai_insights"
MODEL_CARDS_TABLE = f"{GOLD}.ml_model_cards"

# Set this to a chat/instruct endpoint in your workspace. The next cell prints the
# endpoints you actually have. Leave it as None to run rule-only, which is a valid
# full run: every table still gets populated.
ENDPOINT_NAME = None
# ENDPOINT_NAME = "databricks-meta-llama-3-3-70b-instruct"

MLFLOW_EXPERIMENT = "/Shared/LogGuardian_ML"

# A service needs this many log lines before its risk score is treated as meaningful.
# Matches the is_scored floor already built into vw_dash_service_overview.
MIN_LINES_FOR_SCORING = 100

print("=" * 60)
print("AI INSIGHTS LAYER")
print("=" * 60)
print(f"Catalog        : {CATALOG}")
print(f"Insights table : {INSIGHTS_TABLE}")
print(f"Cards table    : {MODEL_CARDS_TABLE}")
print(f"Endpoint       : {ENDPOINT_NAME or 'none (rule-based only)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Which serving endpoints exist here
# MAGIC
# MAGIC Run this, copy a chat-capable name into `ENDPOINT_NAME` above, and re-run from the
# MAGIC top. If the list is empty or the call fails, Foundation Model APIs are not available
# MAGIC in this workspace — carry on, the rule-based path covers it.

# COMMAND ----------

try:
    from databricks.sdk import WorkspaceClient

    _endpoints = list(WorkspaceClient().serving_endpoints.list())
    if _endpoints:
        print(f"{len(_endpoints)} serving endpoint(s) visible:\n")
        for _e in _endpoints:
            _task = getattr(_e, "task", None) or "unspecified"
            print(f"  {_e.name:<55} task={_task}")
    else:
        print("No serving endpoints visible in this workspace.")
except Exception as _e:
    print(f"Could not list serving endpoints: {type(_e).__name__}: {_e}")
    print("This is not fatal. The rule-based path does not need an endpoint.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section A — Read the facts
# MAGIC
# MAGIC Every view is small: one row per service, per log level, per hour. They all fit in
# MAGIC pandas comfortably, and pulling them local keeps the rest of this notebook plain
# MAGIC Python rather than Spark.
# MAGIC
# MAGIC `load()` returns an empty frame instead of raising if a view is missing, so a
# MAGIC partially built Gold layer degrades to fewer insights rather than a failed run.

# COMMAND ----------

import json
import math
from datetime import datetime

import pandas as pd


def load(view_name):
    """Read a dashboard view into pandas. Returns an empty frame if it does not exist."""
    try:
        df = spark.table(f"{GOLD}.{view_name}").toPandas()
        print(f"  {view_name:<28} {len(df):>6} rows   {len(df.columns)} cols")
        return df
    except Exception as e:
        print(f"  {view_name:<28} MISSING  ({type(e).__name__})")
        return pd.DataFrame()


print("Loading dashboard views")
print("-" * 60)
overview = load("vw_dash_service_overview")
latency = load("vw_dash_service_latency")
log_level = load("vw_dash_log_level")
http_status = load("vw_dash_http_status")
hourly = load("vw_dash_hourly_trend")
hourly_latency = load("vw_dash_hourly_latency")
daily = load("vw_dash_daily_trend")
topic = load("vw_dash_topic")

if overview.empty:
    raise RuntimeError(
        "vw_dash_service_overview is empty or missing. Run dashboard_views.sql first — "
        "this notebook reads the views, it does not compute from silver_logs."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defensive column access
# MAGIC
# MAGIC Column names drifted once already during this project (`risk_score_v2` that never
# MAGIC existed, a `vw_das_log_level` typo). `pick()` accepts several candidate names and
# MAGIC returns the first that is present, so a rename upstream produces a missing number
# MAGIC rather than a `KeyError` halfway through a write.

# COMMAND ----------


def pick(row, *names, default=None):
    """First present, non-null value among the named fields."""
    for n in names:
        if n in row and row[n] is not None:
            v = row[n]
            if isinstance(v, float) and math.isnan(v):
                continue
            return v
    return default


def num(v, default=0):
    """Coerce a Decimal / numpy scalar / None into a plain Python number."""
    if v is None:
        return default
    try:
        f = float(v)
    except (TypeError, ValueError):
        return default
    if math.isnan(f):
        return default
    return int(f) if f.is_integer() else round(f, 4)


def col_of(df, *names):
    for n in names:
        if n in df.columns:
            return n
    return None


# Latency, keyed by service, so a service with no HTTP requests simply has no entry.
latency_by_service = {}
if not latency.empty:
    svc_col = col_of(latency, "service")
    for _, r in latency.iterrows():
        latency_by_service[r[svc_col]] = {
            "p50_response_time": num(pick(r, "p50_response_time", "p50"), None),
            "p95_response_time": num(pick(r, "p95_response_time", "p95"), None),
            "avg_response_time": num(pick(r, "avg_response_time", "avg"), None),
            "latency_samples": num(pick(r, "latency_samples", "request_count"), None),
        }

print(f"Latency measured for {len(latency_by_service)} of {len(overview)} services")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section B — Build the fact dictionaries
# MAGIC
# MAGIC One dict per service, plus one for the estate. These dicts are the *only* thing the
# MAGIC model is ever shown, and they are stored verbatim in `ai_insights.evidence` so any
# MAGIC sentence on screen can be checked against the numbers behind it.

# COMMAND ----------

service_facts = []

for _, r in overview.iterrows():
    svc = r["service"]
    log_lines = num(pick(r, "log_lines"))
    f = {
        "service": svc,
        "log_lines": log_lines,
        "http_requests": num(pick(r, "http_requests")),
        "latency_samples": num(pick(r, "latency_samples")),
        "error_count": num(pick(r, "error_count")),
        "warning_count": num(pick(r, "warning_count")),
        "critical_count": num(pick(r, "critical_count")),
        "anomaly_count": num(pick(r, "anomaly_count")),
        "error_rate_pct": num(pick(r, "error_rate")),
        "warning_rate_pct": num(pick(r, "warning_rate")),
        "critical_rate_pct": num(pick(r, "critical_rate")),
        "anomaly_rate_pct": num(pick(r, "anomaly_rate")),
        "risk_score": num(pick(r, "risk_score")),
        "health_status": pick(r, "health_status", default="unknown"),
        "is_scored": bool(pick(r, "is_scored", default=log_lines >= MIN_LINES_FOR_SCORING)),
        "latency_coverage": pick(r, "latency_coverage", default="not measured"),
    }
    f.update(latency_by_service.get(svc, {
        "p50_response_time": None,
        "p95_response_time": None,
        "avg_response_time": None,
    }))
    service_facts.append(f)

estate_log_lines = sum(f["log_lines"] for f in service_facts)
for f in service_facts:
    f["share_of_estate_pct"] = (
        round(100.0 * f["log_lines"] / estate_log_lines, 4) if estate_log_lines else 0
    )

service_facts.sort(key=lambda f: (-f["risk_score"] if f["is_scored"] else 1, -f["log_lines"]))

# ---- Estate-level facts -----------------------------------------------------------
scored = [f for f in service_facts if f["is_scored"]]
unscored = [f for f in service_facts if not f["is_scored"]]
top = scored[0] if scored else service_facts[0]

level_counts = {}
if not log_level.empty:
    lvl_col = col_of(log_level, "log_level")
    cnt_col = col_of(log_level, "log_lines", "line_count", "log_count")
    level_counts = {str(r[lvl_col]): num(r[cnt_col]) for _, r in log_level.iterrows()}

status_counts = {}
if not http_status.empty:
    cls_col = col_of(http_status, "status_class", "status_code")
    req_col = col_of(http_status, "request_count", "requests", "log_lines")
    for _, r in http_status.iterrows():
        status_counts[str(r[cls_col])] = status_counts.get(str(r[cls_col]), 0) + num(r[req_col])

hours_observed = len(hourly) if not hourly.empty else None
peak_hour = None
if not hourly.empty:
    ts_col = col_of(hourly, "event_hour_ts", "event_hour", "hour_ts")
    ln_col = col_of(hourly, "log_lines", "line_count")
    if ts_col and ln_col:
        pk = hourly.loc[hourly[ln_col].idxmax()]
        peak_hour = {"hour": str(pk[ts_col]), "log_lines": num(pk[ln_col])}

# vw_dash_log_level and vw_dash_service_overview are two independent routes to the same
# severity totals. The level view is authoritative — it aggregates every line by level
# with no per-service grouping — so prefer it, and record any disagreement rather than
# silently picking one. A mismatch means a service is missing from one of the two views.
_sum_from_services = {
    "ERROR": sum(f["error_count"] for f in service_facts),
    "WARNING": sum(f["warning_count"] for f in service_facts),
    "CRITICAL": sum(f["critical_count"] for f in service_facts),
}
_severity_disagreement = {
    lvl: {"from_log_level_view": level_counts[lvl], "from_service_overview": _sum_from_services[lvl]}
    for lvl in _sum_from_services
    if lvl in level_counts and level_counts[lvl] != _sum_from_services[lvl]
}
if _severity_disagreement:
    print("WARNING: severity totals disagree between views")
    print(json.dumps(_severity_disagreement, indent=2))

estate_facts = {
    "total_log_lines": estate_log_lines,
    "total_http_requests": sum(f["http_requests"] for f in service_facts),
    "services_total": len(service_facts),
    "services_scored": len(scored),
    "services_below_volume_floor": len(unscored),
    "services_with_measured_latency": len(latency_by_service),
    "total_errors": level_counts.get("ERROR", _sum_from_services["ERROR"]),
    "total_warnings": level_counts.get("WARNING", _sum_from_services["WARNING"]),
    "total_criticals": level_counts.get("CRITICAL", _sum_from_services["CRITICAL"]),
    "severity_totals_disagree": _severity_disagreement or None,
    "total_anomaly_labels": sum(f["anomaly_count"] for f in service_facts),
    "highest_risk_service": top["service"],
    "highest_risk_score": top["risk_score"],
    "highest_risk_health_status": top["health_status"],
    "log_level_counts": level_counts,
    "http_status_class_counts": status_counts,
    "hours_observed": hours_observed,
    "peak_hour": peak_hour,
    "kafka_topics": (
        {str(r[col_of(topic, "kafka_topic", "topic")]): num(r[col_of(topic, "log_lines")])
         for _, r in topic.iterrows()}
        if not topic.empty else {}
    ),
}

print(json.dumps(estate_facts, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section C — The deterministic narrative
# MAGIC
# MAGIC String formatting over the same facts, no model involved. This is what ships when
# MAGIC the endpoint is unavailable, so it has to be genuinely readable rather than a
# MAGIC placeholder.
# MAGIC
# MAGIC Two project-specific facts are hardcoded here because they are the two things a
# MAGIC general-purpose model gets wrong about this dataset:
# MAGIC
# MAGIC 1. A null latency means the service logged **no HTTP requests**. It does not mean
# MAGIC    zero seconds, and it does not mean the service is fast or idle.
# MAGIC 2. `log_lines` counts log lines. `http_requests` counts requests. They are
# MAGIC    different denominators and must never be mixed.

# COMMAND ----------

SEVERITY_OF_STATUS = {
    "Critical": "critical",
    "Degraded": "degraded",
    "Degrading": "degraded",
    "Healthy": "healthy",
    "Low volume": "healthy",
}


def severity_for(f):
    return SEVERITY_OF_STATUS.get(str(f.get("health_status")), "healthy")


def latency_sentence(f):
    if not f["latency_samples"]:
        return (
            "No HTTP requests were logged for this service, so response time is not "
            "measured. That is an absence of measurement, not a latency of zero — the "
            f"service emitted {f['log_lines']:,} log lines, none of which were request "
            "completions."
        )
    p50, p95 = f.get("p50_response_time"), f.get("p95_response_time")
    if p50 is None or p95 is None:
        return (
            f"{f['http_requests']:,} HTTP requests were logged, with response time "
            "captured but percentiles unavailable."
        )
    spread = (p95 / p50) if p50 else None
    tail = ""
    if spread and spread >= 2:
        tail = (
            f" The 95th percentile is {spread:.1f}x the median, so the slow tail is "
            "materially slower than typical traffic."
        )
    return (
        f"Across {f['latency_samples']:,} timed requests the median response time is "
        f"{p50:.3f}s and the 95th percentile is {p95:.3f}s.{tail}"
    )


def severity_sentence(f):
    ll = f["log_lines"]
    if f["error_rate_pct"] >= 50:
        return (
            f"Effectively everything this logger emits is an error: {f['error_count']:,} "
            f"of {ll:,} lines. That is characteristic of a logger whose purpose is to "
            "report failures, so the rate is measured against its own output rather "
            "than against service traffic, and it should be read as a volume of "
            "failures rather than as a failure percentage."
        )
    parts = []
    if f["critical_count"]:
        parts.append(f"{f['critical_count']:,} critical")
    if f["error_count"]:
        parts.append(f"{f['error_count']:,} error")
    if f["warning_count"]:
        parts.append(f"{f['warning_count']:,} warning")
    if not parts:
        return (
            f"None of its {ll:,} lines were logged above INFO — no warnings, errors or "
            "criticals at all in the capture window."
        )
    joined = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]
    return (
        f"It logged {joined} lines out of {ll:,}, putting its error rate at "
        f"{f['error_rate_pct']}% and its warning rate at {f['warning_rate_pct']}%."
    )


def anomaly_sentence(f):
    if not f["anomaly_count"]:
        return ""
    return (
        f" {f['anomaly_count']:,} lines carry an anomaly label, which comes from the "
        "instance-ID join against the labelled anomaly set in the Silver layer. These "
        "are ground-truth labels, not predictions from the anomaly model."
    )


def rule_service_narrative(f):
    volume = (
        f"{f['service']} logged {f['log_lines']:,} lines, "
        f"{share(f['share_of_estate_pct'])} of everything captured."
    )
    if not f["is_scored"]:
        return (
            f"{volume} That is below the {MIN_LINES_FOR_SCORING}-line floor this project "
            "uses before scoring a service, so its rates are arithmetically real but "
            f"statistically meaningless — one error in {f['log_lines']:,} lines reads as "
            f"{f['error_rate_pct']}%. It is excluded from the risk ranking for that "
            "reason rather than because it looks healthy. " + latency_sentence(f)
        )
    return (
        f"{volume} {severity_sentence(f)}{anomaly_sentence(f)} {latency_sentence(f)} "
        f"Its weighted risk score is {f['risk_score']} out of 100, which places it "
        f"in the {f['health_status'].lower()} band."
    )


def rule_service_action(f):
    if not f["is_scored"]:
        return (
            "Leave it out of alerting until it produces enough volume to score. Revisit "
            "if its line count grows past the floor."
        )
    if f["error_rate_pct"] >= 50:
        return (
            "Read this as a failure feed rather than a failing service: trace the "
            "reported task failures back to the services that raised them, because this "
            "logger is where they surface, not where they originate."
        )
    if f["critical_count"]:
        return f"Investigate the {f['critical_count']:,} critical line(s) directly — at this volume they are individually reviewable."
    if f["warning_rate_pct"] >= 5:
        return (
            "Sample the warnings before alerting on them. A warning rate this "
            "concentrated in one service is usually one repeated condition, not many."
        )
    if not f["latency_samples"]:
        return (
            "No action on latency. To make this service measurable you would need "
            "request-completion logging, which this capture does not include."
        )
    return "No action needed. Keep it in the baseline for comparison."


def plural(n, singular, plural_form=None):
    return singular if n == 1 else (plural_form or singular + "s")


def share(pct):
    """Never render a real, non-zero share as 0.0%, and never trail four decimals."""
    if pct == 0:
        return "0%"
    if pct < 0.01:
        return "<0.01%"
    return f"{round(pct, 2):g}%"


def rule_estate_narrative(e):
    lvl = e["log_level_counts"]
    info = lvl.get("INFO", 0)
    info_share = round(100.0 * info / e["total_log_lines"], 1) if e["total_log_lines"] else 0
    latency_note = (
        f"Only {e['services_with_measured_latency']} of {e['services_total']} services log "
        "HTTP request completions, so latency is measured over a genuine minority of the "
        "estate and the remaining services show as not measured rather than as zero."
    )
    anomaly_note = (
        f" {e['total_anomaly_labels']:,} lines carry anomaly labels from the Silver join."
        if e["total_anomaly_labels"] else
        " No lines carry anomaly labels, which is worth checking against the Silver join."
    )
    n_unscored = e["services_below_volume_floor"]
    floor_note = (
        f" {n_unscored} {plural(n_unscored, 'service')} "
        f"{plural(n_unscored, 'sits', 'sit')} below the volume floor and "
        f"{plural(n_unscored, 'is', 'are')} excluded from ranking."
        if n_unscored else ""
    )
    return (
        f"{e['total_log_lines']:,} log lines were captured across {e['services_total']} "
        f"services, of which {e['total_http_requests']:,} were HTTP requests. "
        f"{info_share}% of all lines are INFO, so severity is rare and concentrated: "
        f"{e['total_warnings']:,} {plural(e['total_warnings'], 'warning')}, "
        f"{e['total_errors']:,} {plural(e['total_errors'], 'error')} and "
        f"{e['total_criticals']:,} {plural(e['total_criticals'], 'critical')} in total. "
        f"The highest weighted risk score is "
        f"{e['highest_risk_score']} at {e['highest_risk_service']}."
        f"{floor_note}{anomaly_note} {latency_note}"
    )


def rule_estate_action(e):
    return (
        f"Start at {e['highest_risk_service']} — it carries the highest weighted score. "
        "Treat the unscored services as unmeasured rather than healthy, and read every "
        "latency figure as covering HTTP-logging services only."
    )


print(rule_estate_narrative(estate_facts))
print()
print("-" * 60)
print(rule_service_narrative(service_facts[0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section D — The model pass
# MAGIC
# MAGIC The endpoint receives the fact dict and a system prompt that forbids it from
# MAGIC introducing any number not in that dict. It returns JSON so the three fields stay
# MAGIC separable. Anything that fails — no endpoint, bad JSON, a number that does not
# MAGIC appear in the facts — falls back to the rule text and is recorded as such.

# COMMAND ----------

SYSTEM_PROMPT = """You are writing short explanatory panels for a log analytics dashboard \
built on OpenStack service logs.

You will be given a JSON object of facts that were already computed in SQL. Write about \
those facts and nothing else.

Hard rules:
- Never state a number that does not appear in the facts. Do not compute new numbers, \
do not estimate, do not round differently.
- If a latency field is null or latency_samples is 0, that means the service logged NO \
HTTP requests. It does NOT mean zero seconds, and it does NOT mean the service is fast, \
idle, or healthy. Say that latency is not measured for it.
- log_lines counts log lines. http_requests counts HTTP requests. They are different \
denominators. Never describe log_lines as requests or traffic.
- anomaly_count is a count of ground-truth labels joined in from a labelled dataset. It \
is NOT a model prediction. Never call it detected, predicted, or flagged by AI.
- A very high error_rate on a low log_lines service is a small-denominator artifact. Say \
so rather than presenting it as the worst service.
- Do not recommend anything the facts cannot support. No speculation about causes you \
cannot see.

Style: plain declarative sentences, no marketing language, no bullet points, no headings. \
Write for an engineer who will check every number you state.

Reply with only a JSON object, no code fence:
{"headline": "one line, under 90 characters, states the finding not the metric",
 "narrative": "3 to 4 sentences",
 "recommended_action": "one sentence, what a responder should do next"}"""


def call_endpoint(facts, scope_hint):
    """Return a dict with headline/narrative/recommended_action, or None on any failure."""
    if not ENDPOINT_NAME:
        return None
    try:
        from mlflow.deployments import get_deploy_client

        client = get_deploy_client("databricks")
        resp = client.predict(
            endpoint=ENDPOINT_NAME,
            inputs={
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"Scope: {scope_hint}\n\nFacts:\n"
                            + json.dumps(facts, indent=2, default=str)
                        ),
                    },
                ],
                "temperature": 0.1,
                "max_tokens": 500,
            },
        )
        text = resp["choices"][0]["message"]["content"].strip()
        if text.startswith("```"):
            text = text.split("```")[1]
            text = text[4:] if text.lower().startswith("json") else text
        out = json.loads(text)
        if not all(k in out and out[k] for k in ("headline", "narrative", "recommended_action")):
            return None
        return {k: str(out[k]).strip() for k in ("headline", "narrative", "recommended_action")}
    except Exception as e:
        print(f"    endpoint call failed ({type(e).__name__}: {e}) — using rule text")
        return None


# COMMAND ----------

# MAGIC %md
# MAGIC ### A cheap guard against invented numbers
# MAGIC
# MAGIC Grounding by prompt alone is a request, not a constraint. This checks every number
# MAGIC the model wrote back against the numbers it was given, and rejects the whole
# MAGIC response if one is unaccounted for. Rejection costs nothing — the rule text is
# MAGIC already sitting there.

# COMMAND ----------

import re

_NUM = re.compile(r"\d[\d,]*\.?\d*")


def allowed_number_strings(facts):
    """Every rendering of every number in the facts that we are willing to see echoed."""
    allowed = set()

    def add(v):
        if isinstance(v, bool) or v is None:
            return
        if isinstance(v, (int, float)):
            f = float(v)
            allowed.update({
                f"{v}", f"{int(f)}" if f.is_integer() else f"{f}",
                f"{int(f):,}" if f.is_integer() else "",
                f"{f:.1f}", f"{f:.2f}", f"{f:.3f}",
            })
        elif isinstance(v, dict):
            for x in v.values():
                add(x)
        elif isinstance(v, (list, tuple)):
            for x in v:
                add(x)

    add(facts)
    allowed.discard("")
    # Percentages and small integers that are structural rather than data.
    allowed.update({"100", "0", "1", "2", "3", "4", "5", "95", "50", str(MIN_LINES_FOR_SCORING)})
    return allowed


def numbers_are_grounded(text, facts):
    allowed = allowed_number_strings(facts)
    for m in _NUM.findall(text or ""):
        s = m.rstrip(".")
        if s in allowed:
            continue
        if s.replace(",", "") in {a.replace(",", "") for a in allowed}:
            continue
        try:
            if f"{float(s.replace(',', '')):.2f}" in {f"{float(a.replace(',', '')):.2f}"
                                                      for a in allowed
                                                      if a.replace(",", "").replace(".", "").isdigit()}:
                continue
        except ValueError:
            pass
        print(f"    rejected: '{s}' is not in the facts")
        return False
    return True


def build_narrative(facts, scope_hint, rule_text, rule_action, rule_headline):
    """Rule text first; the model only replaces it if it passes the grounding check."""
    llm = call_endpoint(facts, scope_hint)
    if llm:
        joined = " ".join([llm["headline"], llm["narrative"], llm["recommended_action"]])
        if numbers_are_grounded(joined, facts):
            return llm["headline"], llm["narrative"], llm["recommended_action"], "llm"
        print("    grounding check failed — using rule text")
    return rule_headline, rule_text, rule_action, "rule"


# COMMAND ----------

# MAGIC %md
# MAGIC ## Section E — Assemble and write `gold.ai_insights`
# MAGIC
# MAGIC `insight_id` is `{scope}:{scope_key}`, so it is stable across runs and the table
# MAGIC overwrites cleanly instead of accumulating duplicates every time the notebook runs.

# COMMAND ----------

rows = []
generated_at = datetime.now()

# ---- Estate ---------------------------------------------------------------------
print("Estate insight")
h, n, a, mode = build_narrative(
    estate_facts,
    "the whole estate",
    rule_estate_narrative(estate_facts),
    rule_estate_action(estate_facts),
    (
        f"{estate_facts['total_log_lines']:,} lines, "
        f"{estate_facts['services_total']} services — highest risk is "
        f"{estate_facts['highest_risk_service']}"
    ),
)
rows.append({
    "insight_id": "estate:all",
    "scope": "estate",
    "scope_key": "all",
    "headline": h,
    "narrative": n,
    "recommended_action": a,
    "severity": (
        "critical" if estate_facts["total_criticals"] else
        "degraded" if estate_facts["total_errors"] else "healthy"
    ),
    "evidence": json.dumps(estate_facts, default=str),
    "source_views": "vw_dash_service_overview,vw_dash_log_level,vw_dash_http_status,vw_dash_hourly_trend,vw_dash_topic",
    "generation_mode": mode,
    "model_endpoint": ENDPOINT_NAME or "none",
    "generated_at": generated_at,
})
print(f"  -> {mode}")

# ---- One per service ------------------------------------------------------------
for f in service_facts:
    print(f"Service insight: {f['service']}")
    rule_headline = (
        f"{f['service']} — {f['health_status']}, risk {f['risk_score']}"
        if f["is_scored"]
        else f"{f['service']} — only {f['log_lines']:,} lines, below the scoring floor"
    )
    h, n, a, mode = build_narrative(
        f, f"the service {f['service']}",
        rule_service_narrative(f), rule_service_action(f), rule_headline,
    )
    rows.append({
        "insight_id": f"service:{f['service']}",
        "scope": "service",
        "scope_key": f["service"],
        "headline": h,
        "narrative": n,
        "recommended_action": a,
        "severity": severity_for(f),
        "evidence": json.dumps(f, default=str),
        "source_views": "vw_dash_service_overview,vw_dash_service_latency",
        "generation_mode": mode,
        "model_endpoint": ENDPOINT_NAME or "none",
        "generated_at": generated_at,
    })
    print(f"  -> {mode}")

print(f"\n{len(rows)} insight rows built")
print(f"  llm  : {sum(1 for r in rows if r['generation_mode'] == 'llm')}")
print(f"  rule : {sum(1 for r in rows if r['generation_mode'] == 'rule')}")

# COMMAND ----------

from pyspark.sql.types import (
    StringType, StructField, StructType, TimestampType,
)

INSIGHTS_SCHEMA = StructType([
    StructField("insight_id", StringType(), False),
    StructField("scope", StringType(), False),
    StructField("scope_key", StringType(), False),
    StructField("headline", StringType(), True),
    StructField("narrative", StringType(), True),
    StructField("recommended_action", StringType(), True),
    StructField("severity", StringType(), True),
    StructField("evidence", StringType(), True),
    StructField("source_views", StringType(), True),
    StructField("generation_mode", StringType(), True),
    StructField("model_endpoint", StringType(), True),
    StructField("generated_at", TimestampType(), True),
])

insights_sdf = spark.createDataFrame(rows, schema=INSIGHTS_SCHEMA)

(
    insights_sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(INSIGHTS_TABLE)
)

print(f"Wrote {insights_sdf.count()} rows to {INSIGHTS_TABLE}")
display(spark.table(INSIGHTS_TABLE).select("insight_id", "severity", "generation_mode", "headline"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section F — `gold.ml_model_cards`
# MAGIC
# MAGIC The descriptive fields are hand-authored because they live in the notebooks and in
# MAGIC your head, not in a table. The metrics are **read from MLflow at run time**, because
# MAGIC neither training notebook saved its outputs — so a hardcoded F1 here would be a
# MAGIC number I made up, and a made-up metric is worse than a missing one.
# MAGIC
# MAGIC `limitations` is the honest column, and it is why this table exists.

# COMMAND ----------


def metrics_from_mlflow(run_name_prefix, experiment_name=MLFLOW_EXPERIMENT):
    """Best-effort metric lookup. Returns a JSON string, never raises."""
    try:
        import mlflow

        exp = mlflow.get_experiment_by_name(experiment_name)
        if exp is None:
            return json.dumps({"status": "experiment_not_found", "experiment": experiment_name})
        runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            filter_string=f"tags.mlflow.runName LIKE '{run_name_prefix}%'",
            order_by=["start_time DESC"],
            max_results=10,
        )
        if runs is None or runs.empty:
            return json.dumps({"status": "no_matching_runs", "run_name_prefix": run_name_prefix})
        out = {}
        for _, r in runs.iterrows():
            name = r.get("tags.mlflow.runName", "unnamed")
            out[name] = {
                c[len("metrics."):]: round(float(r[c]), 4)
                for c in runs.columns
                if c.startswith("metrics.") and pd.notna(r[c])
            }
        return json.dumps({"status": "read_from_mlflow", "runs": out})
    except Exception as e:
        return json.dumps({"status": "lookup_failed", "error": f"{type(e).__name__}: {e}"})


anomaly_metrics = metrics_from_mlflow("Binary_")
print("Anomaly model metrics:", anomaly_metrics[:400])

# COMMAND ----------

model_cards = [
    {
        "model_name": "Binary anomaly classifier",
        "notebook": "07_Binary_Anomaly_Model_Training",
        "objective": (
            "Classify an individual OpenStack log event as anomalous or normal."
        ),
        "target_definition": (
            "is_anomaly, built in 03_Silver_Transformation by left-joining a labelled "
            "anomaly instance-ID list onto silver_logs on instance_id and filling "
            "non-matches with 0. Every line belonging to a labelled instance is therefore "
            "labelled anomalous, which makes the label instance-level rather than "
            "line-level — a line can be labelled anomalous while being an ordinary INFO "
            "message from a bad instance."
        ),
        "feature_summary": (
            "13 assembled inputs: one-hot service, log_file, log_level, http_method, "
            "response_category; numeric status_code, response_time, event_hour, event_day, "
            "event_month, event_weekday, is_weekend, instance_exists. Identifier and Kafka "
            "metadata columns dropped."
        ),
        "algorithm": (
            "Spark ML pipeline (StringIndexer, OneHotEncoder, VectorAssembler) into "
            "LogisticRegression, DecisionTreeClassifier and RandomForestClassifier. "
            "80/20 split, seed 42. Majority class undersampled to 5:1 on the training "
            "set only; test set left at natural distribution. Random Forest saved as the "
            "chosen model."
        ),
        "metrics_json": anomaly_metrics,
        "limitations": (
            "instance_exists is in the feature set and the label is derived from an "
            "instance-ID join, so the two are related by construction and the feature "
            "should be reviewed for leakage. The label is instance-level, so per-line "
            "precision is not directly interpretable. The saved artifact is the bare "
            "classifier rather than the fitted pipeline, so the indexer, encoder and "
            "assembler stages are not persisted with it and the model cannot be scored on "
            "raw input without refitting them. Metrics were computed against an "
            "undersampled training distribution and a natural test distribution."
        ),
        "status": "production_candidate",
    },
    {
        "model_name": "Service health classifier",
        "notebook": "08_Service_Health_Prediction",
        "objective": (
            "Predict whether a service is Healthy or Degrading from a single log event."
        ),
        "target_definition": (
            "service_health, a deterministic rule over log_level, response_time, "
            "severity_score, status_code and is_anomaly: Critical when log_level is "
            "CRITICAL, or ERROR with response_time >= 0.6, or severity_score >= 4, or "
            "status_code >= 500; Degrading when log_level is ERROR or WARNING, or "
            "is_anomaly = 1, or response_time > 0.35, or status_code >= 400; Healthy "
            "otherwise. The Critical class is then dropped, leaving a two-class problem."
        ),
        "feature_summary": (
            "15 assembled inputs: numeric status_code, response_time, event_hour, "
            "event_month, event_year, day_of_week, hour, month, is_weekend, severity_score; "
            "one-hot service, log_file, log_level, http_method, response_category."
        ),
        "algorithm": (
            "LogisticRegression, DecisionTreeClassifier and RandomForestClassifier, each "
            "under 3-fold CrossValidator on a parameter grid, selected on weighted F1. "
            "80/20 split, seed 42. Inverse-frequency class weights on the training set."
        ),
        "metrics_json": json.dumps({
            "status": "not_logged",
            "reason": (
                "Notebook 08 performs no MLflow logging and saved no cell outputs, so no "
                "metric value for this model exists anywhere in the project. Any number "
                "quoted here would be fabricated."
            ),
        }),
        "limitations": (
            "TARGET LEAKAGE, confirmed. The label is a closed-form Boolean function of "
            "log_level, response_time, severity_score, status_code and is_anomaly, and "
            "four of those five — log_level, response_time, status_code, severity_score — "
            "are in the feature vector. severity_score is itself a deterministic mapping "
            "of log_level, and response_category is a deterministic bucketing of "
            "response_time, so the leaking columns appear twice over. The model is "
            "recovering an if/else statement it was handed, and any F1 it reports measures "
            "the pipeline's ability to memorise a rule rather than to predict anything. "
            "Separately: the feature projection built earlier in the notebook is discarded "
            "by a later reassignment, class weights are hardcoded for labels 0 and 1 while "
            "the indexer was fit over three classes, and a variable-name typo raises "
            "NameError if Random Forest wins selection. Kept in the project as a worked "
            "example of how leakage is diagnosed, not as a result."
        ),
        "status": "demo_only",
    },
    {
        "model_name": "Early warning predictor",
        "notebook": "09_Early_Warning_Prediciton",
        "objective": (
            "Predict a service degradation some horizon ahead of it occurring."
        ),
        "target_definition": "Not finalised. No forward-looking label was constructed.",
        "feature_summary": "Not finalised.",
        "algorithm": "Not trained.",
        "metrics_json": json.dumps({"status": "not_trained"}),
        "limitations": (
            "Paused deliberately. A genuine early-warning target needs a forward-looking "
            "window label — degradation observed in the next N minutes, built from an "
            "as-of timestamp — and the capture window here is roughly three days, which is "
            "too short to hold out a credible future period. Shipping it against a "
            "same-instant label would reproduce the leakage already present in notebook 08."
        ),
        "status": "paused",
    },
]

cards_sdf = spark.createDataFrame(pd.DataFrame(model_cards))

(
    cards_sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(MODEL_CARDS_TABLE)
)

print(f"Wrote {cards_sdf.count()} model cards to {MODEL_CARDS_TABLE}")
display(spark.table(MODEL_CARDS_TABLE).select("model_name", "notebook", "status"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model insights
# MAGIC
# MAGIC One `ai_insights` row per model, so the UI's Models tab reads from the same table
# MAGIC as every other panel rather than special-casing model text.

# COMMAND ----------

model_rows = []
for card in model_cards:
    sev = {"production_candidate": "healthy", "demo_only": "degraded", "paused": "degraded"}[card["status"]]
    model_rows.append({
        "insight_id": f"model:{card['model_name']}",
        "scope": "model",
        "scope_key": card["model_name"],
        "headline": f"{card['model_name']} — {card['status'].replace('_', ' ')}",
        "narrative": card["objective"] + " " + card["limitations"],
        "recommended_action": (
            "Report metrics from MLflow, not from this card."
            if card["status"] == "production_candidate"
            else "Do not quote this model's performance as a result."
        ),
        "severity": sev,
        "evidence": json.dumps({k: card[k] for k in ("notebook", "algorithm", "status", "metrics_json")}),
        "source_views": "ml_model_cards",
        "generation_mode": "rule",
        "model_endpoint": "none",
        "generated_at": generated_at,
    })

(
    spark.createDataFrame(model_rows, schema=INSIGHTS_SCHEMA).write
    .format("delta").mode("append").saveAsTable(INSIGHTS_TABLE)
)
print(f"Appended {len(model_rows)} model insight rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section G — Verification
# MAGIC
# MAGIC Every row must have all three text fields, a parseable `evidence` blob, and a
# MAGIC recognised severity. `bad_rows` must be 0.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                          AS row_count,
# MAGIC   COUNT(DISTINCT insight_id)                                        AS distinct_ids,
# MAGIC   SUM(CASE WHEN headline           IS NULL OR headline = ''           THEN 1 ELSE 0 END)
# MAGIC   + SUM(CASE WHEN narrative        IS NULL OR narrative = ''          THEN 1 ELSE 0 END)
# MAGIC   + SUM(CASE WHEN recommended_action IS NULL OR recommended_action = '' THEN 1 ELSE 0 END)
# MAGIC   + SUM(CASE WHEN severity NOT IN ('healthy','degraded','critical')   THEN 1 ELSE 0 END)
# MAGIC   + SUM(CASE WHEN get_json_object(evidence, '$') IS NULL              THEN 1 ELSE 0 END)
# MAGIC                                                                     AS bad_rows,
# MAGIC   SUM(CASE WHEN generation_mode = 'llm'  THEN 1 ELSE 0 END)          AS llm_rows,
# MAGIC   SUM(CASE WHEN generation_mode = 'rule' THEN 1 ELSE 0 END)          AS rule_rows
# MAGIC FROM `log-analytics`.gold.ai_insights;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT scope, scope_key, severity, generation_mode, headline
# MAGIC FROM `log-analytics`.gold.ai_insights
# MAGIC ORDER BY
# MAGIC   CASE scope WHEN 'estate' THEN 0 WHEN 'service' THEN 1 ELSE 2 END,
# MAGIC   CASE severity WHEN 'critical' THEN 0 WHEN 'degraded' THEN 1 ELSE 2 END,
# MAGIC   scope_key;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done
# MAGIC
# MAGIC `gold.ai_insights` and `gold.ml_model_cards` are populated and verified. The
# MAGIC Streamlit app in `app/` reads both, alongside the same eight `vw_dash_*` views the
# MAGIC Superset dashboard reads, so the two front ends cannot disagree.
# MAGIC
# MAGIC If `llm_rows` is 0, the run was rule-based. That is a complete, shippable run — set
# MAGIC `ENDPOINT_NAME` from the list printed near the top and re-run to upgrade the prose.

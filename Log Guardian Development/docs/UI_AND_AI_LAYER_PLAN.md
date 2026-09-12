# Log Guardian — UI and AI layer

Architecture, data contract and a three-day schedule for the last two stages of the
project: the AI intelligence layer and the viewer-facing UI.

Decided on 2026-08-25:

| Decision | Choice |
|---|---|
| UI framework | Streamlit |
| How charts appear | Native Plotly charts reading the same `vw_dash_*` Gold views Superset reads |
| Where narratives are generated | Precomputed into `gold.ai_insights` by a notebook, **plus** a live grounded Q&A box in the app |
| Model provider | Databricks Foundation Model serving endpoint |

---

## 1. Why this shape

The dashboard and the UI must never disagree. If Superset computed its numbers one way
and the Streamlit app computed them another, the project would have two truths and a
reviewer would find the seam. So both read the **same eight views**, and neither one does
any arithmetic of its own:

```
                    `log-analytics`.silver.silver_logs
                                  │
                     8 × vw_dash_* views  (dashboard_views.sql)
                    ┌─────────────┴──────────────┐
                    │                            │
              Apache Superset              Streamlit UI
              (BI deliverable)             (viewer-facing app)
                                                 │
                                          gold.ai_insights
                                          gold.ml_model_cards
                                                 ▲
                                     notebook 09_AI_Insights_Layer
                                                 │
                                  Databricks Foundation Model endpoint
```

Narratives are **precomputed in the pipeline, not in the frontend.** Three reasons this is
the better engineering choice and not just the faster one:

1. The AI layer becomes a stage of the data pipeline with a table as its output, which is
   what makes it a data engineering deliverable rather than a frontend feature.
2. Page loads are instant and cost nothing, because the app reads a table.
3. The text in your screenshots is reproducible. A live call regenerates different prose
   every refresh, which is a poor thing to demo and an impossible thing to screenshot twice.

The Q&A box is the one live call, because a question can't be precomputed.

**Superset is not embedded.** Embedding a Superset dashboard in another page needs the
embedded SDK, the `EMBEDDED_SUPERSET` feature flag and a backend that mints guest tokens —
most of a day, and a day you don't have. Reading the same views directly produces the same
charts with none of that, and Superset survives as an independent deliverable you can show
on its own.

---

## 2. Grounding: how the AI layer is stopped from inventing numbers

This is the part that decides whether the AI layer is a credit or a liability. The rule:

> The model never sees the database. It sees a dictionary of numbers that were already
> computed in SQL, and its only job is to turn those numbers into sentences.

Notebook 09 builds one `facts` dict per service from `vw_dash_service_overview` and
`vw_dash_service_latency`, then passes it to the model with instructions to use nothing
else. Every number in the resulting narrative is therefore traceable to a view column.
The dict is stored next to the narrative in `ai_insights.evidence`, so any claim on screen
can be checked against the numbers it was generated from.

Two project-specific rules are written into the system prompt, because a general-purpose
model gets both of them wrong:

- A null latency means **the service logged no HTTP requests**. It does not mean zero
  seconds, and it does not mean the service is fast or idle. The service has log lines.
- `log_lines` counts log lines, not requests. `http_requests` counts requests.

**Every narrative also has a deterministic rule-based version**, composed from the same
facts with string formatting and no model at all. It runs first and always succeeds; the
model output replaces it only if the endpoint call returns usable text. So:

- If Foundation Model APIs aren't enabled in your workspace region, the app still works
  and every panel still has an explanation.
- `generation_mode` on each row records which path produced it, so the distinction is
  never hidden.

That fallback is what keeps a live demo from failing in front of an audience.

---

## 3. Data contract

Two new Gold tables. Both are small — tens of rows — and written by notebook 09.

### `gold.ai_insights`

| column | type | meaning |
|---|---|---|
| `insight_id` | string | `{scope}:{scope_key}` — stable, so re-runs overwrite rather than accumulate |
| `scope` | string | `estate`, `service`, or `model` |
| `scope_key` | string | service name, model name, or `all` |
| `headline` | string | one line, shown in the mono eyebrow |
| `narrative` | string | 3–4 sentences of prose, the panel body |
| `recommended_action` | string | one sentence, what a responder should do next |
| `severity` | string | `healthy` / `degraded` / `critical` — drives the panel's left rule colour |
| `evidence` | string | JSON of the exact facts the narrative was generated from |
| `source_views` | string | comma-separated view names, shown as lineage in the UI |
| `generation_mode` | string | `llm` or `rule` |
| `model_endpoint` | string | which serving endpoint produced it, or `none` |
| `generated_at` | timestamp | run time |

### `gold.ml_model_cards`

One row per model, hand-authored in the notebook because these facts live in your head and
in MLflow rather than in a table.

| column | meaning |
|---|---|
| `model_name`, `notebook`, `objective` | what it is and where it lives |
| `target_definition` | how the label was built |
| `feature_summary` | what went in |
| `algorithm`, `metrics_json` | how it was trained and how it scored |
| `limitations` | **the honest column** |
| `status` | `production_candidate` / `demo_only` / `paused` |

`limitations` is where the target leakage in notebook 08 gets written down. The
service-health label is a deterministic rule over `log_level`, `response_time`,
`severity_score`, `status_code` and `is_anomaly`, and those same columns are in the feature
set — so the model is recovering an if/else statement and its F1 measures nothing. Naming
that in the artifact is worth more than a retrained model you don't have time to validate,
and a reviewer who spots it themselves after reading a confident claim will trust nothing
else on the page. `status` for that model is `demo_only`.

---

## 4. The app

Four tabs. Each chart is followed — not flanked — by its narrative, because the reading
order should be see, then understand.

**Services** — KPI row, risk leaderboard, log level mix, volume trend. The estate-level
narrative sits under the leaderboard.

**Service detail** — pick a service; its facts, latency percentiles and status mix, with
that service's narrative and recommended action. Services with no measured latency show an
explicit "no HTTP requests logged" panel instead of an empty chart, which turns the
project's central data quality finding into a visible feature.

**Models** — the two model cards, with metrics and the limitations column shown rather
than buried.

**Ask** — a text box. The question plus a compact table of every service's facts goes to
the endpoint with instructions to answer only from that table and to say so when the answer
isn't there.

Design notes, so the UI doesn't read as a default Streamlit app: colour encodes `log_level`
on an escalation ramp (teal → ochre → brick → wine) rather than decorating; IBM Plex in
three widths, mono for every number so figures align in columns; each panel carries a
monospaced eyebrow naming the view the numbers came from, which puts lineage on screen.

---

## 5. Three-day schedule

**Day 1 — Tuesday 26 Aug. Data and dashboard.**
Run `dashboard_views.sql` top to bottom in a Databricks SQL editor, then run the
verification query at the bottom and confirm every `bad_rows` is `0`. Run the distinct-dates
check to decide daily vs hourly trend charts. In Superset, create one dataset per view via
SQL Lab → `SELECT * FROM <view>` → Save as dataset, then build the charts and assemble the
dashboard. Import `09_AI_Insights_Layer.py` into Databricks, set `ENDPOINT_NAME` from the
endpoint list the notebook prints, run it, confirm both tables are populated.

**Day 2 — Wednesday 27 Aug. The app.**
Create a SQL warehouse token, fill in `app/.env`, `pip install -r app/requirements.txt`,
`streamlit run app/app.py`. Work through the four tabs against real data. This is where
you'll find the small mismatches — a service name that's longer than the chip, a trend with
two data points — and they're all quick.

**Day 3 — Thursday 28 Aug. Deployment and write-up.**
`docker compose up` the app next to Superset, or push to Streamlit Community Cloud. Write
the README that's currently empty: architecture diagram, the NULL finding, the leakage
disclosure, screenshots of Superset and the app. Apply whichever of the six code fixes you
have time for — `risk_score` COALESCE and the `response_category` "Not Measured" branch are
the two that change numbers, so do those first.

**If you lose a day, cut in this order:** the Ask tab (the precomputed narratives already
carry the AI story), then the hourly charts, then the model cards tab. Do not cut the
verification query or the README.

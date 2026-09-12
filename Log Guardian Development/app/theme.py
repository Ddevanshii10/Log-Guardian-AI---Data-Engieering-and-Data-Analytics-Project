"""Design system for the Log Guardian app.

Direction — "the engineering log sheet". The subject is service telemetry read by
someone on call, so the page borrows from a control-room printout rather than a
terminal: a cool paper ground, hairline rules, and every figure set in mono so
columns of numbers align optically.

The one deliberate risk: narrative prose is set in a **serif** while every measured
figure is set in **mono**. That split is the signature. Mono means "this was
measured"; serif means "this was explained". A reader can tell at a glance which
parts of the page are data and which parts are interpretation, which is exactly the
distinction that matters in a project where some of the text is machine-generated.

The severity ramp is deliberately identical to the one used in the Superset
dashboard's `label_colors`, so WARNING is the same ochre in both front ends. The two
deliverables differ in ground (Superset is dark, this is light) but never in
encoding.
"""

# --- Palette ----------------------------------------------------------------------
# Cool paper rather than warm cream, and an ink-blue accent rather than terracotta.
PAPER      = "#F2F4F6"   # page ground
PANEL      = "#FFFFFF"   # panel fill
RULE       = "#D6DCE2"   # hairlines and borders
INK        = "#14181F"   # primary text
INK_SOFT   = "#5A646E"   # secondary text, eyebrows
INK_FAINT  = "#8A939C"   # tertiary, disabled

# Severity ramp — shared with Superset. Escalation, not decoration.
INFO       = "#2F6F8F"   # deep blue
WARNING    = "#C08A17"   # ochre
ERROR      = "#C0453B"   # brick
CRITICAL   = "#7B2D42"   # wine
NEUTRAL    = "#8A939C"   # not measured / below floor

SEVERITY_COLOR = {
    "healthy":  INFO,
    "degraded": WARNING,
    "critical": CRITICAL,
    "unknown":  NEUTRAL,
}

LEVEL_COLOR = {
    "INFO": INFO,
    "WARNING": WARNING,
    "ERROR": ERROR,
    "CRITICAL": CRITICAL,
}

HEALTH_COLOR = {
    "Healthy": INFO,
    "Degraded": WARNING,
    "Degrading": WARNING,
    "Critical": CRITICAL,
    "Low volume": NEUTRAL,
}

SANS  = "'IBM Plex Sans', system-ui, -apple-system, sans-serif"
MONO  = "'IBM Plex Mono', ui-monospace, 'SF Mono', Consolas, monospace"
SERIF = "'IBM Plex Serif', Georgia, serif"


CSS = f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Serif:ital,wght@0,400;0,500;1,400&display=swap');

.stApp {{ background: {PAPER}; }}
html, body, [class*="css"] {{ font-family: {SANS}; color: {INK}; }}

/* Kill Streamlit's default top padding — the masthead is the first thing, not whitespace */
.block-container {{ padding-top: 1.6rem; padding-bottom: 3rem; max-width: 1400px; }}
#MainMenu, footer, header {{ visibility: hidden; }}

/* ---- Masthead ---------------------------------------------------------------- */
.lg-masthead {{
  border-bottom: 2px solid {INK};
  padding-bottom: .7rem; margin-bottom: 1.4rem;
  display: flex; align-items: baseline; justify-content: space-between; gap: 1rem;
  flex-wrap: wrap;
}}
.lg-wordmark {{
  font-family: {SERIF}; font-size: 1.85rem; font-weight: 500;
  letter-spacing: -.01em; color: {INK}; line-height: 1;
}}
.lg-wordmark em {{ font-style: italic; color: {INK_SOFT}; font-weight: 400; }}
.lg-standfirst {{
  font-family: {MONO}; font-size: .7rem; color: {INK_SOFT};
  text-transform: uppercase; letter-spacing: .09em; text-align: right;
}}

/* ---- The lineage eyebrow: the signature element ----------------------------- */
/* Every panel names the exact Gold view its numbers came from. Provenance on
   screen is the thing that makes this a data engineering artifact. */
.lg-eyebrow {{
  font-family: {MONO}; font-size: .655rem; color: {INK_SOFT};
  text-transform: uppercase; letter-spacing: .1em;
  display: flex; align-items: center; gap: .5rem; margin-bottom: .45rem;
}}
.lg-eyebrow::before {{
  content: ""; width: 14px; height: 1px; background: {INK_SOFT}; flex: none;
}}

/* ---- Figures ---------------------------------------------------------------- */
.lg-kpi {{
  background: {PANEL}; border: 1px solid {RULE}; border-top: 2px solid {INK};
  padding: .85rem 1rem 1rem; height: 100%;
}}
.lg-kpi-label {{
  font-family: {MONO}; font-size: .655rem; color: {INK_SOFT};
  text-transform: uppercase; letter-spacing: .1em; margin-bottom: .35rem;
}}
.lg-kpi-value {{
  font-family: {MONO}; font-size: 2.05rem; font-weight: 500; line-height: 1;
  color: {INK}; font-variant-numeric: tabular-nums; letter-spacing: -.02em;
}}
.lg-kpi-note {{
  font-family: {SANS}; font-size: .74rem; color: {INK_SOFT};
  margin-top: .4rem; line-height: 1.35;
}}

/* ---- Narrative panel: serif prose, severity as a left rule ------------------ */
.lg-narrative {{
  background: {PANEL}; border: 1px solid {RULE};
  border-left: 3px solid var(--sev, {INFO});
  padding: 1rem 1.15rem 1.05rem; margin: .1rem 0 1rem;
}}
.lg-headline {{
  font-family: {SANS}; font-size: .96rem; font-weight: 600;
  color: {INK}; margin-bottom: .5rem; line-height: 1.3;
}}
.lg-prose {{
  font-family: {SERIF}; font-size: .945rem; line-height: 1.62; color: {INK};
}}
.lg-action {{
  font-family: {SANS}; font-size: .845rem; color: {INK};
  margin-top: .8rem; padding-top: .7rem; border-top: 1px solid {RULE};
}}
.lg-action b {{
  font-family: {MONO}; font-size: .655rem; font-weight: 600; color: {INK_SOFT};
  text-transform: uppercase; letter-spacing: .1em; display: block; margin-bottom: .25rem;
}}

/* ---- Chips ------------------------------------------------------------------ */
.lg-chip {{
  font-family: {MONO}; font-size: .615rem; font-weight: 500;
  text-transform: uppercase; letter-spacing: .08em;
  padding: .16rem .44rem; border: 1px solid currentColor; white-space: nowrap;
}}
.lg-chip-llm  {{ color: {INFO}; }}
.lg-chip-rule {{ color: {INK_SOFT}; }}

/* ---- Plain panel ------------------------------------------------------------ */
.lg-panel {{
  background: {PANEL}; border: 1px solid {RULE}; padding: .9rem 1rem 1rem;
  margin-bottom: 1rem;
}}
.lg-note {{
  font-family: {SANS}; font-size: .82rem; color: {INK_SOFT}; line-height: 1.5;
}}
.lg-section {{
  font-family: {MONO}; font-size: .7rem; color: {INK};
  text-transform: uppercase; letter-spacing: .12em; font-weight: 600;
  border-bottom: 1px solid {INK}; padding-bottom: .35rem;
  margin: 1.6rem 0 1rem;
}}

/* ---- Streamlit component overrides ----------------------------------------- */
.stTabs [data-baseweb="tab-list"] {{ gap: 0; border-bottom: 1px solid {RULE}; }}
.stTabs [data-baseweb="tab"] {{
  font-family: {MONO}; font-size: .715rem; text-transform: uppercase;
  letter-spacing: .09em; padding: .55rem 1.05rem; background: transparent;
}}
.stTabs [aria-selected="true"] {{
  color: {INK} !important; border-bottom: 2px solid {INK} !important;
}}
[data-testid="stMetricValue"], .stDataFrame {{ font-family: {MONO}; }}
.stDataFrame {{ font-variant-numeric: tabular-nums; font-size: .8rem; }}
div[data-testid="stExpander"] details {{ border: 1px solid {RULE}; background: {PANEL}; }}
.stTextInput input, .stSelectbox div[data-baseweb="select"] {{ font-family: {MONO}; font-size: .85rem; }}
.stButton button {{
  font-family: {MONO}; font-size: .72rem; text-transform: uppercase;
  letter-spacing: .08em; border-radius: 0; border: 1px solid {INK};
  background: {INK}; color: {PAPER};
}}
.stButton button:hover {{ background: {INFO}; border-color: {INFO}; color: #fff; }}
</style>
"""


# --- Plotly ------------------------------------------------------------------------
def plotly_layout(height=300, showlegend=False, **kw):
    """Shared layout so every chart in the app is visually one family."""
    base = dict(
        height=height,
        showlegend=showlegend,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="IBM Plex Mono, monospace", size=11, color=INK_SOFT),
        margin=dict(l=8, r=8, t=8, b=8),
        xaxis=dict(gridcolor=RULE, zerolinecolor=RULE, linecolor=RULE,
                   showgrid=False, ticks="outside", tickcolor=RULE),
        yaxis=dict(gridcolor=RULE, zerolinecolor=RULE, linecolor=RULE,
                   showgrid=True, ticks="outside", tickcolor=RULE),
        hoverlabel=dict(bgcolor=PANEL, bordercolor=INK,
                        font=dict(family="IBM Plex Mono, monospace", size=11, color=INK)),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0,
                    font=dict(size=10)),
    )
    base.update(kw)
    return base


# --- HTML fragments ---------------------------------------------------------------
def eyebrow(*views):
    """The lineage line. Names the exact view(s) the panel's numbers came from."""
    return f'<div class="lg-eyebrow">{" · ".join(views)}</div>'


def kpi(label, value, note=""):
    return (
        f'<div class="lg-kpi"><div class="lg-kpi-label">{label}</div>'
        f'<div class="lg-kpi-value">{value}</div>'
        f'<div class="lg-kpi-note">{note}</div></div>'
    )


def narrative_panel(headline, prose, action, severity="healthy",
                    mode="rule", views=()):
    color = SEVERITY_COLOR.get(severity, INFO)
    chip_cls = "lg-chip-llm" if mode == "llm" else "lg-chip-rule"
    chip_text = "model written" if mode == "llm" else "rule written"
    lineage = (
        f'<span style="font-family:{MONO};font-size:.655rem;color:{INK_SOFT};'
        f'text-transform:uppercase;letter-spacing:.1em;">{" · ".join(views)}</span>'
        if views else ""
    )
    return (
        f'<div class="lg-narrative" style="--sev:{color};">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;'
        f'gap:1rem;margin-bottom:.5rem;">{lineage}'
        f'<span class="lg-chip {chip_cls}">{chip_text}</span></div>'
        f'<div class="lg-headline">{headline}</div>'
        f'<div class="lg-prose">{prose}</div>'
        f'<div class="lg-action"><b>Recommended next step</b>{action}</div>'
        f'</div>'
    )

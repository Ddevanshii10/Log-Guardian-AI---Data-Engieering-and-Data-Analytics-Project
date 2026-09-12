/**
 * Log Guardian — Dynamic Dashboard & AI Intelligence Controller
 * Renders exact Superset-matching analytics charts and coordinates Gemini AI "Why?" explanations.
 *
 * Features:
 *  - Single-request dashboard load via /api/dashboard/full
 *  - Auto-refresh (60s default) with live countdown display
 *  - Manual refresh button with loading state
 *  - Graceful error banner on backend connection failure
 *  - Live Anomaly Feed with animated rows
 *  - 9 Chart.js charts matching Superset Gold view aggregations
 *  - 4-Part Grounded AI "Why?" modal via Gemini 2.5 Flash
 */

// ─── Application State ────────────────────────────────────────────────────────
let dashboardData = null;
let modelCardsData = [];
let chartInstances = {};

// ─── Auto-Refresh State ───────────────────────────────────────────────────────
const AUTO_REFRESH_INTERVAL = 60; // seconds
let refreshCountdown = AUTO_REFRESH_INTERVAL;
let refreshTimer = null;
let countdownTimer = null;
let isRefreshing = false;

// ─── DOM References ───────────────────────────────────────────────────────────
const errorBanner = document.getElementById("error-banner");
const errorBannerText = document.getElementById("error-banner-text");
const lastRefreshChip = document.getElementById("last-refresh-chip");
const btnManualRefresh = document.getElementById("btn-manual-refresh");
const toggleAutoRefresh = document.getElementById("toggle-auto-refresh");
const refreshCountdownEl = document.getElementById("refresh-countdown");

// ─── Chart.js Global Theme ────────────────────────────────────────────────────
if (typeof Chart !== "undefined") {
  Chart.defaults.color = "#94A3B8";
  Chart.defaults.font.family = "'IBM Plex Mono', monospace";
  Chart.defaults.font.size = 11;
  Chart.defaults.plugins.legend.display = false;
  Chart.defaults.plugins.tooltip.backgroundColor = "#080B12";
  Chart.defaults.plugins.tooltip.borderColor = "#1E2636";
  Chart.defaults.plugins.tooltip.borderWidth = 1;
  Chart.defaults.plugins.tooltip.titleColor = "#FFFFFF";
  Chart.defaults.plugins.tooltip.bodyColor = "#38BDF8";
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  initNavigation();
  initModalHandlers();
  initAIExplorer();
  initRefreshControls();
  loadDashboardData(/* initial = */ true);
});

// ─── Tab Navigation ───────────────────────────────────────────────────────────
function initNavigation() {
  const navItems = document.querySelectorAll(".nav-item");
  navItems.forEach(item => {
    item.addEventListener("click", () => {
      navItems.forEach(n => n.classList.remove("active"));
      document.querySelectorAll(".tab-page").forEach(p => p.classList.remove("active"));
      item.classList.add("active");
      const target = document.getElementById(item.getAttribute("data-tab"));
      if (target) target.classList.add("active");
    });
  });
}

// ─── Refresh Controls Setup ───────────────────────────────────────────────────
function initRefreshControls() {
  if (btnManualRefresh) {
    btnManualRefresh.addEventListener("click", () => {
      if (!isRefreshing) {
        resetCountdown();
        loadDashboardData(false);
      }
    });
  }

  if (toggleAutoRefresh) {
    toggleAutoRefresh.addEventListener("change", () => {
      if (toggleAutoRefresh.checked) {
        startAutoRefresh();
      } else {
        stopAutoRefresh();
        if (refreshCountdownEl) refreshCountdownEl.textContent = "off";
      }
    });
  }
}

function startAutoRefresh() {
  stopAutoRefresh(); // clear any existing timers
  refreshCountdown = AUTO_REFRESH_INTERVAL;
  updateCountdownDisplay();

  // Countdown tick every second
  countdownTimer = setInterval(() => {
    if (!isRefreshing) {
      refreshCountdown--;
      updateCountdownDisplay();
      if (refreshCountdown <= 0) {
        refreshCountdown = AUTO_REFRESH_INTERVAL;
        loadDashboardData(false);
      }
    }
  }, 1000);
}

function stopAutoRefresh() {
  if (countdownTimer) { clearInterval(countdownTimer); countdownTimer = null; }
  if (refreshTimer) { clearTimeout(refreshTimer); refreshTimer = null; }
}

function resetCountdown() {
  refreshCountdown = AUTO_REFRESH_INTERVAL;
  updateCountdownDisplay();
}

function updateCountdownDisplay() {
  if (refreshCountdownEl) {
    refreshCountdownEl.textContent = refreshCountdown > 0 ? `${refreshCountdown}s` : "now";
  }
}

function setRefreshingState(active) {
  isRefreshing = active;
  if (btnManualRefresh) {
    if (active) {
      btnManualRefresh.disabled = true;
      btnManualRefresh.textContent = "⟳ Loading…";
    } else {
      btnManualRefresh.disabled = false;
      btnManualRefresh.textContent = "↻ Refresh";
    }
  }
}

function updateLastRefreshStamp() {
  if (lastRefreshChip) {
    const now = new Date();
    const hh = String(now.getHours()).padStart(2, "0");
    const mm = String(now.getMinutes()).padStart(2, "0");
    const ss = String(now.getSeconds()).padStart(2, "0");
    lastRefreshChip.textContent = `🕐 ${hh}:${mm}:${ss}`;
  }
}

// ─── Error Banner Helpers ─────────────────────────────────────────────────────
function showErrorBanner(msg) {
  if (errorBanner && errorBannerText) {
    errorBannerText.textContent = msg || "Data connection lost — showing last known snapshot.";
    errorBanner.classList.remove("hidden");
  }
}

function hideErrorBanner() {
  if (errorBanner) errorBanner.classList.add("hidden");
}

// ─── Primary Data Load ────────────────────────────────────────────────────────
async function loadDashboardData(isInitial = false) {
  setRefreshingState(true);

  try {
    const [dashRes, modelsRes] = await Promise.all([
      fetch("/api/dashboard/full"),
      fetch("/api/models")
    ]);

    if (!dashRes.ok || !modelsRes.ok) {
      throw new Error(`Backend returned ${dashRes.status} / ${modelsRes.status}`);
    }

    dashboardData = await dashRes.json();
    modelCardsData = await modelsRes.json();

    hideErrorBanner();
    updateLastRefreshStamp();

    renderKPIs();
    renderAllCharts();
    renderAnomalyFeed();
    renderServiceDetailTable();
    renderMLSection();
    populateExplorerDropdown();

  } catch (err) {
    console.error("Failed to load dashboard telemetry:", err);
    showErrorBanner("Data connection lost — showing last known snapshot. " + err.message);
    // If we have stale data, still render it so the UI isn't empty
    if (dashboardData) {
      renderKPIs();
      renderAllCharts();
      renderAnomalyFeed();
      renderServiceDetailTable();
    }
  } finally {
    setRefreshingState(false);
    // Start auto-refresh on first load
    if (isInitial && toggleAutoRefresh && toggleAutoRefresh.checked) {
      startAutoRefresh();
    }
  }
}

// ─── KPI Cards ───────────────────────────────────────────────────────────────
function renderKPIs() {
  if (!dashboardData?.kpis) return;
  const k = dashboardData.kpis;
  const el = id => document.getElementById(id);
  if (el("kpi-log-lines"))    el("kpi-log-lines").textContent    = Number(k.total_log_lines).toLocaleString();
  if (el("kpi-http-requests"))el("kpi-http-requests").textContent= Number(k.total_http_requests).toLocaleString();
  if (el("kpi-warning-rate")) el("kpi-warning-rate").textContent = `${k.warning_rate_pct}%`;
  if (el("kpi-anomalies"))    el("kpi-anomalies").textContent    = Number(k.total_anomalies).toLocaleString();
}

// ─── Live Anomaly Feed ────────────────────────────────────────────────────────
function renderAnomalyFeed() {
  const feedList = document.getElementById("anomaly-feed-list");
  if (!feedList || !dashboardData) return;

  feedList.innerHTML = "";

  const anomalies = dashboardData.anomalies || [];
  if (anomalies.length === 0) {
    feedList.innerHTML = `<div style="color:var(--text-faint); font-size:0.75rem; font-family:var(--font-mono); padding:0.5rem 0;">No anomaly events in current window.</div>`;
    return;
  }

  // Generate staggered timestamps backwards from now
  const now = new Date();
  anomalies.forEach((a, i) => {
    const eventTime = new Date(now.getTime() - i * 47000 - Math.random() * 30000);
    const hh = String(eventTime.getHours()).padStart(2, "0");
    const mm = String(eventTime.getMinutes()).padStart(2, "0");

    const severity = a.severity_score >= 2 ? "high" : a.severity_score === 1 ? "medium" : "low";
    const severityClass = severity === "high" ? "" : severity;

    const row = document.createElement("div");
    row.className = `anomaly-row ${severityClass}`;
    row.style.animationDelay = `${i * 0.08}s`;
    row.innerHTML = `
      <span class="anomaly-row-time">${hh}:${mm}</span>
      <span class="anomaly-row-service">${a.service}</span>
      <span class="anomaly-row-count">×${a.anomaly_occurrences}</span>
      <span class="anomaly-row-desc">${a.description}</span>
      <span class="anomaly-row-why">
        <button class="chart-why-btn" onclick="openWhyModal('${a.service}', 'anomaly')">
          <span>✨ Why?</span>
        </button>
      </span>
    `;
    feedList.appendChild(row);
  });
}

// ─── All Charts ───────────────────────────────────────────────────────────────
function renderAllCharts() {
  if (!dashboardData) return;

  const services        = dashboardData.services       || [];
  const hourly          = dashboardData.hourly_trend   || [];
  const hourlyLatency   = dashboardData.hourly_latency || [];
  const serviceLatency  = dashboardData.service_latency|| [];
  const kpis            = dashboardData.kpis           || {};

  // Destroy all previous instances cleanly before re-creating
  Object.entries(chartInstances).forEach(([, c]) => { try { c?.destroy(); } catch {} });
  chartInstances = {};

  // ── 1. Service Risk Leaderboard (Horizontal Bar) ──────────────────────────
  const riskCtx = document.getElementById("chart-risk-leaderboard")?.getContext("2d");
  if (riskCtx) {
    const sorted = [...services].sort((a, b) => b.risk_score - a.risk_score);
    chartInstances.risk = new Chart(riskCtx, {
      type: "bar",
      data: {
        labels: sorted.map(s => s.service),
        datasets: [{
          data: sorted.map(s => s.risk_score),
          backgroundColor: sorted.map(s =>
            s.risk_score > 10 ? "#EF4444" : s.risk_score > 2 ? "#F59E0B" : "#0284C7"
          ),
          borderRadius: 2
        }]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.05)" }, title: { display: true, text: "Risk Score" } },
          y: { grid: { display: false } }
        }
      }
    });
  }

  // ── 2. Log Level Distribution (Horizontal Bar) ────────────────────────────
  const levelCtx = document.getElementById("chart-log-level")?.getContext("2d");
  if (levelCtx) {
    const lc = kpis.log_level_counts || { INFO: 277122, WARNING: 4509, ERROR: 265, CRITICAL: 1 };
    chartInstances.level = new Chart(levelCtx, {
      type: "bar",
      data: {
        labels: ["INFO", "WARNING", "ERROR", "CRITICAL"],
        datasets: [{
          data: [lc.INFO, lc.WARNING, lc.ERROR, lc.CRITICAL],
          backgroundColor: ["#0284C7", "#C08A17", "#C0453B", "#7B2D42"],
          borderRadius: 2
        }]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.05)" }, title: { display: true, text: "log lines" } },
          y: { grid: { display: false } }
        }
      }
    });
  }

  // ── 3. Warnings by Service (Horizontal Bar) ───────────────────────────────
  const warnCtx = document.getElementById("chart-warnings-service")?.getContext("2d");
  if (warnCtx) {
    const warnSvcs = services.filter(s => s.warning_count > 0).sort((a, b) => b.warning_count - a.warning_count);
    chartInstances.warnings = new Chart(warnCtx, {
      type: "bar",
      data: {
        labels: warnSvcs.map(s => s.service),
        datasets: [{
          data: warnSvcs.map(s => s.warning_count),
          backgroundColor: warnSvcs.map(s =>
            s.warning_count > 1000 ? "#EF4444" : s.warning_count > 100 ? "#F59E0B" : "#0284C7"
          ),
          borderRadius: 2
        }]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.05)" }, title: { display: true, text: "warnings" } },
          y: { grid: { display: false } }
        }
      }
    });
  }

  // ── 4. HTTP Status Mix (Donut) ────────────────────────────────────────────
  const httpCtx = document.getElementById("chart-http-mix")?.getContext("2d");
  if (httpCtx) {
    const sc = kpis.http_status_counts || { "200 success": 142328, "4xx client error": 685, "5xx server error": 218 };
    chartInstances.http = new Chart(httpCtx, {
      type: "doughnut",
      data: {
        labels: Object.keys(sc),
        datasets: [{
          data: Object.values(sc),
          backgroundColor: ["#0284C7", "#6366F1", "#A855F7"],
          borderColor: "#0C0F17",
          borderWidth: 2
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "68%",
        plugins: {
          legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 10 } } }
        }
      }
    });
  }

  // ── 5. Log Volume Over Time (Area Line) ───────────────────────────────────
  const volCtx = document.getElementById("chart-volume-trend")?.getContext("2d");
  if (volCtx) {
    chartInstances.volume = new Chart(volCtx, {
      type: "line",
      data: {
        labels: hourly.map(h => h.hour),
        datasets: [{
          label: "Log Lines",
          data: hourly.map(h => h.log_lines),
          borderColor: "#0284C7",
          backgroundColor: "rgba(2, 132, 199, 0.15)",
          fill: true,
          tension: 0.35,
          pointRadius: 2,
          pointHoverRadius: 5
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.04)" } },
          y: { grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "log lines" } }
        }
      }
    });
  }

  // ── 6. Severity Over Time (Multi-line) ───────────────────────────────────
  const sevCtx = document.getElementById("chart-severity-trend")?.getContext("2d");
  if (sevCtx) {
    chartInstances.severity = new Chart(sevCtx, {
      type: "line",
      data: {
        labels: hourly.map(h => h.hour),
        datasets: [
          { label: "Warning",  data: hourly.map(h => h.warnings), borderColor: "#0284C7", pointRadius: 2, tension: 0.3 },
          { label: "Error",    data: hourly.map(h => h.errors),   borderColor: "#F59E0B", pointRadius: 2, tension: 0.3 },
          { label: "Critical", data: hourly.map(h => h.critical), borderColor: "#EF4444", pointRadius: 2, tension: 0.3 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 10 } } } },
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.04)" } },
          y: { grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "events" } }
        }
      }
    });
  }

  // ── 7. Latency Over Time (Multi-line) ────────────────────────────────────
  const latCtx = document.getElementById("chart-latency-trend")?.getContext("2d");
  if (latCtx) {
    const maxLatency = hourlyLatency.length
      ? Math.max(...hourlyLatency.map(l => l.p95_response_time)) * 1.2
      : 0.5;
    chartInstances.latency = new Chart(latCtx, {
      type: "line",
      data: {
        labels: hourlyLatency.map(l => l.hour),
        datasets: [
          { label: "P95 Latency", data: hourlyLatency.map(l => l.p95_response_time), borderColor: "#38BDF8", pointRadius: 2, tension: 0.3 },
          { label: "Avg Latency", data: hourlyLatency.map(l => l.avg_response_time), borderColor: "#0284C7", pointRadius: 2, tension: 0.3 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 10 } } } },
        scales: {
          x: { grid: { color: "rgba(255,255,255,0.04)" } },
          y: { grid: { color: "rgba(255,255,255,0.04)" }, min: 0, max: maxLatency, title: { display: true, text: "seconds" } }
        }
      }
    });
  }

  // ── 8. Latency by Service (Grouped Bar) ──────────────────────────────────
  const svclatCtx = document.getElementById("chart-service-latency")?.getContext("2d");
  if (svclatCtx) {
    chartInstances.svclat = new Chart(svclatCtx, {
      type: "bar",
      data: {
        labels: serviceLatency.map(s => s.service),
        datasets: [
          { label: "P95 Response Time", data: serviceLatency.map(s => s.p95_response_time), backgroundColor: "#38BDF8", borderRadius: 2 },
          { label: "Avg Response Time", data: serviceLatency.map(s => s.avg_response_time), backgroundColor: "#6366F1", borderRadius: 2 }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: true, position: "bottom", labels: { boxWidth: 10, font: { size: 10 } } } },
        scales: {
          x: { grid: { display: false } },
          y: { grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "seconds" } }
        }
      }
    });
  }

  // ── 9. Ingestion by Kafka Topic (Bar) ────────────────────────────────────
  const topicCtx = document.getElementById("chart-kafka-topics")?.getContext("2d");
  if (topicCtx) {
    const topics = kpis.kafka_topics || { "openstack-abnormal": 32000, "openstack-normal1": 105000, "openstack-normal2": 144897 };
    chartInstances.topics = new Chart(topicCtx, {
      type: "bar",
      data: {
        labels: Object.keys(topics),
        datasets: [{
          data: Object.values(topics),
          backgroundColor: ["#EF4444", "#0284C7", "#0284C7"],
          borderRadius: 3
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: { grid: { display: false } },
          y: { grid: { color: "rgba(255,255,255,0.04)" }, title: { display: true, text: "log lines" } }
        }
      }
    });
  }
}

// ─── Service Detail Table ─────────────────────────────────────────────────────
function renderServiceDetailTable() {
  const tbody = document.getElementById("superset-service-table-body");
  if (!tbody || !dashboardData) return;
  tbody.innerHTML = "";

  dashboardData.services.forEach(s => {
    let statusClass = "healthy";
    if (s.health_status === "Critical") statusClass = "critical";
    else if (s.health_status === "Degraded") statusClass = "degraded";
    else if (s.health_status === "Low volume") statusClass = "low-volume";

    const riskColor = s.risk_score > 5 ? "#EF4444" : s.risk_score > 0 ? "#F59E0B" : "#38BDF8";

    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td><strong style="color:#FFFFFF;">${s.service}</strong></td>
      <td>${Number(s.log_lines).toLocaleString()}</td>
      <td>${Number(s.http_requests).toLocaleString()}</td>
      <td>${Number(s.latency_samples).toLocaleString()}</td>
      <td>${Number(s.error_count).toLocaleString()}</td>
      <td>${s.error_rate}%</td>
      <td>${Number(s.warning_count).toLocaleString()}</td>
      <td>${s.warning_rate}%</td>
      <td>${Number(s.anomaly_count).toLocaleString()}</td>
      <td style="color:${riskColor}; font-weight:600;">${s.risk_score.toFixed(2)}</td>
      <td><span class="chip-status ${statusClass}">${s.health_status}</span></td>
      <td><span class="chip-cov ${s.latency_coverage === 'measured' ? 'measured' : ''}">${s.latency_coverage}</span></td>
      <td>
        <button class="chart-why-btn" onclick="openWhyModal('${s.service}', 'service_health')">
          <span>✨ Why?</span>
        </button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

// ─── ML Section Application State ─────────────────────────────────────────────
let mlSelectedModelIndex = 0;
let mlSelectedMetricKey = null;
let mlAnomalyFilters = {
  search: "",
  logLevel: "",
  responseCategory: ""
};
let mlExpandedCards = new Set();
let mlFiltersInitialized = false;

// ─── ML Tab Controller & Modular Functions ────────────────────────────────────
function renderMLSection() {
  if (!dashboardData) return;

  renderMLSummary();
  renderModelSelector();
  renderSelectedModel();
  renderMetricTabs();
  renderMetricComparison();
  renderAnomalyFilters();
  renderFilteredAnomalies();
  renderModelCards();
}

// 1. ML Summary Bar
function renderMLSummary() {
  const availableEl = document.getElementById("ml-kpi-available");
  const prodEl = document.getElementById("ml-kpi-prod-candidates");
  const demoEl = document.getElementById("ml-kpi-demo-experimental");
  const totalAnomaliesEl = document.getElementById("ml-kpi-total-anomalies");

  const models = modelCardsData || [];
  const anomalies = dashboardData?.anomalies || [];

  const prodCount = models.filter(m => m.status === "production_candidate").length;
  const demoCount = models.filter(m => m.status === "demo_only" || m.status === "experimental").length;
  const totalAnomalies = anomalies.reduce((sum, a) => sum + (Number(a.anomaly_occurrences) || 0), 0);

  if (availableEl) availableEl.textContent = models.length;
  if (prodEl) prodEl.textContent = prodCount;
  if (demoEl) demoEl.textContent = demoCount;
  if (totalAnomaliesEl) totalAnomaliesEl.textContent = totalAnomalies.toLocaleString();
}

// 2. Model Selector Dropdown
function renderModelSelector() {
  const select = document.getElementById("ml-model-select");
  if (!select) return;

  const models = modelCardsData || [];
  if (models.length === 0) {
    select.innerHTML = `<option value="-1">No models available</option>`;
    return;
  }

  // Preserve index if valid
  if (mlSelectedModelIndex >= models.length) {
    mlSelectedModelIndex = 0;
  }

  select.innerHTML = models.map((m, idx) => `
    <option value="${idx}" ${idx === mlSelectedModelIndex ? "selected" : ""}>
      ${m.model_name} (${m.status.replace(/_/g, " ")})
    </option>
  `).join("");

  // Attach change event listener once
  if (!select.dataset.listenerAttached) {
    select.addEventListener("change", (e) => {
      mlSelectedModelIndex = parseInt(e.target.value, 10);
      renderSelectedModel();
      renderMetricComparison();
      renderModelCards();
    });
    select.dataset.listenerAttached = "true";
  }
}

// 3. Selected Model Detail Panel
function renderSelectedModel() {
  const models = modelCardsData || [];
  if (models.length === 0) return;

  const card = models[mlSelectedModelIndex] || models[0];

  const titleEl = document.getElementById("ml-detail-title");
  const notebookEl = document.getElementById("ml-detail-notebook");
  const objectiveEl = document.getElementById("ml-detail-objective");
  const algoEl = document.getElementById("ml-detail-algorithm");
  const limitationsEl = document.getElementById("ml-detail-limitations");
  const badgeEl = document.getElementById("ml-detail-status-badge");

  if (titleEl) titleEl.textContent = card.model_name;
  if (notebookEl) notebookEl.textContent = card.notebook;
  if (objectiveEl) objectiveEl.textContent = card.objective;
  if (algoEl) algoEl.textContent = card.algorithm;
  if (limitationsEl) limitationsEl.textContent = card.limitations;

  if (badgeEl) {
    const isHealthy = card.status === "production_candidate";
    badgeEl.className = `chip-status ${isHealthy ? "healthy" : "degraded"}`;
    badgeEl.textContent = card.status.replace(/_/g, " ");
  }
}

// 4. Metric Selector Tabs & Comparison Visualization
function renderMetricTabs() {
  const container = document.getElementById("ml-metric-tabs");
  if (!container) return;

  const models = modelCardsData || [];
  // Extract all unique metric keys present across models
  const allMetricKeys = Array.from(new Set(models.flatMap(m => Object.keys(m.metrics || {}))));

  if (allMetricKeys.length === 0) {
    container.innerHTML = `<span style="font-size:0.75rem; color:var(--text-faint);">No metrics reported</span>`;
    return;
  }

  if (!mlSelectedMetricKey || !allMetricKeys.includes(mlSelectedMetricKey)) {
    mlSelectedMetricKey = allMetricKeys[0];
  }

  container.innerHTML = allMetricKeys.map(key => `
    <button class="metric-tab-btn ${key === mlSelectedMetricKey ? "active" : ""}" data-metric="${key}">
      ${key}
    </button>
  `).join("");

  container.querySelectorAll(".metric-tab-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      mlSelectedMetricKey = btn.getAttribute("data-metric");
      renderMetricTabs();
      renderMetricComparison();
    });
  });
}

function renderMetricComparison() {
  const canvas = document.getElementById("chart-ml-metric-comparison");
  if (!canvas) return;

  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const models = modelCardsData || [];
  if (models.length === 0 || !mlSelectedMetricKey) return;

  if (chartInstances.mlComparison) {
    try { chartInstances.mlComparison.destroy(); } catch {}
  }

  const labels = models.map(m => m.model_name.length > 25 ? m.model_name.substring(0, 22) + "…" : m.model_name);
  const dataValues = models.map(m => m.metrics?.[mlSelectedMetricKey] ?? 0);
  const backgroundColors = models.map((_, idx) => idx === mlSelectedModelIndex ? "#38BDF8" : "rgba(2, 132, 199, 0.45)");
  const borderColors = models.map((_, idx) => idx === mlSelectedModelIndex ? "#38BDF8" : "#0284C7");

  chartInstances.mlComparison = new Chart(ctx, {
    type: "bar",
    data: {
      labels: labels,
      datasets: [{
        label: mlSelectedMetricKey,
        data: dataValues,
        backgroundColor: backgroundColors,
        borderColor: borderColors,
        borderWidth: 1,
        borderRadius: 3
      }]
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        x: {
          min: 0,
          max: 1.0,
          grid: { color: "rgba(255,255,255,0.05)" },
          title: { display: true, text: mlSelectedMetricKey }
        },
        y: {
          grid: { display: false }
        }
      },
      plugins: {
        tooltip: {
          callbacks: {
            label: (item) => `${mlSelectedMetricKey}: ${item.raw.toFixed(4)}`
          }
        }
      }
    }
  });
}

// 5. Anomaly Filters & Table Interactivity
function renderAnomalyFilters() {
  const searchInput = document.getElementById("ml-search-service");
  const logLevelSelect = document.getElementById("ml-filter-loglevel");
  const categorySelect = document.getElementById("ml-filter-category");
  const clearBtn = document.getElementById("ml-btn-clear-filters");

  const anomalies = dashboardData?.anomalies || [];

  if (!mlFiltersInitialized) {
    // Populate Log Level options
    if (logLevelSelect) {
      const levels = Array.from(new Set(anomalies.map(a => a.log_level).filter(Boolean)));
      logLevelSelect.innerHTML = `<option value="">All Log Levels</option>` +
        levels.map(l => `<option value="${l}">${l}</option>`).join("");
    }

    // Populate Response Category options if available
    if (categorySelect) {
      const categories = Array.from(new Set(anomalies.map(a => a.response_category).filter(Boolean)));
      const catContainer = document.getElementById("ml-filter-category-container");
      if (categories.length > 0) {
        if (catContainer) catContainer.style.display = "block";
        categorySelect.innerHTML = `<option value="">All Categories</option>` +
          categories.map(c => `<option value="${c}">${c}</option>`).join("");
      } else if (catContainer) {
        catContainer.style.display = "none";
      }
    }

    // Attach listeners
    if (searchInput) {
      searchInput.addEventListener("input", e => {
        mlAnomalyFilters.search = e.target.value;
        renderFilteredAnomalies();
      });
    }

    if (logLevelSelect) {
      logLevelSelect.addEventListener("change", e => {
        mlAnomalyFilters.logLevel = e.target.value;
        renderFilteredAnomalies();
      });
    }

    if (categorySelect) {
      categorySelect.addEventListener("change", e => {
        mlAnomalyFilters.responseCategory = e.target.value;
        renderFilteredAnomalies();
      });
    }

    if (clearBtn) {
      clearBtn.addEventListener("click", () => {
        mlAnomalyFilters = { search: "", logLevel: "", responseCategory: "" };
        if (searchInput) searchInput.value = "";
        if (logLevelSelect) logLevelSelect.value = "";
        if (categorySelect) categorySelect.value = "";
        renderFilteredAnomalies();
      });
    }

    mlFiltersInitialized = true;
  }
}

function renderFilteredAnomalies() {
  const tbody = document.getElementById("ml-anomalies-table-body");
  if (!tbody || !dashboardData) return;

  const anomalies = dashboardData.anomalies || [];
  const search = mlAnomalyFilters.search.trim().toLowerCase();
  const logLevel = mlAnomalyFilters.logLevel;
  const category = mlAnomalyFilters.responseCategory;

  const filtered = anomalies.filter(a => {
    const matchesSearch = !search || (a.service && a.service.toLowerCase().includes(search));
    const matchesLevel = !logLevel || a.log_level === logLevel;
    const matchesCat = !category || a.response_category === category;
    return matchesSearch && matchesLevel && matchesCat;
  });

  tbody.innerHTML = "";
  if (filtered.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; color:var(--text-faint); padding:1.5rem 0;">No matching anomalies found.</td></tr>`;
  } else {
    filtered.forEach(a => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><strong style="color:#FFFFFF;">${a.service}</strong></td>
        <td><span class="chip-status healthy">${a.log_level}</span></td>
        <td style="color:#F59E0B;">${a.response_category || "-"}</td>
        <td>${a.status_code !== undefined ? a.status_code : "-"}</td>
        <td style="color:#EF4444; font-weight:600;">${a.anomaly_occurrences}</td>
        <td style="color:#94A3B8; font-size:0.75rem;">${a.description || "-"}</td>
        <td>
          <button class="chart-why-btn" onclick="openWhyModal('${a.service}', 'anomaly')">
            <span>✨ Why?</span>
          </button>
        </td>
      `;
      tbody.appendChild(tr);
    });
  }

  renderAnomalySummary(filtered);
}

function renderAnomalySummary(filteredAnomalies) {
  const occurrencesEl = document.getElementById("ml-summary-total-occurrences");
  const servicesEl = document.getElementById("ml-summary-affected-services");
  const highestEl = document.getElementById("ml-summary-highest-service");

  const totalOccurrences = filteredAnomalies.reduce((sum, a) => sum + (Number(a.anomaly_occurrences) || 0), 0);
  const affectedServices = new Set(filteredAnomalies.map(a => a.service)).size;

  let highestService = "--";
  if (filteredAnomalies.length > 0) {
    const serviceCounts = {};
    filteredAnomalies.forEach(a => {
      serviceCounts[a.service] = (serviceCounts[a.service] || 0) + (Number(a.anomaly_occurrences) || 0);
    });
    const sorted = Object.entries(serviceCounts).sort((a, b) => b[1] - a[1]);
    if (sorted.length > 0) {
      highestService = `${sorted[0][0]} (${sorted[0][1]})`;
    }
  }

  if (occurrencesEl) occurrencesEl.textContent = totalOccurrences.toLocaleString();
  if (servicesEl) servicesEl.textContent = affectedServices;
  if (highestEl) highestEl.textContent = highestService;
}

// 6. Model Cards Expand/Collapse
function renderModelCards() {
  const cardsContainer = document.getElementById("ml-model-cards-container");
  if (!cardsContainer) return;

  cardsContainer.innerHTML = "";
  (modelCardsData || []).forEach((card, idx) => {
    const div = document.createElement("div");
    const isExpanded = mlExpandedCards.has(idx);
    div.className = `ml-card ${isExpanded ? "expanded" : ""}`;

    const statusClass = card.status === "production_candidate" ? "healthy" : "degraded";

    const metricsHtml = Object.entries(card.metrics || {}).map(([k, v]) => `
      <div class="ml-metric-cell">
        <div class="ml-metric-key">${k}</div>
        <div class="ml-metric-val">${typeof v === "number" ? v.toFixed(4) : v}</div>
      </div>
    `).join("");

    // Identify primary score for collapsed view (e.g. F1 Score or first metric)
    const collapsedScore = card.metrics?.["F1 Score"] !== undefined
      ? `F1 Score: <span>${card.metrics["F1 Score"].toFixed(4)}</span>`
      : Object.entries(card.metrics || {})[0]
        ? `${Object.entries(card.metrics)[0][0]}: <span>${typeof Object.entries(card.metrics)[0][1] === "number" ? Object.entries(card.metrics)[0][1].toFixed(4) : Object.entries(card.metrics)[0][1]}</span>`
        : "";

    div.innerHTML = `
      <div class="ml-card-header">
        <div>
          <div class="ml-card-eyebrow">${card.notebook}</div>
          <h3 class="ml-card-title">${card.model_name}</h3>
        </div>
        <span class="chip-status ${statusClass}">${card.status.replace(/_/g, " ")}</span>
      </div>

      <div class="ml-card-collapsed-summary">
        <div class="ml-collapsed-metric">${collapsedScore}</div>
        <button class="ml-card-toggle" data-idx="${idx}">View details ▾</button>
      </div>

      <div class="ml-card-details">
        <p style="font-size:0.8rem; color:#94A3B8; margin-bottom:0.4rem;"><strong>Objective:</strong> ${card.objective}</p>
        <p style="font-size:0.75rem; color:#64748B;"><strong>Algorithm:</strong> <code>${card.algorithm}</code></p>
        <div class="ml-metrics-grid">${metricsHtml}</div>
        <div class="ml-limitations"><strong>Governance Disclosure:</strong> ${card.limitations}</div>
        <button class="ml-card-toggle" data-idx="${idx}" style="margin-top:0.75rem;">Hide details ▴</button>
      </div>
    `;

    // Attach expand/collapse toggle listener
    div.querySelectorAll(".ml-card-toggle").forEach(btn => {
      btn.addEventListener("click", e => {
        e.stopPropagation();
        if (mlExpandedCards.has(idx)) {
          mlExpandedCards.delete(idx);
        } else {
          mlExpandedCards.add(idx);
        }
        renderModelCards();
      });
    });

    cardsContainer.appendChild(div);
  });
}

// ─── AI Explorer Dropdown ─────────────────────────────────────────────────────
function populateExplorerDropdown() {
  const select = document.getElementById("ai-explorer-service-select");
  if (!select || !dashboardData) return;
  select.innerHTML = "";
  dashboardData.services.forEach(s => {
    const opt = document.createElement("option");
    opt.value = s.service;
    opt.textContent = `${s.service} (${s.health_status})`;
    select.appendChild(opt);
  });
}

// ─── "Why?" Modal ─────────────────────────────────────────────────────────────
const modalBackdrop    = document.getElementById("why-modal");
const modalCloseBtn    = document.getElementById("modal-btn-close");
const modalLoaderBox   = document.getElementById("modal-loader-box");
const modalContentGrid = document.getElementById("modal-content-grid");
const modalBadgeService= document.getElementById("modal-badge-service");
const modalBadgeHealth = document.getElementById("modal-badge-health");
const modalBadgeModel  = document.getElementById("modal-badge-model");
const modalHeadingText = document.getElementById("modal-heading-text");
const modalSectionWhat = document.getElementById("modal-section-what");
const modalSectionWhy  = document.getElementById("modal-section-why");
const modalSectionImpact=document.getElementById("modal-section-impact");
const modalSectionNext = document.getElementById("modal-section-next");
const modalGroundingJson=document.getElementById("modal-grounding-json");
const btnCopyDiag      = document.getElementById("btn-copy-diag");

function initModalHandlers() {
  if (!modalCloseBtn || !modalBackdrop) return;

  modalCloseBtn.addEventListener("click", () => modalBackdrop.classList.add("hidden"));
  modalBackdrop.addEventListener("click", e => {
    if (e.target === modalBackdrop) modalBackdrop.classList.add("hidden");
  });
  document.addEventListener("keydown", e => {
    if (e.key === "Escape" && !modalBackdrop.classList.contains("hidden")) {
      modalBackdrop.classList.add("hidden");
    }
  });

  if (btnCopyDiag) {
    btnCopyDiag.addEventListener("click", () => {
      const what  = modalSectionWhat?.textContent  || "";
      const why   = modalSectionWhy?.textContent   || "";
      const impact= modalSectionImpact?.textContent|| "";
      const steps = Array.from(modalSectionNext?.children || []).map(li => `- ${li.textContent}`).join("\n");
      const full  = `LOG GUARDIAN AI DIAGNOSIS\n${modalHeadingText?.textContent || ""}\n\n1. WHAT HAPPENED:\n${what}\n\n2. WHY DID THIS HAPPEN:\n${why}\n\n3. IMPACT:\n${impact}\n\n4. NEXT STEPS:\n${steps}`;

      navigator.clipboard.writeText(full).then(() => {
        btnCopyDiag.textContent = "✅ Copied!";
        setTimeout(() => { btnCopyDiag.textContent = "📋 Copy Diagnosis"; }, 2000);
      });
    });
  }
}

async function openWhyModal(targetName, explanationType = "service_health") {
  if (!modalBackdrop) return;
  modalBackdrop.classList.remove("hidden");
  modalLoaderBox?.classList.remove("hidden");
  modalContentGrid?.classList.add("hidden");

  if (modalBadgeService) modalBadgeService.textContent = targetName;
  if (modalBadgeModel)   modalBadgeModel.textContent   = "Querying Gemini…";

  const svc = (dashboardData?.services || []).find(s => s.service.toLowerCase() === targetName.toLowerCase());
  if (modalBadgeHealth) {
    modalBadgeHealth.textContent = svc
      ? svc.health_status
      : (explanationType === "anomaly" ? "Anomaly Pattern" : "Telemetry Aggregate");
  }

  try {
    const res = await fetch("/api/ai/explain", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ service_name: targetName, explanation_type: explanationType })
    });

    if (!res.ok) throw new Error(`AI endpoint returned ${res.status}`);

    const data = await res.json();
    const exp  = data.explanation;

    if (modalHeadingText) modalHeadingText.textContent = exp.headline || `Diagnostic Analysis: ${targetName}`;
    if (modalSectionWhat) modalSectionWhat.textContent = exp.what_happened   || "No telemetry observed.";
    if (modalSectionWhy)  modalSectionWhy.textContent  = exp.why_did_this_happen || "Computed from Gold views.";
    if (modalSectionImpact) modalSectionImpact.textContent = exp.impact || "Operational impact within nominal parameters.";

    if (modalSectionNext) {
      modalSectionNext.innerHTML = "";
      (exp.what_to_check_next || ["Maintain baseline monitoring."]).forEach(step => {
        const li = document.createElement("li");
        li.textContent = step;
        modalSectionNext.appendChild(li);
      });
    }

    if (modalGroundingJson) modalGroundingJson.textContent = JSON.stringify(data.facts_evaluated, null, 2);
    if (modalBadgeModel)    modalBadgeModel.textContent = exp.model || (exp.generation_mode === "gemini_ai" ? "Gemini 2.5 Flash" : "Rule Engine");

    modalLoaderBox?.classList.add("hidden");
    modalContentGrid?.classList.remove("hidden");

  } catch (err) {
    console.error("AI Explanation request failed:", err);
    if (modalSectionWhat) modalSectionWhat.textContent = "Failed to communicate with AI service: " + err.message;
    modalLoaderBox?.classList.add("hidden");
    modalContentGrid?.classList.remove("hidden");
  }
}

// ─── AI Explorer Page ─────────────────────────────────────────────────────────
function initAIExplorer() {
  const btn      = document.getElementById("btn-run-ai-explorer");
  const resultBox= document.getElementById("ai-explorer-result-body");
  const badge    = document.getElementById("ai-explorer-status-badge");
  const select   = document.getElementById("ai-explorer-service-select");

  if (!btn || !resultBox) return;

  btn.addEventListener("click", async () => {
    const serviceName = select?.value || "";
    const expMode = document.querySelector("input[name='exp-mode']:checked")?.value || "service_health";

    if (!serviceName) return;

    if (badge) badge.textContent = "Evaluating…";
    resultBox.innerHTML = `<div class="modal-loader-box"><div class="ai-spinner"></div><div class="loader-caption mono">Generating grounded AI explanation for ${serviceName}…</div></div>`;

    try {
      const res = await fetch("/api/ai/explain", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ service_name: serviceName, explanation_type: expMode })
      });
      const data = await res.json();
      const exp  = data.explanation;

      if (badge) badge.textContent = exp.generation_mode === "gemini_ai" ? "Gemini 2.5 Flash" : "Grounded Rule Engine";

      resultBox.innerHTML = `
        <div class="modal-content-grid">
          <div class="modal-section-card">
            <div class="modal-section-title"><span class="num-badge">1</span> What Happened?</div>
            <div class="modal-section-body serif">${exp.what_happened || ""}</div>
          </div>
          <div class="modal-section-card">
            <div class="modal-section-title"><span class="num-badge">2</span> Why Did This Happen?</div>
            <div class="modal-section-body serif">${exp.why_did_this_happen || ""}</div>
          </div>
          <div class="modal-section-card">
            <div class="modal-section-title"><span class="num-badge">3</span> Potential Operational Impact</div>
            <div class="modal-section-body serif">${exp.impact || ""}</div>
          </div>
          <div class="modal-section-card highlight-action">
            <div class="modal-section-title"><span class="num-badge">4</span> What Should Be Checked Next?</div>
            <ul class="modal-section-list serif">
              ${(exp.what_to_check_next || []).map(s => `<li>${s}</li>`).join("")}
            </ul>
          </div>
        </div>
      `;
    } catch (err) {
      if (badge) badge.textContent = "Error";
      resultBox.innerHTML = `<p style="color:#EF4444; font-family:var(--font-mono); font-size:0.8rem; padding:1rem 0;">Failed to generate explanation: ${err.message}</p>`;
    }
  });
}

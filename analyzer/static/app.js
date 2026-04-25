const state = {
  db: null,
  databases: [],
  table: null,
  columns: [],
  numericColumns: [],
  visibleRows: [],
  visibleColumns: [],
  analysis: null,
  searchTimer: null,
};

const databaseList = document.querySelector("#databaseList");
const databaseMessage = document.querySelector("#databaseMessage");
const dbPath = document.querySelector("#dbPath");
const uploadForm = document.querySelector("#uploadForm");
const databaseUpload = document.querySelector("#databaseUpload");
const uploadButton = document.querySelector("#uploadButton");
const tableList = document.querySelector("#tableList");
const chartTypeSelect = document.querySelector("#chartTypeSelect");
const dimensionSelect = document.querySelector("#dimensionSelect");
const metricSelect = document.querySelector("#metricSelect");
const aggregationSelect = document.querySelector("#aggregationSelect");
const drawChartButton = document.querySelector("#drawChart");
const chart = document.querySelector("#chart");
const chartTitle = document.querySelector("#chartTitle");
const chartNote = document.querySelector("#chartNote");
const chartTotal = document.querySelector("#chartTotal");
const summaryGrid = document.querySelector("#summaryGrid");
const dataTitle = document.querySelector("#dataTitle");
const searchInput = document.querySelector("#searchInput");
const rowLimitSelect = document.querySelector("#rowLimitSelect");
const rowNote = document.querySelector("#rowNote");
const tablePreview = document.querySelector("#tablePreview");
const profileTable = document.querySelector("#profileTable");
const fieldCards = document.querySelector("#fieldCards");
const downloadCsvButton = document.querySelector("#downloadCsv");
const missionTitle = document.querySelector("#missionTitle");
const missionNarrative = document.querySelector("#missionNarrative");
const coverageHeadline = document.querySelector("#coverageHeadline");
const coverageDetail = document.querySelector("#coverageDetail");
const watchHeadline = document.querySelector("#watchHeadline");
const watchDetail = document.querySelector("#watchDetail");
const freshnessHeadline = document.querySelector("#freshnessHeadline");
const freshnessDetail = document.querySelector("#freshnessDetail");
const signalCoverage = document.querySelector("#signalCoverage");
const confidenceBands = document.querySelector("#confidenceBands");
const formatPulse = document.querySelector("#formatPulse");
const watchlistPanel = document.querySelector("#watchlistPanel");
const activityPanel = document.querySelector("#activityPanel");

const palette = ["#0b7d67", "#bb4b43", "#bd9b23", "#355f70", "#3f7e61", "#867135"];
const dbPreferenceKey = "sqlite-visual-explorer-db";

async function getJson(url, options = {}) {
  const response = await fetch(`${window.APP_BASE || ""}${url}`, options);
  const payload = await response.json();
  if (!response.ok) throw new Error(payload.error || "Request failed");
  return payload;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function renderError(target, message) {
  target.innerHTML = `<div class="error">${escapeHtml(message)}</div>`;
}

function setDatabaseMessage(message = "", isError = false) {
  databaseMessage.textContent = message;
  databaseMessage.classList.toggle("status-error", isError);
  databaseMessage.classList.toggle("status-success", Boolean(message) && !isError);
}

function databaseApiUrl(path, extraParams = {}) {
  const params = new URLSearchParams(extraParams);
  if (state.db?.id) params.set("db", state.db.id);
  const query = params.toString();
  return query ? `${path}?${query}` : path;
}

function rememberDatabase(id) {
  try {
    window.localStorage.setItem(dbPreferenceKey, id);
  } catch {}
}

function preferredDatabaseId() {
  try {
    return window.localStorage.getItem(dbPreferenceKey);
  } catch {
    return null;
  }
}

function formatNumber(value, maximumFractionDigits = 2) {
  return Number(value || 0).toLocaleString(undefined, { maximumFractionDigits });
}

function formatPercent(value, maximumFractionDigits = 1) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "--";
  return `${(Number(value) * 100).toLocaleString(undefined, { maximumFractionDigits })}%`;
}

function truncateLabel(value, length = 28) {
  const label = String(value || "(blank)");
  return label.length > length ? `${label.slice(0, length - 1)}...` : label;
}

function setSelectOptions(select, options, selectedValue = "") {
  select.innerHTML = options
    .map((option) => `<option value="${escapeHtml(option.value)}" ${option.value === selectedValue ? "selected" : ""}>${escapeHtml(option.label)}</option>`)
    .join("");
}

function isNumericType(type) {
  return ["INT", "REAL", "NUM", "DEC", "FLOAT", "DOUBLE"].some((marker) => String(type || "").toUpperCase().includes(marker));
}

function renderSummary(items) {
  summaryGrid.innerHTML = items
    .map(
      (item, index) => `
        <article class="summary-card summary-card-${index + 1}">
          <p class="eyebrow">${escapeHtml(item.label)}</p>
          <strong>${escapeHtml(item.value)}</strong>
          ${item.note ? `<span>${escapeHtml(item.note)}</span>` : ""}
        </article>
      `
    )
    .join("");
}

function renderTable(target, columns, rows) {
  if (!rows.length) {
    target.innerHTML = '<div class="chart-empty">No rows found.</div>';
    return;
  }
  const head = columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("");
  const body = rows
    .map((row) => `<tr>${columns.map((column) => `<td title="${escapeHtml(row[column])}">${escapeHtml(row[column])}</td>`).join("")}</tr>`)
    .join("");
  target.innerHTML = `<table><thead><tr>${head}</tr></thead><tbody>${body}</tbody></table>`;
}

function clearAnalyzerPanels(message = "Select a table to activate analyzer panels.") {
  missionTitle.textContent = "Select a table to inspect signal health";
  missionNarrative.textContent = message;
  coverageHeadline.textContent = "--";
  coverageDetail.textContent = "Awaiting table selection";
  watchHeadline.textContent = "--";
  watchDetail.textContent = "No watchlist yet";
  freshnessHeadline.textContent = "--";
  freshnessDetail.textContent = "No update clock yet";
  signalCoverage.innerHTML = '<div class="chart-empty">Coverage telemetry will appear here.</div>';
  confidenceBands.innerHTML = '<div class="chart-empty">Confidence bands will appear here.</div>';
  formatPulse.innerHTML = '<div class="chart-empty">Format mix will appear here.</div>';
  watchlistPanel.innerHTML = '<div class="chart-empty">Watchlist signals will appear here.</div>';
  activityPanel.innerHTML = '<div class="chart-empty">Recent signature activity will appear here.</div>';
}

function clearWorkspace(message = "Choose a table to explore this database.") {
  state.table = null;
  state.columns = [];
  state.numericColumns = [];
  state.visibleRows = [];
  state.visibleColumns = [];
  state.analysis = null;
  chartTitle.textContent = "Select a table";
  chartNote.textContent = "";
  chartTotal.textContent = "";
  dataTitle.textContent = "Rows";
  rowNote.textContent = "";
  chart.innerHTML = `<div class="chart-empty">${escapeHtml(message)}</div>`;
  tablePreview.innerHTML = '<div class="chart-empty">Select a table to preview rows.</div>';
  profileTable.innerHTML = '<div class="chart-empty">Select a table to inspect its columns.</div>';
  fieldCards.innerHTML = "";
  summaryGrid.innerHTML = "";
  tableList.innerHTML = '<div class="chart-empty">Loading tables...</div>';
  clearAnalyzerPanels("Load a machinery log registry to surface signal coverage, confidence drift, and recent mapping changes.");
}

function setCurrentDatabase(database) {
  state.db = database;
  dbPath.textContent = database ? database.path : "No database selected";
  if (database?.id) rememberDatabase(database.id);
}

function renderDatabases() {
  if (!state.databases.length) {
    databaseList.innerHTML = '<div class="chart-empty">No databases loaded.</div>';
    return;
  }

  databaseList.innerHTML = state.databases
    .map(
      (database) => `
        <article class="database-card ${database.id === state.db?.id ? "active" : ""}">
          <button class="database-switch" type="button" data-db-id="${escapeHtml(database.id)}">
            <strong>${escapeHtml(database.name)}</strong>
            <span>${database.uploaded ? "Uploaded" : "Workspace default"}</span>
          </button>
          <div class="database-actions">
            ${database.deletable ? `<button class="database-remove secondary-button" type="button" data-remove-db-id="${escapeHtml(database.id)}">Remove</button>` : '<span class="database-locked">Pinned</span>'}
          </div>
        </article>
      `
    )
    .join("");

  databaseList.querySelectorAll("[data-db-id]").forEach((button) => {
    button.addEventListener("click", () => switchDatabase(button.dataset.dbId).catch((error) => setDatabaseMessage(error.message, true)));
  });

  databaseList.querySelectorAll("[data-remove-db-id]").forEach((button) => {
    button.addEventListener("click", async () => {
      const dbId = button.dataset.removeDbId;
      const db = state.databases.find((item) => item.id === dbId);
      if (!db) return;
      if (!window.confirm(`Remove ${db.name}?`)) return;
      try {
        await deleteDatabase(dbId);
      } catch (error) {
        setDatabaseMessage(error.message, true);
      }
    });
  });
}

async function loadDatabases({ preferredId = null } = {}) {
  const payload = await getJson(preferredId ? `/api/databases?db=${encodeURIComponent(preferredId)}` : "/api/databases");
  state.databases = payload.databases;
  setCurrentDatabase(payload.current_database);
  renderDatabases();
}

async function switchDatabase(dbId) {
  if (!dbId || dbId === state.db?.id) return;
  const payload = await getJson(`/api/databases?db=${encodeURIComponent(dbId)}`);
  state.databases = payload.databases;
  setCurrentDatabase(payload.current_database);
  renderDatabases();
  searchInput.value = "";
  clearWorkspace("Loading tables...");
  setDatabaseMessage(`Switched to ${payload.current_database.name}`);
  await loadTables();
}

async function uploadDatabase(event) {
  event.preventDefault();
  if (!databaseUpload.files.length) {
    setDatabaseMessage("Choose a SQLite file first.", true);
    return;
  }

  setDatabaseMessage("Uploading database...");
  uploadButton.disabled = true;
  try {
    const formData = new FormData();
    formData.append("database", databaseUpload.files[0]);
    const payload = await getJson("/api/databases/upload", { method: "POST", body: formData });
    state.databases = payload.databases;
    setCurrentDatabase(payload.current_database);
    renderDatabases();
    databaseUpload.value = "";
    searchInput.value = "";
    clearWorkspace("Loading tables...");
    setDatabaseMessage(payload.message);
    await loadTables();
  } finally {
    uploadButton.disabled = false;
  }
}

async function deleteDatabase(dbId) {
  const payload = await getJson(`/api/databases/${encodeURIComponent(dbId)}`, { method: "DELETE" });
  state.databases = payload.databases;
  setCurrentDatabase(payload.current_database);
  renderDatabases();
  searchInput.value = "";
  clearWorkspace("Loading tables...");
  setDatabaseMessage(payload.message);
  await loadTables();
}

async function loadTables() {
  if (!state.db) {
    clearWorkspace("Choose or upload a database to begin.");
    return;
  }

  const payload = await getJson(databaseApiUrl("/api/tables"));
  setCurrentDatabase(payload.database);
  renderDatabases();

  if (!payload.tables.length) {
    tableList.innerHTML = '<div class="chart-empty">No SQLite tables found.</div>';
    renderSummary([
      { label: "Database", value: payload.database.name, note: "Active source" },
      { label: "Path", value: payload.database.path, note: "Read-only connection" },
      { label: "Tables", value: 0, note: "No machinery datasets loaded" },
      { label: "Rows", value: 0, note: "No table selected" },
    ]);
    clearAnalyzerPanels("This database has no user tables, so the analyzer has nothing to score yet.");
    return;
  }

  tableList.innerHTML = payload.tables
    .map((table) => `<button class="table-button" type="button" data-table="${escapeHtml(table.name)}">${escapeHtml(table.name)}<span>${Number(table.row_count).toLocaleString()} rows</span></button>`)
    .join("");

  tableList.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", () => selectTable(button.dataset.table).catch((error) => setDatabaseMessage(error.message, true)));
  });

  await selectTable(payload.tables[0].name);
}

function updateAnalyzerHero(table, analysis) {
  const tracked = analysis.signal_coverage.filter((item) => item.count > 0).length;
  missionTitle.textContent = `${table} fab telemetry briefing`;
  missionNarrative.textContent = `The analyzer scanned ${formatNumber(analysis.sampled_rows, 0)} rows from ${formatNumber(analysis.row_count, 0)} total records to score capture health, mapping quality, and edit activity for machine log signatures.`;
  coverageHeadline.textContent = `${tracked}/8`;
  coverageDetail.textContent = tracked ? "Tracked signals represented in this table" : "No tracked extraction fields detected";
  watchHeadline.textContent = analysis.watchlist.length ? `${analysis.watchlist.length} flags` : "Stable";
  watchDetail.textContent = analysis.watchlist.length ? analysis.watchlist[0].reason : "No urgent signature drift detected";
  freshnessHeadline.textContent = analysis.latest_update ? analysis.latest_update.slice(0, 10) : "--";
  freshnessDetail.textContent = `Average confidence ${analysis.avg_confidence === null ? "--" : formatPercent(analysis.avg_confidence)}`;
}

function renderSignalCoverage(analysis) {
  const active = analysis.signal_coverage.filter((item) => item.count > 0);
  if (!active.length) {
    signalCoverage.innerHTML = '<div class="chart-empty">No tracked fab telemetry fields were detected in this table.</div>';
    return;
  }
  signalCoverage.innerHTML = active
    .map(
      (item) => `
        <article class="signal-card">
          <p class="eyebrow">${escapeHtml(item.signal.replaceAll("_", " "))}</p>
          <strong>${escapeHtml(`${item.coverage_pct}%`)}</strong>
          <p class="signal-meta">${escapeHtml(formatNumber(item.count, 0))} signatures carrying this field</p>
          <div class="meter"><span style="width:${Math.min(item.coverage_pct, 100)}%"></span></div>
        </article>
      `
    )
    .join("");
}

function renderConfidenceBands(analysis) {
  const bands = [
    { label: "High", value: analysis.confidence_bands.high, className: "band-high" },
    { label: "Medium", value: analysis.confidence_bands.medium, className: "band-medium" },
    { label: "Low", value: analysis.confidence_bands.low, className: "band-low" },
  ];
  const total = bands.reduce((sum, band) => sum + band.value, 0);
  if (!total) {
    confidenceBands.innerHTML = '<div class="chart-empty">No confidence data was found for this table.</div>';
    return;
  }
  confidenceBands.innerHTML = `<div class="band-grid">${bands.map((band) => {
    const pct = total ? (band.value / total) * 100 : 0;
    return `<div class="band-row"><span class="band-label">${escapeHtml(band.label)}</span><div class="band-bar"><span class="${band.className}" style="width:${pct}%"></span></div><strong>${escapeHtml(formatNumber(band.value, 0))}</strong></div>`;
  }).join("")}</div>`;
}

function renderFormatPulse(analysis) {
  if (!analysis.formats.length) {
    formatPulse.innerHTML = '<div class="chart-empty">No format dimension was detected in this table.</div>';
    return;
  }
  formatPulse.innerHTML = `<div class="list-stack">${analysis.formats.map((item) => `<article class="list-item"><strong>${escapeHtml(item.label)}</strong><div class="list-inline"><span class="pill">${escapeHtml(formatNumber(item.count, 0))} signatures</span></div></article>`).join("")}</div>`;
}

function renderWatchlist(analysis) {
  if (!analysis.watchlist.length) {
    watchlistPanel.innerHTML = '<div class="chart-empty">No mappings currently match the watchlist heuristics.</div>';
    return;
  }
  watchlistPanel.innerHTML = `<div class="list-stack">${analysis.watchlist.map((item) => `<article class="list-item"><strong>${escapeHtml(item.signature)}</strong><div class="list-inline"><span class="pill">${escapeHtml(item.format)}</span><span>${escapeHtml(item.confidence === null ? "No confidence" : formatPercent(item.confidence))}</span><span>${escapeHtml(`${formatNumber(item.hit_count, 0)} hits`)}</span></div><p class="list-meta">${escapeHtml(item.reason)}</p></article>`).join("")}</div>`;
}

function renderActivity(analysis) {
  if (!analysis.recent_activity.length) {
    activityPanel.innerHTML = '<div class="chart-empty">No change timestamps were detected for this table.</div>';
    return;
  }
  activityPanel.innerHTML = `<div class="list-stack">${analysis.recent_activity.map((item) => `<article class="list-item"><strong>${escapeHtml(item.signature)}</strong><div class="list-inline"><span class="pill">${escapeHtml(item.format)}</span><span>${escapeHtml(item.updated_at || "No timestamp")}</span></div><p class="list-meta">${escapeHtml(`${item.confidence === null ? "No confidence score" : `Confidence ${formatPercent(item.confidence)}`} • ${formatNumber(item.hit_count, 0)} hits`)}</p></article>`).join("")}</div>`;
}

function renderFieldCards(profile) {
  fieldCards.innerHTML = profile.columns
    .map((column) => {
      const fillRate = profile.row_count ? Math.round((column.non_null / profile.row_count) * 100) : 0;
      const distinctRate = profile.row_count ? Math.round((column.distinct_count / profile.row_count) * 100) : 0;
      return `<article class="field-card"><div><p class="eyebrow">${escapeHtml(column.type || "TEXT")}</p><h3>${escapeHtml(column.name)}</h3></div><div class="field-stat"><span>Filled</span><strong>${fillRate}%</strong></div><div class="meter"><span style="width:${fillRate}%"></span></div><div class="field-foot"><span>${formatNumber(column.distinct_count, 0)} unique</span><span>${distinctRate}% distinct</span></div></article>`;
    })
    .join("");
}

async function selectTable(table) {
  state.table = table;
  searchInput.value = "";
  tableList.querySelectorAll("button").forEach((button) => button.classList.toggle("active", button.dataset.table === table));

  const [schema, profile, analyzer] = await Promise.all([
    getJson(databaseApiUrl(`/api/schema/${encodeURIComponent(table)}`)),
    getJson(databaseApiUrl(`/api/profile/${encodeURIComponent(table)}`)),
    getJson(databaseApiUrl(`/api/analyzer/${encodeURIComponent(table)}`)),
  ]);

  setCurrentDatabase(schema.database);
  state.columns = schema.columns;
  state.numericColumns = schema.columns.filter((column) => isNumericType(column.type));
  state.analysis = analyzer.analysis;

  setSelectOptions(dimensionSelect, state.columns.map((column) => ({ value: column.name, label: `${column.name} (${column.type || "TEXT"})` })));
  setSelectOptions(metricSelect, state.numericColumns.length ? state.numericColumns.map((column) => ({ value: column.name, label: column.name })) : [{ value: "", label: "No numeric columns" }]);
  metricSelect.disabled = aggregationSelect.value === "count";

  renderSummary([
    { label: "Active Table", value: table, note: schema.database.name },
    { label: "Avg Confidence", value: state.analysis.avg_confidence === null ? "--" : formatPercent(state.analysis.avg_confidence), note: "Pattern quality score" },
    { label: "Observed Hits", value: state.analysis.total_hits === null ? "--" : formatNumber(state.analysis.total_hits, 0), note: "Across sampled mappings" },
    { label: "Tracked Signals", value: state.analysis.signal_coverage.filter((item) => item.count > 0).length, note: "Fab telemetry fields detected" },
  ]);

  updateAnalyzerHero(table, state.analysis);
  renderSignalCoverage(state.analysis);
  renderConfidenceBands(state.analysis);
  renderFormatPulse(state.analysis);
  renderWatchlist(state.analysis);
  renderActivity(state.analysis);
  renderFieldCards(profile);
  renderTable(profileTable, ["name", "type", "non_null", "null_count", "distinct_count", "min_value", "max_value"], profile.columns);
  await loadRows();
  await drawChart();
}

async function loadRows() {
  if (!state.table) return;
  const payload = await getJson(databaseApiUrl(`/api/rows/${encodeURIComponent(state.table)}`, { limit: rowLimitSelect.value, search: searchInput.value.trim() }));
  state.visibleRows = payload.rows;
  state.visibleColumns = payload.columns;
  dataTitle.textContent = `${state.table} rows`;
  rowNote.textContent = `${formatNumber(payload.rows.length, 0)} shown of ${formatNumber(payload.total, 0)}`;
  renderTable(tablePreview, payload.columns, payload.rows);
}

function chartData(payload) {
  return payload.data.filter((point) => point.label !== null).map((point) => ({ label: String(point.label || "(blank)"), value: Number(point.value) || 0 }));
}

function renderBarChart(payload) {
  const data = chartData(payload);
  if (!data.length) {
    chart.innerHTML = '<div class="chart-empty">No chart data found.</div>';
    return;
  }
  const width = 920;
  const rowHeight = 34;
  const labelWidth = 210;
  const valueWidth = 90;
  const topPadding = 18;
  const height = Math.max(300, topPadding + data.length * rowHeight + 20);
  const maxValue = Math.max(...data.map((point) => Number(point.value) || 0), 1);
  const chartWidth = width - labelWidth - valueWidth - 40;

  const rows = data
    .map((point, index) => {
      const y = topPadding + index * rowHeight;
      const barWidth = Math.max((point.value / maxValue) * chartWidth, point.value > 0 ? 2 : 0);
      return `<text class="bar-label" x="0" y="${y + 20}">${escapeHtml(truncateLabel(point.label))}</text><rect class="bar" x="${labelWidth}" y="${y + 6}" width="${barWidth}" height="18" rx="4" style="fill:${palette[index % palette.length]}"></rect><text class="bar-value" x="${labelWidth + barWidth + 10}" y="${y + 20}">${escapeHtml(formatNumber(point.value))}</text>`;
    })
    .join("");

  chart.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(payload.value_label)} by ${escapeHtml(payload.dimension)}"><line class="axis" x1="${labelWidth}" x2="${labelWidth}" y1="0" y2="${height}"></line>${rows}</svg>`;
}

function renderColumnChart(payload) {
  const data = chartData(payload).slice(0, 20);
  if (!data.length) {
    chart.innerHTML = '<div class="chart-empty">No chart data found.</div>';
    return;
  }
  const width = 920;
  const height = 360;
  const padding = { top: 28, right: 28, bottom: 82, left: 58 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const maxValue = Math.max(...data.map((point) => point.value), 1);
  const slot = plotWidth / data.length;
  const barWidth = Math.max(Math.min(slot * 0.62, 44), 10);

  const bars = data
    .map((point, index) => {
      const barHeight = (point.value / maxValue) * plotHeight;
      const x = padding.left + index * slot + (slot - barWidth) / 2;
      const y = padding.top + plotHeight - barHeight;
      return `<rect class="bar" x="${x}" y="${y}" width="${barWidth}" height="${barHeight}" rx="5" style="fill:${palette[index % palette.length]}"></rect><text class="bar-value" x="${x + barWidth / 2}" y="${y - 7}" text-anchor="middle">${escapeHtml(formatNumber(point.value))}</text><text class="bar-label column-label" x="${x + barWidth / 2}" y="${height - 44}" text-anchor="end" transform="rotate(-40 ${x + barWidth / 2} ${height - 44})">${escapeHtml(truncateLabel(point.label, 12))}</text>`;
    })
    .join("");

  chart.innerHTML = `<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(payload.value_label)} by ${escapeHtml(payload.dimension)}"><line class="axis" x1="${padding.left}" x2="${padding.left}" y1="${padding.top}" y2="${padding.top + plotHeight}"></line><line class="axis" x1="${padding.left}" x2="${width - padding.right}" y1="${padding.top + plotHeight}" y2="${padding.top + plotHeight}"></line>${bars}</svg>`;
}

function polarToCartesian(cx, cy, radius, angleInDegrees) {
  const angleInRadians = ((angleInDegrees - 90) * Math.PI) / 180;
  return { x: cx + radius * Math.cos(angleInRadians), y: cy + radius * Math.sin(angleInRadians) };
}

function donutSegment(cx, cy, radius, startAngle, endAngle) {
  const start = polarToCartesian(cx, cy, radius, endAngle);
  const end = polarToCartesian(cx, cy, radius, startAngle);
  const largeArcFlag = endAngle - startAngle <= 180 ? "0" : "1";
  return `M ${start.x} ${start.y} A ${radius} ${radius} 0 ${largeArcFlag} 0 ${end.x} ${end.y}`;
}

function renderDonutChart(payload) {
  const data = chartData(payload).slice(0, 12);
  const total = data.reduce((sum, point) => sum + point.value, 0);
  if (!data.length || !total) {
    chart.innerHTML = '<div class="chart-empty">No chart data found.</div>';
    return;
  }
  let angle = 0;
  const paths = data
    .map((point, index) => {
      const sweep = (point.value / total) * 360;
      const path = donutSegment(220, 180, 118, angle, angle + sweep);
      angle += sweep;
      return `<path d="${path}" class="donut-segment" style="stroke:${palette[index % palette.length]}"></path>`;
    })
    .join("");

  const legend = data
    .map((point, index) => {
      const y = 62 + index * 24;
      const pct = Math.round((point.value / total) * 100);
      return `<rect x="480" y="${y - 11}" width="12" height="12" rx="3" style="fill:${palette[index % palette.length]}"></rect><text class="bar-label" x="504" y="${y}">${escapeHtml(truncateLabel(point.label, 26))}</text><text class="bar-value" x="770" y="${y}" text-anchor="end">${pct}%</text>`;
    })
    .join("");

  chart.innerHTML = `<svg viewBox="0 0 920 360" role="img" aria-label="${escapeHtml(payload.value_label)} by ${escapeHtml(payload.dimension)}">${paths}<circle cx="220" cy="180" r="76" fill="#fff"></circle><text class="donut-total" x="220" y="175" text-anchor="middle">${escapeHtml(formatNumber(total, 0))}</text><text class="bar-value" x="220" y="202" text-anchor="middle">total</text>${legend}</svg>`;
}

async function drawChart() {
  if (!state.table || !state.columns.length) return;
  const aggregation = aggregationSelect.value;
  const params = { dimension: dimensionSelect.value, aggregation, limit: "30" };
  if (aggregation !== "count") {
    if (!metricSelect.value) {
      renderError(chart, "Select a numeric metric for this aggregation.");
      return;
    }
    params.metric = metricSelect.value;
  }
  const payload = await getJson(databaseApiUrl(`/api/chart/${encodeURIComponent(state.table)}`, params));
  chartTitle.textContent = `${payload.value_label} by ${payload.dimension}`;
  chartNote.textContent = `${payload.data.length} grouped values`;
  chartTotal.textContent = formatNumber(payload.data.reduce((sum, point) => sum + Number(point.value || 0), 0));

  if (chartTypeSelect.value === "donut") renderDonutChart(payload);
  else if (chartTypeSelect.value === "column") renderColumnChart(payload);
  else renderBarChart(payload);
}

function downloadVisibleRows() {
  if (!state.visibleRows.length) return;
  const header = state.visibleColumns.map(csvCell).join(",");
  const rows = state.visibleRows.map((row) => state.visibleColumns.map((column) => csvCell(row[column])).join(","));
  const blob = new Blob([[header, ...rows].join("\n")], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `${state.table || "sqlite"}-visible-rows.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

function csvCell(value) {
  const text = String(value ?? "");
  return `"${text.replaceAll('"', '""')}"`;
}

aggregationSelect.addEventListener("change", () => {
  metricSelect.disabled = aggregationSelect.value === "count";
  drawChart().catch((error) => renderError(chart, error.message));
});
chartTypeSelect.addEventListener("change", () => drawChart().catch((error) => renderError(chart, error.message)));
dimensionSelect.addEventListener("change", () => drawChart().catch((error) => renderError(chart, error.message)));
metricSelect.addEventListener("change", () => drawChart().catch((error) => renderError(chart, error.message)));
drawChartButton.addEventListener("click", () => drawChart().catch((error) => renderError(chart, error.message)));
rowLimitSelect.addEventListener("change", () => loadRows().catch((error) => renderError(tablePreview, error.message)));
downloadCsvButton.addEventListener("click", downloadVisibleRows);
searchInput.addEventListener("input", () => {
  clearTimeout(state.searchTimer);
  state.searchTimer = setTimeout(() => loadRows().catch((error) => renderError(tablePreview, error.message)), 250);
});
uploadForm.addEventListener("submit", (event) => {
  uploadDatabase(event).catch((error) => {
    uploadButton.disabled = false;
    setDatabaseMessage(error.message, true);
  });
});

async function init() {
  clearWorkspace("Loading databases...");
  const preferredId = preferredDatabaseId();
  try {
    await loadDatabases({ preferredId });
  } catch (error) {
    if (preferredId) await loadDatabases();
    else throw error;
  }
  setDatabaseMessage("");
  await loadTables();
}

init().catch((error) => {
  setDatabaseMessage(error.message, true);
  renderError(databaseList, error.message);
  renderError(tableList, error.message);
  renderError(chart, error.message);
});

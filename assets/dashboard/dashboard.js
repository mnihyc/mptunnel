(function () {
  "use strict";

  const STATUS_ENDPOINT = "/api/status";
  const PEER_ENDPOINT = "/api/diagnostics/peer";
  const EXPECTED_SCHEMA = "mptunnel.management.v2";
  const TOKEN_STORAGE_KEY = "mptunnel.dashboard.bearer";
  const REFRESH_INTERVAL_MS = 2000;
  const REQUEST_TIMEOUT_MS = 8000;
  const STALE_AFTER_MS = 6500;
  const MAX_CHART_SAMPLES = 300;

  const elements = {
    notice: byId("notice"),
    noticeText: byId("notice-text"),
    connectionDot: byId("connection-dot"),
    connectionLabel: byId("connection-label"),
    roleLabel: byId("role-label"),
    freshnessLabel: byId("freshness-label"),
    refreshButton: byId("refresh-button"),
    accessButton: byId("access-button"),
    srStatus: byId("sr-status"),
    overviewTimestamp: byId("overview-timestamp"),
    pathsTabCount: byId("paths-tab-count"),
    sessionsTabCount: byId("sessions-tab-count"),
    pathUnderlayFilter: byId("path-underlay-filter"),
    pathStateFilter: byId("path-state-filter"),
    sessionFilter: byId("session-filter"),
    pathsBody: byId("paths-body"),
    pathsEmpty: byId("paths-empty"),
    sessionsBody: byId("sessions-body"),
    sessionsEmpty: byId("sessions-empty"),
    flowsBody: byId("flows-body"),
    flowsEmpty: byId("flows-empty"),
    trafficBreakdownBody: byId("traffic-breakdown-body"),
    servicesList: byId("services-list"),
    inboundsWrap: byId("inbounds-wrap"),
    inboundsList: byId("inbounds-list"),
    trafficChart: byId("traffic-chart"),
    trafficChartEmpty: byId("traffic-chart-empty"),
    flowsChart: byId("flows-chart"),
    flowsChartEmpty: byId("flows-chart-empty"),
    peerCapability: byId("peer-capability"),
    peerAllowBadge: byId("peer-allow-badge"),
    peerSessionSelect: byId("peer-session-select"),
    peerRequestButton: byId("peer-request-button"),
    peerRequestState: byId("peer-request-state"),
    peerResult: byId("peer-result"),
    peerResultSummary: byId("peer-result-summary"),
    peerPathsBody: byId("peer-paths-body"),
    peerPathsEmpty: byId("peer-paths-empty"),
    diagnosticsMetrics: byId("diagnostics-metrics"),
    diagnosticsNotes: byId("diagnostics-notes"),
    authDialog: byId("auth-dialog"),
    authForm: byId("auth-form"),
    authMessage: byId("auth-message"),
    authError: byId("auth-error"),
    tokenInput: byId("token-input"),
    forgetTokenButton: byId("forget-token-button"),
    authCancelButton: byId("auth-cancel-button"),
    kpiToRate: byId("kpi-to-rate"),
    kpiToTotal: byId("kpi-to-total"),
    kpiFromRate: byId("kpi-from-rate"),
    kpiFromTotal: byId("kpi-from-total"),
    kpiActiveFlows: byId("kpi-active-flows"),
    kpiFlowDetail: byId("kpi-flow-detail"),
    kpiLivePaths: byId("kpi-live-paths"),
    kpiPathDetail: byId("kpi-path-detail"),
    kpiQueue: byId("kpi-queue"),
    kpiFlight: byId("kpi-flight"),
    kpiPathRate: byId("kpi-path-rate"),
    kpiPathPacing: byId("kpi-path-pacing")
  };

  const state = {
    status: null,
    bearerToken: readStoredToken(),
    fetching: false,
    peerFetching: false,
    authenticationRequired: false,
    lastReceivedAt: 0,
    lastError: null,
    peerResult: null,
    selectedPeerSessionKey: "",
    selectedTab: "overview"
  };

  class HttpError extends Error {
    constructor(status, message, body) {
      super(message);
      this.name = "HttpError";
      this.status = status;
      this.body = body;
    }
  }

  function byId(id) {
    return document.getElementById(id);
  }

  function readStoredToken() {
    try {
      return window.sessionStorage.getItem(TOKEN_STORAGE_KEY) || "";
    } catch (_error) {
      return "";
    }
  }

  function storeToken(token) {
    try {
      if (token) {
        window.sessionStorage.setItem(TOKEN_STORAGE_KEY, token);
      } else {
        window.sessionStorage.removeItem(TOKEN_STORAGE_KEY);
      }
    } catch (_error) {
      // In-memory authentication still works when storage is unavailable.
    }
  }

  function clearToken() {
    state.bearerToken = "";
    storeToken("");
    elements.tokenInput.value = "";
  }

  function createElement(tagName, className, text) {
    const node = document.createElement(tagName);
    if (className) {
      node.className = className;
    }
    if (text !== undefined && text !== null) {
      node.textContent = String(text);
    }
    return node;
  }

  function replaceText(element, value) {
    element.textContent = value === undefined || value === null ? "--" : String(value);
  }

  function appendCell(row, label, content, className) {
    const cell = createElement("td", className || "");
    cell.dataset.label = label;
    if (content instanceof Node) {
      cell.append(content);
    } else {
      cell.textContent = content === undefined || content === null ? "--" : String(content);
    }
    row.append(cell);
    return cell;
  }

  function appendMetric(list, label, value) {
    list.append(createElement("dt", "", label), createElement("dd", "", value));
  }

  function asArray(value) {
    return Array.isArray(value) ? value : [];
  }

  function asObject(value) {
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  }

  function finiteNumber(value, fallback) {
    const number = Number(value);
    return Number.isFinite(number) ? number : (fallback === undefined ? 0 : fallback);
  }

  function unsignedBigInt(value) {
    const normalized = String(value === undefined || value === null ? "0" : value);
    if (!/^\d+$/.test(normalized)) {
      return 0n;
    }
    try {
      return BigInt(normalized);
    } catch (_error) {
      return 0n;
    }
  }

  function formatBigQuantity(value, units, base) {
    const amount = unsignedBigInt(value);
    const radix = BigInt(base);
    let divisor = 1n;
    let unitIndex = 0;
    while (unitIndex < units.length - 1 && amount >= divisor * radix) {
      divisor *= radix;
      unitIndex += 1;
    }
    if (unitIndex === 0) {
      return amount.toLocaleString() + " " + units[unitIndex];
    }
    const tenths = (amount * 10n + divisor / 2n) / divisor;
    const whole = tenths / 10n;
    const fraction = tenths % 10n;
    const valueText = fraction === 0n ? whole.toString() : whole.toString() + "." + fraction.toString();
    return valueText + " " + units[unitIndex];
  }

  function formatBytes(value) {
    return formatBigQuantity(value, ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"], 1024);
  }

  function formatCount(value) {
    return formatBigQuantity(value, ["", "K", "M", "B", "T", "Q"], 1000).trim();
  }

  function formatBitRate(value) {
    const rate = Math.max(0, finiteNumber(value));
    const units = ["bps", "Kbps", "Mbps", "Gbps", "Tbps", "Pbps"];
    let scaled = rate;
    let index = 0;
    while (scaled >= 1000 && index < units.length - 1) {
      scaled /= 1000;
      index += 1;
    }
    const precision = scaled >= 100 ? 0 : scaled >= 10 ? 1 : 2;
    let numberText = scaled.toFixed(precision);
    if (numberText.includes(".")) {
      numberText = numberText.replace(/0+$/, "").replace(/\.$/, "");
    }
    return numberText + " " + units[index];
  }

  function formatChartRate(value) {
    const rate = Math.max(0, finiteNumber(value));
    if (rate >= 1e12) return (rate / 1e12).toFixed(rate >= 1e13 ? 0 : 1) + "T";
    if (rate >= 1e9) return (rate / 1e9).toFixed(rate >= 1e10 ? 0 : 1) + "G";
    if (rate >= 1e6) return (rate / 1e6).toFixed(rate >= 1e7 ? 0 : 1) + "M";
    if (rate >= 1e3) return (rate / 1e3).toFixed(rate >= 1e4 ? 0 : 1) + "K";
    return Math.round(rate).toString();
  }

  function formatDuration(milliseconds) {
    const value = Math.max(0, finiteNumber(milliseconds));
    if (value < 1000) return Math.round(value) + " ms";
    if (value < 60000) return (value / 1000).toFixed(value < 10000 ? 1 : 0) + " s";
    if (value < 3600000) return Math.floor(value / 60000) + "m " + Math.floor((value % 60000) / 1000) + "s";
    if (value < 86400000) return Math.floor(value / 3600000) + "h " + Math.floor((value % 3600000) / 60000) + "m";
    return Math.floor(value / 86400000) + "d " + Math.floor((value % 86400000) / 3600000) + "h";
  }

  function formatRelative(timestamp) {
    const elapsed = Date.now() - finiteNumber(timestamp);
    if (!timestamp || elapsed < 0) return "just now";
    if (elapsed < 1500) return "just now";
    if (elapsed < 60000) return Math.floor(elapsed / 1000) + "s ago";
    if (elapsed < 3600000) return Math.floor(elapsed / 60000) + "m ago";
    if (elapsed < 86400000) return Math.floor(elapsed / 3600000) + "h ago";
    return Math.floor(elapsed / 86400000) + "d ago";
  }

  function formatWallTime(timestamp) {
    const date = new Date(finiteNumber(timestamp));
    if (Number.isNaN(date.getTime())) return "--";
    return new Intl.DateTimeFormat(undefined, {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit"
    }).format(date);
  }

  function formatRtt(milliseconds) {
    const value = finiteNumber(milliseconds);
    if (value <= 0) return "--";
    return (value < 10 ? value.toFixed(2) : value < 100 ? value.toFixed(1) : Math.round(value).toString()) + " ms";
  }

  function formatRttMicros(microseconds) {
    const value = finiteNumber(microseconds);
    return value > 0 ? formatRtt(value / 1000) : "--";
  }

  function formatPpm(value) {
    const ppm = Math.max(0, finiteNumber(value));
    if (ppm === 0) return "0%";
    const percent = ppm / 10000;
    if (percent < 0.001) return "<0.001%";
    return (percent < 0.1 ? percent.toFixed(3) : percent < 10 ? percent.toFixed(2) : percent.toFixed(1)) + "%";
  }

  function formatIdentifier(value) {
    const stringValue = String(value === undefined || value === null || value === "" ? "--" : value);
    return stringValue;
  }

  function titleCase(value) {
    return String(value || "unknown")
      .split(/[_-]/)
      .filter(Boolean)
      .map(function (part) { return part.charAt(0).toUpperCase() + part.slice(1); })
      .join(" ");
  }

  function carrierLabel(underlay) {
    return underlay === "udp" ? "QUIC" : underlay === "tcp" ? "TCP" : titleCase(underlay);
  }

  function serviceLabel(item) {
    const tag = item && item.service_tag ? String(item.service_tag) : "";
    const service = titleCase(item && item.service ? item.service : "service");
    const index = finiteNumber(item && item.service_index);
    return tag || service + " " + index;
  }

  function safeStateClass(value) {
    const stateName = String(value || "unknown").toLowerCase();
    const allowed = [
      "active", "listening", "available", "suspect", "draining",
      "backup", "failed", "disabled", "unavailable"
    ];
    return allowed.includes(stateName) ? stateName : "unknown";
  }

  function stateIndicator(value) {
    const normalized = String(value || "unknown").toLowerCase();
    return createElement(
      "span",
      "state-cell state-cell--" + safeStateClass(normalized),
      titleCase(normalized)
    );
  }

  function badge(text, kind) {
    return createElement("span", "badge badge--" + (kind || "neutral"), text);
  }

  async function requestJson(path, options) {
    const requestOptions = options || {};
    const controller = new AbortController();
    const timeout = window.setTimeout(function () { controller.abort(); }, REQUEST_TIMEOUT_MS);
    const headers = new Headers(requestOptions.headers || {});
    headers.set("Accept", "application/json");
    if (state.bearerToken) {
      headers.set("Authorization", "Bearer " + state.bearerToken);
    }
    try {
      const response = await window.fetch(path, {
        method: requestOptions.method || "GET",
        headers: headers,
        body: requestOptions.body,
        credentials: "omit",
        cache: "no-store",
        signal: controller.signal
      });
      const responseText = await response.text();
      let body = null;
      if (responseText) {
        try {
          body = JSON.parse(responseText);
        } catch (_error) {
          throw new HttpError(response.status, "Management API returned invalid JSON", null);
        }
      }
      if (!response.ok) {
        const message = body && typeof body.error === "string"
          ? body.error
          : "Management API request failed (HTTP " + response.status + ")";
        throw new HttpError(response.status, message, body);
      }
      return body;
    } catch (error) {
      if (error && error.name === "AbortError") {
        throw new Error("Management API request timed out");
      }
      throw error;
    } finally {
      window.clearTimeout(timeout);
    }
  }

  function validateStatus(payload) {
    if (!payload || typeof payload !== "object") {
      throw new Error("Management status response is empty");
    }
    if (payload.schema !== EXPECTED_SCHEMA) {
      throw new Error("Unsupported management schema: " + String(payload.schema || "missing"));
    }
    return payload;
  }

  async function refreshStatus(source) {
    if (state.fetching) return;
    if (!state.bearerToken) {
      handleUnauthorized("Authentication required");
      return;
    }
    state.fetching = true;
    elements.refreshButton.disabled = true;
    elements.refreshButton.setAttribute("aria-busy", "true");
    updateConnectionState();
    try {
      const payload = validateStatus(await requestJson(STATUS_ENDPOINT));
      state.status = payload;
      state.lastReceivedAt = Date.now();
      state.lastError = null;
      state.authenticationRequired = false;
      renderDashboard();
      if (source === "manual" || source === "auth") {
        announce("Runtime status refreshed");
      }
    } catch (error) {
      state.lastError = error;
      if (error instanceof HttpError && error.status === 401) {
        handleUnauthorized(source === "auth" ? "Token rejected" : "Authentication required");
      } else {
        updateConnectionState();
        if (source === "manual") {
          announce(error && error.message ? error.message : "Status refresh failed");
        }
      }
    } finally {
      state.fetching = false;
      elements.refreshButton.disabled = false;
      elements.refreshButton.removeAttribute("aria-busy");
      updateConnectionState();
    }
  }

  function handleUnauthorized(message) {
    state.authenticationRequired = true;
    clearToken();
    state.lastError = new HttpError(401, message, null);
    updateConnectionState();
    showAuthDialog(message);
  }

  function showAuthDialog(message) {
    elements.authMessage.textContent = message || "Enter the token configured for this endpoint.";
    elements.authError.textContent = "";
    elements.tokenInput.value = "";
    if (!elements.authDialog.open) {
      elements.authDialog.showModal();
    }
    window.setTimeout(function () { elements.tokenInput.focus(); }, 0);
  }

  function closeAuthDialog() {
    elements.tokenInput.value = "";
    elements.authError.textContent = "";
    if (elements.authDialog.open) {
      elements.authDialog.close();
    }
  }

  function announce(message) {
    elements.srStatus.textContent = "";
    window.setTimeout(function () { elements.srStatus.textContent = message; }, 10);
  }

  function setNotice(kind, message, visible) {
    elements.notice.className = "notice notice--" + kind;
    elements.notice.hidden = !visible;
    elements.noticeText.textContent = message;
  }

  function setConnection(kind, label, freshness) {
    elements.connectionDot.className = "status-dot status-dot--" + kind;
    elements.connectionLabel.textContent = label;
    elements.freshnessLabel.textContent = freshness;
  }

  function updateConnectionState() {
    const status = state.status;
    if (state.authenticationRequired) {
      setConnection("error", "Access required", "Status unavailable");
      setNotice("error", "Authentication is required for management status.", true);
      return;
    }
    if (!status) {
      if (state.fetching) {
        setConnection("loading", "Connecting", "Waiting for status");
        setNotice("loading", "Loading runtime status", true);
      } else if (state.lastError) {
        setConnection("offline", "Offline", "No status received");
        setNotice("offline", state.lastError.message || "Management API is unavailable.", true);
      } else {
        setConnection("loading", "Connecting", "Waiting for status");
        setNotice("loading", "Loading runtime status", true);
      }
      return;
    }

    const generatedAt = finiteNumber(status.generated_unix_ms);
    const sampleAge = generatedAt > 0 ? Math.max(0, Date.now() - generatedAt) : Infinity;
    const freshness = generatedAt > 0 ? "Updated " + formatRelative(generatedAt) : "Update time unavailable";
    if (state.lastError) {
      setConnection("offline", "Refresh failed", freshness);
      setNotice("offline", "Live refresh failed. Showing the last received runtime sample.", true);
    } else if (sampleAge > STALE_AFTER_MS) {
      setConnection("stale", "Stale", freshness);
      setNotice("stale", "Runtime status is stale. The last received sample remains visible.", true);
    } else if (state.fetching) {
      setConnection("loading", "Refreshing", freshness);
      setNotice("loading", "Refreshing runtime status", true);
    } else {
      setConnection("live", "Live", freshness);
      setNotice("loading", "", false);
    }
  }

  function renderDashboard() {
    if (!state.status) return;
    renderHeader();
    renderKpis();
    renderTrafficBreakdown();
    renderServices();
    renderPaths();
    renderSessions();
    renderDiagnostics();
    drawCharts();
    updateConnectionState();
  }

  function renderHeader() {
    const status = state.status;
    replaceText(elements.roleLabel, titleCase(status.role));
    elements.overviewTimestamp.textContent = "Sample " + formatWallTime(status.generated_unix_ms) + " / " + formatRelative(status.generated_unix_ms);
    elements.pathsTabCount.textContent = String(asArray(status.paths).length);
    elements.sessionsTabCount.textContent = String(asArray(status.sessions).length);
  }

  function renderKpis() {
    const summary = asObject(state.status.summary);
    const traffic = asObject(state.status.traffic);
    const total = asObject(traffic.total);
    const rates = asObject(traffic.rates);
    const activePaths = finiteNumber(summary.active_paths);
    const livePathCount = activePaths;

    replaceText(elements.kpiToRate, formatBitRate(rates.to_peer_bps));
    replaceText(elements.kpiToTotal, formatBytes(total.to_peer_bytes) + " transferred");
    replaceText(elements.kpiFromRate, formatBitRate(rates.from_peer_bps));
    replaceText(elements.kpiFromTotal, formatBytes(total.from_peer_bytes) + " transferred");
    replaceText(elements.kpiActiveFlows, formatCount(summary.active_flows));
    replaceText(
      elements.kpiFlowDetail,
      formatCount(summary.active_reliable_flows) + " reliable / " +
        formatCount(summary.active_datagram_flows) + " datagram"
    );
    replaceText(elements.kpiLivePaths, String(livePathCount) + " / " + String(finiteNumber(summary.path_count)));
    replaceText(
      elements.kpiPathDetail,
      String(finiteNumber(summary.configured_path_count)) + " configured / " +
        String(finiteNumber(summary.suspect_paths)) + " suspect / " +
        String(finiteNumber(summary.failed_paths)) + " failed"
    );
    replaceText(elements.kpiQueue, formatBytes(summary.queue_bytes));
    replaceText(elements.kpiFlight, formatBytes(summary.bytes_in_flight) + " in flight");
    replaceText(elements.kpiPathRate, formatBitRate(summary.path_delivery_rate_bps));
    replaceText(elements.kpiPathPacing, formatBitRate(summary.path_pacing_rate_bps) + " pacing");
  }

  function renderTrafficBreakdown() {
    const traffic = asObject(state.status.traffic);
    elements.trafficBreakdownBody.replaceChildren();
    [
      ["Reliable", asObject(traffic.reliable)],
      ["Datagram", asObject(traffic.datagram)]
    ].forEach(function (entry) {
      const kind = entry[1];
      const io = asObject(kind.io);
      const flows = asObject(kind.flows);
      const row = createElement("tr");
      appendCell(row, "Class", entry[0], "cell-primary");
      appendCell(row, "To peer", formatBytes(io.to_peer_bytes));
      appendCell(row, "From peer", formatBytes(io.from_peer_bytes));
      appendCell(row, "Active", formatCount(flows.active));
      appendCell(row, "Completed", formatCount(flows.completed));
      appendCell(row, "Failed", formatCount(flows.failed));
      elements.trafficBreakdownBody.append(row);
    });
  }

  function renderServices() {
    const services = asObject(state.status.services);
    elements.servicesList.replaceChildren();
    appendMetric(elements.servicesList, "MPP outbounds", formatCount(services.mpp_outbounds));
    appendMetric(elements.servicesList, "MPP inbounds", formatCount(services.mpp_inbounds));
    appendMetric(elements.servicesList, "Local inbounds", formatCount(services.local_inbounds));
    appendMetric(elements.servicesList, "Path listeners", formatCount(services.configured_path_listeners));
    appendMetric(elements.servicesList, "Uptime", formatDuration(state.status.uptime_ms));
    appendMetric(elements.servicesList, "Schema", String(state.status.schema || "--"));

    const inbounds = asArray(state.status.local_inbounds);
    elements.inboundsList.replaceChildren();
    elements.inboundsWrap.hidden = inbounds.length === 0;
    inbounds.forEach(function (inbound) {
      const row = createElement("li");
      const name = createElement("strong", "", inbound.tag || titleCase(inbound.protocol));
      const details = [];
      details.push(titleCase(inbound.protocol));
      if (asArray(inbound.listen).length > 0) details.push(asArray(inbound.listen).join(", "));
      if (inbound.name) details.push(String(inbound.name));
      if (inbound.auth_required) details.push("authenticated");
      row.append(name, createElement("span", "", details.join(" / ")));
      elements.inboundsList.append(row);
    });
  }

  function pathEffectiveState(path) {
    return path && path.manual_disabled ? "disabled" : String(path && path.state ? path.state : "unknown").toLowerCase();
  }

  function renderPaths() {
    const underlayFilter = elements.pathUnderlayFilter.value;
    const stateFilter = elements.pathStateFilter.value;
    const paths = asArray(state.status.paths).filter(function (path) {
      const underlayMatches = underlayFilter === "all" || path.underlay === underlayFilter;
      const currentState = pathEffectiveState(path);
      const stateMatches = stateFilter === "all" || currentState === stateFilter;
      return underlayMatches && stateMatches;
    });
    elements.pathsBody.replaceChildren();
    elements.pathsEmpty.hidden = paths.length !== 0;
    paths.forEach(function (path) {
      const row = createElement("tr");
      const currentState = pathEffectiveState(path);
      appendCell(row, "State", stateIndicator(currentState));

      const identity = createElement("div");
      const pathName = path.path_id !== undefined && path.path_id !== null
        ? "Path " + formatIdentifier(path.path_id)
        : path.configured_index !== undefined && path.configured_index !== null
          ? "Configured " + String(path.configured_index)
          : formatIdentifier(path.id);
      identity.append(createElement("span", "cell-primary", pathName));
      identity.append(createElement("span", "cell-secondary", path.endpoint || formatIdentifier(path.id)));
      appendCell(row, "Path", identity);

      const service = createElement("div");
      service.append(createElement("span", "cell-primary", serviceLabel(path)));
      service.append(createElement("span", "cell-secondary", titleCase(path.service) + " " + finiteNumber(path.service_index)));
      appendCell(row, "Service", service);

      const carrier = createElement("div");
      carrier.append(createElement("span", "cell-primary", carrierLabel(path.underlay)));
      carrier.append(createElement("span", "cell-secondary", path.underlay === "udp" ? "UDP underlay" : "TCP underlay"));
      appendCell(row, "Carrier", carrier);

      appendCell(row, "Usage", path.usage ? stateIndicator(path.usage) : "--");

      const rtt = createElement("div");
      rtt.append(createElement("span", "cell-primary", formatRtt(path.srtt_ms)));
      rtt.append(createElement("span", "cell-secondary", "jitter " + formatRtt(path.jitter_ms)));
      appendCell(row, "RTT", rtt);

      const delivery = createElement("div");
      delivery.append(createElement("span", "cell-primary", formatBitRate(path.delivery_rate_bps)));
      delivery.append(createElement("span", "cell-secondary", formatBitRate(path.pacing_rate_bps) + " pacing"));
      appendCell(row, "Delivery", delivery);

      const loss = createElement("div");
      loss.append(createElement("span", "cell-primary", formatPpm(path.loss_ppm)));
      loss.append(createElement("span", "cell-secondary", formatPpm(path.ecn_ppm) + " ECN"));
      appendCell(row, "Loss", loss);

      appendCell(row, "Queue", formatBytes(path.queue_bytes));

      const flight = createElement("div");
      flight.append(createElement("span", "cell-primary", formatBytes(path.bytes_in_flight)));
      flight.append(createElement("span", "cell-secondary", formatBytes(path.data_level_bytes_in_flight) + " data level"));
      appendCell(row, "In flight", flight);

      const flows = createElement("div");
      flows.append(createElement("span", "cell-primary", formatCount(path.active_flows)));
      flows.append(createElement("span", "cell-secondary", formatCount(path.active_latency_sensitive_flows) + " latency sensitive"));
      appendCell(row, "Flows", flows);
      elements.pathsBody.append(row);
    });
  }

  function renderSessions() {
    const query = elements.sessionFilter.value.trim().toLowerCase();
    const sessions = asArray(state.status.sessions).filter(function (session) {
      if (!query) return true;
      return [
        session.state,
        session.session_id,
        session.service,
        session.service_tag,
        session.service_index
      ].some(function (value) { return String(value === undefined || value === null ? "" : value).toLowerCase().includes(query); });
    });
    elements.sessionsBody.replaceChildren();
    elements.sessionsEmpty.hidden = sessions.length !== 0;
    sessions.forEach(function (session) {
      const row = createElement("tr");
      appendCell(row, "State", stateIndicator(session.state));
      appendCell(row, "Session", formatIdentifier(session.session_id), "cell-mono");
      appendCell(row, "Service", serviceLabel(session));
      appendCell(row, "Carriers", formatCount(session.carrier_count));
      const countSuffix = session.active_flow_counts_complete === false ? "+" : "";
      appendCell(row, "Reliable flows", formatCount(session.active_reliable_flows) + countSuffix);
      appendCell(row, "Datagram flows", formatCount(session.active_datagram_flows) + countSuffix);
      elements.sessionsBody.append(row);
    });

    const flows = asArray(state.status.flows).filter(function (flow) {
      if (!query) return true;
      return [
        flow.flow_kind,
        flow.flow_id,
        flow.session_id,
        flow.target,
        flow.service,
        flow.service_tag,
        flow.service_index
      ].some(function (value) { return String(value === undefined || value === null ? "" : value).toLowerCase().includes(query); });
    });
    elements.flowsBody.replaceChildren();
    elements.flowsEmpty.hidden = flows.length !== 0;
    flows.forEach(function (flow) {
      const row = createElement("tr");
      appendCell(row, "Kind", badge(titleCase(flow.flow_kind), flow.flow_kind === "datagram" ? "warning" : "neutral"));
      appendCell(row, "Flow", formatIdentifier(flow.flow_id), "cell-mono");
      appendCell(row, "Service", serviceLabel(flow));
      appendCell(row, "Session", formatIdentifier(flow.session_id), "cell-mono");
      appendCell(row, "Target", flow.target ? String(flow.target) : "Multiple targets");
      appendCell(row, "Age", formatDuration(flow.age_ms));
      appendCell(row, "Idle", formatDuration(flow.idle_ms));
      const io = asObject(flow.io);
      appendCell(row, "To peer", formatBytes(io.to_peer_bytes));
      appendCell(row, "From peer", formatBytes(io.from_peer_bytes));
      elements.flowsBody.append(row);
    });
  }

  function peerControl() {
    return asObject(asObject(state.status.controls).peer_diagnostics);
  }

  function renderDiagnostics() {
    const diagnostics = asObject(state.status.diagnostics);
    const control = peerControl();
    const peerSessions = asArray(diagnostics.peer_sessions).map(asObject);

    elements.peerAllowBadge.className = "badge " + (diagnostics.peer_diagnostics_allowed ? "badge--success" : "badge--neutral");
    elements.peerAllowBadge.textContent = diagnostics.peer_diagnostics_allowed ? "Incoming allowed" : "Incoming off";

    elements.peerSessionSelect.replaceChildren();
    if (peerSessions.length === 0) {
      elements.peerSessionSelect.append(createElement("option", "", "No connected peer"));
      elements.peerSessionSelect.disabled = true;
      state.selectedPeerSessionKey = "";
    } else {
      peerSessions.forEach(function (session) {
        const option = createElement(
          "option",
          "",
          serviceLabel(session) + " / Session " + formatIdentifier(session.session_id)
        );
        option.value = peerSessionKey(session);
        elements.peerSessionSelect.append(option);
      });
      const keys = peerSessions.map(peerSessionKey);
      if (!keys.includes(state.selectedPeerSessionKey)) {
        state.selectedPeerSessionKey = keys[0];
      }
      elements.peerSessionSelect.value = state.selectedPeerSessionKey;
      elements.peerSessionSelect.disabled = false;
    }

    const supported = Boolean(control.supported) && peerSessions.length > 0;
    elements.peerRequestButton.disabled = !supported || state.peerFetching;
    elements.peerRequestButton.setAttribute("aria-busy", state.peerFetching ? "true" : "false");
    if (supported) {
      elements.peerCapability.textContent = peerSessions.length + " authenticated peer " + (peerSessions.length === 1 ? "session" : "sessions");
    } else {
      elements.peerCapability.textContent = control.reason || "No authenticated peer control carrier";
    }

    elements.diagnosticsMetrics.replaceChildren();
    appendMetric(elements.diagnosticsMetrics, "Peer sessions", formatCount(peerSessions.length));
    appendMetric(elements.diagnosticsMetrics, "Cached peer results", formatCount(asArray(diagnostics.peer_results).length));
    appendMetric(elements.diagnosticsMetrics, "Flow detail capacity", formatCount(diagnostics.active_flow_detail_capacity));
    appendMetric(elements.diagnosticsMetrics, "Active flow detail overflow", formatCount(diagnostics.active_flow_detail_overflow));
    appendMetric(elements.diagnosticsMetrics, "Flow detail overflow total", formatCount(diagnostics.active_flow_detail_overflow_total));
    appendMetric(elements.diagnosticsMetrics, "Path control", asObject(asObject(state.status.controls).path).supported ? "Available" : "Unavailable");
    appendMetric(elements.diagnosticsMetrics, "Peer responses", diagnostics.peer_diagnostics_allowed ? "Allowed" : "Disabled");

    elements.diagnosticsNotes.replaceChildren();
    asArray(diagnostics.notes).forEach(function (note) {
      elements.diagnosticsNotes.append(createElement("li", "", String(note)));
    });

    renderSelectedPeerResult();
  }

  function peerSessionKey(session) {
    return String(session.service) + ":" + String(session.service_index) + ":" + String(session.session_id);
  }

  function selectedPeerSession() {
    return asArray(asObject(state.status.diagnostics).peer_sessions)
      .map(asObject)
      .find(function (session) { return peerSessionKey(session) === state.selectedPeerSessionKey; }) || null;
  }

  function newestCachedPeerResult(session) {
    if (!session) return null;
    const results = asArray(asObject(state.status.diagnostics).peer_results)
      .filter(function (result) {
        return String(result.session_id) === String(session.session_id) &&
          String(result.service) === String(session.service) &&
          Number(result.service_index) === Number(session.service_index);
      })
      .sort(function (left, right) { return finiteNumber(right.received_unix_ms) - finiteNumber(left.received_unix_ms); });
    return results[0] || null;
  }

  function renderSelectedPeerResult() {
    const selectedSession = selectedPeerSession();
    const explicit = state.peerResult && selectedSession &&
      String(state.peerResult.session_id) === String(selectedSession.session_id) &&
      String(state.peerResult.service) === String(selectedSession.service) &&
      Number(state.peerResult.service_index) === Number(selectedSession.service_index)
      ? state.peerResult
      : null;
    renderPeerResult(explicit || newestCachedPeerResult(selectedSession));
  }

  function renderPeerResult(result) {
    if (!result) {
      elements.peerResult.hidden = true;
      elements.peerResultSummary.replaceChildren();
      elements.peerPathsBody.replaceChildren();
      return;
    }
    elements.peerResult.hidden = false;
    elements.peerResultSummary.replaceChildren();
    appendMetric(elements.peerResultSummary, "Result", titleCase(result.code));
    appendMetric(elements.peerResultSummary, "Service", serviceLabel(result));
    appendMetric(elements.peerResultSummary, "Session", formatIdentifier(result.session_id));
    appendMetric(elements.peerResultSummary, "Request", formatIdentifier(result.request_id));
    appendMetric(elements.peerResultSummary, "Received", formatRelative(result.received_unix_ms));

    const paths = asArray(result.paths);
    elements.peerPathsBody.replaceChildren();
    elements.peerPathsEmpty.hidden = paths.length !== 0;
    paths.forEach(function (path) {
      const row = createElement("tr");
      appendCell(row, "State", stateIndicator(path.state));
      appendCell(row, "Path", formatIdentifier(path.path_id), "cell-mono");
      appendCell(row, "Carrier", carrierLabel(path.underlay));
      appendCell(row, "Usage", stateIndicator(path.usage));

      const rtt = createElement("div");
      rtt.append(createElement("span", "cell-primary", formatRttMicros(path.srtt_us)));
      rtt.append(createElement("span", "cell-secondary", "jitter " + formatRttMicros(path.jitter_us)));
      appendCell(row, "RTT", rtt);

      const delivery = createElement("div");
      delivery.append(createElement("span", "cell-primary", formatBitRate(path.delivery_rate_bps)));
      delivery.append(createElement("span", "cell-secondary", formatBitRate(path.pacing_rate_bps) + " pacing"));
      appendCell(row, "Delivery", delivery);

      const loss = createElement("div");
      loss.append(createElement("span", "cell-primary", formatPpm(path.loss_ppm)));
      loss.append(createElement("span", "cell-secondary", formatPpm(path.ecn_ppm) + " ECN"));
      appendCell(row, "Loss", loss);
      appendCell(row, "Queue", formatBytes(path.queue_bytes));
      appendCell(row, "In flight", formatBytes(path.bytes_in_flight));
      elements.peerPathsBody.append(row);
    });
  }

  async function requestPeerStatus() {
    if (state.peerFetching) return;
    const session = selectedPeerSession();
    if (!session) return;
    const payload = {
      service: session.service,
      service_index: session.service_index,
      session_id: session.session_id
    };

    state.peerFetching = true;
    elements.peerRequestButton.disabled = true;
    elements.peerRequestButton.setAttribute("aria-busy", "true");
    elements.peerRequestState.className = "inline-status is-loading";
    elements.peerRequestState.textContent = "Requesting current peer path status";
    try {
      const result = await requestJson(PEER_ENDPOINT, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });
      state.peerResult = result;
      elements.peerRequestState.className = "inline-status";
      elements.peerRequestState.textContent = "Peer response received " + formatRelative(result.received_unix_ms);
      renderSelectedPeerResult();
      announce("Peer path status received");
      await refreshStatus("peer");
    } catch (error) {
      if (error instanceof HttpError && error.status === 401) {
        handleUnauthorized("Authentication required for peer diagnostics");
      }
      elements.peerRequestState.className = "inline-status is-error";
      elements.peerRequestState.textContent = error && error.message ? error.message : "Peer diagnostics request failed";
      announce(elements.peerRequestState.textContent);
    } finally {
      state.peerFetching = false;
      elements.peerRequestButton.removeAttribute("aria-busy");
      if (state.status) renderDiagnostics();
    }
  }

  function switchTab(tabName, focusTab) {
    const tabs = Array.from(document.querySelectorAll("[role='tab'][data-tab]"));
    const selected = tabs.find(function (tab) { return tab.dataset.tab === tabName; }) || tabs[0];
    if (!selected) return;
    state.selectedTab = selected.dataset.tab;
    tabs.forEach(function (tab) {
      const active = tab === selected;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
      tab.tabIndex = active ? 0 : -1;
      const panel = byId(tab.getAttribute("aria-controls"));
      if (panel) panel.hidden = !active;
    });
    if (focusTab) selected.focus();
    if (state.selectedTab === "overview") {
      window.requestAnimationFrame(drawCharts);
    }
  }

  function handleTabKeydown(event) {
    const tabs = Array.from(document.querySelectorAll("[role='tab'][data-tab]"));
    const currentIndex = tabs.indexOf(event.currentTarget);
    if (currentIndex < 0) return;
    let nextIndex = null;
    if (event.key === "ArrowRight") nextIndex = (currentIndex + 1) % tabs.length;
    if (event.key === "ArrowLeft") nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
    if (event.key === "Home") nextIndex = 0;
    if (event.key === "End") nextIndex = tabs.length - 1;
    if (nextIndex === null) return;
    event.preventDefault();
    switchTab(tabs[nextIndex].dataset.tab, true);
  }

  function niceMaximum(value, integerOnly) {
    const maximum = Math.max(integerOnly ? 1 : 0.001, value);
    const exponent = Math.floor(Math.log10(maximum));
    const magnitude = Math.pow(10, exponent);
    const normalized = maximum / magnitude;
    let step;
    if (normalized <= 1) step = 1;
    else if (normalized <= 2) step = 2;
    else if (normalized <= 5) step = 5;
    else step = 10;
    const result = step * magnitude;
    if (!integerOnly) return result;
    // Four equal grid intervals need an integer multiple of four; otherwise
    // rounded flow labels can repeat even though the grid lines differ.
    return Math.ceil(Math.max(4, result) / 4) * 4;
  }

  function prepareCanvas(canvas) {
    const frame = canvas.parentElement;
    const rect = frame.getBoundingClientRect();
    const width = Math.max(280, Math.floor(rect.width));
    const height = Math.max(180, Math.floor(rect.height));
    const dpr = Math.min(2, Math.max(1, window.devicePixelRatio || 1));
    const targetWidth = Math.floor(width * dpr);
    const targetHeight = Math.floor(height * dpr);
    if (canvas.width !== targetWidth || canvas.height !== targetHeight) {
      canvas.width = targetWidth;
      canvas.height = targetHeight;
    }
    const context = canvas.getContext("2d");
    context.setTransform(dpr, 0, 0, dpr, 0, 0);
    context.clearRect(0, 0, width, height);
    return { context: context, width: width, height: height };
  }

  function drawLineChart(canvas, samples, series, options) {
    const surface = prepareCanvas(canvas);
    const context = surface.context;
    const width = surface.width;
    const height = surface.height;
    const padding = { top: 14, right: 12, bottom: 29, left: 50 };
    const plotWidth = Math.max(1, width - padding.left - padding.right);
    const plotHeight = Math.max(1, height - padding.top - padding.bottom);
    const values = [];
    series.forEach(function (line) {
      samples.forEach(function (sample) { values.push(Math.max(0, finiteNumber(line.value(sample)))); });
    });
    const maximum = niceMaximum(values.length > 0 ? Math.max.apply(null, values) : 0, options.integerOnly);

    context.fillStyle = "#ffffff";
    context.fillRect(0, 0, width, height);
    context.font = "11px ui-sans-serif, system-ui, sans-serif";
    context.textBaseline = "middle";
    context.lineWidth = 1;

    for (let grid = 0; grid <= 4; grid += 1) {
      const ratio = grid / 4;
      const y = padding.top + plotHeight * ratio;
      const axisValue = maximum * (1 - ratio);
      context.beginPath();
      context.strokeStyle = grid === 4 ? "#b8c3ce" : "#e4e9ee";
      context.moveTo(padding.left, Math.round(y) + 0.5);
      context.lineTo(width - padding.right, Math.round(y) + 0.5);
      context.stroke();
      context.fillStyle = "#6b7885";
      context.textAlign = "right";
      context.fillText(options.axisLabel(axisValue), padding.left - 7, y);
    }

    const firstTime = samples.length > 0 ? finiteNumber(samples[0].timestamp_unix_ms) : 0;
    const lastTime = samples.length > 0 ? finiteNumber(samples[samples.length - 1].timestamp_unix_ms) : 0;
    context.fillStyle = "#6b7885";
    context.textBaseline = "bottom";
    context.textAlign = "left";
    context.fillText(firstTime ? formatWallTime(firstTime) : "--", padding.left, height - 3);
    context.textAlign = "right";
    context.fillText(lastTime ? formatWallTime(lastTime) : "--", width - padding.right, height - 3);

    if (samples.length === 0) return;
    const timeSpan = Math.max(1, lastTime - firstTime);
    series.forEach(function (line) {
      context.beginPath();
      context.lineWidth = 2;
      context.lineJoin = "round";
      context.lineCap = "round";
      context.strokeStyle = line.color;
      samples.forEach(function (sample, index) {
        const timestamp = finiteNumber(sample.timestamp_unix_ms);
        const xRatio = samples.length === 1 ? 1 : (timestamp - firstTime) / timeSpan;
        const x = padding.left + Math.max(0, Math.min(1, xRatio)) * plotWidth;
        const value = Math.max(0, finiteNumber(line.value(sample)));
        const y = padding.top + plotHeight - Math.min(1, value / maximum) * plotHeight;
        if (index === 0) context.moveTo(x, y);
        else context.lineTo(x, y);
      });
      context.stroke();
      if (samples.length === 1) {
        const value = Math.max(0, finiteNumber(line.value(samples[0])));
        const y = padding.top + plotHeight - Math.min(1, value / maximum) * plotHeight;
        context.beginPath();
        context.fillStyle = line.color;
        context.arc(padding.left + plotWidth, y, 3, 0, Math.PI * 2);
        context.fill();
      }
    });
  }

  function drawCharts() {
    if (!state.status || state.selectedTab !== "overview") return;
    const trends = asArray(asObject(state.status.traffic).trends).slice(-MAX_CHART_SAMPLES);
    const rateHasData = trends.length > 1 && trends.some(function (sample) {
      return finiteNumber(sample.to_peer_bps) > 0 || finiteNumber(sample.from_peer_bps) > 0;
    });
    elements.trafficChartEmpty.hidden = rateHasData;
    elements.flowsChartEmpty.hidden = trends.length > 0;
    drawLineChart(
      elements.trafficChart,
      trends,
      [
        { color: "#2563b9", value: function (sample) { return sample.to_peer_bps; } },
        { color: "#198754", value: function (sample) { return sample.from_peer_bps; } }
      ],
      { integerOnly: false, axisLabel: formatChartRate }
    );
    drawLineChart(
      elements.flowsChart,
      trends,
      [{ color: "#a45b08", value: function (sample) { return sample.active_flows; } }],
      { integerOnly: true, axisLabel: function (value) { return Math.round(value).toString(); } }
    );
    const rates = asObject(asObject(state.status.traffic).rates);
    elements.trafficChart.setAttribute(
      "aria-label",
      "Forwarded traffic history. Current to peer " + formatBitRate(rates.to_peer_bps) +
        ", from peer " + formatBitRate(rates.from_peer_bps) + "."
    );
    elements.flowsChart.setAttribute(
      "aria-label",
      "Active flow history. Current active flows " + formatCount(asObject(state.status.summary).active_flows) + "."
    );
  }

  function bindEvents() {
    Array.from(document.querySelectorAll("[role='tab'][data-tab]")).forEach(function (tab) {
      tab.addEventListener("click", function () { switchTab(tab.dataset.tab, false); });
      tab.addEventListener("keydown", handleTabKeydown);
    });
    elements.refreshButton.addEventListener("click", function () { refreshStatus("manual"); });
    elements.accessButton.addEventListener("click", function () {
      showAuthDialog(state.bearerToken ? "Replace the bearer token stored for this tab." : "Enter the token configured for this endpoint.");
    });
    elements.pathUnderlayFilter.addEventListener("change", renderPaths);
    elements.pathStateFilter.addEventListener("change", renderPaths);
    elements.sessionFilter.addEventListener("input", renderSessions);
    elements.peerSessionSelect.addEventListener("change", function () {
      state.selectedPeerSessionKey = elements.peerSessionSelect.value;
      renderSelectedPeerResult();
    });
    elements.peerRequestButton.addEventListener("click", requestPeerStatus);

    elements.authForm.addEventListener("submit", function (event) {
      event.preventDefault();
      const token = elements.tokenInput.value;
      if (!token) {
        elements.authError.textContent = "A token is required.";
        elements.tokenInput.focus();
        return;
      }
      state.bearerToken = token;
      storeToken(token);
      state.authenticationRequired = false;
      closeAuthDialog();
      refreshStatus("auth");
    });
    elements.forgetTokenButton.addEventListener("click", function () {
      clearToken();
      state.authenticationRequired = false;
      closeAuthDialog();
      announce("Stored management token removed");
      refreshStatus("manual");
    });
    elements.authCancelButton.addEventListener("click", closeAuthDialog);
    elements.authDialog.addEventListener("close", function () {
      elements.tokenInput.value = "";
      elements.authError.textContent = "";
    });
    elements.authDialog.addEventListener("cancel", function () {
      elements.tokenInput.value = "";
      elements.authError.textContent = "";
    });

    document.addEventListener("visibilitychange", function () {
      if (!document.hidden && !state.authenticationRequired) {
        refreshStatus("visibility");
      }
    });

    if ("ResizeObserver" in window) {
      const observer = new ResizeObserver(function () {
        if (state.selectedTab === "overview") window.requestAnimationFrame(drawCharts);
      });
      observer.observe(elements.trafficChart.parentElement);
      observer.observe(elements.flowsChart.parentElement);
    } else {
      window.addEventListener("resize", function () { window.requestAnimationFrame(drawCharts); });
    }
  }

  function startPolling() {
    window.setInterval(function () {
      if (!document.hidden && !state.authenticationRequired && !state.fetching) {
        refreshStatus("poll");
      }
    }, REFRESH_INTERVAL_MS);
    window.setInterval(function () {
      if (!document.hidden) {
        updateConnectionState();
        if (state.status) {
          elements.overviewTimestamp.textContent = "Sample " + formatWallTime(state.status.generated_unix_ms) + " / " + formatRelative(state.status.generated_unix_ms);
        }
        if (state.status && !elements.peerResult.hidden) {
          renderSelectedPeerResult();
        }
      }
    }, 1000);
  }

  function initialize() {
    bindEvents();
    switchTab("overview", false);
    updateConnectionState();
    refreshStatus("initial");
    startPolling();
  }

  initialize();
})();

const state = {
  refreshMs: 5000,
  showRawJson: true,
};

const text = (value) => {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  return String(value);
};

const escapeHtml = (value) =>
  text(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

const badgeClass = (value) => `badge badge-${text(value).replace(/[^A-Za-z0-9_-]/g, "_")}`;

const badge = (value, label = null) =>
  `<span class="${badgeClass(value)}">${escapeHtml(label ?? value)}</span>`;

const metric = (label, value) => `
  <div class="metric">
    <div class="metric-label">${escapeHtml(label)}</div>
    <div class="metric-value">${escapeHtml(value)}</div>
  </div>
`;

const rawJson = (value) => {
  if (!state.showRawJson) {
    return "";
  }
  return `
    <details>
      <summary>JSON</summary>
      <pre>${escapeHtml(JSON.stringify(value, null, 2))}</pre>
    </details>
  `;
};

const reasonList = (reasons) => {
  const items = Array.isArray(reasons) ? reasons : [];
  if (items.length === 0) {
    return '<span class="muted">-</span>';
  }
  return `
    <div class="reason-list">
      ${items.map((item) => `<span class="reason">${escapeHtml(item)}</span>`).join("")}
    </div>
  `;
};

const renderCounts = (targetId, counts) => {
  const target = document.getElementById(targetId);
  const entries = Object.entries(counts || {}).filter(([, count]) => Number(count) > 0);
  target.innerHTML = entries.length
    ? entries.map(([key, count]) => badge(key, `${key} ${count}`)).join("")
    : badge("UNKNOWN", "no rows");
};

const renderSafety = (snapshot) => {
  const safety = snapshot.safety || {};
  document.getElementById("safety-badges").innerHTML = [
    badge(safety.trading_mode || "OBSERVE", `mode ${text(safety.trading_mode || "OBSERVE")}`),
    badge(safety.live_sim_allowed ? "LIVE_SIM" : "OBSERVE", `LIVE_SIM ${safety.live_sim_allowed}`),
    badge(
      safety.live_real_allowed ? "LIVE_REAL" : "OBSERVE",
      `LIVE_REAL ${safety.live_real_allowed}`,
    ),
    badge("OBSERVE", `routing ${safety.order_routing_enabled}`),
    badge("OBSERVE", `controls ${safety.order_controls_available}`),
    badge("OBSERVE", `AI execution ${safety.ai_execution_available}`),
  ].join("");

  document.getElementById("safety-warnings").innerHTML = (safety.warnings || [])
    .map((warning) => `<li>${escapeHtml(warning)}</li>`)
    .join("");
};

const renderSystem = (snapshot) => {
  const system = snapshot.system || {};
  const gateway = snapshot.gateway || {};
  const ai = (snapshot.ai_sidecar || {}).status || {};
  document.getElementById("system-cards").innerHTML = [
    metric("API health", system.api_health || "ok"),
    metric("token_required", system.token_required),
    metric("last_event_received_at", gateway.last_event_received_at),
    metric("last_heartbeat_at", gateway.last_heartbeat_at),
    metric("queued / acked / failed", [
      gateway.queued_command_count ?? 0,
      gateway.acked_command_count ?? 0,
      gateway.failed_command_count ?? 0,
    ].join(" / ")),
    metric("order_commands_allowed", gateway.order_commands_allowed),
    metric("AI Sidecar enabled", ai.enabled),
    metric("OpenAI client available", ai.openai_client_available),
  ].join("");
};

const renderPipeline = (snapshot) => {
  const funnel = ((snapshot.pipeline_summary || {}).funnel || []);
  document.getElementById("pipeline-funnel").innerHTML = funnel
    .map(
      (step) => `
        <div class="funnel-step">
          <div class="funnel-label">${escapeHtml(step.label)}</div>
          <div class="funnel-count">${escapeHtml(step.count)}</div>
        </div>
      `,
    )
    .join("");
};

const renderThemes = (snapshot) => {
  const themes = snapshot.themes || {};
  const rows = themes.latest_snapshots || [];
  renderCounts("theme-state-counts", themes.state_counts || {});
  if (rows.length === 0) {
    document.getElementById("themes-table").innerHTML = emptyState("최신 테마 snapshot이 없습니다.");
    return;
  }
  document.getElementById("themes-table").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>테마</th>
          <th>상태</th>
          <th>품질</th>
          <th>리더</th>
          <th>fresh / rising</th>
          <th>거래대금</th>
          <th>delta 1m / 3m / 5m</th>
          <th>상세</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (row) => `
              <tr>
                <td class="code-cell">${escapeHtml(row.theme_name)}</td>
                <td>${badge(row.state)}</td>
                <td>${badge(row.quality_status)}</td>
                <td>${escapeHtml(row.leading_name)}<br /><span class="muted">${escapeHtml(row.leading_code)}</span></td>
                <td>${percent(row.fresh_coverage_ratio)} / ${percent(row.rising_ratio)}</td>
                <td>${number(row.total_trade_value)}</td>
                <td>${number(row.trade_value_delta_1m)} / ${number(row.trade_value_delta_3m)} / ${number(row.trade_value_delta_5m)}</td>
                <td>${rawJson(row)}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
};

const renderCandidates = (snapshot) => {
  const candidates = snapshot.candidates || {};
  const rows = candidates.candidates || [];
  renderCounts("candidate-state-counts", candidates.state_counts || {});
  if (rows.length === 0) {
    document.getElementById("candidates-table").innerHTML = emptyState("활성 candidate가 없습니다.");
    return;
  }
  document.getElementById("candidates-table").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>candidate_instance_id</th>
          <th>종목</th>
          <th>상태</th>
          <th>테마</th>
          <th>market</th>
          <th>sources</th>
          <th>reason</th>
          <th>last_seen_at</th>
          <th>상세</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (row) => `
              <tr>
                <td class="code-cell">${escapeHtml(row.candidate_instance_id)}</td>
                <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                <td>${badge(row.state)}</td>
                <td>${escapeHtml(row.theme_name)}<br /><span class="muted">${escapeHtml(row.theme_state)} / ${escapeHtml(row.theme_role)}</span></td>
                <td>${badge(row.market_readiness_status || "UNKNOWN")}</td>
                <td>${escapeHtml(row.active_source_count)}</td>
                <td>${reasonList(row.reason_codes)}</td>
                <td>${escapeHtml(row.last_seen_at)}</td>
                <td>${rawJson(row)}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
};

const renderStrategy = (snapshot) => {
  const strategy = snapshot.strategy || {};
  const rows = strategy.latest_observations || [];
  renderCounts("strategy-status-counts", strategy.status_counts || {});
  if (rows.length === 0) {
    document.getElementById("strategy-table").innerHTML = emptyState("최신 strategy observation이 없습니다.");
    return;
  }
  document.getElementById("strategy-table").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>candidate_instance_id</th>
          <th>종목</th>
          <th>status</th>
          <th>setup</th>
          <th>score</th>
          <th>confidence</th>
          <th>reason</th>
          <th>evaluated_at</th>
          <th>상세</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (row) => `
              <tr>
                <td class="code-cell">${escapeHtml(row.candidate_instance_id)}</td>
                <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                <td>${badge(row.overall_status)}</td>
                <td>${escapeHtml(row.primary_setup_type)}<br /><span class="muted">${escapeHtml(row.primary_setup_status)}</span></td>
                <td>${decimal(row.score)}</td>
                <td>${decimal(row.confidence)}</td>
                <td>${reasonList(row.reason_codes)}</td>
                <td>${escapeHtml(row.evaluated_at)}</td>
                <td>${rawJson(row)}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
};

const renderRisk = (snapshot) => {
  const risk = snapshot.risk || {};
  const rows = risk.latest_observations || [];
  renderCounts("risk-status-counts", risk.status_counts || {});
  if (rows.length === 0) {
    document.getElementById("risk-table").innerHTML = emptyState("최신 risk observation이 없습니다.");
    return;
  }
  document.getElementById("risk-table").innerHTML = `
    <table>
      <thead>
        <tr>
          <th>candidate_instance_id</th>
          <th>종목</th>
          <th>status</th>
          <th>severity</th>
          <th>block / caution / pass</th>
          <th>reason</th>
          <th>evaluated_at</th>
          <th>상세</th>
        </tr>
      </thead>
      <tbody>
        ${rows
          .map(
            (row) => `
              <tr>
                <td class="code-cell">${escapeHtml(row.candidate_instance_id)}</td>
                <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                <td>${badge(row.overall_status)}</td>
                <td>${badge(row.max_severity)}</td>
                <td>${escapeHtml(row.blocked_count)} / ${escapeHtml(row.caution_count)} / ${escapeHtml(row.pass_count)}</td>
                <td>${reasonList(row.reason_codes)}</td>
                <td>${escapeHtml(row.evaluated_at)}</td>
                <td>${rawJson(row)}</td>
              </tr>
            `,
          )
          .join("")}
      </tbody>
    </table>
  `;
};

const renderErrors = (snapshot) => {
  const recent = ((snapshot.recent_events || {}).gateway_events || []).slice(0, 8);
  const errors = snapshot.errors || {};
  const groups = [
    ["Gateway recent events", recent],
    ["Market projection errors", errors.market_projection_errors || []],
    ["Theme projection errors", errors.theme_projection_errors || []],
    ["Candidate projection errors", errors.candidate_projection_errors || []],
    ["Strategy errors", errors.strategy_errors || []],
    ["Risk errors", errors.risk_errors || []],
    ["Gateway problem events", errors.gateway_problem_events || []],
    ["Gateway command failures", errors.gateway_command_failures || []],
  ];
  document.getElementById("events-errors").innerHTML = groups
    .map(([title, rows]) => logGroup(title, rows))
    .join("");
};

const renderAi = (snapshot) => {
  const ai = snapshot.ai_sidecar || {};
  const status = ai.status || {};
  document.getElementById("ai-status").innerHTML = [
    metric("enabled", status.enabled),
    metric("execution_api_available", status.execution_api_available),
    metric("openai_client_available", status.openai_client_available),
    metric("insight_count", ai.insight_count || 0),
  ].join("");

  const insights = ai.insights || [];
  document.getElementById("ai-insights").innerHTML = insights.length
    ? insights
        .map(
          (item) => `
            <article class="log-card">
              <h3>${escapeHtml(item.task_type)} · ${escapeHtml(item.severity)}</h3>
              <p>${escapeHtml(item.summary)}</p>
              <p class="muted">${escapeHtml(item.created_at)}</p>
              ${rawJson(item)}
            </article>
          `,
        )
        .join("")
    : emptyState("표시할 AI insight가 없습니다.");
};

const logGroup = (title, rows) => `
  <article class="log-card">
    <h3>${escapeHtml(title)} <span class="muted">(${rows.length})</span></h3>
    ${
      rows.length
        ? rows
            .slice(0, 8)
            .map(
              (row) => `
                <div class="muted">${escapeHtml(row.created_at || row.received_at || row.event_ts || row.evaluated_at)}</div>
                ${rawJson(row)}
              `,
            )
            .join("")
        : '<p class="muted">최근 항목 없음</p>'
    }
  </article>
`;

const emptyState = (message) => `<div class="empty-state">${escapeHtml(message)}</div>`;

const number = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString("ko-KR") : "-";
};

const decimal = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toFixed(2) : "-";
};

const percent = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? `${(parsed * 100).toFixed(1)}%` : "-";
};

const showFetchError = (message) => {
  const banner = document.getElementById("error-banner");
  banner.textContent = message;
  banner.classList.remove("hidden");
};

const clearFetchError = () => {
  const banner = document.getElementById("error-banner");
  banner.textContent = "";
  banner.classList.add("hidden");
};

const renderSnapshot = (snapshot) => {
  state.showRawJson = Boolean((snapshot.system || {}).dashboard?.show_raw_json ?? true);
  state.refreshMs = Number((snapshot.system || {}).dashboard?.refresh_sec || 5) * 1000;
  document.getElementById("generated-at").textContent = `generated_at: ${text(snapshot.generated_at)}`;
  document.getElementById("refresh-state").textContent = `${state.refreshMs / 1000}초 자동 갱신`;
  renderSafety(snapshot);
  renderSystem(snapshot);
  renderPipeline(snapshot);
  renderThemes(snapshot);
  renderCandidates(snapshot);
  renderStrategy(snapshot);
  renderRisk(snapshot);
  renderErrors(snapshot);
  renderAi(snapshot);
};

const refreshDashboard = async () => {
  try {
    const response = await fetch("/api/dashboard/snapshot?detail=summary");
    if (!response.ok) {
      throw new Error(`snapshot fetch failed: ${response.status}`);
    }
    const snapshot = await response.json();
    clearFetchError();
    renderSnapshot(snapshot);
  } catch (error) {
    showFetchError(`Dashboard snapshot을 가져오지 못했습니다: ${error.message}`);
  } finally {
    window.setTimeout(refreshDashboard, state.refreshMs);
  }
};

window.addEventListener("DOMContentLoaded", () => {
  refreshDashboard();
});

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

const cssToken = (value) =>
  text(value)
    .replace(/[^A-Za-z0-9_-]/g, "_");

const badgeClass = (value) => `badge badge-${cssToken(value)}`;

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
  if (!items.length) {
    return '<span class="muted">-</span>';
  }
  return `<div class="reason-list">${items
    .map((item) => `<span class="reason">${escapeHtml(item)}</span>`)
    .join("")}</div>`;
};

const emptyState = (message) => `<div class="empty-state">${escapeHtml(message)}</div>`;

const number = (value) => {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed.toLocaleString("ko-KR") : "-";
};

const renderSafety = (snapshot) => {
  const safety = snapshot.safety || {};
  document.getElementById("safety-badges").innerHTML = [
    badge(safety.trading_mode || "OBSERVE", `mode ${text(safety.trading_mode || "OBSERVE")}`),
    badge(safety.live_sim_allowed ? "LIVE_SIM" : "OBSERVE", `LIVE_SIM ${safety.live_sim_allowed}`),
    badge("OBSERVE", `LIVE_REAL ${safety.live_real_allowed}`),
    badge("OBSERVE", `controls ${safety.order_controls_available}`),
    badge("OBSERVE", `AI execution ${safety.ai_execution_available}`),
  ].join("");
  document.getElementById("safety-warnings").innerHTML = (safety.warnings || [])
    .slice(0, 6)
    .map((warning) => `<li>${escapeHtml(warning)}</li>`)
    .join("");
};

const renderSystem = (snapshot) => {
  const system = snapshot.system || {};
  const gateway = snapshot.gateway || {};
  const liveSim = snapshot.live_sim || {};
  const liveStatus = liveSim.status || {};
  const noBuy = snapshot.no_buy_sentinel || {};
  document.getElementById("system-badges").innerHTML = [
    badge(system.mode || "OBSERVE"),
    badge(liveStatus.kill_switch ? "BLOCKED" : "OBSERVE", `kill ${text(liveStatus.kill_switch)}`),
    badge("OBSERVE", `read only ${text(noBuy.read_only)}`),
  ].join("");
  document.getElementById("system-cards").innerHTML = [
    metric("Core", system.api_health || "ok"),
    metric("Gateway heartbeat", gateway.last_heartbeat_at),
    metric("Gateway queued / failed", `${gateway.queued_command_count ?? 0} / ${gateway.failed_command_count ?? 0}`),
    metric("Kiwoom mode", `${text(liveStatus.account_mode)} / ${text(liveStatus.server_mode)}`),
    metric("LIVE_SIM flags", `${text(liveStatus.enabled)} / ${text(liveStatus.order_routing_enabled)}`),
    metric("Account/Broker", `${text(liveStatus.account_mode)} / ${text(liveStatus.broker_env)}`),
  ].join("");
};

const renderRealtimeSubscription = (snapshot) => {
  const plan = snapshot.realtime_subscription || {};
  const counts = plan.counts || {};
  const registered = plan.registered_realtime_codes || [];
  const registerTargets = plan.register_targets || [];
  const removeTargets = plan.remove_targets || [];
  const missingCandidates = plan.missing_candidate_subscriptions || [];
  document.getElementById("realtime-subscription-badges").innerHTML = [
    badge(plan.status || "UNKNOWN"),
    badge("OBSERVE", `queue ${text(plan.queue_commands)}`),
    badge(plan.exchange || "KRX", `exchange ${text(plan.exchange || "KRX")}`),
  ].join("");
  document.getElementById("realtime-subscription-status").innerHTML = [
    metric("Registered", counts.already_registered_count ?? registered.length),
    metric("Plan register/remove", `${counts.planned_register_count || 0} / ${counts.planned_remove_count || 0}`),
    metric("Anchors", counts.anchor_count || 0),
    metric("Condition/Candidate", `${counts.condition_count || 0} / ${counts.candidate_count || 0}`),
    metric("Theme watchset", counts.theme_watchset_count || 0),
    metric("Missing candidates", counts.missing_candidate_subscription_count || 0),
  ].join("");
  document.getElementById("realtime-subscription-tables").innerHTML = [
    miniList("Registered", registered, (code) => code),
    subscriptionTable("Plan register", registerTargets),
    subscriptionTable("Plan remove", removeTargets),
    subscriptionTable("Missing candidate subs", missingCandidates),
    miniList("Reason summary", Object.entries(plan.reason_summary || {}), ([reason, count]) => `${reason}: ${count}`),
  ].join("");
};

const renderMarketTheme = (snapshot) => {
  const themes = snapshot.themes || {};
  const noBuy = snapshot.no_buy_sentinel || {};
  const themeStage = ((noBuy.stage_summary || {}).theme || {});
  const rows = [
    ...(themes.top_leading_themes || []),
    ...(themes.top_spreading_themes || []),
  ].slice(0, 6);
  document.getElementById("market-theme-badges").innerHTML = [
    badge("OBSERVE", `watchset ${themeStage.watchset_count || 0}`),
    badge(themeStage.data_wait_count ? "DATA_WAIT" : "OBSERVE", `DATA_WAIT ${themeStage.data_wait_count || 0}`),
  ].join("");
  document.getElementById("market-theme-status").innerHTML = [
    metric("Theme snapshots", themeStage.snapshot_count || 0),
    metric("Watchset", themeStage.watchset_count || 0),
    metric("DATA_WAIT", themeStage.data_wait_count || 0),
  ].join("");
  document.getElementById("market-theme-table").innerHTML = rows.length
    ? table(
        ["테마", "상태", "리더", "fresh/rising"],
        rows.map((row) => [
          row.theme_name,
          badge(row.state),
          `${text(row.leading_name)} ${text(row.leading_code)}`,
          `${pct(row.fresh_coverage_ratio)} / ${pct(row.rising_ratio)}`,
        ]),
      )
    : emptyState("표시할 top theme가 없습니다.");
};

const renderCandidatePlan = (snapshot) => {
  const noBuy = snapshot.no_buy_sentinel || {};
  const stage = noBuy.stage_summary || {};
  const candidateStage = stage.candidate || {};
  const entryStage = stage.entry_timing || {};
  const planStage = stage.order_plan || {};
  const rows = noBuy.top_near_miss || [];
  document.getElementById("candidate-plan-badges").innerHTML = [
    badge("OBSERVE", `PLAN_READY ${noBuy.plan_ready_count || 0}`),
    badge("WATCH", `WAIT_RETRY ${entryStage.wait_retry_count || 0}`),
    badge(entryStage.data_wait_count ? "DATA_WAIT" : "OBSERVE", `DATA_WAIT ${entryStage.data_wait_count || 0}`),
  ].join("");
  document.getElementById("candidate-plan-status").innerHTML = [
    metric("Active candidates", candidateStage.active_count || 0),
    metric("Latest plans", planStage.latest_count || 0),
    metric("Buy eligible", noBuy.buy_eligible_count || 0),
  ].join("");
  document.getElementById("candidate-plan-table").innerHTML = rows.length
    ? nearMissTable(rows.slice(0, 6))
    : emptyState("near-miss 후보가 없습니다.");
};

const renderLiveSimOps = (snapshot) => {
  const liveSim = snapshot.live_sim || {};
  const status = liveSim.status || {};
  const reconcile = liveSim.reconcile_status || {};
  const operating = liveSim.operating || {};
  const latestRun = operating.latest_run || {};
  const commandCounts = operating.command_counts_last_run || {};
  document.getElementById("live-sim-badges").innerHTML = [
    badge(status.enabled ? "ENABLED" : "OBSERVE", `enabled ${text(status.enabled)}`),
    badge(status.kill_switch ? "BLOCKED" : "OBSERVE", `kill ${text(status.kill_switch)}`),
    badge((status.safety_gate || {}).status || "UNKNOWN", `gate ${text((status.safety_gate || {}).status)}`),
    badge(operating.preflight_status || "UNKNOWN", `preflight ${text(operating.preflight_status)}`),
    badge("OBSERVE", "LIVE_REAL false"),
  ].join("");
  document.getElementById("live-sim-status").innerHTML = [
    metric("Operating mode", operating.current_operating_mode || "-"),
    metric("Latest operating run", latestRun.status ? `${latestRun.status} / ${latestRun.mode}` : "-"),
    metric("Last command counts", `${commandCounts.buy || 0} / ${commandCounts.cancel || 0} / ${commandCounts.exit || 0}`),
    metric("Today intents/orders", `${status.intent_count || 0} / ${status.order_count || 0}`),
    metric("Open orders", status.open_order_count || 0),
    metric("Open positions", status.open_position_count || 0),
    metric("Cancel candidates", status.cancel_pending_count || 0),
    metric("Exit signals", status.active_exit_signal_count || 0),
    metric("Reconcile", reconcile.status || "-"),
  ].join("");
  document.getElementById("live-sim-tables").innerHTML = [
    miniList("Operating warnings", operating.warnings || [], (row) => row),
    miniList("Operating blocks", operating.blocking_reasons || [], (row) => row),
    miniList("Open positions", liveSim.open_positions || [], (row) => `${row.name} ${row.code} · ${row.status}`),
    miniList("Open orders", liveSim.recent_orders || [], (row) => `${row.name} ${row.code} · ${row.status}`),
    miniList("Reconcile", liveSim.recent_reconcile_snapshots || [], (row) => `${row.status} · mismatch ${row.mismatch_count || 0}`),
  ].join("");
};

const renderAiAdvisory = (snapshot) => {
  const advisory = snapshot.ai_advisory || {};
  const status = advisory.status || {};
  const noBuyAi = (snapshot.no_buy_sentinel || {}).ai_summary || {};
  const scores = advisory.top_scores || [];
  document.getElementById("ai-advisory-badges").innerHTML = [
    badge(status.enabled ? "ENABLED" : "OBSERVE", `enabled ${text(status.enabled)}`),
    badge("OBSERVE", "advisory-only"),
    badge(noBuyAi.classification || "NONE", text(noBuyAi.classification || "NONE")),
  ].join("");
  document.getElementById("ai-advisory-status").innerHTML = [
    metric("provider/model", `${text(noBuyAi.provider || status.provider)} / ${text(noBuyAi.model || status.model)}`),
    metric("latest run", noBuyAi.latest_run_status || "-"),
    metric("selected", noBuyAi.selected_count || 0),
    metric("top score/conf", `${number(noBuyAi.top_score)} / ${number(noBuyAi.top_confidence)}`),
    metric("fallback/error", `${text(noBuyAi.fallback_used)} / ${number(noBuyAi.error_count)}`),
    metric("no trade reason", noBuyAi.no_trade_reason || "-"),
  ].join("");
  document.getElementById("ai-advisory-scores").innerHTML = scores.length
    ? table(
        ["종목", "selected", "score", "confidence", "analysis"],
        scores.slice(0, 8).map((row) => [
          `${text(row.code)} ${text(row.candidate_instance_id)}`,
          badge(row.selected ? "SELECTED" : "OBSERVE", row.selected),
          number(row.score),
          number(row.confidence),
          row.analysis,
        ]),
      )
    : emptyState("AI Candidate Scorer 결과가 없습니다.");
};

const renderNoBuy = (snapshot) => {
  const noBuy = snapshot.no_buy_sentinel || {};
  const stage = noBuy.stage_summary || {};
  document.getElementById("no-buy-badges").innerHTML = [
    badge(noBuy.status || "UNKNOWN"),
    badge(noBuy.no_buy_detected ? "BLOCKED" : "OBSERVE", `no buy ${text(noBuy.no_buy_detected)}`),
    badge("OBSERVE", "read only"),
  ].join("");
  document.getElementById("no-buy-status").innerHTML = [
    metric("status", noBuy.status || "-"),
    metric("intent/order/command", `${noBuy.intent_count || 0} / ${noBuy.order_count || 0} / ${noBuy.command_count || 0}`),
    metric("PLAN_READY", noBuy.plan_ready_count || 0),
    metric("Buy eligible", noBuy.buy_eligible_count || 0),
    metric("AI selected", noBuy.ai_selected_count || 0),
    metric("session", noBuy.market_session || "-"),
  ].join("");
  document.getElementById("no-buy-stage-summary").innerHTML = Object.entries(stage)
    .map(([key, value]) => `
      <article class="reason-card">
        <h3>${escapeHtml(key)}</h3>
        ${rawSummary(value)}
      </article>
    `)
    .join("");
  document.getElementById("no-buy-near-miss").innerHTML = (noBuy.top_near_miss || []).length
    ? nearMissTable(noBuy.top_near_miss)
    : emptyState("No-Buy Sentinel near-miss가 없습니다.");
  document.getElementById("no-buy-checklist").innerHTML = (noBuy.operator_checklist || [])
    .map((item) => `<li>${escapeHtml(item)}</li>`)
    .join("");
};

const renderPipeline = (snapshot) => {
  const pipeline = snapshot.pipeline_summary || {};
  const stages = pipeline.stage_statuses || [];
  const funnel = pipeline.funnel || [];
  const stageRows = stages.map((stage) => [
    stage.stage,
    badge(stage.status || "UNKNOWN"),
    number(stage.count),
    text(stage.updated_at),
    reasonList(stage.reason_codes),
  ]);
  const stageHtml = stages.length
    ? `<div class="table-wrap pipeline-stage-table">${table(["Stage", "상태", "count", "updated", "reason"], stageRows)}</div>`
    : emptyState("운영 파이프라인 stage 요약이 없습니다.");
  const funnelHtml = funnel
    .map((step) => `
      <div class="funnel-step">
        <div class="funnel-label">${escapeHtml(step.label)}</div>
        <div class="funnel-count">${escapeHtml(step.count)}</div>
      </div>
    `)
    .join("");
  document.getElementById("pipeline-funnel").innerHTML = `${stageHtml}<div class="funnel">${funnelHtml}</div>`;
  const errors = snapshot.errors || {};
  document.getElementById("events-errors").innerHTML = [
    logGroup("Gateway events", (snapshot.recent_events || {}).gateway_events || []),
    logGroup("Projection errors", [
      ...(errors.market_projection_errors || []),
      ...(errors.theme_projection_errors || []),
      ...(errors.candidate_projection_errors || []),
    ]),
    logGroup("LIVE_SIM errors", errors.live_sim_errors || []),
  ].join("");
};

const renderAiExplanations = (snapshot) => {
  const explanations = snapshot.ai_explanations || {};
  const cards = explanations.latest_cards || explanations.cards || [];
  document.getElementById("ai-explanation-counts").innerHTML = [
    badge("OBSERVE", "읽기 전용"),
    badge("OBSERVE", "실행 버튼 없음"),
    badge("OBSERVE", `cards ${cards.length}`),
  ].join("");
  document.getElementById("ai-explanation-status").innerHTML = [
    metric("RCA reports", explanations.rca_report_count || 0),
    metric("AI insights", explanations.ai_insight_count || 0),
    metric("Request failures", explanations.ai_request_failure_count || 0),
  ].join("");
  document.getElementById("ai-explanation-cards").innerHTML = cards.length
    ? cards.slice(0, 6).map(aiCard).join("")
    : emptyState("AI 설명 카드가 없습니다.");
};

const nearMissTable = (rows) =>
  table(
    ["종목", "Plan", "AI", "Primary block", "reason", "확인"],
    rows.map((row) => [
      `${text(row.name)} ${text(row.code)}`,
      `${text(row.order_plan_status)} / ${text(row.entry_timing_state)}`,
      `${text(row.ai_selected)} · ${number(row.ai_score)} / ${number(row.ai_confidence)}`,
      `${text(row.primary_block_stage)} / ${text(row.primary_block_type)}`,
      reasonList(row.reason_codes),
      row.operator_hint,
    ]),
  );

const subscriptionTable = (title, rows) => `
  <article class="log-card">
    <h3>${escapeHtml(title)} <span class="muted">(${rows.length})</span></h3>
    ${
      rows.length
        ? table(
            ["종목", "source", "reason"],
            rows.slice(0, 8).map((row) => [
              `${text(row.name)} ${text(row.code)}`,
              (row.source_types || [row.state || row.action || "-"]).join(", "),
              reasonList(row.reason_codes),
            ]),
          )
        : '<p class="muted">최근 항목 없음</p>'
    }
  </article>
`;

const table = (headers, rows) => `
  <table>
    <thead>
      <tr>${headers.map((item) => `<th>${escapeHtml(item)}</th>`).join("")}</tr>
    </thead>
    <tbody>
      ${rows
        .map((row) => `<tr>${row.map((cell) => `<td>${cell}</td>`).join("")}</tr>`)
        .join("")}
    </tbody>
  </table>
`;

const miniList = (title, rows, format) => `
  <article class="log-card">
    <h3>${escapeHtml(title)} <span class="muted">(${rows.length})</span></h3>
    ${
      rows.length
        ? `<ul class="mini-list">${rows.slice(0, 6).map((row) => `<li>${escapeHtml(format(row))}</li>`).join("")}</ul>`
        : '<p class="muted">최근 항목 없음</p>'
    }
  </article>
`;

const logGroup = (title, rows) => `
  <article class="log-card">
    <h3>${escapeHtml(title)} <span class="muted">(${rows.length})</span></h3>
    ${
      rows.length
        ? rows
            .slice(0, 6)
            .map((row) => `<div class="muted">${escapeHtml(row.created_at || row.received_at || row.event_ts || row.evaluated_at)}</div>${rawJson(row)}`)
            .join("")
        : '<p class="muted">최근 항목 없음</p>'
    }
  </article>
`;

const aiCard = (card) => `
  <article class="ai-explanation-card">
    <div class="ai-card-topline">
      <div>
        <p class="eyebrow">${escapeHtml(card.card_type_label || card.card_type)}</p>
        <h3>${escapeHtml(card.title)}</h3>
      </div>
      <div class="ai-card-badges">
        ${badge(card.status || "UNKNOWN")}
        ${badge(card.severity || "INFO")}
      </div>
    </div>
    <p class="ai-card-summary">${escapeHtml(card.summary)}</p>
    ${rawJson(card)}
  </article>
`;

const rawSummary = (value) => {
  if (!value || typeof value !== "object") {
    return `<p class="muted">${escapeHtml(value)}</p>`;
  }
  return Object.entries(value)
    .slice(0, 5)
    .map(([key, item]) => `<p><span class="muted">${escapeHtml(key)}:</span> ${escapeHtml(JSON.stringify(item))}</p>`)
    .join("");
};

const pct = (value) => {
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
  renderRealtimeSubscription(snapshot);
  renderMarketTheme(snapshot);
  renderCandidatePlan(snapshot);
  renderLiveSimOps(snapshot);
  renderAiAdvisory(snapshot);
  renderNoBuy(snapshot);
  renderPipeline(snapshot);
  renderAiExplanations(snapshot);
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

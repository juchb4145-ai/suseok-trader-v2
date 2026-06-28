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

const cssToken = (value) =>
  text(value)
    .toLowerCase()
    .replaceAll("_", "-")
    .replace(/[^a-z0-9-]/g, "-");

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

const renderDryRun = (snapshot) => {
  const dryRun = snapshot.dry_run || {};
  const status = dryRun.status || {};
  const intents = dryRun.recent_intents || [];
  const positions = dryRun.positions || [];
  const exitEngine = dryRun.exit_engine || {};
  const exitEvaluations = exitEngine.recent_exit_evaluations || [];
  const exitSignals = exitEngine.recent_exit_signals || [];
  document.getElementById("dry-run-badges").innerHTML = [
    badge(status.enabled ? "ENABLED" : "OBSERVE", `enabled ${status.enabled}`),
    badge("OBSERVE", `intent ${status.intent_creation_enabled}`),
    badge("OBSERVE", `fill ${status.simulated_fill_enabled}`),
    badge("OBSERVE", `routing ${status.order_routing_enabled}`),
    badge("OBSERVE", `broker sent ${status.broker_order_sent}`),
  ].join("");
  document.getElementById("dry-run-status").innerHTML = [
    metric("active positions", status.active_position_count || 0),
    metric("intents", status.intent_count || 0),
    metric("orders", status.order_count || 0),
    metric("executions", status.execution_count || 0),
    metric("safety gate", (status.safety_gate || {}).status || "UNKNOWN"),
    metric("gateway command enabled", status[`gateway${"_"}command_enabled`]),
  ].join("");
  document.getElementById("dry-run-intents").innerHTML = intents.length
    ? `
      <table>
        <thead>
          <tr>
            <th>intent</th>
            <th>종목</th>
            <th>status</th>
            <th>수량 / 금액</th>
            <th>source</th>
            <th>created_at</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${intents
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.dry_run_intent_id)}</td>
                  <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                  <td>${badge(row.status)}</td>
                  <td>${number(row.quantity)} / ${number(row.notional)}</td>
                  <td>${escapeHtml(row.source)}</td>
                  <td>${escapeHtml(row.created_at)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("DRY_RUN intent가 없습니다.");
  document.getElementById("dry-run-positions").innerHTML = positions.length
    ? `
      <table>
        <thead>
          <tr>
            <th>position</th>
            <th>종목</th>
            <th>status</th>
            <th>수량</th>
            <th>평균가 / 현재가</th>
            <th>평가손익</th>
            <th>updated_at</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${positions
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.dry_run_position_id)}</td>
                  <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                  <td>${badge(row.status)}</td>
                  <td>${number(row.quantity)}</td>
                  <td>${number(row.avg_price)} / ${number(row.last_price)}</td>
                  <td>${number(row.unrealized_pnl)}</td>
                  <td>${escapeHtml(row.updated_at)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("DRY_RUN position이 없습니다.");
  document.getElementById("dry-run-exit-badges").innerHTML = [
    badge(exitEngine.enabled ? "ENABLED" : "OBSERVE", `enabled ${exitEngine.enabled}`),
    badge("OBSERVE", `intent ${exitEngine.intent_creation_enabled}`),
    badge("OBSERVE", `order ${exitEngine.order_creation_enabled}`),
    badge("OBSERVE", `fill ${exitEngine.simulated_fill_enabled}`),
    badge("OBSERVE", `broker sent ${exitEngine.broker_order_sent}`),
  ].join("");
  document.getElementById("dry-run-exit-status").innerHTML = [
    metric("evaluations", exitEngine.evaluation_count || 0),
    metric("signals", exitEngine.signal_count || 0),
    metric("exit intents", exitEngine.exit_intent_count || 0),
    metric("exit orders", exitEngine.exit_order_count || 0),
    metric("exit executions", exitEngine.exit_execution_count || 0),
    metric("gateway allowed", exitEngine[`gateway${"_"}command_allowed`]),
  ].join("");
  document.getElementById("dry-run-exit-evaluations").innerHTML = exitEvaluations.length
    ? `
      <table>
        <thead>
          <tr>
            <th>evaluation</th>
            <th>position</th>
            <th>종목</th>
            <th>status</th>
            <th>primary</th>
            <th>손익</th>
            <th>hold_sec</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${exitEvaluations
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.exit_evaluation_id)}</td>
                  <td class="code-cell">${escapeHtml(row.dry_run_position_id)}</td>
                  <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                  <td>${badge(row.status)}</td>
                  <td>${badge(row.primary_signal_type || "NONE")}</td>
                  <td>${number(row.unrealized_pnl)} / ${number(row.unrealized_pnl_pct)}%</td>
                  <td>${number(row.hold_sec)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("DRY_RUN exit evaluation이 없습니다.");
  document.getElementById("dry-run-exit-signals").innerHTML = exitSignals.length
    ? `
      <table>
        <thead>
          <tr>
            <th>signal</th>
            <th>position</th>
            <th>type</th>
            <th>severity</th>
            <th>price / threshold</th>
            <th>reason</th>
            <th>observed_at</th>
          </tr>
        </thead>
        <tbody>
          ${exitSignals
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.exit_signal_id)}</td>
                  <td class="code-cell">${escapeHtml(row.dry_run_position_id)}</td>
                  <td>${badge(row.signal_type)}</td>
                  <td>${badge(row.severity)}</td>
                  <td>${number(row.current_price)} / ${number(row.threshold_value)}</td>
                  <td>${reasonList(row.reason_codes)}</td>
                  <td>${escapeHtml(row.observed_at)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("DRY_RUN exit signal이 없습니다.");
};

const renderLiveSim = (snapshot) => {
  const liveSim = snapshot.live_sim || {};
  const status = liveSim.status || {};
  const safetyGate = liveSim.safety_gate || {};
  const intents = liveSim.recent_intents || [];
  const orders = liveSim.recent_orders || [];
  const executions = liveSim.recent_executions || [];
  const rejections = liveSim.recent_rejections || [];
  const reconcile = liveSim.recent_reconcile_snapshots || [];
  const positions = liveSim.open_positions || [];
  const exitSignals = liveSim.active_exit_signals || [];
  const cancelIntents = liveSim.recent_cancel_intents || [];
  const lifecycleEvents = liveSim.recent_lifecycle_events || [];
  const reviews = liveSim.latest_live_sim_review_reports || [];
  const reviewErrors = liveSim.latest_live_sim_review_errors || [];
  document.getElementById("live-sim-badges").innerHTML = [
    badge(status.enabled ? "ENABLED" : "OBSERVE", `enabled ${status.enabled}`),
    badge(status.kill_switch ? "BLOCKED" : "OBSERVE", `kill ${status.kill_switch}`),
    badge("OBSERVE", `routing ${status.order_routing_enabled}`),
    badge("OBSERVE", `gateway ${status[`gateway${"_"}command_enabled`]}`),
    badge("OBSERVE", "LIVE_REAL false"),
    badge(safetyGate.status || "UNKNOWN", `gate ${safetyGate.status || "UNKNOWN"}`),
  ].join("");
  document.getElementById("live-sim-status").innerHTML = [
    metric("account mode", status.account_mode),
    metric("broker env", status.broker_env),
    metric("server mode", status.server_mode),
    metric("intents", status.intent_count || 0),
    metric("orders", status.order_count || 0),
    metric("executions", status.execution_count || 0),
    metric("open positions", status.open_position_count || 0),
    metric("cancel pending", status.cancel_pending_count || 0),
    metric("exit signals", status.active_exit_signal_count || 0),
    metric("rejections", status.rejection_count || 0),
    metric("open orders", status.open_order_count || 0),
    metric("max order notional", number(status.max_order_notional)),
    metric("order controls", liveSim.order_controls_available),
  ].join("");
  document.getElementById("live-sim-intents").innerHTML = intents.length
    ? `
      <table>
        <thead>
          <tr>
            <th>intent</th>
            <th>종목</th>
            <th>status</th>
            <th>side/type</th>
            <th>수량 / 금액</th>
            <th>idempotency</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${intents
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.live_sim_intent_id)}</td>
                  <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                  <td>${badge(row.status)}</td>
                  <td>${escapeHtml(row.side)} / ${escapeHtml(row.order_type)}</td>
                  <td>${number(row.quantity)} / ${number(row.notional)}</td>
                  <td class="code-cell">${escapeHtml(row.idempotency_key)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("LIVE_SIM intent가 없습니다.");
  document.getElementById("live-sim-orders").innerHTML = orders.length
    ? `
      <table>
        <thead>
          <tr>
            <th>order</th>
            <th>command</th>
            <th>종목</th>
            <th>status</th>
            <th>수량 / 체결</th>
            <th>broker_order_no</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${orders
            .map(
              (row) => `
                <tr>
                  <td class="code-cell">${escapeHtml(row.live_sim_order_id)}</td>
                  <td class="code-cell">${escapeHtml(row[`gateway${"_"}command_id`])}</td>
                  <td>${escapeHtml(row.name)}<br /><span class="muted">${escapeHtml(row.code)}</span></td>
                  <td>${badge(row.status)}</td>
                  <td>${number(row.quantity)} / ${number(row.filled_quantity)}</td>
                  <td>${escapeHtml(row.broker_order_no)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("LIVE_SIM order가 없습니다.");
  document.getElementById("live-sim-events").innerHTML = [
    logGroup("LIVE_SIM open positions", positions),
    logGroup("LIVE_SIM active exit signals", exitSignals),
    logGroup("LIVE_SIM cancel intents", cancelIntents),
    logGroup("LIVE_SIM executions", executions),
    logGroup("LIVE_SIM rejections", rejections),
    logGroup("LIVE_SIM reconcile", reconcile),
    logGroup("LIVE_SIM lifecycle", lifecycleEvents),
  ].join("");
  document.getElementById("live-sim-reviews").innerHTML = [
    logGroup("LIVE_SIM review reports", reviews),
    logGroup("LIVE_SIM review errors", reviewErrors),
  ].join("");
};

const renderAiAdvisory = (snapshot) => {
  const advisory = snapshot.ai_advisory || {};
  const status = advisory.status || {};
  const latestRun = advisory.latest_run || {};
  const scores = advisory.top_scores || [];
  const errors = advisory.latest_errors || [];
  document.getElementById("ai-advisory-badges").innerHTML = [
    badge(status.enabled ? "ENABLED" : "OBSERVE", `enabled ${status.enabled}`),
    badge("OBSERVE", "advisory-only"),
    badge("OBSERVE", "no order side effects"),
    badge(status.external_enabled ? "ENABLED" : "OBSERVE", `external ${status.external_enabled}`),
    badge(status.fallback_used ? "CAUTION" : "OBSERVE", `fallback ${status.fallback_used}`),
    badge(latestRun.status || "UNKNOWN", `latest ${latestRun.status || "none"}`),
  ].join("");
  document.getElementById("ai-advisory-status").innerHTML = [
    metric("provider", status.provider || "mock"),
    metric("model", status.model),
    metric("external provider", status.external_provider || "none"),
    metric("external model", status.external_model),
    metric("external call attempted", status.external_call_attempted),
    metric("latency_ms", status.latest_latency_ms),
    metric("strict_json", status.strict_json),
    metric("store_raw_response", status.store_raw_response),
    metric("candidate_count", latestRun.candidate_count || 0),
    metric("selected_count", latestRun.selected_count || 0),
    metric("error_count", status.error_count || 0),
    metric("invalid_schema_count", status.invalid_schema_error_count || 0),
    metric("order side effects", advisory.no_order_side_effects),
  ].join("");
  document.getElementById("ai-advisory-scores").innerHTML = scores.length
    ? `
      <table>
        <thead>
          <tr>
            <th>종목</th>
            <th>selected</th>
            <th>score</th>
            <th>confidence</th>
            <th>analysis</th>
            <th>flags</th>
            <th>상세</th>
          </tr>
        </thead>
        <tbody>
          ${scores
            .map(
              (row) => `
                <tr>
                  <td>${escapeHtml(row.code)}<br /><span class="muted">${escapeHtml(row.candidate_instance_id)}</span></td>
                  <td>${badge(row.selected ? "SELECTED" : "OBSERVE", row.selected)}</td>
                  <td>${number(row.score)}</td>
                  <td>${number(row.confidence)}</td>
                  <td>${escapeHtml(row.analysis)}</td>
                  <td>${reasonList(row.flags)}</td>
                  <td>${rawJson(row)}</td>
                </tr>
              `,
            )
            .join("")}
        </tbody>
      </table>
    `
    : emptyState("아직 AI Candidate Scorer 결과가 없습니다.");
  document.getElementById("ai-advisory-errors").innerHTML = logGroup(
    "AI advisory errors",
    errors,
  );
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
    ["DRY_RUN errors", errors.dry_run_errors || []],
    ["DRY_RUN exit errors", errors.dry_run_exit_errors || []],
    ["LIVE_SIM errors", errors.live_sim_errors || []],
    ["Gateway problem events", errors.gateway_problem_events || []],
    ["Gateway command failures", errors[`gateway${"_"}command_failures`] || []],
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
    metric("tools_enabled", status.tools_enabled),
    metric("order_tools_enabled", status.order_tools_enabled),
    metric("recent_request_count", ai.recent_request_count || 0),
    metric("insight_count", ai.insight_count || 0),
    metric("rca_report_count", ai.rca_report_count || 0),
    metric("live_sim_review_report_count", ai.live_sim_review_report_count || 0),
    metric("codex_draft_count", ai.codex_draft_count || 0),
  ].join("");

  const insights = ai.insights || [];
  const requests = ai.requests || [];
  const rcaReports = ai.latest_rca_reports || [];
  const requestCards = requests.length
    ? requests
        .slice(0, 5)
        .map(
          (item) => `
            <article class="log-card">
              <h3>${escapeHtml(item.task_type)} · ${escapeHtml(item.status)}</h3>
              <p class="muted">${escapeHtml(item.created_at)}</p>
              ${rawJson(item)}
            </article>
          `,
        )
        .join("")
    : emptyState("표시할 AI request가 없습니다.");
  const rcaCards = rcaReports.length
    ? rcaReports
        .slice(0, 5)
        .map(
          (item) => `
            <article class="log-card">
              <h3>RCA · ${escapeHtml(item.report_type)} · ${escapeHtml(item.status)}</h3>
              <p>${escapeHtml(item.summary)}</p>
              <p class="muted">${escapeHtml(item.generated_at)}</p>
              ${rawJson(item)}
            </article>
          `,
        )
        .join("")
    : emptyState("표시할 RCA report가 없습니다.");
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
  document.getElementById("ai-requests").innerHTML = `${rcaCards}${requestCards}`;
};

const renderAiExplanations = (snapshot) => {
  const explanations = snapshot.ai_explanations || {};
  const cards = explanations.latest_cards || explanations.cards || [];
  const statusEntries = Object.entries(explanations.status_counts || {}).filter(
    ([, count]) => Number(count) > 0,
  );
  document.getElementById("ai-explanation-counts").innerHTML = [
    badge("OBSERVE", "읽기 전용"),
    badge("OBSERVE", "거래 영향 없음"),
    badge("OBSERVE", "실행 버튼 없음"),
    badge("OBSERVE", `cards ${cards.length}`),
    ...statusEntries.map(([key, count]) => badge(key, `${key} ${count}`)),
  ].join("");
  document.getElementById("ai-explanation-status").innerHTML = [
    metric("rca_report_count", explanations.rca_report_count || 0),
    metric("live_sim_review_report_count", explanations.live_sim_review_report_count || 0),
    metric("ai_insight_count", explanations.ai_insight_count || 0),
    metric("ai_request_failure_count", explanations.ai_request_failure_count || 0),
    metric("context_warning_count", explanations.context_warning_count || 0),
  ].join("");
  document.getElementById("ai-explanation-cards").innerHTML = cards.length
    ? cards.map((card) => aiExplanationCard(card)).join("")
    : emptyState("아직 RCA/AI insight가 없습니다. CLI 또는 API에서 report를 생성하면 여기에 표시됩니다.");
};

const aiExplanationCard = (card) => {
  const statusClass = `status-${cssToken(card.status)}`;
  const severityClass = `severity-${cssToken(card.severity)}`;
  const checks = Array.isArray(card.suggested_checks) ? card.suggested_checks : [];
  const warnings = Array.isArray(card.warnings) ? card.warnings : [];
  const sections = Array.isArray(card.report_sections) ? card.report_sections : [];
  const codexPrompt = card.card_type === "CODEX_PROMPT_DRAFT" ? codexPromptBlock(card) : "";
  return `
    <article class="ai-explanation-card ${statusClass} ${severityClass}" id="${escapeHtml(card.card_id)}">
      <div class="ai-card-topline">
        <div>
          <p class="eyebrow">${escapeHtml(card.card_type_label || card.card_type)}</p>
          <h3>${escapeHtml(card.title)}</h3>
          <p class="muted">${escapeHtml(card.subtitle)}</p>
        </div>
        <div class="ai-card-badges">
          <span class="ai-status-badge ${statusClass}">${escapeHtml(card.status_label || card.status)}</span>
          <span class="ai-status-badge ${severityClass}">${escapeHtml(card.severity_label || card.severity)}</span>
          <span class="readonly-badge">${escapeHtml(card.read_only_badge || "읽기 전용")}</span>
          <span class="no-side-effect-badge">${escapeHtml(card.no_side_effect_badge || "거래 영향 없음")}</span>
        </div>
      </div>
      <p class="ai-card-summary">${escapeHtml(card.summary)}</p>
      <div class="ai-card-meta">
        <span>${escapeHtml(card.root_cause_category_label || card.root_cause_category)}</span>
        <span>${escapeHtml(card.related_entity_type)}:${escapeHtml(card.related_entity_id)}</span>
        <span>${escapeHtml(card.trade_date)}</span>
        <span>${escapeHtml(card.generated_at)}</span>
      </div>
      ${card.root_cause ? `<p class="muted">${escapeHtml(card.root_cause)}</p>` : ""}
      ${aiCardList("점검", checks)}
      ${aiCardList("주의", warnings)}
      <div class="ai-card-links">
        ${card.draft_id ? `<span>draft ${escapeHtml(card.draft_id)}</span>` : ""}
        ${card.rca_report_id ? `<span>report ${escapeHtml(card.rca_report_id)}</span>` : ""}
        ${card.live_sim_review_id ? `<span>review ${escapeHtml(card.live_sim_review_id)}</span>` : ""}
        ${card.live_sim_order_id ? `<span>live_sim_order ${escapeHtml(card.live_sim_order_id)}</span>` : ""}
        ${card.reconcile_id ? `<span>reconcile ${escapeHtml(card.reconcile_id)}</span>` : ""}
        ${card.ai_insight_id ? `<span>insight ${escapeHtml(card.ai_insight_id)}</span>` : ""}
        ${card.ai_request_id ? `<span>request ${escapeHtml(card.ai_request_id)}</span>` : ""}
        ${card.context_id ? `<span>context ${escapeHtml(card.context_id)}</span>` : ""}
      </div>
      ${codexPrompt}
      ${aiReportSections(sections)}
      ${rawJson(card)}
    </article>
  `;
};

const codexPromptBlock = (card) => {
  const textareaId = `${cssToken(card.card_id)}-text`;
  return `
    <div class="codex-prompt-block">
      <textarea id="${escapeHtml(textareaId)}" readonly spellcheck="false">${escapeHtml(card.prompt_text || card.prompt_preview || "")}</textarea>
      <button type="button" class="copy-button" data-copy-target="${escapeHtml(textareaId)}">텍스트 복사</button>
    </div>
  `;
};

const aiCardList = (title, items) => {
  if (!items.length) {
    return "";
  }
  return `
    <div class="ai-card-list">
      <strong>${escapeHtml(title)}</strong>
      <ul>
        ${items.map((item) => `<li>${escapeHtml(item)}</li>`).join("")}
      </ul>
    </div>
  `;
};

const aiReportSections = (sections) => {
  if (!sections.length) {
    return "";
  }
  return `
    <details class="ai-section-details">
      <summary>세부 보기</summary>
      <div class="ai-section-list">
        ${sections
          .map(
            (section) => `
              <div class="ai-section-row">
                <strong>${escapeHtml(section.section_name)}</strong>
                <span>${badge(section.status)}</span>
                <span>${badge(section.severity)}</span>
                <p>${escapeHtml(section.summary)}</p>
              </div>
            `,
          )
          .join("")}
      </div>
    </details>
  `;
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
  renderDryRun(snapshot);
  renderLiveSim(snapshot);
  renderAiAdvisory(snapshot);
  renderErrors(snapshot);
  renderAi(snapshot);
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
  document.addEventListener("click", async (event) => {
    const button = event.target.closest("[data-copy-target]");
    if (!button) {
      return;
    }
    const target = document.getElementById(button.dataset.copyTarget);
    if (!target) {
      return;
    }
    target.select();
    try {
      await navigator.clipboard.writeText(target.value);
      button.textContent = "복사됨";
      window.setTimeout(() => {
        button.textContent = "텍스트 복사";
      }, 1200);
    } catch {
      document.execCommand("copy");
    }
  });
  refreshDashboard();
});

param(
    [string]$CoreUrl = $(if ($env:GATEWAY_CORE_URL) { $env:GATEWAY_CORE_URL } else { "http://127.0.0.1:8000" }),
    [int]$CorePort = 8000,
    [string]$Token = $(if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }),
    [string]$TradeDate = "",
    [string]$ConditionName = $env:KIWOOM_CONDITION_NAME,
    [string]$ConditionProfilesFile = $(if ($env:KIWOOM_CONDITION_PROFILES_FILE) { $env:KIWOOM_CONDITION_PROFILES_FILE } else { "configs\condition_profiles\market_open_profiles.json" }),
    [string]$ConditionProfilesJson = $env:KIWOOM_CONDITION_PROFILES,
    [string]$RealtimeCodes = $env:KIWOOM_REALTIME_CODES,
    [string]$RealtimeExchange = $(if ($env:KIWOOM_REALTIME_EXCHANGE) { $env:KIWOOM_REALTIME_EXCHANGE } else { "krx" }),
    [string]$MarketIndexEnabled = $(if ($env:KIWOOM_MARKET_INDEX_ENABLED) { $env:KIWOOM_MARKET_INDEX_ENABLED } else { "true" }),
    [string]$MarketIndexRealtimeEnabled = $(if ($env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED) { $env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED } else { "true" }),
    [string]$MarketIndexTrBootstrapEnabled = $(if ($env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED) { $env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED } else { "false" }),
    [string]$MarketIndexCodes = $(if ($env:KIWOOM_MARKET_INDEX_CODES) { $env:KIWOOM_MARKET_INDEX_CODES } else { "KOSPI,KOSDAQ" }),
    [string]$MarketIndexScreenNo = $(if ($env:KIWOOM_MARKET_INDEX_SCREEN_NO) { $env:KIWOOM_MARKET_INDEX_SCREEN_NO } else { "5700" }),
    [string]$MarketIndexPollSec = $(if ($env:KIWOOM_MARKET_INDEX_POLL_SEC) { $env:KIWOOM_MARKET_INDEX_POLL_SEC } else { "60.0" }),
    [string]$ThemeRefreshTradingSession = $(if ($env:THEME_REFRESH_TRADING_SESSION) { $env:THEME_REFRESH_TRADING_SESSION } else { "NXT" }),
    [string]$ThemeRefreshQueueMarketScanCommands = $(if ($env:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS) { $env:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS } else { "true" }),
    [string]$ThemeRefreshQueueRealtimeCommands = $(if ($env:THEME_REFRESH_QUEUE_REALTIME_COMMANDS) { $env:THEME_REFRESH_QUEUE_REALTIME_COMMANDS } else { "" }),
    [int]$ThemeRefreshRequestTimeoutSec = 120,
    [int]$CoreWaitSeconds = 30,
    [int]$GatewayWaitSeconds = 30,
    [string]$DbPath = "",
    [ValidateSet("DRY_RUN", "PRICE_TICK_ONLY", "TR_RESPONSE_ONLY", "CONDITION_EVENT_ONLY", "MARKET_DATA_LIMITED", "MARKET_DATA_FULL_GUARDED")]
    [string]$MarketDataOperatingMode = "MARKET_DATA_FULL_GUARDED",
    [ValidateRange(1, 3)]
    [int]$MarketDataGlobalSkipBudget = 3,
    [switch]$RealtimeFidValidation,
    [switch]$DisableStockRealtime,
    [switch]$DisableMarketIndexRealtime,
    [switch]$AppendOnlyEvidence,
    [switch]$MarketScanParserVerified,
    [switch]$AllowOperatingDatabase,
    [switch]$MarketReferenceProjectionValidation,
    [switch]$MarketReferenceLimitedCutover,
    [switch]$RunObserveCycle,
    [switch]$RunCore,
    [switch]$RunGateway,
    [switch]$RunThemeRefreshLoop,
    [switch]$RunAll
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
$Python32 = Join-Path $Root "venv_32\Scripts\python.exe"
if (-not (Test-Path $Python64)) { $Python64 = "python" }
if (-not (Test-Path $Python32)) { $Python32 = "python" }

function Import-DotEnv {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    foreach ($RawLine in Get-Content -LiteralPath $Path -Encoding UTF8) {
        $Line = $RawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($Line) -or $Line.StartsWith("#")) {
            continue
        }

        $Separator = $Line.IndexOf("=")
        if ($Separator -lt 1) {
            continue
        }

        $Name = $Line.Substring(0, $Separator).Trim()
        $Value = $Line.Substring($Separator + 1).Trim()
        if (
            ($Value.StartsWith('"') -and $Value.EndsWith('"')) -or
            ($Value.StartsWith("'") -and $Value.EndsWith("'"))
        ) {
            $Value = $Value.Substring(1, $Value.Length - 2)
        }

        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

function Quote-CommandArgument {
    param([string]$Value)

    if ($null -eq $Value) {
        return '""'
    }
    if ($Value -notmatch '[\s"]') {
        return $Value
    }

    return '"' + ($Value -replace '\\(?=\\*")', '$0$0' -replace '"', '\"') + '"'
}

function Resolve-BoolSetting {
    param(
        [string]$Name,
        [string]$Value,
        [bool]$Default
    )

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Default
    }

    $Normalized = $Value.Trim().ToLowerInvariant()
    if (@("1", "true", "yes", "y", "on") -contains $Normalized) {
        return $true
    }
    if (@("0", "false", "no", "n", "off") -contains $Normalized) {
        return $false
    }

    throw "$Name must be true or false. Got: $Value"
}

function Write-ObserveEnvOverrideFile {
    param([string]$Label)

    $BaseEnvPath = Join-Path $Root ".env"
    $BaseContent = ""
    if (Test-Path -LiteralPath $BaseEnvPath) {
        $BaseContent = Get-Content -LiteralPath $BaseEnvPath -Raw -Encoding UTF8
    }

    $OverridePath = Join-Path ([System.IO.Path]::GetTempPath()) "suseok_$($Label)_observe.env"
    $OverrideLines = @(
        "",
        "# Generated by start_market_open_observe.ps1 for safe OBSERVE runtime.",
        "TRADING_PROFILE=$($env:TRADING_PROFILE)",
        "TRADING_MODE=$($env:TRADING_MODE)",
        "TRADING_ALLOW_LIVE_SIM=$($env:TRADING_ALLOW_LIVE_SIM)",
        "TRADING_ALLOW_LIVE_REAL=$($env:TRADING_ALLOW_LIVE_REAL)",
        "TRADING_DB_PATH=$($env:TRADING_DB_PATH)",
        "DRY_RUN_ORDER_ROUTING_ENABLED=$($env:DRY_RUN_ORDER_ROUTING_ENABLED)",
        "DRY_RUN_GATEWAY_COMMAND_ENABLED=$($env:DRY_RUN_GATEWAY_COMMAND_ENABLED)",
        "DRY_RUN_EXIT_ENGINE_ENABLED=$($env:DRY_RUN_EXIT_ENGINE_ENABLED)",
        "DRY_RUN_EXIT_INTENT_CREATION_ENABLED=$($env:DRY_RUN_EXIT_INTENT_CREATION_ENABLED)",
        "DRY_RUN_EXIT_ORDER_CREATION_ENABLED=$($env:DRY_RUN_EXIT_ORDER_CREATION_ENABLED)",
        "DRY_RUN_EXIT_SIMULATED_FILL_ENABLED=$($env:DRY_RUN_EXIT_SIMULATED_FILL_ENABLED)",
        "DRY_RUN_EXIT_ORDER_ROUTING_ENABLED=$($env:DRY_RUN_EXIT_ORDER_ROUTING_ENABLED)",
        "DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED=$($env:DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED)",
        "LIVE_SIM_ENABLED=$($env:LIVE_SIM_ENABLED)",
        "LIVE_SIM_ORDER_ROUTING_ENABLED=$($env:LIVE_SIM_ORDER_ROUTING_ENABLED)",
        "LIVE_SIM_GATEWAY_COMMAND_ENABLED=$($env:LIVE_SIM_GATEWAY_COMMAND_ENABLED)",
        "LIVE_SIM_ALLOW_BUY=$($env:LIVE_SIM_ALLOW_BUY)",
        "LIVE_SIM_ALLOW_SELL=$($env:LIVE_SIM_ALLOW_SELL)",
        "LIVE_SIM_ALLOW_EXIT_SELL=$($env:LIVE_SIM_ALLOW_EXIT_SELL)",
        "LIVE_SIM_REPRICE_ENABLED=$($env:LIVE_SIM_REPRICE_ENABLED)",
        "LIVE_SIM_PILOT_PIPELINE_ENABLED=$($env:LIVE_SIM_PILOT_PIPELINE_ENABLED)",
        "LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND=$($env:LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND)",
        "LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED=$($env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED)",
        "LIVE_SIM_CANCEL_ENABLED=$($env:LIVE_SIM_CANCEL_ENABLED)",
        "LIVE_SIM_CANCEL_UNFILLED_ENABLED=$($env:LIVE_SIM_CANCEL_UNFILLED_ENABLED)",
        "LIVE_SIM_CANCEL_KILL_SWITCH=$($env:LIVE_SIM_CANCEL_KILL_SWITCH)",
        "LIVE_SIM_EXIT_ENGINE_ENABLED=$($env:LIVE_SIM_EXIT_ENGINE_ENABLED)",
        "LIVE_SIM_EXIT_ORDER_CREATION_ENABLED=$($env:LIVE_SIM_EXIT_ORDER_CREATION_ENABLED)",
        "LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED=$($env:LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED)",
        "LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED=$($env:LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED)",
        "LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED=$($env:LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED)",
        "LIVE_SIM_OPERATING_CYCLE_ENABLED=$($env:LIVE_SIM_OPERATING_CYCLE_ENABLED)",
        "LIVE_SIM_OPERATING_LOOP_ENABLED=$($env:LIVE_SIM_OPERATING_LOOP_ENABLED)",
        "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS=$($env:LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS)",
        "LIVE_SIM_KILL_SWITCH=$($env:LIVE_SIM_KILL_SWITCH)",
        "PROJECTION_OUTBOX_WORKER_ENABLED=$($env:PROJECTION_OUTBOX_WORKER_ENABLED)",
        "PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED=$($env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED)",
        "PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED=$($env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED)",
        "PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED=$($env:PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED)",
        "PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED=$($env:PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED)",
        "PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED=$($env:PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED)",
        "PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED=$($env:PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_ALLOW_CANDIDATE_INGEST_IN_WORKER=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_ALLOW_CANDIDATE_INGEST_IN_WORKER)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA)",
        "GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13=$($env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13)",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH=$($env:GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH)",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15=$($env:GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15)",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH=$($env:GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH)",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18=$($env:GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18)",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED=$($env:GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED)",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED=$($env:GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED)",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH=$($env:GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH)",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE=$($env:GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE)",
        "GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20=$($env:GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20)",
        "LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED=$($env:LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED)",
        "LIVE_SIM_LIFECYCLE_WORKER_ENABLED=$($env:LIVE_SIM_LIFECYCLE_WORKER_ENABLED)",
        "LIVE_SIM_LIFECYCLE_CUTOVER_DRY_RUN_ENABLED=$($env:LIVE_SIM_LIFECYCLE_CUTOVER_DRY_RUN_ENABLED)",
        "LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED=$($env:LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED)",
        "LIVE_SIM_LIFECYCLE_GLOBAL_KILL_SWITCH=$($env:LIVE_SIM_LIFECYCLE_GLOBAL_KILL_SWITCH)",
        "LIVE_SIM_LIFECYCLE_INLINE_FALLBACK_ENABLED=$($env:LIVE_SIM_LIFECYCLE_INLINE_FALLBACK_ENABLED)",
        "MARKET_SCAN_ENABLED=$($env:MARKET_SCAN_ENABLED)",
        "MARKET_SCAN_PARSER_STATUS=$($env:MARKET_SCAN_PARSER_STATUS)",
        "KIWOOM_MARKET_INDEX_ENABLED=$($env:KIWOOM_MARKET_INDEX_ENABLED)",
        "KIWOOM_MARKET_INDEX_REALTIME_ENABLED=$($env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED)",
        "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED=$($env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED)",
        "KIWOOM_MARKET_INDEX_CODES=$($env:KIWOOM_MARKET_INDEX_CODES)",
        "KIWOOM_MARKET_INDEX_SCREEN_NO=$($env:KIWOOM_MARKET_INDEX_SCREEN_NO)",
        "KIWOOM_MARKET_INDEX_POLL_SEC=$($env:KIWOOM_MARKET_INDEX_POLL_SEC)",
        "AI_EXTERNAL_LLM_ALLOW_NETWORK=$($env:AI_EXTERNAL_LLM_ALLOW_NETWORK)",
        "AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS=$($env:AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS)"
    )
    if (-not [string]::IsNullOrWhiteSpace($env:TRADING_CORE_TOKEN)) {
        $OverrideLines += "TRADING_CORE_TOKEN=$($env:TRADING_CORE_TOKEN)"
    }
    if (-not [string]::IsNullOrWhiteSpace($env:GATEWAY_CORE_TOKEN)) {
        $OverrideLines += "GATEWAY_CORE_TOKEN=$($env:GATEWAY_CORE_TOKEN)"
    }
    if ($MarketReferenceValidationRequested) {
        $OverrideLines += @(
            "CONDITION_FUSION_SWEEP_ENABLED=$($env:CONDITION_FUSION_SWEEP_ENABLED)",
            "INCREMENTAL_EVALUATION_WORKER_ENABLED=$($env:INCREMENTAL_EVALUATION_WORKER_ENABLED)",
            "EVENT_STORE_RETENTION_ENABLED=$($env:EVENT_STORE_RETENTION_ENABLED)"
        )
    }

    Set-Content `
        -LiteralPath $OverridePath `
        -Value (($BaseContent.TrimEnd()) + "`r`n" + ($OverrideLines -join "`r`n") + "`r`n") `
        -Encoding UTF8
    $env:TRADING_ENV_FILE = $OverridePath
}

function Start-DetachedRuntimeProcess {
    param(
        [string]$Label,
        [string]$FilePath,
        [string[]]$Arguments,
        [bool]$Hidden = $true
    )

    $LogDir = Join-Path $Root "logs\runtime"
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    $Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $SafeLabel = $Label.ToLowerInvariant() -replace "[^a-z0-9]+", "_"
    $SafeLabel = $SafeLabel.Trim("_")
    $OutLog = Join-Path $LogDir "$($SafeLabel)_$Stamp.out.log"
    $ErrLog = Join-Path $LogDir "$($SafeLabel)_$Stamp.err.log"

    $StartInfo = @{
        FilePath = $FilePath
        ArgumentList = ($Arguments | ForEach-Object { Quote-CommandArgument $_ }) -join " "
        WorkingDirectory = $Root
        PassThru = $true
        RedirectStandardOutput = $OutLog
        RedirectStandardError = $ErrLog
    }
    if ($Hidden) {
        $StartInfo.WindowStyle = "Hidden"
    }

    $Process = Start-Process @StartInfo
    Write-Host "$Label process started. PID=$($Process.Id)"
    Write-Host "  stdout: $OutLog"
    Write-Host "  stderr: $ErrLog"
    return $Process
}

function Wait-CoreHealth {
    param(
        [string]$Url,
        [int]$WaitSeconds
    )

    $Deadline = (Get-Date).AddSeconds($WaitSeconds)
    while ((Get-Date) -lt $Deadline) {
        try {
            Invoke-RestMethod -Uri "$($Url.TrimEnd('/'))/health" -TimeoutSec 3 | Out-Null
            Write-Host "Core health check passed."
            return $true
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    throw "Core health check did not pass within $WaitSeconds seconds."
}

function Resolve-WorkspacePath {
    param([string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return [System.IO.Path]::GetFullPath($Path)
    }
    return [System.IO.Path]::GetFullPath((Join-Path $Root $Path))
}

function Assert-CoreObserveSafety {
    param(
        [string]$Url,
        [string]$ExpectedDbPath,
        [bool]$RequireRealtimeOnly,
        [bool]$RequireAppendOnlyConfiguration
    )

    $BaseUrl = $Url.TrimEnd('/')
    $Status = Invoke-RestMethod -Uri "$BaseUrl/api/status" -TimeoutSec 5
    if ($Status.profile -ne "OBSERVE" -or $Status.mode -ne "OBSERVE") {
        throw "Core is not OBSERVE-safe: profile=$($Status.profile) mode=$($Status.mode)"
    }
    if ($Status.live_sim_allowed -or $Status.live_real_allowed) {
        throw "Core live permission is enabled: live_sim=$($Status.live_sim_allowed) live_real=$($Status.live_real_allowed)"
    }
    $ActualDbPath = Resolve-WorkspacePath -Path ([string]$Status.database_path)
    if ($ActualDbPath -ne $ExpectedDbPath) {
        throw "Core DB path mismatch: expected=$ExpectedDbPath actual=$ActualDbPath"
    }

    if ($RequireRealtimeOnly) {
        $Bootstrap = Invoke-RestMethod -Uri "$BaseUrl/api/operator/market-index/tr-bootstrap/status" -TimeoutSec 5
        if ($Bootstrap.enabled) {
            throw "TR bootstrap must remain disabled in realtime evidence modes."
        }
    }

    if ($RequireAppendOnlyConfiguration) {
        $Readiness = Invoke-RestMethod -Uri "$BaseUrl/api/operator/append-only-readiness/status" -TimeoutSec 10
        if (-not $Readiness.configuration.ready) {
            $Blocked = @($Readiness.configuration.blocked_gates) -join ","
            throw "Append-only evidence configuration is not armed: $Blocked"
        }
        Write-Host "Append-only configuration gate passed. readiness=$($Readiness.status)"
    }

    Write-Host "Core OBSERVE preflight passed. DB=$ActualDbPath"
}

function Start-KiwoomGatewayDetached {
    $GatewayScript = Join-Path $PSScriptRoot "start_kiwoom_gateway_visible.ps1"
    $GatewayScriptParams = @{
        CoreUrl = $CoreUrl
        Token = $Token
        RealtimeExchange = $RealtimeExchange
        MarketIndexEnabled = $env:KIWOOM_MARKET_INDEX_ENABLED
        MarketIndexRealtimeEnabled = $env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED
        MarketIndexTrBootstrapEnabled = $env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED
        MarketIndexCodes = $MarketIndexCodes
        MarketIndexScreenNo = $MarketIndexScreenNo
        MarketIndexPollSec = $MarketIndexPollSec
        Detached = $true
        Log = $true
        WaitSeconds = $GatewayWaitSeconds
    }
    if (-not [string]::IsNullOrWhiteSpace($ConditionProfilesFile)) {
        $GatewayScriptParams.ConditionProfilesFile = $ConditionProfilesFile
    } elseif (-not [string]::IsNullOrWhiteSpace($ConditionProfilesJson)) {
        $GatewayScriptParams.ConditionProfilesJson = $ConditionProfilesJson
    } elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
        $GatewayScriptParams.ConditionName = $ConditionName
    } else {
        $GatewayScriptParams.DisableConditions = $true
    }
    if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
        $GatewayScriptParams.RealtimeCodes = $RealtimeCodes
    }
    if ($RealtimeFidValidation -or $DisableStockRealtime) {
        $GatewayScriptParams.DisableRealtimeCodes = $true
    }

    & $GatewayScript @GatewayScriptParams
}

function Start-ThemeRefreshLoopDetached {
    $LoopScript = Join-Path $PSScriptRoot "start_theme_refresh_loop.ps1"
    $LoopScriptArgs = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $LoopScript,
        "-CoreUrl", $CoreUrl,
        "-Token", $Token,
        "-TradingSession", $ThemeRefreshTradingSession,
        "-QueueMarketScanCommands", $ThemeRefreshQueueMarketScanCommands,
        "-RequestTimeoutSec", [string]$ThemeRefreshRequestTimeoutSec
    )
    if (-not [string]::IsNullOrWhiteSpace($TradeDate)) {
        $LoopScriptArgs += @("-TradeDate", $TradeDate)
    }
    if (-not [string]::IsNullOrWhiteSpace($ThemeRefreshQueueRealtimeCommands)) {
        $LoopScriptArgs += @("-QueueRealtimeCommands", $ThemeRefreshQueueRealtimeCommands)
    }

    Start-DetachedRuntimeProcess `
        -Label "theme_refresh_loop" `
        -FilePath "powershell.exe" `
        -Arguments $LoopScriptArgs `
        -Hidden $true | Out-Null
}

Import-DotEnv -Path (Join-Path $Root ".env")

if ($RealtimeFidValidation -and $AppendOnlyEvidence) {
    throw "RealtimeFidValidation and AppendOnlyEvidence are mutually exclusive."
}
if ($MarketScanParserVerified -and -not $AppendOnlyEvidence) {
    throw "MarketScanParserVerified is only allowed with AppendOnlyEvidence."
}
$EvidenceModeRequested = $RealtimeFidValidation -or $AppendOnlyEvidence
$OperatingDbPath = Resolve-WorkspacePath -Path $env:TRADING_DB_PATH
if ($EvidenceModeRequested -and [string]::IsNullOrWhiteSpace($DbPath)) {
    throw "DbPath is required for realtime/evidence validation modes."
}
$ResolvedDbPath = if ([string]::IsNullOrWhiteSpace($DbPath)) {
    $OperatingDbPath
} else {
    Resolve-WorkspacePath -Path $DbPath
}
if (
    $EvidenceModeRequested -and
    -not $AllowOperatingDatabase -and
    $ResolvedDbPath -eq $OperatingDbPath
) {
    throw "Evidence mode refuses the operating DB without -AllowOperatingDatabase. DB=$ResolvedDbPath"
}
$env:TRADING_DB_PATH = $ResolvedDbPath

if ([string]::IsNullOrWhiteSpace($Token)) {
    $Token = if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }
}

$RunCoreRequested = $RunAll -or $RunCore
$RunGatewayRequested = $RunAll -or $RunGateway
$RunThemeRefreshLoopRequested = ($RunAll -or $RunThemeRefreshLoop) -and -not $RealtimeFidValidation
$MarketReferenceValidationRequested = (
    $MarketReferenceProjectionValidation -or
    $MarketReferenceLimitedCutover -or
    $AppendOnlyEvidence
)
if ($RealtimeFidValidation) {
    $ConditionName = ""
    $ConditionProfilesFile = ""
    $ConditionProfilesJson = ""
    $RealtimeCodes = ""
}

$env:TRADING_PROFILE = "OBSERVE"
$env:TRADING_MODE = "OBSERVE"
$env:TRADING_ALLOW_LIVE_SIM = "false"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:DRY_RUN_ORDER_ROUTING_ENABLED = "false"
$env:DRY_RUN_GATEWAY_COMMAND_ENABLED = "false"
$env:DRY_RUN_EXIT_ENGINE_ENABLED = "false"
$env:DRY_RUN_EXIT_INTENT_CREATION_ENABLED = "false"
$env:DRY_RUN_EXIT_ORDER_CREATION_ENABLED = "false"
$env:DRY_RUN_EXIT_SIMULATED_FILL_ENABLED = "false"
$env:DRY_RUN_EXIT_ORDER_ROUTING_ENABLED = "false"
$env:DRY_RUN_EXIT_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ENABLED = "false"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_ALLOW_BUY = "false"
$env:LIVE_SIM_ALLOW_SELL = "false"
$env:LIVE_SIM_ALLOW_EXIT_SELL = "false"
$env:LIVE_SIM_REPRICE_ENABLED = "false"
$env:LIVE_SIM_PILOT_PIPELINE_ENABLED = "false"
$env:LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND = "false"
$env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED = "false"
$env:LIVE_SIM_CANCEL_ENABLED = "false"
$env:LIVE_SIM_CANCEL_UNFILLED_ENABLED = "false"
$env:LIVE_SIM_CANCEL_KILL_SWITCH = "true"
$env:LIVE_SIM_EXIT_ENGINE_ENABLED = "false"
$env:LIVE_SIM_EXIT_ORDER_CREATION_ENABLED = "false"
$env:LIVE_SIM_EXIT_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_EXIT_EOD_FLATTEN_ENABLED = "false"
$env:LIVE_SIM_RECONCILE_REQUEST_BROKER_SNAPSHOT_ENABLED = "false"
$env:LIVE_SIM_OPERATING_CYCLE_ENABLED = "false"
$env:LIVE_SIM_OPERATING_LOOP_ENABLED = "false"
$env:LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS = "false"
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:PROJECTION_OUTBOX_WORKER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_APPLY_PROJECTION_ENABLED = if ($MarketReferenceValidationRequested) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_MARKET_DATA_APPLY_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_MARKET_REFERENCE_APPLY_ENABLED = if ($MarketReferenceValidationRequested) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_MARKET_INDEX_APPLY_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_MARKET_REGIME_APPLY_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:PROJECTION_OUTBOX_MARKET_SCAN_APPLY_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE = if ($AppendOnlyEvidence) { $MarketDataOperatingMode } else { "OFF" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_KILL_SWITCH = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { [string]$MarketDataGlobalSkipBudget } else { "0" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_AUTO_ROLLBACK_ENABLED = "true"
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_PRICE_TICK_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_TR_RESPONSE_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_DATA_APPEND_ONLY_CONDITION_EVENT_ALLOW_CANDIDATE_INGEST_IN_WORKER = "false"
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_DRY_RUN_ENABLED = if ($MarketReferenceValidationRequested) { "true" } else { "false" }
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_CUTOVER_ENABLED = if ($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_GLOBAL_KILL_SWITCH = if ($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_SKIP_PER_MINUTE = if ($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_MAX_PENDING_WITHIN_SLA = "1"
$env:GATEWAY_MARKET_REFERENCE_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR13 = if ($MarketReferenceLimitedCutover -or $AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_INDEX_APPEND_ONLY_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_INDEX_APPEND_ONLY_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_INDEX_APPEND_ONLY_GLOBAL_KILL_SWITCH = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_INDEX_APPEND_ONLY_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_INDEX_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR15 = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_REGIME_APPEND_ONLY_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_REGIME_APPEND_ONLY_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_REGIME_APPEND_ONLY_GLOBAL_KILL_SWITCH = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_REGIME_APPEND_ONLY_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_REGIME_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR18 = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_SCAN_APPEND_ONLY_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_SCAN_APPEND_ONLY_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:GATEWAY_MARKET_SCAN_APPEND_ONLY_GLOBAL_KILL_SWITCH = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:GATEWAY_MARKET_SCAN_APPEND_ONLY_MAX_SKIP_PER_MINUTE = if ($AppendOnlyEvidence) { "1" } else { "0" }
$env:GATEWAY_MARKET_SCAN_APPEND_ONLY_EFFECTIVE_SKIP_DISABLED_IN_PR20 = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:LIVE_SIM_LIFECYCLE_CONSUMER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:LIVE_SIM_LIFECYCLE_WORKER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:LIVE_SIM_LIFECYCLE_CUTOVER_DRY_RUN_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:LIVE_SIM_LIFECYCLE_CUTOVER_ENABLED = if ($AppendOnlyEvidence) { "true" } else { "false" }
$env:LIVE_SIM_LIFECYCLE_GLOBAL_KILL_SWITCH = if ($AppendOnlyEvidence) { "false" } else { "true" }
$env:LIVE_SIM_LIFECYCLE_INLINE_FALLBACK_ENABLED = "true"
if ($MarketReferenceValidationRequested) {
    $env:CONDITION_FUSION_SWEEP_ENABLED = "false"
    $env:INCREMENTAL_EVALUATION_WORKER_ENABLED = "false"
    $env:EVENT_STORE_RETENTION_ENABLED = "false"
}
$env:AI_EXTERNAL_LLM_ALLOW_NETWORK = "false"
$env:AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS = "false"

if ($RunThemeRefreshLoopRequested) {
    $env:MARKET_SCAN_ENABLED = "true"
    if ([string]::IsNullOrWhiteSpace($env:MARKET_SCAN_INTERVAL_SEC)) {
        $env:MARKET_SCAN_INTERVAL_SEC = "120"
    }
}
if ($AppendOnlyEvidence) {
    $env:MARKET_SCAN_ENABLED = "true"
    if ($MarketScanParserVerified) {
        $env:MARKET_SCAN_PARSER_STATUS = "KOA_STUDIO_VERIFIED"
    }
} elseif ($RealtimeFidValidation) {
    $env:MARKET_SCAN_ENABLED = "false"
}

if (-not [string]::IsNullOrWhiteSpace($Token)) {
    $env:TRADING_CORE_TOKEN = $Token
    $env:GATEWAY_CORE_TOKEN = $Token
}

if (($RunGatewayRequested -or $RunThemeRefreshLoopRequested) -and [string]::IsNullOrWhiteSpace($Token)) {
    throw "TRADING_CORE_TOKEN or GATEWAY_CORE_TOKEN is required to start Gateway or theme refresh loop."
}

$MarketIndexEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_ENABLED" -Value $MarketIndexEnabled -Default $true
$MarketIndexRealtimeEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED" -Value $MarketIndexRealtimeEnabled -Default $true
$MarketIndexTrBootstrapEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED" -Value $MarketIndexTrBootstrapEnabled -Default $false
if ($EvidenceModeRequested) {
    $MarketIndexEnabledValue = -not $DisableMarketIndexRealtime
    $MarketIndexRealtimeEnabledValue = -not $DisableMarketIndexRealtime
    $MarketIndexTrBootstrapEnabledValue = $false
}
$env:KIWOOM_MARKET_INDEX_ENABLED = if ($MarketIndexEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED = if ($MarketIndexRealtimeEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED = if ($MarketIndexTrBootstrapEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_CODES = $MarketIndexCodes
$env:KIWOOM_MARKET_INDEX_SCREEN_NO = $MarketIndexScreenNo
$env:KIWOOM_MARKET_INDEX_POLL_SEC = $MarketIndexPollSec

Write-ObserveEnvOverrideFile -Label "market_open"

$ResolvedConditionProfiles = ""
$ConditionProfileSource = ""
$ConditionProfileCount = 0
if (-not [string]::IsNullOrWhiteSpace($ConditionProfilesFile)) {
    $ProfileCandidate = $ConditionProfilesFile
    if (-not [System.IO.Path]::IsPathRooted($ProfileCandidate)) {
        $ProfileCandidate = Join-Path $Root $ProfileCandidate
    }
    $ProfilePath = (Resolve-Path -LiteralPath $ProfileCandidate).Path
    $ResolvedConditionProfiles = Get-Content -LiteralPath $ProfilePath -Raw -Encoding UTF8
    $ConditionProfileSource = "file:$ProfilePath"
    $env:KIWOOM_CONDITION_PROFILES_FILE = $ProfilePath
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionProfilesJson)) {
    $ResolvedConditionProfiles = $ConditionProfilesJson
    $ConditionProfileSource = "json/env"
}
if (-not [string]::IsNullOrWhiteSpace($ResolvedConditionProfiles)) {
    $env:KIWOOM_CONDITION_PROFILES = $ResolvedConditionProfiles
    try {
        $ParsedProfiles = $ResolvedConditionProfiles | ConvertFrom-Json
        if ($ParsedProfiles.profiles) {
            $ConditionProfileCount = @($ParsedProfiles.profiles).Count
        } elseif ($ParsedProfiles -is [array]) {
            $ConditionProfileCount = @($ParsedProfiles).Count
        } else {
            $ConditionProfileCount = 1
        }
    } catch {
        $ConditionProfileCount = -1
    }
}
$ConditionMode = if (-not [string]::IsNullOrWhiteSpace($ResolvedConditionProfiles)) {
    "MULTI_PROFILE"
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
    "LEGACY_SINGLE"
} else {
    "NONE"
}

Write-Host "Market-open OBSERVE profile is prepared."
Write-Host "LIVE_REAL=false, LIVE_SIM routing=false, queue_commands default remains false."
Write-Host "Evidence mode: realtime_fid=$($RealtimeFidValidation.IsPresent) append_only=$($AppendOnlyEvidence.IsPresent)"
Write-Host "Market-data operating mode: $($env:GATEWAY_MARKET_DATA_APPEND_ONLY_OPERATING_MODE) global_budget=$($env:GATEWAY_MARKET_DATA_APPEND_ONLY_GLOBAL_MAX_SKIP_PER_MINUTE)/min"
Write-Host "Database path: $ResolvedDbPath"
Write-Host "Market reference projection validation: $($MarketReferenceProjectionValidation.IsPresent)"
Write-Host "Market reference limited cutover: $($MarketReferenceLimitedCutover.IsPresent)"
Write-Host "Core URL: $CoreUrl"
Write-Host "Dashboard URL: $CoreUrl/dashboard"
Write-Host "Condition mode: $ConditionMode"
if ($ConditionMode -eq "MULTI_PROFILE") {
    Write-Host "Condition profile source: $ConditionProfileSource"
    Write-Host "Condition profile count: $ConditionProfileCount"
} elseif ($ConditionMode -eq "LEGACY_SINGLE") {
    Write-Host "Legacy condition name: $ConditionName"
}
Write-Host "Market index adapter: enabled=$MarketIndexEnabledValue realtime=$MarketIndexRealtimeEnabledValue tr_bootstrap=$MarketIndexTrBootstrapEnabledValue codes=$MarketIndexCodes"
Write-Host ""
Write-Host "64-bit Core command:"
Write-Host "  $Python64 -m uvicorn apps.core_api:app --host 127.0.0.1 --port $CorePort"
Write-Host ""
Write-Host "32-bit Kiwoom Gateway command:"
$GatewayCommand = @(
    "$Python32 -m apps.kiwoom_gateway",
    "--core-url $CoreUrl",
    "--token `$env:GATEWAY_CORE_TOKEN",
    "--observe-only",
    "--auto-login",
    "--no-threaded-login",
    "--realtime-exchange $RealtimeExchange",
    "--realtime-recover-interval-sec 300"
)
if ($ConditionMode -eq "MULTI_PROFILE") {
    $GatewayCommand += "--condition-profiles `$env:KIWOOM_CONDITION_PROFILES"
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
    $GatewayCommand += "--condition-name `"$ConditionName`""
    $GatewayCommand += "--condition-realtime"
}
if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
    $GatewayCommand += "--realtime-codes `"$RealtimeCodes`""
}
if ($MarketIndexEnabledValue) {
    $GatewayCommand += "--market-index-enabled"
} else {
    $GatewayCommand += "--no-market-index-enabled"
}
if ($MarketIndexRealtimeEnabledValue) {
    $GatewayCommand += "--market-index-realtime-enabled"
} else {
    $GatewayCommand += "--no-market-index-realtime-enabled"
}
if ($MarketIndexTrBootstrapEnabledValue) {
    $GatewayCommand += "--market-index-tr-bootstrap-enabled"
} else {
    $GatewayCommand += "--no-market-index-tr-bootstrap-enabled"
}
$GatewayCommand += "--market-index-codes `"$MarketIndexCodes`""
$GatewayCommand += "--market-index-screen-no $MarketIndexScreenNo"
$GatewayCommand += "--market-index-poll-sec $MarketIndexPollSec"
Write-Host "  $($GatewayCommand -join ' ')"
Write-Host ""
Write-Host "RCA command:"
Write-Host "  $Python64 -m tools.ops_market_open_rca --core-url $CoreUrl --token `$env:TRADING_CORE_TOKEN"
Write-Host ""
Write-Host "Observe cycle command:"
Write-Host "  $Python64 -m tools.run_market_open_observe_cycle"
Write-Host ""
Write-Host "One-shot launcher command:"
Write-Host "  .\tools\start_market_open_observe.ps1 -RunAll"
Write-Host ""
Write-Host "Theme refresh loop command:"
Write-Host "  .\tools\start_theme_refresh_loop.ps1 -CoreUrl $CoreUrl -Token `$env:TRADING_CORE_TOKEN -TradingSession $ThemeRefreshTradingSession"
Write-Host ""
Write-Host "Read-only check endpoints after Core/Gateway start:"
Write-Host "  $CoreUrl/health"
Write-Host "  $CoreUrl/api/status"
Write-Host "  $CoreUrl/api/gateway/status"
Write-Host "  $CoreUrl/api/gateway/events/recent?limit=20"
Write-Host "  $CoreUrl/api/market-data/status"
Write-Host "  $CoreUrl/api/dashboard/snapshot"

if ($RunObserveCycle) {
    $ObserveCycleArgs = @("-m", "tools.run_market_open_observe_cycle")
    if (-not [string]::IsNullOrWhiteSpace($TradeDate)) {
        $ObserveCycleArgs += @("--trade-date", $TradeDate)
    }
    & $Python64 @ObserveCycleArgs
}

if ($RunCoreRequested) {
    $CoreArgs = @(
        "-m", "uvicorn",
        "apps.core_api:app",
        "--host", "127.0.0.1",
        "--port", [string]$CorePort
    )
    if (
        $RunAll -or
        $RunGatewayRequested -or
        $RunThemeRefreshLoopRequested -or
        $EvidenceModeRequested
    ) {
        Start-DetachedRuntimeProcess `
            -Label "core" `
            -FilePath $Python64 `
            -Arguments $CoreArgs `
            -Hidden $true | Out-Null
        Wait-CoreHealth -Url $CoreUrl -WaitSeconds $CoreWaitSeconds
        Assert-CoreObserveSafety `
            -Url $CoreUrl `
            -ExpectedDbPath $ResolvedDbPath `
            -RequireRealtimeOnly $EvidenceModeRequested `
            -RequireAppendOnlyConfiguration (
                $AppendOnlyEvidence.IsPresent -and
                $MarketDataOperatingMode -eq "MARKET_DATA_FULL_GUARDED"
            )
    } else {
        & $Python64 @CoreArgs
    }
}

if ($RunGatewayRequested) {
    Start-KiwoomGatewayDetached
}

if ($RunThemeRefreshLoopRequested) {
    Start-ThemeRefreshLoopDetached
}

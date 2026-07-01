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
    [switch]$RunObserveCycle,
    [switch]$RunCore
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
$Python32 = Join-Path $Root "venv_32\Scripts\python.exe"
if (-not (Test-Path $Python64)) { $Python64 = "python" }
if (-not (Test-Path $Python32)) { $Python32 = "python" }

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

$env:TRADING_PROFILE = "OBSERVE"
$env:TRADING_MODE = "OBSERVE"
$env:TRADING_ALLOW_LIVE_SIM = "false"
$env:TRADING_ALLOW_LIVE_REAL = "false"
$env:LIVE_SIM_ENABLED = "false"
$env:LIVE_SIM_ORDER_ROUTING_ENABLED = "false"
$env:LIVE_SIM_GATEWAY_COMMAND_ENABLED = "false"
$env:LIVE_SIM_PILOT_PIPELINE_ENABLED = "false"
$env:LIVE_SIM_PILOT_AUTO_QUEUE_COMMAND = "false"
$env:LIVE_SIM_ORDER_PLAN_ROUTING_ENABLED = "false"
$env:LIVE_SIM_KILL_SWITCH = "true"
$env:AI_EXTERNAL_LLM_ALLOW_NETWORK = "false"
$env:AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS = "false"

if (-not [string]::IsNullOrWhiteSpace($Token)) {
    $env:TRADING_CORE_TOKEN = $Token
    $env:GATEWAY_CORE_TOKEN = $Token
}

$MarketIndexEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_ENABLED" -Value $MarketIndexEnabled -Default $true
$MarketIndexRealtimeEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED" -Value $MarketIndexRealtimeEnabled -Default $true
$MarketIndexTrBootstrapEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED" -Value $MarketIndexTrBootstrapEnabled -Default $false
$env:KIWOOM_MARKET_INDEX_ENABLED = if ($MarketIndexEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED = if ($MarketIndexRealtimeEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED = if ($MarketIndexTrBootstrapEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_CODES = $MarketIndexCodes
$env:KIWOOM_MARKET_INDEX_SCREEN_NO = $MarketIndexScreenNo
$env:KIWOOM_MARKET_INDEX_POLL_SEC = $MarketIndexPollSec

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
Write-Host "  $Python64 -m uvicorn apps.core_api:app --host 127.0.0.1 --port $CorePort --reload"
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
Write-Host "Read-only check endpoints after Core/Gateway start:"
Write-Host "  $CoreUrl/health"
Write-Host "  $CoreUrl/api/status"
Write-Host "  $CoreUrl/api/gateway/status"
Write-Host "  $CoreUrl/api/gateway/events/recent?limit=20"
Write-Host "  $CoreUrl/api/market-data/status"
Write-Host "  $CoreUrl/api/dashboard/snapshot"

if ($RunObserveCycle) {
    $Args = @("-m", "tools.run_market_open_observe_cycle")
    if (-not [string]::IsNullOrWhiteSpace($TradeDate)) {
        $Args += @("--trade-date", $TradeDate)
    }
    & $Python64 @Args
}

if ($RunCore) {
    & $Python64 -m uvicorn apps.core_api:app --host 127.0.0.1 --port $CorePort --reload
}

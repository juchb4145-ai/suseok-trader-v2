param(
    [string]$CoreUrl = $(if ($env:GATEWAY_CORE_URL) { $env:GATEWAY_CORE_URL } else { "http://127.0.0.1:8000" }),
    [int]$CorePort = 8000,
    [string]$Token = $(if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }),
    [string]$TradeDate = "",
    [string]$ConditionName = $env:KIWOOM_CONDITION_NAME,
    [string]$RealtimeCodes = $env:KIWOOM_REALTIME_CODES,
    [switch]$RunObserveCycle,
    [switch]$RunCore
)

$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
$Python32 = Join-Path $Root "venv_32\Scripts\python.exe"
if (-not (Test-Path $Python64)) { $Python64 = "python" }
if (-not (Test-Path $Python32)) { $Python32 = "python" }

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

Write-Host "Market-open OBSERVE profile is prepared."
Write-Host "LIVE_REAL=false, LIVE_SIM routing=false, queue_commands default remains false."
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
    "--auto-login"
)
if (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
    $GatewayCommand += "--condition-name `"$ConditionName`""
    $GatewayCommand += "--condition-realtime"
}
if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
    $GatewayCommand += "--realtime-codes `"$RealtimeCodes`""
}
Write-Host "  $($GatewayCommand -join ' ')"
Write-Host ""
Write-Host "RCA command:"
Write-Host "  $Python64 -m tools.ops_market_open_rca --core-url $CoreUrl --token `$env:TRADING_CORE_TOKEN"
Write-Host ""
Write-Host "Observe cycle command:"
Write-Host "  $Python64 -m tools.run_market_open_observe_cycle"

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

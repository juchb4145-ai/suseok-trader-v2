param(
    [string]$CoreUrl = "http://127.0.0.1:8040",
    [int]$CorePort = 8040,
    [string]$Token = "",
    [string]$EvidenceDbPath = $(Join-Path $env:LOCALAPPDATA "SuseokTrading\evidence\append-only-10day.sqlite3"),
    [string]$TradeDate = $(Get-Date -Format "yyyy-MM-dd"),
    [ValidateRange(1, 30)]
    [int]$SettleSec = 3,
    [switch]$KeepCore
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python64)) { $Python64 = "python" }

function Import-DotEnv {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) { return }
    foreach ($RawLine in Get-Content -LiteralPath $Path -Encoding UTF8) {
        $Line = $RawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($Line) -or $Line.StartsWith("#")) { continue }
        $Separator = $Line.IndexOf("=")
        if ($Separator -lt 1) { continue }
        $Name = $Line.Substring(0, $Separator).Trim()
        $Value = $Line.Substring($Separator + 1).Trim().Trim('"').Trim("'")
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
}

Import-DotEnv -Path (Join-Path $Root ".env")
if ([string]::IsNullOrWhiteSpace($Token)) {
    $Token = if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }
}
if ([string]::IsNullOrWhiteSpace($Token)) {
    throw "TRADING_CORE_TOKEN or GATEWAY_CORE_TOKEN is required."
}
$ResolvedDbPath = [System.IO.Path]::GetFullPath($EvidenceDbPath)
$SessionStatePath = "$ResolvedDbPath.session.json"
if (-not (Test-Path -LiteralPath $SessionStatePath)) {
    throw "Session state is missing. Refusing to synthesize a daily baseline: $SessionStatePath"
}

$GatewayProcesses = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "python.exe" -and
    $_.CommandLine -match "apps\.kiwoom_gateway" -and
    $_.CommandLine -like "*$CoreUrl*"
}
$ThemeProcesses = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -in @("powershell.exe", "pwsh.exe") -and
    $_.CommandLine -match "start_theme_refresh_loop\.ps1" -and
    $_.CommandLine -like "*$CoreUrl*"
}
@($GatewayProcesses) + @($ThemeProcesses) | ForEach-Object {
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}
Start-Sleep -Seconds $SettleSec

$Arguments = @(
    (Join-Path $PSScriptRoot "ops_append_only_daily_evidence.py"),
    "--core-url", $CoreUrl,
    "--token", $Token,
    "--expected-db-path", $ResolvedDbPath,
    "--session-state-path", $SessionStatePath,
    "--trade-date", $TradeDate,
    "--settle-sec", [string]$SettleSec
)
& $Python64 @Arguments
$CloseExitCode = $LASTEXITCODE
if ($CloseExitCode -ne 0) {
    Write-Warning "Daily evidence close failed. Core remains running for inspection."
    exit $CloseExitCode
}

if (-not $KeepCore) {
    $Listeners = Get-NetTCPConnection -LocalPort $CorePort -State Listen -ErrorAction SilentlyContinue
    foreach ($Listener in @($Listeners)) {
        $Process = Get-CimInstance Win32_Process -Filter "ProcessId=$($Listener.OwningProcess)"
        if ($null -eq $Process -or $Process.CommandLine -notmatch "uvicorn apps\.core_api:app") {
            throw "Refusing to stop unexpected listener on port $CorePort. PID=$($Listener.OwningProcess)"
        }
        Stop-Process -Id $Listener.OwningProcess -Force
    }
}

foreach ($Name in @("suseok_kiwoom_gateway_observe.env", "suseok_market_open_observe.env")) {
    $Path = Join-Path ([System.IO.Path]::GetTempPath()) $Name
    if (Test-Path -LiteralPath $Path) { Remove-Item -LiteralPath $Path -Force }
}
Remove-Item -LiteralPath $SessionStatePath -Force

Write-Host "Daily evidence close completed. Persistent DB retained: $ResolvedDbPath"

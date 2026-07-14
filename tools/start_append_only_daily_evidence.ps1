param(
    [string]$CoreUrl = "http://127.0.0.1:8040",
    [int]$CorePort = 8040,
    [string]$Token = "",
    [string]$EvidenceDbPath = $(Join-Path $env:LOCALAPPDATA "SuseokTrading\evidence\append-only-10day.sqlite3"),
    [string]$TradeDate = $(Get-Date -Format "yyyy-MM-dd"),
    [ValidateRange(1, 3)]
    [int]$GlobalBudget = 3,
    [ValidateRange(0, 60)]
    [int]$GatewayStabilizeSec = 15,
    [ValidateRange(1, 3)]
    [int]$GatewayStartAttempts = 2,
    [ValidateRange(30, 240)]
    [int]$KeeperIntervalSec = 45,
    [ValidateRange(100, 1000)]
    [int]$KeeperReconcileLimit = 500,
    [switch]$KrxTradingDayConfirmed
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python64)) {
    $Python64 = (Get-Command python -ErrorAction Stop).Source
}

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

function Get-CommandCount {
    param($Status)
    if ($null -ne $Status.total_count) { return [int]$Status.total_count }
    $Count = 0
    if ($null -ne $Status.counts) {
        foreach ($Property in $Status.counts.PSObject.Properties) {
            $Count += [int]$Property.Value
        }
    }
    return $Count
}

function Quote-CommandArgument {
    param([string]$Value)
    if ($null -eq $Value) { return '""' }
    if ($Value -notmatch '[\s"]') { return $Value }
    return '"' + ($Value -replace '\\(?=\\*")', '$0$0' -replace '"', '\"') + '"'
}

Import-DotEnv -Path (Join-Path $Root ".env")
if ([string]::IsNullOrWhiteSpace($Token)) {
    $Token = if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }
}
if ([string]::IsNullOrWhiteSpace($Token)) {
    throw "TRADING_CORE_TOKEN or GATEWAY_CORE_TOKEN is required."
}

if (-not $KrxTradingDayConfirmed) {
    throw "KrxTradingDayConfirmed is required after checking the official KRX calendar."
}
$ParsedTradeDate = [datetime]::MinValue
if (-not [datetime]::TryParseExact(
    $TradeDate,
    "yyyy-MM-dd",
    [Globalization.CultureInfo]::InvariantCulture,
    [Globalization.DateTimeStyles]::None,
    [ref]$ParsedTradeDate
)) {
    throw "TradeDate must use yyyy-MM-dd. Got: $TradeDate"
}
if ($ParsedTradeDate.Date -ne (Get-Date).Date) {
    throw "Historical or future evidence start is forbidden. TradeDate=$TradeDate"
}
if ($ParsedTradeDate.DayOfWeek -in @([DayOfWeek]::Saturday, [DayOfWeek]::Sunday)) {
    throw "KRX evidence start is forbidden on weekends. TradeDate=$TradeDate"
}

$ResolvedDbPath = [System.IO.Path]::GetFullPath($EvidenceDbPath)
$TempRoot = [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath())
if ($ResolvedDbPath.StartsWith($TempRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Persistent 10-day evidence DB cannot be stored under TEMP. DB=$ResolvedDbPath"
}
$DbDirectory = Split-Path -Parent $ResolvedDbPath
if (-not (Test-Path -LiteralPath $DbDirectory)) {
    New-Item -ItemType Directory -Path $DbDirectory -Force | Out-Null
}
$SessionStatePath = "$ResolvedDbPath.session.json"
if (Test-Path -LiteralPath $SessionStatePath) {
    throw "An unfinished evidence session already exists: $SessionStatePath"
}

$Listener = Get-NetTCPConnection -LocalPort $CorePort -State Listen -ErrorAction SilentlyContinue
if ($Listener) {
    throw "Core port is already in use. Port=$CorePort PID=$($Listener.OwningProcess)"
}

$StartScript = Join-Path $PSScriptRoot "start_market_open_observe.ps1"
$Parameters = @{
    CoreUrl = $CoreUrl
    CorePort = $CorePort
    TradeDate = $TradeDate
    DbPath = $ResolvedDbPath
    AppendOnlyEvidence = $true
    MarketScanParserVerified = $true
    MarketDataOperatingMode = "MARKET_DATA_FULL_GUARDED"
    MarketDataGlobalSkipBudget = $GlobalBudget
    ThemeRefreshTradingSession = "KRX"
    ThemeRefreshQueueMarketScanCommands = "true"
    ThemeRefreshQueueRealtimeCommands = "false"
    Token = $Token
}

$CoreParameters = $Parameters.Clone()
$CoreParameters.RunCore = $true
& $StartScript @CoreParameters

$CommandStatus = Invoke-RestMethod -Uri "$CoreUrl/api/gateway/commands/status" -TimeoutSec 10
$SessionState = [ordered]@{
    format = "append-only-daily-session/v1"
    trade_date = $TradeDate
    core_url = $CoreUrl.TrimEnd('/')
    database_path = $ResolvedDbPath
    command_count = Get-CommandCount -Status $CommandStatus
    failed_command_count = [int]$CommandStatus.counts.FAILED
    order_command_count = [int]$CommandStatus.order_command_count
    created_at = [datetime]::UtcNow.ToString("o")
    official_krx_calendar_confirmed = $true
}
$SessionState | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $SessionStatePath -Encoding UTF8

$GatewayParameters = $Parameters.Clone()
$GatewayParameters.RunGateway = $true
$GatewayReady = $false
for ($StartAttempt = 1; $StartAttempt -le $GatewayStartAttempts; $StartAttempt++) {
    & $StartScript @GatewayParameters
    for ($PollAttempt = 0; $PollAttempt -lt 30; $PollAttempt++) {
        Start-Sleep -Seconds 1
        try {
            $GatewayStatus = Invoke-RestMethod -Uri "$CoreUrl/api/gateway/status" -TimeoutSec 5
            if (
                $GatewayStatus.kiwoom_logged_in -and
                $GatewayStatus.condition_load_state -eq "LOADED"
            ) {
                $GatewayReady = $true
                break
            }
        } catch {
            continue
        }
    }
    if ($GatewayReady) { break }
    $StaleGatewayProcesses = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "python.exe" -and
        $_.CommandLine -match "apps\.kiwoom_gateway" -and
        $_.CommandLine -like "*$CoreUrl*"
    }
    $StaleGatewayProcesses | ForEach-Object {
        Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
    }
    if ($StartAttempt -lt $GatewayStartAttempts) {
        Write-Warning "Gateway did not stabilize; retrying after 5 seconds."
        Start-Sleep -Seconds 5
    }
}
if (-not $GatewayReady) {
    throw "Gateway did not reach logged-in/condition-loaded state. Session state retained."
}
if ($GatewayStabilizeSec -gt 0) {
    Start-Sleep -Seconds $GatewayStabilizeSec
}

$ThemeParameters = $Parameters.Clone()
$ThemeParameters.RunThemeRefreshLoop = $true
& $StartScript @ThemeParameters

$env:TRADING_CORE_TOKEN = $Token
$env:GATEWAY_CORE_TOKEN = $Token
$KeeperScript = Join-Path $PSScriptRoot "ops_append_only_evidence_keeper.py"
$KeeperStopPath = "$SessionStatePath.keeper.stop"
if (Test-Path -LiteralPath $KeeperStopPath) {
    Remove-Item -LiteralPath $KeeperStopPath -Force
}
$KeeperArguments = @(
    $KeeperScript,
    "--core-url", $CoreUrl,
    "--expected-db-path", $ResolvedDbPath,
    "--trade-date", $TradeDate,
    "--interval-sec", [string]$KeeperIntervalSec,
    "--reconcile-limit", [string]$KeeperReconcileLimit,
    "--outbox-limit", "100",
    "--outbox-max-batches", "5",
    "--stop-file", $KeeperStopPath
)
$LogDir = Join-Path $Root "logs\runtime"
New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
$KeeperStamp = Get-Date -Format "yyyyMMdd_HHmmss"
$KeeperOutLog = Join-Path $LogDir "append_only_evidence_keeper_$KeeperStamp.out.log"
$KeeperErrLog = Join-Path $LogDir "append_only_evidence_keeper_$KeeperStamp.err.log"
$KeeperProcess = Start-Process `
    -FilePath $Python64 `
    -ArgumentList (($KeeperArguments | ForEach-Object { Quote-CommandArgument $_ }) -join " ") `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $KeeperOutLog `
    -RedirectStandardError $KeeperErrLog `
    -PassThru
Start-Sleep -Seconds 2
if ($KeeperProcess.HasExited) {
    throw "Append-only evidence keeper exited during startup. stderr=$KeeperErrLog"
}
$SessionState["keeper_pid"] = $KeeperProcess.Id
$SessionState["keeper_interval_sec"] = $KeeperIntervalSec
$SessionState["keeper_reconcile_limit"] = $KeeperReconcileLimit
$SessionState["keeper_started_at"] = [datetime]::UtcNow.ToString("o")
$SessionState["keeper_stop_path"] = $KeeperStopPath
$SessionState | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $SessionStatePath -Encoding UTF8

Write-Host "Persistent append-only evidence session started."
Write-Host "Trade date: $TradeDate"
Write-Host "Evidence DB: $ResolvedDbPath"
Write-Host "Session state: $SessionStatePath"
Write-Host "Evidence keeper: PID=$($KeeperProcess.Id) interval=$KeeperIntervalSec sec reconcile_limit=$KeeperReconcileLimit"
Write-Host "  stdout: $KeeperOutLog"
Write-Host "  stderr: $KeeperErrLog"
Write-Host "Close command:"
Write-Host "  .\tools\close_append_only_daily_evidence.ps1 -CoreUrl $CoreUrl -CorePort $CorePort -EvidenceDbPath `"$ResolvedDbPath`" -TradeDate $TradeDate"

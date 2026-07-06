param(
    [switch]$DryRun,
    [switch]$SkipNaverImport,
    [string]$GatewayPython = ""
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$EnvFile = Join-Path $Root ".env"
$CorePort = 8000
$CoreUrl = "http://127.0.0.1:$CorePort"
$Python64 = Join-Path $Root "venv_64\Scripts\python.exe"
$DefaultGatewayPython = Join-Path $Root "venv_32\Scripts\python.exe"
$LaunchLogDir = Join-Path $Root "logs\live_sim_pilot"
$RuntimeLogDir = Join-Path $LaunchLogDir "runtime"
$PidFile = Join-Path $LaunchLogDir "pids.json"
$ReportRoot = Join-Path $Root "reports\live_sim_pilot_launch"
$Stamp = Get-Date -Format "yyyy-MM-dd_HHmmss"
$ReportDir = Join-Path $ReportRoot $Stamp
$ReportFile = Join-Path $ReportDir "launch_report.md"

$script:StartedProcesses = New-Object System.Collections.Generic.List[object]
$script:LaunchResults = New-Object System.Collections.Generic.List[object]
$script:PidState = [ordered]@{
    launcher = "start_live_sim_pilot.ps1"
    started_at = (Get-Date).ToString("o")
    root = $Root
    core_url = $CoreUrl
    dry_run = [bool]$DryRun
    report_file = $ReportFile
    processes = [ordered]@{}
}

function Write-Info {
    param([string]$Message)
    Write-Host $Message -ForegroundColor Cyan
}

function Write-Ok {
    param([string]$Message)
    Write-Host "✅ $Message" -ForegroundColor Green
}

function Write-WarnKo {
    param([string]$Message)
    Write-Host "⚠ $Message" -ForegroundColor Yellow
}

function Write-FailKo {
    param([string]$Message)
    Write-Host "❌ $Message" -ForegroundColor Red
}

function Add-Result {
    param(
        [string]$Step,
        [string]$Name,
        [bool]$Passed,
        [string]$Message,
        [object]$Details = $null
    )

    $script:LaunchResults.Add([ordered]@{
        step = $Step
        name = $Name
        passed = $Passed
        message = $Message
        details = $Details
        checked_at = (Get-Date).ToString("o")
    }) | Out-Null

    if ($Passed) {
        Write-Ok "$Step - ${Name}: $Message"
    } else {
        Write-FailKo "$Step - ${Name}: $Message"
    }
}

function Import-LiveSimDotEnv {
    param([string]$Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        throw ".env 파일을 찾을 수 없습니다: $Path"
    }

    $Values = New-Object "System.Collections.Generic.Dictionary[string,string]" ([System.StringComparer]::OrdinalIgnoreCase)
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

        $Values[$Name] = $Value
        [Environment]::SetEnvironmentVariable($Name, $Value, "Process")
    }
    return $Values
}

function Get-EnvSetting {
    param(
        [string]$Name,
        [string]$Default = ""
    )
    $Value = [Environment]::GetEnvironmentVariable($Name, "Process")
    if ([string]::IsNullOrWhiteSpace($Value)) {
        return $Default
    }
    return $Value
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
    throw "$Name 값은 true/false여야 합니다. 현재값: $Value"
}

function Assert-EnvEquals {
    param(
        [string]$Name,
        [string]$Expected
    )
    $Actual = Get-EnvSetting -Name $Name
    if ($Actual.Trim().ToUpperInvariant() -ne $Expected.Trim().ToUpperInvariant()) {
        throw "$Name=$Expected 이어야 합니다. 현재값: $Actual"
    }
}

function Assert-EnvBool {
    param(
        [string]$Name,
        [bool]$Expected
    )
    $Actual = Resolve-BoolSetting -Name $Name -Value (Get-EnvSetting -Name $Name) -Default (-not $Expected)
    if ($Actual -ne $Expected) {
        throw "$Name=$($Expected.ToString().ToLowerInvariant()) 이어야 합니다. 현재값: $(Get-EnvSetting -Name $Name)"
    }
}

function Test-TcpPortOpen {
    param([int]$Port)

    $Client = [System.Net.Sockets.TcpClient]::new()
    try {
        $Async = $Client.BeginConnect("127.0.0.1", $Port, $null, $null)
        if (-not $Async.AsyncWaitHandle.WaitOne(300, $false)) {
            return $false
        }
        $Client.EndConnect($Async)
        return $true
    } catch {
        return $false
    } finally {
        $Client.Close()
    }
}

function Get-RuntimeProcesses {
    $SelfPid = $PID
    return @(
        Get-CimInstance -ClassName Win32_Process |
            Where-Object {
                $null -ne $_.CommandLine -and
                [int]$_.ProcessId -ne $SelfPid -and
                ($_.CommandLine -match "apps\.core_api:app" -or $_.CommandLine -match "apps\.kiwoom_gateway")
            } |
            Select-Object ProcessId, ParentProcessId, Name, CommandLine
    )
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

function Mask-Secret {
    param([string]$Value)

    if ([string]::IsNullOrWhiteSpace($Value)) {
        return "<empty>"
    }
    if ($Value.Length -le 8) {
        return "****"
    }
    return "$($Value.Substring(0, 4))...$($Value.Substring($Value.Length - 4))"
}

function Get-DisplayArguments {
    param([string[]]$Arguments)

    $Display = New-Object System.Collections.Generic.List[string]
    for ($Index = 0; $Index -lt $Arguments.Count; $Index++) {
        $Value = [string]$Arguments[$Index]
        $Previous = if ($Index -gt 0) { [string]$Arguments[$Index - 1] } else { "" }
        if ($Previous -eq "--token") {
            $Display.Add((Mask-Secret -Value $Value)) | Out-Null
        } elseif ($Previous -eq "--condition-profiles") {
            $Display.Add("<condition-profiles-json>") | Out-Null
        } elseif ($Value.Length -gt 160) {
            $Display.Add("<long-value>") | Out-Null
        } else {
            $Display.Add($Value) | Out-Null
        }
    }
    return [string[]]$Display
}

function Save-PidState {
    New-Item -ItemType Directory -Force -Path $LaunchLogDir | Out-Null
    $script:PidState | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $PidFile -Encoding UTF8
}

function Start-DetachedProcess {
    param(
        [string]$Label,
        [string]$FilePath,
        [string[]]$Arguments
    )

    New-Item -ItemType Directory -Force -Path $RuntimeLogDir | Out-Null
    $SafeLabel = $Label.ToLowerInvariant() -replace "[^a-z0-9]+", "_"
    $OutLog = Join-Path $RuntimeLogDir "$($SafeLabel)_$Stamp.out.log"
    $ErrLog = Join-Path $RuntimeLogDir "$($SafeLabel)_$Stamp.err.log"
    $ArgumentLine = ($Arguments | ForEach-Object { Quote-CommandArgument $_ }) -join " "

    $Process = Start-Process `
        -FilePath $FilePath `
        -ArgumentList $ArgumentLine `
        -WorkingDirectory $Root `
        -PassThru `
        -WindowStyle Hidden `
        -RedirectStandardOutput $OutLog `
        -RedirectStandardError $ErrLog

    $Record = [ordered]@{
        label = $Label
        pid = $Process.Id
        file_path = $FilePath
        arguments = Get-DisplayArguments -Arguments $Arguments
        stdout_log = $OutLog
        stderr_log = $ErrLog
        started_at = (Get-Date).ToString("o")
    }
    $script:StartedProcesses.Add($Record) | Out-Null
    $script:PidState.processes[$Label] = $Record
    Save-PidState

    Write-Ok "$Label 프로세스 기동 PID=$($Process.Id)"
    Write-Host "  stdout: $OutLog"
    Write-Host "  stderr: $ErrLog"
    return $Process
}

function Stop-StartedProcesses {
    if ($script:StartedProcesses.Count -eq 0) {
        return
    }
    Write-WarnKo "실패 정리를 시작합니다. 이미 띄운 프로세스를 역순으로 종료합니다."
    $Items = @($script:StartedProcesses)
    [array]::Reverse($Items)
    foreach ($Item in $Items) {
        try {
            $Process = Get-Process -Id ([int]$Item.pid) -ErrorAction SilentlyContinue
            if ($null -ne $Process) {
                Stop-Process -Id ([int]$Item.pid) -Force -ErrorAction Stop
                Write-WarnKo "$($Item.label) PID=$($Item.pid) 종료"
            }
        } catch {
            Write-WarnKo "$($Item.label) PID=$($Item.pid) 종료 실패: $($_.Exception.Message)"
        }
    }
}

function Invoke-CoreGet {
    param(
        [string]$Path,
        [int]$TimeoutSec = 5
    )
    return Invoke-RestMethod -Uri "$CoreUrl$Path" -TimeoutSec $TimeoutSec
}

function Wait-CoreHealth {
    $Deadline = (Get-Date).AddSeconds(30)
    $LastError = ""
    while ((Get-Date) -lt $Deadline) {
        try {
            Invoke-CoreGet -Path "/health" -TimeoutSec 3 | Out-Null
            return $true
        } catch {
            $LastError = $_.Exception.Message
            Start-Sleep -Seconds 2
        }
    }
    throw "Core /health가 30초 안에 200을 반환하지 않았습니다. 마지막 오류: $LastError"
}

function Get-TimestampAgeSec {
    param([object]$Value)

    if ($null -eq $Value -or [string]::IsNullOrWhiteSpace([string]$Value)) {
        return [double]::PositiveInfinity
    }
    try {
        $Parsed = [DateTimeOffset]::Parse([string]$Value)
        return [Math]::Max(0, ([DateTimeOffset]::UtcNow - $Parsed.ToUniversalTime()).TotalSeconds)
    } catch {
        return [double]::PositiveInfinity
    }
}

function Test-RecentTimestamp {
    param(
        [object]$Value,
        [int]$MaxAgeSec
    )
    return (Get-TimestampAgeSec -Value $Value) -le $MaxAgeSec
}

function Test-SimulationValue {
    param([object]$Value)
    return ([string]$Value).Trim().ToUpperInvariant() -eq "SIMULATION"
}

function Invoke-ExternalCommand {
    param(
        [string]$Name,
        [string]$FilePath,
        [string[]]$Arguments,
        [switch]$AllowFailure
    )

    Write-Info "$Name 실행: $FilePath $($Arguments -join ' ')"
    $Output = & $FilePath @Arguments 2>&1
    $ExitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }
    $Text = ($Output | Out-String).Trim()
    if ($Text) {
        Write-Host $Text
    }
    if ($ExitCode -ne 0) {
        $Message = "$Name 실패(exit=$ExitCode)"
        if ($AllowFailure) {
            Write-WarnKo "$Message. Naver import 최신성 preflight가 WARN이 되어 큐잉을 막을 수 있습니다."
            return [ordered]@{ status = "WARN"; exit_code = $ExitCode; output = $Text }
        }
        throw $Message
    }
    return [ordered]@{ status = "PASS"; exit_code = $ExitCode; output = $Text }
}

function Resolve-ConditionProfile {
    $ProfilesJson = Get-EnvSetting -Name "KIWOOM_CONDITION_PROFILES"
    if (-not [string]::IsNullOrWhiteSpace($ProfilesJson)) {
        return [ordered]@{ mode = "MULTI_PROFILE"; source = "env:KIWOOM_CONDITION_PROFILES"; json = $ProfilesJson }
    }

    $ProfilesFile = Get-EnvSetting -Name "KIWOOM_CONDITION_PROFILES_FILE"
    if ([string]::IsNullOrWhiteSpace($ProfilesFile)) {
        $DefaultFile = Join-Path $Root "configs\condition_profiles\market_open_profiles.json"
        if (Test-Path -LiteralPath $DefaultFile) {
            $ProfilesFile = $DefaultFile
        }
    } elseif (-not [System.IO.Path]::IsPathRooted($ProfilesFile)) {
        $ProfilesFile = Join-Path $Root $ProfilesFile
    }

    if (-not [string]::IsNullOrWhiteSpace($ProfilesFile) -and (Test-Path -LiteralPath $ProfilesFile)) {
        $Json = Get-Content -LiteralPath $ProfilesFile -Raw -Encoding UTF8
        [Environment]::SetEnvironmentVariable("KIWOOM_CONDITION_PROFILES", $Json, "Process")
        [Environment]::SetEnvironmentVariable("KIWOOM_CONDITION_PROFILES_FILE", (Resolve-Path -LiteralPath $ProfilesFile).Path, "Process")
        return [ordered]@{ mode = "MULTI_PROFILE"; source = "file:$ProfilesFile"; json = $Json }
    }

    $ConditionName = Get-EnvSetting -Name "KIWOOM_CONDITION_NAME"
    if (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
        return [ordered]@{ mode = "LEGACY_SINGLE"; source = "env:KIWOOM_CONDITION_NAME"; name = $ConditionName }
    }

    return [ordered]@{ mode = "NONE"; source = ""; json = ""; name = "" }
}

function Build-GatewayArguments {
    param([object]$ConditionProfile)

    $PollWaitSec = Get-EnvSetting -Name "GATEWAY_COMMAND_WAIT_SEC" -Default "1.0"
    $CommandLimit = Get-EnvSetting -Name "GATEWAY_COMMAND_LIMIT" -Default "20"
    $RealtimeExchange = Get-EnvSetting -Name "KIWOOM_REALTIME_EXCHANGE" -Default "krx"
    $RealtimeCodes = Get-EnvSetting -Name "KIWOOM_REALTIME_CODES"
    $MarketIndexCodes = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_CODES" -Default "KOSPI,KOSDAQ"
    $MarketIndexScreenNo = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_SCREEN_NO" -Default "5700"
    $MarketIndexPollSec = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_POLL_SEC" -Default "60.0"

    $MarketIndexEnabled = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_ENABLED" -Value (Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_ENABLED") -Default $true
    $MarketIndexRealtimeEnabled = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED" -Value (Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED") -Default $true
    $MarketIndexTrBootstrapEnabled = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED" -Value (Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED") -Default $false

    $Args = @(
        "-m", "apps.kiwoom_gateway",
        "--core-url", $CoreUrl,
        "--token", (Get-EnvSetting -Name "GATEWAY_CORE_TOKEN"),
        "--auto-login",
        "--no-threaded-login",
        "--no-observe-only",
        "--poll-wait-sec", $PollWaitSec,
        "--command-limit", $CommandLimit,
        "--heartbeat-interval-sec", "2",
        "--realtime-exchange", $RealtimeExchange,
        "--realtime-recover-interval-sec", "300"
    )

    if ($ConditionProfile.mode -eq "MULTI_PROFILE") {
        $Args += @("--condition-profiles", [string]$ConditionProfile.json)
    } elseif ($ConditionProfile.mode -eq "LEGACY_SINGLE") {
        $Args += @("--condition-name", [string]$ConditionProfile.name, "--condition-realtime")
    }
    if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
        $Args += @("--realtime-codes", $RealtimeCodes)
    }
    if ($MarketIndexEnabled) {
        $Args += "--market-index-enabled"
    } else {
        $Args += "--no-market-index-enabled"
    }
    if ($MarketIndexRealtimeEnabled) {
        $Args += "--market-index-realtime-enabled"
    } else {
        $Args += "--no-market-index-realtime-enabled"
    }
    if ($MarketIndexTrBootstrapEnabled) {
        $Args += "--market-index-tr-bootstrap-enabled"
    } else {
        $Args += "--no-market-index-tr-bootstrap-enabled"
    }
    $Args += @(
        "--market-index-codes", $MarketIndexCodes,
        "--market-index-screen-no", $MarketIndexScreenNo,
        "--market-index-poll-sec", $MarketIndexPollSec
    )
    return $Args
}

function Wait-GatewayReady {
    param([datetime]$Deadline)

    $Latest = $null
    while ((Get-Date) -lt $Deadline) {
        try {
            $Latest = Invoke-CoreGet -Path "/api/gateway/status" -TimeoutSec 5
            if (($Latest.kiwoom_logged_in -eq $true) -and (Test-SimulationValue -Value $Latest.server_mode)) {
                return @{ passed = $true; data = $Latest }
            }
        } catch {
            $Latest = @{ error = $_.Exception.Message }
        }
        Start-Sleep -Seconds 3
    }
    return @{ passed = $false; data = $Latest }
}

function Wait-LatestPriceTick {
    param([datetime]$Deadline)

    $Latest = $null
    while ((Get-Date) -lt $Deadline) {
        try {
            $Data = Invoke-CoreGet -Path "/api/market-data/ticks/latest?limit=1" -TimeoutSec 5
            $Tick = @($Data.ticks | Select-Object -First 1)[0]
            $Latest = $Tick
            if ($null -ne $Tick -and (Test-RecentTimestamp -Value $Tick.event_ts -MaxAgeSec 60)) {
                return @{ passed = $true; data = $Tick }
            }
        } catch {
            $Latest = @{ error = $_.Exception.Message }
        }
        Start-Sleep -Seconds 3
    }
    return @{ passed = $false; data = $Latest }
}

function Wait-LatestMarketIndexTick {
    param([datetime]$Deadline)

    $Latest = $null
    while ((Get-Date) -lt $Deadline) {
        try {
            $Data = Invoke-CoreGet -Path "/api/market-indexes/latest?limit=1" -TimeoutSec 5
            $Tick = @($Data.ticks | Select-Object -First 1)[0]
            $Latest = $Tick
            if ($null -ne $Tick -and (Test-RecentTimestamp -Value $Tick.event_ts -MaxAgeSec 120)) {
                return @{ passed = $true; data = $Tick }
            }
        } catch {
            $Latest = @{ error = $_.Exception.Message }
        }
        Start-Sleep -Seconds 3
    }
    return @{ passed = $false; data = $Latest }
}

function Wait-ConditionLoaded {
    param([datetime]$Deadline)

    $Latest = $null
    while ((Get-Date) -lt $Deadline) {
        try {
            $Latest = Invoke-CoreGet -Path "/api/gateway/status" -TimeoutSec 5
            if ([string]$Latest.condition_load_state -eq "LOADED") {
                return @{ passed = $true; data = $Latest }
            }
        } catch {
            $Latest = @{ error = $_.Exception.Message }
        }
        Start-Sleep -Seconds 3
    }
    return @{ passed = $false; data = $Latest }
}

function Wait-NewOperatingRun {
    param(
        [string]$BaselineRunId,
        [datetime]$Deadline
    )

    $Latest = $null
    while ((Get-Date) -lt $Deadline) {
        try {
            $Data = Invoke-CoreGet -Path "/api/live-sim/operator/runs/latest" -TimeoutSec 5
            $Latest = $Data.run
            if ($null -ne $Latest -and [string]$Latest.run_id -ne $BaselineRunId) {
                return @{ passed = $true; data = $Latest }
            }
        } catch {
            $Latest = @{ error = $_.Exception.Message }
        }
        Start-Sleep -Seconds 3
    }
    return @{ passed = $false; data = $Latest }
}

function Write-LaunchReport {
    param(
        [string]$Status,
        [object]$EnvSummary,
        [object]$Preflight = $null,
        [object]$LiveSimStatus = $null
    )

    if ($DryRun) {
        return
    }
    New-Item -ItemType Directory -Force -Path $ReportDir | Out-Null
    $Lines = New-Object System.Collections.Generic.List[string]
    $Lines.Add("# LIVE_SIM 파일럿 기동 리포트") | Out-Null
    $Lines.Add("") | Out-Null
    $Lines.Add("- 생성시각: $(Get-Date -Format o)") | Out-Null
    $Lines.Add("- 상태: $Status") | Out-Null
    $Lines.Add("- 대시보드: $CoreUrl/dashboard") | Out-Null
    $Lines.Add("- PID 파일: $PidFile") | Out-Null
    $Lines.Add("") | Out-Null
    $Lines.Add("## 단계별 결과") | Out-Null
    $Lines.Add("") | Out-Null
    $Lines.Add("| 단계 | 항목 | 결과 | 메시지 |") | Out-Null
    $Lines.Add("| --- | --- | --- | --- |") | Out-Null
    foreach ($Result in $script:LaunchResults) {
        $Mark = if ($Result.passed) { "✅" } else { "❌" }
        $Message = ([string]$Result.message).Replace("|", "\|")
        $Lines.Add("| $($Result.step) | $($Result.name) | $Mark | $Message |") | Out-Null
    }
    $Lines.Add("") | Out-Null
    $Lines.Add("## 주요 .env 값") | Out-Null
    $Lines.Add("") | Out-Null
    foreach ($Name in $EnvSummary.Keys) {
        $Lines.Add("- ${Name}: $($EnvSummary[$Name])") | Out-Null
    }
    $Lines.Add("") | Out-Null
    $Lines.Add("## 프로세스") | Out-Null
    $Lines.Add("") | Out-Null
    foreach ($Key in $script:PidState.processes.Keys) {
        $Proc = $script:PidState.processes[$Key]
        $Lines.Add("- $Key PID=$($Proc.pid)") | Out-Null
        $Lines.Add("  - stdout: $($Proc.stdout_log)") | Out-Null
        $Lines.Add("  - stderr: $($Proc.stderr_log)") | Out-Null
    }
    if ($null -ne $LiveSimStatus) {
        $Lines.Add("") | Out-Null
        $Lines.Add("## LIVE_SIM Safety Gate") | Out-Null
        $Lines.Add("") | Out-Null
        $Safety = $LiveSimStatus.safety_gate
        $Lines.Add("- status: $($Safety.status)") | Out-Null
        $Lines.Add("- gateway_orderable: $($Safety.gateway_orderable)") | Out-Null
        $Lines.Add("- blocking_reasons: $($Safety.blocking_reasons -join ', ')") | Out-Null
        $Lines.Add("- warnings: $($Safety.warnings -join ', ')") | Out-Null
    }
    if ($null -ne $Preflight) {
        $Lines.Add("") | Out-Null
        $Lines.Add("## Preflight") | Out-Null
        $Lines.Add("") | Out-Null
        $Lines.Add("- status: $($Preflight.status)") | Out-Null
        $Lines.Add("- warnings: $($Preflight.warnings -join ', ')") | Out-Null
        $Lines.Add("- blocking_reasons: $($Preflight.blocking_reasons -join ', ')") | Out-Null
        $Lines.Add("") | Out-Null
        $Lines.Add("### Checks") | Out-Null
        foreach ($Check in $Preflight.checks) {
            $Lines.Add("- [$($Check.status)] $($Check.name): $($Check.message)") | Out-Null
        }
    }

    $Lines | Set-Content -LiteralPath $ReportFile -Encoding UTF8
}

function Build-EnvSummary {
    return [ordered]@{
        TRADING_PROFILE = Get-EnvSetting -Name "TRADING_PROFILE"
        TRADING_MODE = Get-EnvSetting -Name "TRADING_MODE"
        TRADING_ALLOW_LIVE_SIM = Get-EnvSetting -Name "TRADING_ALLOW_LIVE_SIM"
        TRADING_ALLOW_LIVE_REAL = Get-EnvSetting -Name "TRADING_ALLOW_LIVE_REAL"
        LIVE_SIM_ENABLED = Get-EnvSetting -Name "LIVE_SIM_ENABLED"
        LIVE_SIM_KILL_SWITCH = Get-EnvSetting -Name "LIVE_SIM_KILL_SWITCH"
        LIVE_SIM_ACCOUNT_MODE = Get-EnvSetting -Name "LIVE_SIM_ACCOUNT_MODE"
        LIVE_SIM_BROKER_ENV = Get-EnvSetting -Name "LIVE_SIM_BROKER_ENV"
        LIVE_SIM_SERVER_MODE = Get-EnvSetting -Name "LIVE_SIM_SERVER_MODE"
        LIVE_SIM_OPERATING_LOOP_ENABLED = Get-EnvSetting -Name "LIVE_SIM_OPERATING_LOOP_ENABLED"
        LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS = Get-EnvSetting -Name "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS"
        KIWOOM_MARKET_INDEX_ENABLED = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_ENABLED"
        KIWOOM_MARKET_INDEX_REALTIME_ENABLED = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED"
        KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED = Get-EnvSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED"
        TRADING_CORE_TOKEN = Mask-Secret -Value (Get-EnvSetting -Name "TRADING_CORE_TOKEN")
        GATEWAY_CORE_TOKEN = Mask-Secret -Value (Get-EnvSetting -Name "GATEWAY_CORE_TOKEN")
    }
}

try {
    Write-Info "[0단계] 안전 게이트"
    Import-LiveSimDotEnv -Path $EnvFile | Out-Null
    if (-not (Test-Path -LiteralPath $Python64)) {
        throw "64-bit Core Python을 찾을 수 없습니다: $Python64"
    }
    if ([string]::IsNullOrWhiteSpace($GatewayPython)) {
        $GatewayPython = $DefaultGatewayPython
    }
    if (-not (Test-Path -LiteralPath $GatewayPython)) {
        throw "32-bit Gateway Python을 찾을 수 없습니다: $GatewayPython"
    }

    Assert-EnvBool -Name "TRADING_ALLOW_LIVE_REAL" -Expected $false
    if ((Get-EnvSetting -Name "TRADING_MODE").Trim().ToUpperInvariant() -eq "LIVE_REAL") {
        throw "TRADING_MODE=LIVE_REAL 은 허용되지 않습니다."
    }
    Assert-EnvEquals -Name "TRADING_MODE" -Expected "LIVE_SIM"
    Assert-EnvBool -Name "TRADING_ALLOW_LIVE_SIM" -Expected $true
    Assert-EnvBool -Name "LIVE_SIM_ENABLED" -Expected $true
    Assert-EnvEquals -Name "LIVE_SIM_ACCOUNT_MODE" -Expected "SIMULATION"
    Assert-EnvEquals -Name "LIVE_SIM_BROKER_ENV" -Expected "SIMULATION"
    Assert-EnvEquals -Name "LIVE_SIM_SERVER_MODE" -Expected "SIMULATION"

    $KillSwitch = Resolve-BoolSetting -Name "LIVE_SIM_KILL_SWITCH" -Value (Get-EnvSetting -Name "LIVE_SIM_KILL_SWITCH") -Default $true
    if ($KillSwitch) {
        Write-WarnKo "LIVE_SIM_KILL_SWITCH=true 입니다. 관찰 기동은 허용하지만 주문 큐잉은 safety gate에서 막힐 수 있습니다."
    } else {
        Write-Ok "LIVE_SIM_KILL_SWITCH=false 확인"
    }

    $TradingToken = Get-EnvSetting -Name "TRADING_CORE_TOKEN"
    $GatewayToken = Get-EnvSetting -Name "GATEWAY_CORE_TOKEN"
    if ([string]::IsNullOrWhiteSpace($TradingToken) -or [string]::IsNullOrWhiteSpace($GatewayToken)) {
        throw "TRADING_CORE_TOKEN과 GATEWAY_CORE_TOKEN은 모두 필요합니다."
    }
    if ($TradingToken -ne $GatewayToken) {
        throw "TRADING_CORE_TOKEN과 GATEWAY_CORE_TOKEN이 일치하지 않습니다."
    }

    if (Test-TcpPortOpen -Port $CorePort) {
        throw "포트 $CorePort 이 이미 열려 있습니다. 중복 기동 금지: .\tools\stop_live_sim_pilot.ps1 또는 기존 Core를 종료하세요."
    }
    $Existing = Get-RuntimeProcesses
    if ($Existing.Count -gt 0) {
        $Summary = ($Existing | ForEach-Object { "PID=$($_.ProcessId) $($_.Name)" }) -join ", "
        throw "이미 실행 중인 Core/Gateway 프로세스가 있습니다: $Summary. 먼저 .\tools\stop_live_sim_pilot.ps1 를 실행하세요."
    }

    Add-Result -Step "0" -Name "안전 게이트" -Passed $true -Message "LIVE_REAL 금지, LIVE_SIM/SIMULATION 설정, 토큰 일치, 중복 기동 없음"
    $EnvSummary = Build-EnvSummary

    $ConditionProfile = Resolve-ConditionProfile
    $GatewayArgs = Build-GatewayArguments -ConditionProfile $ConditionProfile

    if ($DryRun) {
        Write-Info "[DryRun] 실행 계획"
        Write-Host "Core: $Python64 -m uvicorn apps.core_api:app --host 127.0.0.1 --port $CorePort"
        $DisplayGatewayArgs = Get-DisplayArguments -Arguments $GatewayArgs
        Write-Host "Gateway: $GatewayPython $($DisplayGatewayArgs -join ' ')"
        Write-Host "Naver import: $(if ($SkipNaverImport) { '건너뜀' } else { "$Python64 -m tools.import_naver_themes" })"
        Write-Host "Theme snapshot rebuild: $Python64 -m tools.rebuild_theme_snapshots"
        Write-Host "기동 검증: gateway/login, price tick, market index tick, condition load, live-sim status, preflight, operating run"
        exit 0
    }

    New-Item -ItemType Directory -Force -Path $LaunchLogDir, $RuntimeLogDir, $ReportDir | Out-Null
    Save-PidState

    Write-Info "[1단계] Core 기동"
    $CoreArgs = @("-m", "uvicorn", "apps.core_api:app", "--host", "127.0.0.1", "--port", [string]$CorePort)
    Start-DetachedProcess -Label "core" -FilePath $Python64 -Arguments $CoreArgs | Out-Null
    Wait-CoreHealth | Out-Null
    Add-Result -Step "1" -Name "Core /health" -Passed $true -Message "Core가 30초 안에 정상 응답"

    Write-Info "[2단계] 장전 준비"
    if ($SkipNaverImport) {
        Write-WarnKo "Naver theme import를 건너뜁니다. naver_import_recent preflight가 WARN이 될 수 있습니다."
        Add-Result -Step "2" -Name "Naver theme import" -Passed $true -Message "사용자 옵션으로 건너뜀"
    } else {
        $NaverResult = Invoke-ExternalCommand -Name "Naver theme import" -FilePath $Python64 -Arguments @("-m", "tools.import_naver_themes") -AllowFailure
        $NaverMessage = if ($NaverResult.status -eq "PASS") {
            "완료"
        } else {
            "실패 허용 WARN: naver_import_recent preflight가 WARN이 될 수 있음"
        }
        Add-Result -Step "2" -Name "Naver theme import" -Passed $true -Message $NaverMessage -Details $NaverResult
    }
    $ThemeResult = Invoke-ExternalCommand -Name "Theme snapshot rebuild" -FilePath $Python64 -Arguments @("-m", "tools.rebuild_theme_snapshots")
    Add-Result -Step "2" -Name "Theme snapshot rebuild" -Passed $true -Message "1회 rebuild 완료" -Details $ThemeResult

    Write-Info "[3단계] Kiwoom Gateway 기동"
    Write-Host "Condition profile: $($ConditionProfile.mode) $($ConditionProfile.source)"
    Start-DetachedProcess -Label "gateway" -FilePath $GatewayPython -Arguments $GatewayArgs | Out-Null
    Add-Result -Step "3" -Name "Gateway process" -Passed $true -Message "Gateway 백그라운드 기동 요청 완료"

    Write-Info "[4단계] 기동 검증"
    $ValidationDeadline = (Get-Date).AddSeconds(180)
    $BaselineRunId = ""
    try {
        $Baseline = Invoke-CoreGet -Path "/api/live-sim/operator/runs/latest" -TimeoutSec 5
        if ($null -ne $Baseline.run) {
            $BaselineRunId = [string]$Baseline.run.run_id
        }
    } catch {
        $BaselineRunId = ""
    }

    $GatewayReady = Wait-GatewayReady -Deadline $ValidationDeadline
    Add-Result -Step "4" -Name "gateway_status" -Passed ([bool]$GatewayReady.passed) -Message "kiwoom_logged_in=$($GatewayReady.data.kiwoom_logged_in), server_mode=$($GatewayReady.data.server_mode)" -Details $GatewayReady.data

    $PriceTick = Wait-LatestPriceTick -Deadline $ValidationDeadline
    $PriceAge = Get-TimestampAgeSec -Value $PriceTick.data.event_ts
    Add-Result -Step "4" -Name "실시간 tick 유입" -Passed ([bool]$PriceTick.passed) -Message "latest_price_tick_at=$($PriceTick.data.event_ts), age_sec=$([Math]::Round($PriceAge, 1))" -Details $PriceTick.data

    $MarketIndexTick = Wait-LatestMarketIndexTick -Deadline $ValidationDeadline
    $MarketIndexAge = Get-TimestampAgeSec -Value $MarketIndexTick.data.event_ts
    $MarketIndexMessage = "latest_market_index_tick_at=$($MarketIndexTick.data.event_ts), age_sec=$([Math]::Round($MarketIndexAge, 1))"
    if (-not $MarketIndexTick.passed) {
        $MarketIndexMessage += ". 지수 실시간 미등록 가능성이 있습니다. KIWOOM_MARKET_INDEX_REALTIME_ENABLED=true 및 KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED=true로 재기동하거나 지수 재등록을 확인하세요."
    }
    Add-Result -Step "4" -Name "지수 실시간" -Passed ([bool]$MarketIndexTick.passed) -Message $MarketIndexMessage -Details $MarketIndexTick.data

    $ConditionLoaded = Wait-ConditionLoaded -Deadline $ValidationDeadline
    Add-Result -Step "4" -Name "condition_load_state" -Passed ([bool]$ConditionLoaded.passed) -Message "condition_load_state=$($ConditionLoaded.data.condition_load_state)" -Details $ConditionLoaded.data

    $LiveSimStatus = Invoke-CoreGet -Path "/api/live-sim/status" -TimeoutSec 10
    $Safety = $LiveSimStatus.safety_gate
    Add-Result -Step "4" -Name "LIVE_SIM safety gate" -Passed $true -Message "status=$($Safety.status), gateway_orderable=$($Safety.gateway_orderable), open_order_count=$($LiveSimStatus.open_order_count)" -Details $LiveSimStatus

    $Mode = Get-EnvSetting -Name "LIVE_SIM_OPERATING_DEFAULT_MODE" -Default "PILOT_FULL_LIFECYCLE"
    $QueueCommands = Resolve-BoolSetting -Name "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS" -Value (Get-EnvSetting -Name "LIVE_SIM_OPERATING_LOOP_QUEUE_COMMANDS") -Default $false
    $PreflightPath = "/api/live-sim/operator/preflight?mode=$Mode&queue_commands=$($QueueCommands.ToString().ToLowerInvariant())"
    $Preflight = Invoke-CoreGet -Path $PreflightPath -TimeoutSec 20
    $PreflightPassed = [string]$Preflight.status -ne "BLOCK"
    Add-Result -Step "4" -Name "run_live_sim_preflight" -Passed $PreflightPassed -Message "status=$($Preflight.status), warnings=$(@($Preflight.warnings).Count), blocking_reasons=$(@($Preflight.blocking_reasons).Count)" -Details $Preflight
    if (-not $PreflightPassed) {
        Write-FailKo "기동은 완료됐지만 주문은 차단 상태입니다."
        foreach ($Reason in @($Preflight.blocking_reasons)) {
            Write-FailKo "BLOCK: $Reason"
        }
    }

    $LoopEnabled = Resolve-BoolSetting -Name "LIVE_SIM_OPERATING_LOOP_ENABLED" -Value (Get-EnvSetting -Name "LIVE_SIM_OPERATING_LOOP_ENABLED") -Default $false
    if ($LoopEnabled) {
        $OperatingDeadline = (Get-Date).AddSeconds(90)
        if ($OperatingDeadline -gt $ValidationDeadline) {
            $OperatingDeadline = $ValidationDeadline
        }
        $NewRun = Wait-NewOperatingRun -BaselineRunId $BaselineRunId -Deadline $OperatingDeadline
        Add-Result -Step "4" -Name "operating loop 새 run" -Passed ([bool]$NewRun.passed) -Message "latest_run_id=$($NewRun.data.run_id), status=$($NewRun.data.status), preflight=$($NewRun.data.preflight_status)" -Details $NewRun.data
    } else {
        Add-Result -Step "4" -Name "operating loop 새 run" -Passed $true -Message "LIVE_SIM_OPERATING_LOOP_ENABLED=false 이므로 확인 생략"
    }

    Write-Info "[5단계] 기동 리포트"
    $AnyFailed = @($script:LaunchResults | Where-Object { -not $_.passed }).Count -gt 0
    $FinalStatus = if ($Preflight.status -eq "BLOCK") { "STARTED_BLOCKED" } elseif ($AnyFailed) { "STARTED_DEGRADED" } else { "STARTED_OK" }
    Write-LaunchReport -Status $FinalStatus -EnvSummary $EnvSummary -Preflight $Preflight -LiveSimStatus $LiveSimStatus
    Add-Result -Step "5" -Name "launch_report" -Passed $true -Message $ReportFile
    Write-LaunchReport -Status $FinalStatus -EnvSummary $EnvSummary -Preflight $Preflight -LiveSimStatus $LiveSimStatus

    Write-Host ""
    Write-Ok "LIVE_SIM 파일럿 런처 완료: $FinalStatus"
    Write-Host "대시보드: $CoreUrl/dashboard"
    Write-Host "기동 리포트: $ReportFile"
    Write-Host "정리: .\tools\stop_live_sim_pilot.ps1"
} catch {
    Write-FailKo $_.Exception.Message
    try {
        $EnvSummaryForFailure = Build-EnvSummary
        Write-LaunchReport -Status "FAILED" -EnvSummary $EnvSummaryForFailure
    } catch {
        Write-WarnKo "실패 리포트 작성도 실패했습니다: $($_.Exception.Message)"
    }
    Stop-StartedProcesses
    exit 1
}

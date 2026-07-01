[CmdletBinding()]
param(
    [string]$CoreUrl = "",
    [string]$Token = "",
    [string]$ConditionName = "",
    [string]$ConditionProfilesFile = "",
    [string]$ConditionProfilesJson = "",
    [string]$RealtimeCodes = "",
    [string]$RealtimeExchange = "",
    [string]$MarketIndexEnabled = "",
    [string]$MarketIndexRealtimeEnabled = "",
    [string]$MarketIndexTrBootstrapEnabled = "",
    [string]$MarketIndexCodes = "",
    [string]$MarketIndexScreenNo = "",
    [string]$MarketIndexPollSec = "",
    [switch]$NoAutoLogin,
    [switch]$NoConditionRealtime,
    [switch]$Detached,
    [switch]$Log,
    [switch]$DryRun,
    [int]$WaitSeconds = 30
)

$ErrorActionPreference = "Stop"

$Utf8NoBom = New-Object System.Text.UTF8Encoding -ArgumentList $false
[Console]::OutputEncoding = $Utf8NoBom
$OutputEncoding = $Utf8NoBom

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$Python32 = Join-Path $Root "venv_32\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python32)) {
    throw "32-bit Python was not found: $Python32"
}

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

Import-DotEnv -Path (Join-Path $Root ".env")

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
$env:AI_CANDIDATE_SCORER_ALLOW_ORDER_ACTIONS = "false"

if ([string]::IsNullOrWhiteSpace($CoreUrl)) {
    $CoreUrl = if ($env:GATEWAY_CORE_URL) { $env:GATEWAY_CORE_URL } else { "http://127.0.0.1:8000" }
}
if ([string]::IsNullOrWhiteSpace($Token)) {
    $Token = if ($env:GATEWAY_CORE_TOKEN) { $env:GATEWAY_CORE_TOKEN } else { $env:TRADING_CORE_TOKEN }
}
if ([string]::IsNullOrWhiteSpace($Token)) {
    throw "GATEWAY_CORE_TOKEN or TRADING_CORE_TOKEN is required."
}
$env:GATEWAY_CORE_TOKEN = $Token
$env:TRADING_CORE_TOKEN = $Token

if ([string]::IsNullOrWhiteSpace($ConditionProfilesJson)) {
    $ConditionProfilesJson = $env:KIWOOM_CONDITION_PROFILES
}
$DefaultConditionProfilesFile = Join-Path $Root "configs\condition_profiles\market_open_profiles.json"
if (
    [string]::IsNullOrWhiteSpace($ConditionProfilesFile) -and
    [string]::IsNullOrWhiteSpace($ConditionProfilesJson)
) {
    $ConditionProfilesFile = if ($env:KIWOOM_CONDITION_PROFILES_FILE) {
        $env:KIWOOM_CONDITION_PROFILES_FILE
    } elseif (Test-Path -LiteralPath $DefaultConditionProfilesFile) {
        $DefaultConditionProfilesFile
    } else {
        ""
    }
}
if ([string]::IsNullOrWhiteSpace($ConditionName)) {
    $ConditionName = $env:KIWOOM_CONDITION_NAME
}
if ([string]::IsNullOrWhiteSpace($RealtimeCodes)) {
    $RealtimeCodes = $env:KIWOOM_REALTIME_CODES
}
if ([string]::IsNullOrWhiteSpace($RealtimeExchange)) {
    $RealtimeExchange = if ($env:KIWOOM_REALTIME_EXCHANGE) { $env:KIWOOM_REALTIME_EXCHANGE } else { "krx" }
}
if ($RealtimeCodes.Trim().ToLowerInvariant() -eq "krx") {
    throw "KIWOOM_REALTIME_CODES must be stock codes. Use KIWOOM_REALTIME_EXCHANGE=krx for KRX registration."
}

$env:KIWOOM_REALTIME_EXCHANGE = $RealtimeExchange
if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
    $env:KIWOOM_REALTIME_CODES = $RealtimeCodes
}
if ([string]::IsNullOrWhiteSpace($env:KIWOOM_PENDING_THREAD_AUDIT_MAX_EVENTS)) {
    $env:KIWOOM_PENDING_THREAD_AUDIT_MAX_EVENTS = "200"
}

$MarketIndexEnabledSource = if ([string]::IsNullOrWhiteSpace($MarketIndexEnabled)) { $env:KIWOOM_MARKET_INDEX_ENABLED } else { $MarketIndexEnabled }
$MarketIndexRealtimeEnabledSource = if ([string]::IsNullOrWhiteSpace($MarketIndexRealtimeEnabled)) { $env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED } else { $MarketIndexRealtimeEnabled }
$MarketIndexTrBootstrapEnabledSource = if ([string]::IsNullOrWhiteSpace($MarketIndexTrBootstrapEnabled)) { $env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED } else { $MarketIndexTrBootstrapEnabled }
$MarketIndexEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_ENABLED" -Value $MarketIndexEnabledSource -Default $true
$MarketIndexRealtimeEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_REALTIME_ENABLED" -Value $MarketIndexRealtimeEnabledSource -Default $true
$MarketIndexTrBootstrapEnabledValue = Resolve-BoolSetting -Name "KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED" -Value $MarketIndexTrBootstrapEnabledSource -Default $false
if ([string]::IsNullOrWhiteSpace($MarketIndexCodes)) {
    $MarketIndexCodes = if ($env:KIWOOM_MARKET_INDEX_CODES) { $env:KIWOOM_MARKET_INDEX_CODES } else { "KOSPI,KOSDAQ" }
}
if ([string]::IsNullOrWhiteSpace($MarketIndexScreenNo)) {
    $MarketIndexScreenNo = if ($env:KIWOOM_MARKET_INDEX_SCREEN_NO) { $env:KIWOOM_MARKET_INDEX_SCREEN_NO } else { "5700" }
}
if ([string]::IsNullOrWhiteSpace($MarketIndexPollSec)) {
    $MarketIndexPollSec = if ($env:KIWOOM_MARKET_INDEX_POLL_SEC) { $env:KIWOOM_MARKET_INDEX_POLL_SEC } else { "60.0" }
}
$env:KIWOOM_MARKET_INDEX_ENABLED = if ($MarketIndexEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED = if ($MarketIndexRealtimeEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED = if ($MarketIndexTrBootstrapEnabledValue) { "true" } else { "false" }
$env:KIWOOM_MARKET_INDEX_CODES = $MarketIndexCodes
$env:KIWOOM_MARKET_INDEX_SCREEN_NO = $MarketIndexScreenNo
$env:KIWOOM_MARKET_INDEX_POLL_SEC = $MarketIndexPollSec

$ResolvedProfiles = ""
$ConditionProfileSource = ""
if (-not [string]::IsNullOrWhiteSpace($ConditionProfilesFile)) {
    $ProfileCandidate = $ConditionProfilesFile
    if (-not [System.IO.Path]::IsPathRooted($ProfileCandidate)) {
        $ProfileCandidate = Join-Path $Root $ProfileCandidate
    }
    $ProfilePath = (Resolve-Path -LiteralPath $ProfileCandidate).Path
    $ResolvedProfiles = Get-Content -LiteralPath $ProfilePath -Raw -Encoding UTF8
    $ConditionProfileSource = "file:$ProfilePath"
    $env:KIWOOM_CONDITION_PROFILES_FILE = $ProfilePath
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionProfilesJson)) {
    $ResolvedProfiles = $ConditionProfilesJson
    $ConditionProfileSource = "json/env"
}
if (-not [string]::IsNullOrWhiteSpace($ResolvedProfiles)) {
    $env:KIWOOM_CONDITION_PROFILES = $ResolvedProfiles
}
$ConditionMode = if (-not [string]::IsNullOrWhiteSpace($ResolvedProfiles)) {
    "MULTI_PROFILE"
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
    "LEGACY_SINGLE"
} else {
    "NONE"
}

$GatewayArgs = @(
    "-m", "apps.kiwoom_gateway",
    "--core-url", $CoreUrl,
    "--observe-only",
    "--no-threaded-login",
    "--realtime-exchange", $RealtimeExchange,
    "--realtime-recover-interval-sec", "300"
)
if (-not $NoAutoLogin) {
    $GatewayArgs += "--auto-login"
}
if (-not [string]::IsNullOrWhiteSpace($ResolvedProfiles)) {
    $GatewayArgs += @("--condition-profiles", $env:KIWOOM_CONDITION_PROFILES)
} elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
    $GatewayArgs += @("--condition-name", $ConditionName)
    if (-not $NoConditionRealtime) {
        $GatewayArgs += "--condition-realtime"
    }
}
if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
    $GatewayArgs += @("--realtime-codes", $RealtimeCodes)
}
if ($MarketIndexEnabledValue) {
    $GatewayArgs += "--market-index-enabled"
} else {
    $GatewayArgs += "--no-market-index-enabled"
}
if ($MarketIndexRealtimeEnabledValue) {
    $GatewayArgs += "--market-index-realtime-enabled"
} else {
    $GatewayArgs += "--no-market-index-realtime-enabled"
}
if ($MarketIndexTrBootstrapEnabledValue) {
    $GatewayArgs += "--market-index-tr-bootstrap-enabled"
} else {
    $GatewayArgs += "--no-market-index-tr-bootstrap-enabled"
}
$GatewayArgs += @(
    "--market-index-codes", $MarketIndexCodes,
    "--market-index-screen-no", $MarketIndexScreenNo,
    "--market-index-poll-sec", $MarketIndexPollSec
)

$SafeArgs = $GatewayArgs | ForEach-Object {
    if ($_.Length -gt 120) {
        "<long-value>"
    } else {
        Quote-CommandArgument $_
    }
}

Write-Host "Kiwoom Gateway OBSERVE launcher"
Write-Host "Root: $Root"
Write-Host "Core URL: $CoreUrl"
Write-Host "Condition mode: $ConditionMode"
if ($ConditionMode -eq "MULTI_PROFILE") {
    Write-Host "Condition profile source: $ConditionProfileSource"
} elseif ($ConditionMode -eq "LEGACY_SINGLE") {
    Write-Host "Legacy condition: $ConditionName"
}
Write-Host "Realtime exchange: $RealtimeExchange"
Write-Host "Realtime codes: $(if ($RealtimeCodes) { $RealtimeCodes } else { '<none>' })"
Write-Host "Market index adapter: enabled=$MarketIndexEnabledValue realtime=$MarketIndexRealtimeEnabledValue tr_bootstrap=$MarketIndexTrBootstrapEnabledValue codes=$MarketIndexCodes"
Write-Host "Command: $Python32 $($SafeArgs -join ' ')"
Write-Host "Dashboard: $CoreUrl/dashboard"
Write-Host ""

if ($DryRun) {
    return
}

$PreviousHeartbeat = ""
try {
    $BeforeStatus = Invoke-RestMethod -Uri "$CoreUrl/api/gateway/status" -TimeoutSec 3
    $PreviousHeartbeat = [string]$BeforeStatus.last_heartbeat_at
} catch {
    $PreviousHeartbeat = ""
}

Push-Location $Root
try {
    if ($Detached) {
        $StartInfo = @{
            FilePath = $Python32
            ArgumentList = ($GatewayArgs | ForEach-Object { Quote-CommandArgument $_ }) -join " "
            WorkingDirectory = $Root
            PassThru = $true
        }
        if ($Log) {
            $LogDir = Join-Path $Root "logs\runtime"
            New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
            $Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
            $StartInfo.RedirectStandardOutput = Join-Path $LogDir "kiwoom_gateway_$Stamp.out.log"
            $StartInfo.RedirectStandardError = Join-Path $LogDir "kiwoom_gateway_$Stamp.err.log"
        }

        $Process = Start-Process @StartInfo
        Write-Host "Gateway process started. PID=$($Process.Id)"

        $Deadline = (Get-Date).AddSeconds($WaitSeconds)
        $SawNewHeartbeat = $false
        while ((Get-Date) -lt $Deadline) {
            try {
                $Status = Invoke-RestMethod -Uri "$CoreUrl/api/gateway/status" -TimeoutSec 3
                $CurrentHeartbeat = [string]$Status.last_heartbeat_at
                if ($CurrentHeartbeat -and $CurrentHeartbeat -ne $PreviousHeartbeat) {
                    Write-Host "Gateway heartbeat: $($Status.last_heartbeat_at)"
                    $SawNewHeartbeat = $true
                    break
                }
            } catch {
                Start-Sleep -Seconds 2
                continue
            }
            Start-Sleep -Seconds 2
        }
        if (-not $SawNewHeartbeat) {
            Write-Warning "No new Gateway heartbeat was observed within $WaitSeconds seconds."
        }
    } else {
        & $Python32 @GatewayArgs
    }
} finally {
    Pop-Location
}

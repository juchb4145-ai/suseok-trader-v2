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
    [string]$ThemeRefreshQueueMarketScanCommands = $(if ($env:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS) { $env:THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS } else { "true" }),
    [string]$ThemeRefreshQueueRealtimeCommands = $(if ($env:THEME_REFRESH_QUEUE_REALTIME_COMMANDS) { $env:THEME_REFRESH_QUEUE_REALTIME_COMMANDS } else { "" }),
    [int]$ThemeRefreshRequestTimeoutSec = 120,
    [int]$CoreWaitSeconds = 30,
    [int]$GatewayWaitSeconds = 30,
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
            return
        } catch {
            Start-Sleep -Seconds 2
        }
    }

    Write-Warning "Core health check did not pass within $WaitSeconds seconds."
}

function Start-KiwoomGatewayDetached {
    $GatewayScript = Join-Path $PSScriptRoot "start_kiwoom_gateway_visible.ps1"
    $Args = @(
        "-CoreUrl", $CoreUrl,
        "-Token", $Token,
        "-RealtimeExchange", $RealtimeExchange,
        "-MarketIndexEnabled", $env:KIWOOM_MARKET_INDEX_ENABLED,
        "-MarketIndexRealtimeEnabled", $env:KIWOOM_MARKET_INDEX_REALTIME_ENABLED,
        "-MarketIndexTrBootstrapEnabled", $env:KIWOOM_MARKET_INDEX_TR_BOOTSTRAP_ENABLED,
        "-MarketIndexCodes", $MarketIndexCodes,
        "-MarketIndexScreenNo", $MarketIndexScreenNo,
        "-MarketIndexPollSec", $MarketIndexPollSec,
        "-Detached",
        "-Log",
        "-WaitSeconds", [string]$GatewayWaitSeconds
    )
    if (-not [string]::IsNullOrWhiteSpace($ConditionProfilesFile)) {
        $Args += @("-ConditionProfilesFile", $ConditionProfilesFile)
    } elseif (-not [string]::IsNullOrWhiteSpace($ConditionProfilesJson)) {
        $Args += @("-ConditionProfilesJson", $ConditionProfilesJson)
    } elseif (-not [string]::IsNullOrWhiteSpace($ConditionName)) {
        $Args += @("-ConditionName", $ConditionName)
    }
    if (-not [string]::IsNullOrWhiteSpace($RealtimeCodes)) {
        $Args += @("-RealtimeCodes", $RealtimeCodes)
    }

    & $GatewayScript @Args
}

function Start-ThemeRefreshLoopDetached {
    $LoopScript = Join-Path $PSScriptRoot "start_theme_refresh_loop.ps1"
    $Args = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $LoopScript,
        "-CoreUrl", $CoreUrl,
        "-Token", $Token,
        "-QueueMarketScanCommands", $ThemeRefreshQueueMarketScanCommands,
        "-RequestTimeoutSec", [string]$ThemeRefreshRequestTimeoutSec
    )
    if (-not [string]::IsNullOrWhiteSpace($TradeDate)) {
        $Args += @("-TradeDate", $TradeDate)
    }
    if (-not [string]::IsNullOrWhiteSpace($ThemeRefreshQueueRealtimeCommands)) {
        $Args += @("-QueueRealtimeCommands", $ThemeRefreshQueueRealtimeCommands)
    }

    Start-DetachedRuntimeProcess `
        -Label "theme_refresh_loop" `
        -FilePath "powershell.exe" `
        -Arguments $Args `
        -Hidden $true | Out-Null
}

Import-DotEnv -Path (Join-Path $Root ".env")

if ([string]::IsNullOrWhiteSpace($Token)) {
    $Token = if ($env:TRADING_CORE_TOKEN) { $env:TRADING_CORE_TOKEN } else { $env:GATEWAY_CORE_TOKEN }
}

$RunCoreRequested = $RunAll -or $RunCore
$RunGatewayRequested = $RunAll -or $RunGateway
$RunThemeRefreshLoopRequested = $RunAll -or $RunThemeRefreshLoop

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

if ($RunThemeRefreshLoopRequested) {
    $env:MARKET_SCAN_ENABLED = "true"
    if ([string]::IsNullOrWhiteSpace($env:MARKET_SCAN_INTERVAL_SEC)) {
        $env:MARKET_SCAN_INTERVAL_SEC = "120"
    }
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
Write-Host "One-shot launcher command:"
Write-Host "  .\tools\start_market_open_observe.ps1 -RunAll"
Write-Host ""
Write-Host "Theme refresh loop command:"
Write-Host "  .\tools\start_theme_refresh_loop.ps1 -CoreUrl $CoreUrl -Token `$env:TRADING_CORE_TOKEN"
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

if ($RunCoreRequested) {
    $CoreArgs = @(
        "-m", "uvicorn",
        "apps.core_api:app",
        "--host", "127.0.0.1",
        "--port", [string]$CorePort,
        "--reload"
    )
    if ($RunAll -or $RunGatewayRequested -or $RunThemeRefreshLoopRequested) {
        Start-DetachedRuntimeProcess `
            -Label "core" `
            -FilePath $Python64 `
            -Arguments $CoreArgs `
            -Hidden $true | Out-Null
        Wait-CoreHealth -Url $CoreUrl -WaitSeconds $CoreWaitSeconds
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

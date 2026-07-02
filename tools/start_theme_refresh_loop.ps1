param(
    [string]$CoreUrl = "",
    [string]$Token = "",
    [int]$IntervalSec = 0,
    [string]$MarketOpenTime = "",
    [string]$MarketCloseTime = "",
    [string]$TradeDate = "",
    [string]$QueueMarketScanCommands = "",
    [string]$QueueRealtimeCommands = "",
    [int]$RequestTimeoutSec = 120,
    [switch]$NoDotEnv,
    [switch]$StopOnError
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

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

function Resolve-StringSetting {
    param(
        [string]$Value,
        [string[]]$EnvNames,
        [string]$Default
    )

    if (-not [string]::IsNullOrWhiteSpace($Value)) {
        return $Value.Trim()
    }
    foreach ($EnvName in $EnvNames) {
        $EnvValue = [Environment]::GetEnvironmentVariable($EnvName, "Process")
        if (-not [string]::IsNullOrWhiteSpace($EnvValue)) {
            return $EnvValue.Trim()
        }
    }
    return $Default
}

function Resolve-PositiveIntSetting {
    param(
        [string]$Name,
        [int]$Value,
        [string[]]$EnvNames,
        [int]$Default
    )

    if ($Value -gt 0) {
        return $Value
    }
    foreach ($EnvName in $EnvNames) {
        $EnvValue = [Environment]::GetEnvironmentVariable($EnvName, "Process")
        if (-not [string]::IsNullOrWhiteSpace($EnvValue)) {
            $Parsed = 0
            if (-not [int]::TryParse($EnvValue.Trim(), [ref]$Parsed) -or $Parsed -lt 1) {
                throw "$Name must be a positive integer. Got: $EnvValue"
            }
            return $Parsed
        }
    }
    return $Default
}

function Resolve-BoolSetting {
    param(
        [string]$Name,
        [string]$Value,
        [string[]]$EnvNames,
        [bool]$Default
    )

    $Candidate = $Value
    if ([string]::IsNullOrWhiteSpace($Candidate)) {
        foreach ($EnvName in $EnvNames) {
            $EnvValue = [Environment]::GetEnvironmentVariable($EnvName, "Process")
            if (-not [string]::IsNullOrWhiteSpace($EnvValue)) {
                $Candidate = $EnvValue
                break
            }
        }
    }
    if ([string]::IsNullOrWhiteSpace($Candidate)) {
        return $Default
    }

    $Normalized = $Candidate.Trim().ToLowerInvariant()
    if (@("1", "true", "yes", "y", "on") -contains $Normalized) {
        return $true
    }
    if (@("0", "false", "no", "n", "off") -contains $Normalized) {
        return $false
    }

    throw "$Name must be true or false. Got: $Candidate"
}

function Resolve-TimeOfDay {
    param(
        [string]$Name,
        [string]$Value
    )

    try {
        return [TimeSpan]::Parse($Value)
    } catch {
        throw "$Name must be a time value such as 09:00:00. Got: $Value"
    }
}

function New-RefreshCycleUri {
    param(
        [string]$BaseUrl,
        [string]$TradeDateValue,
        [bool]$QueueMarketScan,
        [bool]$QueueRealtime
    )

    $Params = @(
        "queue_market_scan_commands=$([System.Uri]::EscapeDataString($QueueMarketScan.ToString().ToLowerInvariant()))",
        "queue_realtime_commands=$([System.Uri]::EscapeDataString($QueueRealtime.ToString().ToLowerInvariant()))"
    )
    if (-not [string]::IsNullOrWhiteSpace($TradeDateValue)) {
        $Params += "trade_date=$([System.Uri]::EscapeDataString($TradeDateValue.Trim()))"
    }
    return "$($BaseUrl.TrimEnd('/'))/api/themes/refresh-cycle/run-once?$($Params -join '&')"
}

function Get-NonZeroOrderDelta {
    param($Response)

    $Deltas = @()
    if ($null -eq $Response -or $null -eq $Response.order_command_delta) {
        return $Deltas
    }

    foreach ($Property in $Response.order_command_delta.PSObject.Properties) {
        $Value = [int]$Property.Value
        if ($Value -ne 0) {
            $Deltas += "$($Property.Name)=$Value"
        }
    }
    return $Deltas
}

function Wait-Until {
    param([datetime]$TargetAt)

    while ((Get-Date) -lt $TargetAt) {
        $RemainingSec = [math]::Ceiling(($TargetAt - (Get-Date)).TotalSeconds)
        if ($RemainingSec -le 0) {
            return
        }
        Start-Sleep -Seconds ([math]::Min($RemainingSec, 30))
    }
}

function Invoke-ThemeRefreshOnce {
    param(
        [string]$Uri,
        [hashtable]$Headers,
        [int]$TimeoutSec,
        [bool]$StopOnHttpError
    )

    try {
        $Response = Invoke-RestMethod `
            -Method Post `
            -Uri $Uri `
            -Headers $Headers `
            -TimeoutSec $TimeoutSec
    } catch {
        $Message = $_.Exception.Message
        if ($_.Exception.Response -and $_.Exception.Response.StatusCode) {
            $Message = "HTTP $([int]$_.Exception.Response.StatusCode): $Message"
        }
        if ($StopOnHttpError) {
            throw
        }
        Write-Warning "Theme refresh run failed: $Message"
        return $null
    }

    $OrderDeltas = Get-NonZeroOrderDelta -Response $Response
    if ($OrderDeltas.Count -gt 0 -or $Response.no_order_side_effects -eq $false) {
        throw "Order command delta detected during theme refresh loop: $($OrderDeltas -join ', ')"
    }

    $RunId = if ($Response.run_id) { $Response.run_id } else { "-" }
    $StatusValue = if ($Response.status) { $Response.status } else { "-" }
    $MarketScanStatus = if ($Response.market_scan -and $Response.market_scan.status) {
        $Response.market_scan.status
    } else {
        "-"
    }
    $ThemeStatus = if ($Response.theme_snapshots -and $Response.theme_snapshots.status) {
        $Response.theme_snapshots.status
    } else {
        "-"
    }
    $LeadershipStatus = if ($Response.leadership -and $Response.leadership.status) {
        $Response.leadership.status
    } else {
        "-"
    }
    $SubscriptionStatus = if (
        $Response.realtime_subscription -and $Response.realtime_subscription.status
    ) {
        $Response.realtime_subscription.status
    } else {
        "-"
    }

    $NowText = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
    Write-Host "[$NowText] run_id=$RunId status=$StatusValue market_scan=$MarketScanStatus theme=$ThemeStatus leadership=$LeadershipStatus realtime_subscription=$SubscriptionStatus"
    if ($StatusValue -ne "COMPLETED") {
        $ErrorsJson = if ($Response.errors) {
            $Response.errors | ConvertTo-Json -Depth 8 -Compress
        } else {
            "[]"
        }
        Write-Warning "Theme refresh cycle returned status=$StatusValue errors=$ErrorsJson"
    }

    return $Response
}

if (-not $NoDotEnv) {
    Import-DotEnv -Path (Join-Path $Root ".env")
}

$ResolvedCoreUrl = Resolve-StringSetting `
    -Value $CoreUrl `
    -EnvNames @("THEME_REFRESH_CORE_URL", "GATEWAY_CORE_URL") `
    -Default "http://127.0.0.1:8000"
$ResolvedToken = Resolve-StringSetting `
    -Value $Token `
    -EnvNames @("TRADING_CORE_TOKEN", "GATEWAY_CORE_TOKEN") `
    -Default ""
$ResolvedIntervalSec = Resolve-PositiveIntSetting `
    -Name "MARKET_SCAN_INTERVAL_SEC" `
    -Value $IntervalSec `
    -EnvNames @("MARKET_SCAN_INTERVAL_SEC") `
    -Default 120
$ResolvedOpenText = Resolve-StringSetting `
    -Value $MarketOpenTime `
    -EnvNames @("THEME_REFRESH_MARKET_OPEN_TIME") `
    -Default "09:00:00"
$ResolvedCloseText = Resolve-StringSetting `
    -Value $MarketCloseTime `
    -EnvNames @("THEME_REFRESH_MARKET_CLOSE_TIME") `
    -Default "15:30:00"
$ResolvedQueueMarketScan = Resolve-BoolSetting `
    -Name "QueueMarketScanCommands" `
    -Value $QueueMarketScanCommands `
    -EnvNames @("THEME_REFRESH_QUEUE_MARKET_SCAN_COMMANDS") `
    -Default $true
$ResolvedQueueRealtime = Resolve-BoolSetting `
    -Name "QueueRealtimeCommands" `
    -Value $QueueRealtimeCommands `
    -EnvNames @("THEME_REFRESH_QUEUE_REALTIME_COMMANDS", "REALTIME_SUBSCRIPTION_QUEUE_COMMANDS") `
    -Default $false

if ([string]::IsNullOrWhiteSpace($ResolvedToken)) {
    throw "TRADING_CORE_TOKEN or GATEWAY_CORE_TOKEN is required for /api/themes/refresh-cycle/run-once."
}
if ($RequestTimeoutSec -lt 1) {
    throw "RequestTimeoutSec must be >= 1."
}

$MarketOpen = Resolve-TimeOfDay -Name "MarketOpenTime" -Value $ResolvedOpenText
$MarketClose = Resolve-TimeOfDay -Name "MarketCloseTime" -Value $ResolvedCloseText
if ($MarketClose -le $MarketOpen) {
    throw "MarketCloseTime must be after MarketOpenTime."
}

$Headers = @{"X-Core-Token" = $ResolvedToken}
$Uri = New-RefreshCycleUri `
    -BaseUrl $ResolvedCoreUrl `
    -TradeDateValue $TradeDate `
    -QueueMarketScan $ResolvedQueueMarketScan `
    -QueueRealtime $ResolvedQueueRealtime

if ($env:MARKET_SCAN_ENABLED -and ([string]$env:MARKET_SCAN_ENABLED).Trim().ToLowerInvariant() -ne "true") {
    Write-Warning "This shell sees MARKET_SCAN_ENABLED=$($env:MARKET_SCAN_ENABLED). Make sure Core was started with MARKET_SCAN_ENABLED=true."
} elseif (-not $env:MARKET_SCAN_ENABLED) {
    Write-Warning "MARKET_SCAN_ENABLED is not set in this shell. Make sure Core was started with MARKET_SCAN_ENABLED=true."
}

Write-Host "Theme refresh loop is ready."
Write-Host "Core URL: $ResolvedCoreUrl"
Write-Host "Market window: $ResolvedOpenText~$ResolvedCloseText"
Write-Host "Interval: $ResolvedIntervalSec sec"
Write-Host "Queue market scan commands: $ResolvedQueueMarketScan"
Write-Host "Queue realtime commands: $ResolvedQueueRealtime"
Write-Host "Endpoint: $Uri"

while ($true) {
    $Now = Get-Date
    $OpenAt = $Now.Date.Add($MarketOpen)
    $CloseAt = $Now.Date.Add($MarketClose)

    if ($Now -lt $OpenAt) {
        Write-Host "[$($Now.ToString('yyyy-MM-dd HH:mm:ss'))] Before market window. Waiting until $($OpenAt.ToString('yyyy-MM-dd HH:mm:ss'))."
        Wait-Until -TargetAt $OpenAt
        continue
    }

    if ($Now -gt $CloseAt) {
        Write-Host "[$($Now.ToString('yyyy-MM-dd HH:mm:ss'))] Market window is closed. Exiting theme refresh loop."
        break
    }

    $StartedAt = Get-Date
    Invoke-ThemeRefreshOnce `
        -Uri $Uri `
        -Headers $Headers `
        -TimeoutSec $RequestTimeoutSec `
        -StopOnHttpError $StopOnError.IsPresent | Out-Null

    $FinishedAt = Get-Date
    if ($FinishedAt -gt $CloseAt) {
        Write-Host "[$($FinishedAt.ToString('yyyy-MM-dd HH:mm:ss'))] Market window closed after the last run. Exiting."
        break
    }

    $NextRunAt = $StartedAt.AddSeconds($ResolvedIntervalSec)
    if ($NextRunAt -gt $CloseAt) {
        Write-Host "[$($FinishedAt.ToString('yyyy-MM-dd HH:mm:ss'))] Next run would be after market close. Exiting."
        break
    }

    Wait-Until -TargetAt $NextRunAt
}

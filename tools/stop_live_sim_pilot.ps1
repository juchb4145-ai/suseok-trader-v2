param(
    [switch]$Force,
    [string]$PidFile = ""
)

$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if ([string]::IsNullOrWhiteSpace($PidFile)) {
    $PidFile = Join-Path $Root "logs\live_sim_pilot\pids.json"
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

function Get-ProcessIfRunning {
    param([int]$ProcessId)
    return Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
}

function Stop-PilotProcess {
    param(
        [string]$Label,
        [object]$Record
    )

    if ($null -eq $Record -or $null -eq $Record.pid) {
        Write-WarnKo "$Label PID 기록이 없습니다."
        return
    }
    $PidValue = [int]$Record.pid
    $Process = Get-ProcessIfRunning -ProcessId $PidValue
    if ($null -eq $Process) {
        Write-Ok "$Label PID=$PidValue 는 이미 종료되어 있습니다."
        return
    }
    Stop-Process -Id $PidValue -Force -ErrorAction Stop
    Write-Ok "$Label PID=$PidValue 종료"
}

function Invoke-CoreGet {
    param(
        [string]$CoreUrl,
        [string]$Path
    )
    return Invoke-RestMethod -Uri "$($CoreUrl.TrimEnd('/'))$Path" -TimeoutSec 5
}

if (-not (Test-Path -LiteralPath $PidFile)) {
    throw "PID 파일을 찾을 수 없습니다: $PidFile"
}

$State = Get-Content -LiteralPath $PidFile -Raw -Encoding UTF8 | ConvertFrom-Json
$CoreUrl = if ($State.core_url) { [string]$State.core_url } else { "http://127.0.0.1:8000" }

Write-Host "LIVE_SIM 파일럿 정리"
Write-Host "PID 파일: $PidFile"
Write-Host "Core URL: $CoreUrl"

$ActiveStatuses = @("COMMAND_QUEUED", "BROKER_ACKED", "PARTIALLY_FILLED")
try {
    $OrdersResult = Invoke-CoreGet -CoreUrl $CoreUrl -Path "/api/live-sim/orders?limit=500"
    $ActiveOrders = @(
        $OrdersResult.orders |
            Where-Object { $ActiveStatuses -contains ([string]$_.status) }
    )
    if ($ActiveOrders.Count -gt 0) {
        Write-FailKo "활성 LIVE_SIM 주문이 있어 정리를 중단합니다. -Force 없이는 종료하지 않습니다."
        $ActiveOrders |
            Select-Object live_sim_order_id, code, name, side, status, quantity, filled_quantity, broker_order_no, gateway_command_id |
            Format-Table -AutoSize
        if (-not $Force) {
            exit 1
        }
        Write-WarnKo "-Force 지정됨: 활성 주문이 있어도 프로세스 종료를 계속합니다."
    } else {
        Write-Ok "활성 LIVE_SIM 주문 없음"
    }
} catch {
    Write-FailKo "활성 주문 조회 실패: $($_.Exception.Message)"
    if (-not $Force) {
        Write-FailKo "활성 주문 확인이 안 된 상태에서는 -Force 없이 종료하지 않습니다."
        exit 1
    }
    Write-WarnKo "-Force 지정됨: 활성 주문 조회 실패에도 종료를 계속합니다."
}

$Gateway = $State.processes.gateway
$Core = $State.processes.core
$ThemeRefresh = $State.processes.theme_refresh
Stop-PilotProcess -Label "theme_refresh" -Record $ThemeRefresh
Stop-PilotProcess -Label "gateway" -Record $Gateway
Stop-PilotProcess -Label "core" -Record $Core

$State | Add-Member -NotePropertyName "stopped_at" -NotePropertyValue (Get-Date).ToString("o") -Force
$State | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $PidFile -Encoding UTF8

Write-Ok "LIVE_SIM 파일럿 정리 완료"
Write-Host "runtime_execution_locks 잔여 락은 Core 재기동 시 자동 정리됩니다. 이 스크립트는 DB를 직접 수정하지 않습니다."

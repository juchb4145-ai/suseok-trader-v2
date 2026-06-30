[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "Medium")]
param(
    [switch]$CoreOnly,
    [switch]$GatewayOnly,
    [switch]$Force
)

$ErrorActionPreference = "Stop"

if ($CoreOnly -and $GatewayOnly) {
    throw "Use either -CoreOnly or -GatewayOnly, not both."
}

$OriginalWhatIfPreference = $WhatIfPreference
try {
    $WhatIfPreference = $false
    Import-Module CimCmdlets -ErrorAction SilentlyContinue
} finally {
    $WhatIfPreference = $OriginalWhatIfPreference
}

$SelfPid = $PID
$Processes = @(
    Get-CimInstance -ClassName Win32_Process |
        Where-Object { $null -ne $_.ProcessId -and [int]$_.ProcessId -ne $SelfPid }
)

function Get-TargetLabel {
    param(
        [string]$CommandLine
    )

    if ([string]::IsNullOrWhiteSpace($CommandLine)) {
        return $null
    }

    $Normalized = $CommandLine.ToLowerInvariant()
    if (-not $GatewayOnly -and $Normalized -match "apps\.core_api:app") {
        return "core"
    }
    if (-not $CoreOnly -and $Normalized -match "apps\.mock_gateway") {
        return "mock_gateway"
    }
    if (-not $CoreOnly -and $Normalized -match "apps\.kiwoom_gateway") {
        return "kiwoom_gateway"
    }

    return $null
}

function Format-CommandLine {
    param(
        [string]$CommandLine
    )

    if ([string]::IsNullOrWhiteSpace($CommandLine)) {
        return "<empty command line>"
    }
    if ($CommandLine.Length -le 180) {
        return $CommandLine
    }

    return "$($CommandLine.Substring(0, 177))..."
}

$ChildrenByParent = @{}
foreach ($Process in $Processes) {
    if ($null -eq $Process.ParentProcessId) {
        continue
    }

    $ParentId = [int]$Process.ParentProcessId
    if (-not $ChildrenByParent.ContainsKey($ParentId)) {
        $ChildrenByParent[$ParentId] = New-Object System.Collections.Generic.List[object]
    }
    $ChildrenByParent[$ParentId].Add($Process)
}

$TargetsById = @{}

function Add-Target {
    param(
        [object]$Process,
        [string]$Label,
        [int]$Depth
    )

    $ProcessId = [int]$Process.ProcessId
    if ($ProcessId -eq $SelfPid -or $Process.Name -ieq "conhost.exe") {
        return
    }

    if (-not $TargetsById.ContainsKey($ProcessId)) {
        $TargetsById[$ProcessId] = [pscustomobject]@{
            Process = $Process
            Label = $Label
            Depth = $Depth
        }
    }

    if ($ChildrenByParent.ContainsKey($ProcessId)) {
        foreach ($Child in $ChildrenByParent[$ProcessId]) {
            Add-Target -Process $Child -Label "$Label child" -Depth ($Depth + 1)
        }
    }
}

foreach ($Process in $Processes) {
    $Label = Get-TargetLabel -CommandLine $Process.CommandLine
    if ($null -ne $Label) {
        Add-Target -Process $Process -Label $Label -Depth 0
    }
}

$Targets = @(
    $TargetsById.Values |
        Sort-Object -Property `
            @{ Expression = { $_.Depth }; Descending = $true },
            @{ Expression = { [int]$_.Process.ProcessId }; Descending = $true }
)

if ($Targets.Count -eq 0) {
    Write-Host "No running Core/Gateway process found."
    return
}

Write-Host "Core/Gateway processes to stop:"
foreach ($Target in $Targets) {
    $Process = $Target.Process
    Write-Host ("- {0}: PID={1}, PPID={2}, Name={3}" -f $Target.Label, $Process.ProcessId, $Process.ParentProcessId, $Process.Name)
    Write-Host ("  {0}" -f (Format-CommandLine -CommandLine $Process.CommandLine))
}
Write-Host ""

$HadError = $false
foreach ($Target in $Targets) {
    $Process = $Target.Process
    $Description = ("{0} PID={1}" -f $Target.Label, $Process.ProcessId)

    if ($PSCmdlet.ShouldProcess($Description, "Stop process")) {
        try {
            Stop-Process -Id ([int]$Process.ProcessId) -Force:$Force -ErrorAction Stop
            Write-Host "Stopped $Description"
        } catch [Microsoft.PowerShell.Commands.ProcessCommandException] {
            Write-Warning "Failed to stop ${Description}: $($_.Exception.Message)"
            $HadError = $true
        } catch [System.ArgumentException] {
            Write-Host "Already stopped $Description"
        }
    }
}

if ($HadError) {
    exit 1
}

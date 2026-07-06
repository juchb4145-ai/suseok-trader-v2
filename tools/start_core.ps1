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

Import-DotEnv -Path (Join-Path $Root ".env")

python -m uvicorn apps.core_api:app --host 127.0.0.1 --port 8000

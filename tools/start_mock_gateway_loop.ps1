param(
    [string]$CoreUrl = $(if ($env:GATEWAY_CORE_URL) { $env:GATEWAY_CORE_URL } else { "http://127.0.0.1:8000" }),
    [string]$Token = $env:GATEWAY_CORE_TOKEN,
    [double]$IntervalSec = 1.0
)

$ErrorActionPreference = "Stop"

python -m apps.mock_gateway --core-url $CoreUrl --token $Token --interval-sec $IntervalSec

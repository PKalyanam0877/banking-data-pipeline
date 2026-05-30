param(
    [string]$ProcessDate = "",
    [string]$ReportPath = "",
    [string]$HistoryPath = "",
    [string]$SummaryPath = "",
    [string]$Channel = ""
)

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/quality/audit/deliver_monitoring_alerts.py"

$scriptArgs = @()
if ($ProcessDate -ne "") {
    $scriptArgs += "--process-date"
    $scriptArgs += $ProcessDate
}
if ($ReportPath -ne "") {
    $scriptArgs += "--report-path"
    $scriptArgs += $ReportPath
}
if ($HistoryPath -ne "") {
    $scriptArgs += "--history-path"
    $scriptArgs += $HistoryPath
}
if ($SummaryPath -ne "") {
    $scriptArgs += "--summary-path"
    $scriptArgs += $SummaryPath
}
if ($Channel -ne "") {
    $scriptArgs += "--channel"
    $scriptArgs += $Channel
}

if (Test-Path $venvPython) {
    & $venvPython $scriptPath @scriptArgs
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython $scriptPath @scriptArgs
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

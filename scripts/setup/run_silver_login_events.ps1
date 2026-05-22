param(
    [string]$ProcessDate = "",
    [string]$BronzeIngestDate = ""
)

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/processing/silver/login_events_bronze_to_silver.py"

$scriptArgs = @()
if ($ProcessDate -ne "") {
    $scriptArgs += "--process-date"
    $scriptArgs += $ProcessDate
}
if ($BronzeIngestDate -ne "") {
    $scriptArgs += "--bronze-ingest-date"
    $scriptArgs += $BronzeIngestDate
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

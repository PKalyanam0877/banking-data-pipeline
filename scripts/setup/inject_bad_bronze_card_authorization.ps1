param(
    [string]$IngestDate = ""
)

$ErrorActionPreference = "Stop"

if ($IngestDate -eq "") {
    Write-Error "IngestDate is required. Example: -IngestDate 2026-05-21"
}

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/quality/quarantine/inject_bad_bronze_card_authorization.py"
$scriptArgs = @(
    "--ingest-date", $IngestDate
)

if (Test-Path $venvPython) {
    & $venvPython $scriptPath @scriptArgs
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython $scriptPath @scriptArgs
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

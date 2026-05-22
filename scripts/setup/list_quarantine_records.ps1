param(
    [string]$QuarantinePrefix = "silver-login-events",
    [string]$ProcessDate = ""
)

$ErrorActionPreference = "Stop"

if ($ProcessDate -eq "") {
    Write-Error "ProcessDate is required. Example: -ProcessDate 2026-05-21"
}

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/quality/quarantine/list_quarantine_records.py"
$scriptArgs = @(
    "--quarantine-prefix", $QuarantinePrefix,
    "--process-date", $ProcessDate
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

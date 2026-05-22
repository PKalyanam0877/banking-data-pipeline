$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/quality/bronze/validate_bronze_objects.py"
$args = @(
    "--prefix", "digital-activity/login-events/",
    "--max-objects", "100"
)

if (Test-Path $venvPython) {
    & $venvPython $scriptPath @args
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython $scriptPath @args
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"

if (Test-Path $venvPython) {
    & $venvPython src/producers/digital_activity/login_event_producer.py
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython src/producers/digital_activity/login_event_producer.py
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

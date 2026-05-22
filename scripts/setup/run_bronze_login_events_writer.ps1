param(
    [string]$IngestDate = ""
)

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/processing/bronze/kafka_to_minio_bronze.py"
$args = @(
    "--topic", "banking.digital-activity.login-events.v1",
    "--bronze-prefix", "digital-activity/login-events",
    "--consumer-group", "bronze-login-events-writer",
    "--max-messages", "10"
)

if ($IngestDate -ne "") {
    $args += "--ingest-date"
    $args += $IngestDate
}

if (Test-Path $venvPython) {
    & $venvPython $scriptPath @args
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython $scriptPath @args
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

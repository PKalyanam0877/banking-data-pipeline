param(
    [string]$IngestDate = ""
)

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/processing/bronze/kafka_to_minio_bronze.py"
$writerArgs = @(
    "--topic", "banking.transaction.card-authorizations.v1",
    "--bronze-prefix", "transaction/card-authorizations",
    "--consumer-group", "bronze-card-authorizations-writer",
    "--max-messages", "1500"
)

if ($IngestDate -ne "") {
    $writerArgs += "--ingest-date"
    $writerArgs += $IngestDate
}

if (Test-Path $venvPython) {
    & $venvPython $scriptPath @writerArgs
    exit $LASTEXITCODE
}

if (Test-Path $dotVenvPython) {
    & $dotVenvPython $scriptPath @writerArgs
    exit $LASTEXITCODE
}

Write-Error "No virtual environment Python executable found. Create one with: python -m venv venv"

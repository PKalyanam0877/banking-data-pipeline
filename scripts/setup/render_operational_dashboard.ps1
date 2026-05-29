param(
    [string]$ProcessDate = "",
    [string]$OutputPath = ""
)

$ErrorActionPreference = "Stop"

$venvPython = "venv/Scripts/python.exe"
$dotVenvPython = ".venv/Scripts/python.exe"
$scriptPath = "src/quality/audit/render_operational_dashboard.py"

$scriptArgs = @()
if ($ProcessDate -ne "") {
    $scriptArgs += "--process-date"
    $scriptArgs += $ProcessDate
}
if ($OutputPath -ne "") {
    $scriptArgs += "--output-path"
    $scriptArgs += $OutputPath
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

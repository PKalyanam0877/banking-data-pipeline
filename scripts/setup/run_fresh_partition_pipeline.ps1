param(
    [Parameter(Mandatory = $true)]
    [string]$ProcessDate,

    [string]$IngestDate = "",
    [string]$RiskIngestDate = "",
    [switch]$SkipProducers,
    [switch]$SkipHealthCheck
)

$ErrorActionPreference = "Stop"

if ($IngestDate -eq "") {
    $IngestDate = $ProcessDate
}

if ($RiskIngestDate -eq "") {
    $RiskIngestDate = $IngestDate
}

function Invoke-PipelineStep {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Name,

        [Parameter(Mandatory = $true)]
        [string]$ScriptPath,

        [string[]]$StepArgs = @()
    )

    Write-Host ""
    Write-Host "==> $Name"
    & powershell -ExecutionPolicy Bypass -File $ScriptPath @StepArgs

    if ($LASTEXITCODE -ne 0) {
        throw "Pipeline step failed: $Name"
    }
}

Write-Host "Fresh partition pipeline"
Write-Host "ProcessDate: $ProcessDate"
Write-Host "IngestDate: $IngestDate"
Write-Host "RiskIngestDate: $RiskIngestDate"

if (-not $SkipHealthCheck) {
    Invoke-PipelineStep `
        -Name "Check platform health" `
        -ScriptPath "scripts/setup/check_platform_health.ps1"
}

if (-not $SkipProducers) {
    Invoke-PipelineStep `
        -Name "Produce card authorization events" `
        -ScriptPath "scripts/setup/run_bulk_card_authorization_producer.ps1"
}

Invoke-PipelineStep `
    -Name "Land card authorizations to Bronze" `
    -ScriptPath "scripts/setup/run_bulk_bronze_card_authorization_writer.ps1" `
    -StepArgs @("-IngestDate", $IngestDate)

if (-not $SkipProducers) {
    Invoke-PipelineStep `
        -Name "Produce login events" `
        -ScriptPath "scripts/setup/run_login_event_producer.ps1"
}

Invoke-PipelineStep `
    -Name "Land login events to Bronze" `
    -ScriptPath "scripts/setup/run_bronze_login_events_writer.ps1" `
    -StepArgs @("-IngestDate", $IngestDate)

if (-not $SkipProducers) {
    Invoke-PipelineStep `
        -Name "Produce risk events" `
        -ScriptPath "scripts/setup/run_risk_event_producer.ps1"
}

Invoke-PipelineStep `
    -Name "Land risk events to Bronze" `
    -ScriptPath "scripts/setup/run_bronze_risk_events_writer.ps1" `
    -StepArgs @("-IngestDate", $RiskIngestDate)

Invoke-PipelineStep `
    -Name "Run Silver card authorizations" `
    -ScriptPath "scripts/setup/run_silver_card_authorizations.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Validate Silver card authorizations" `
    -ScriptPath "scripts/setup/validate_silver_card_authorizations.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Run Silver login events" `
    -ScriptPath "scripts/setup/run_silver_login_events.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate, "-BronzeIngestDate", $IngestDate)

Invoke-PipelineStep `
    -Name "Validate Silver login events" `
    -ScriptPath "scripts/setup/validate_silver_login_events.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Run Gold transaction monitoring" `
    -ScriptPath "scripts/setup/run_gold_transaction_monitoring.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Validate Gold transaction monitoring" `
    -ScriptPath "scripts/setup/validate_gold_transaction_monitoring.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Run Gold fraud investigation" `
    -ScriptPath "scripts/setup/run_gold_fraud_investigation.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate, "-RiskIngestDate", $RiskIngestDate)

Invoke-PipelineStep `
    -Name "Validate Gold fraud investigation" `
    -ScriptPath "scripts/setup/validate_gold_fraud_investigation.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Refresh Gold pipeline health" `
    -ScriptPath "scripts/setup/run_gold_pipeline_health.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Invoke-PipelineStep `
    -Name "Show latest pipeline health" `
    -ScriptPath "scripts/setup/show_latest_pipeline_health.ps1" `
    -StepArgs @("-ProcessDate", $ProcessDate)

Write-Host ""
Write-Host "Fresh partition pipeline completed successfully."

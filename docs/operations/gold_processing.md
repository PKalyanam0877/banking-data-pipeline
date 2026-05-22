# Gold Processing

Gold processing creates business-ready datasets from Silver records.

The first Gold data product is the transaction monitoring dashboard dataset.

Input:

```text
s3://banking-silver/transaction/card-authorizations/process_date=YYYY-MM-DD/
```

Output:

```text
s3://banking-gold/transaction-monitoring/dashboard/process_date=YYYY-MM-DD/part-00000.jsonl
```

## Metrics

The current dashboard dataset groups card authorization records by:

- `authorization_status`
- `channel`
- `merchant_country`
- `merchant_category_code`

Metrics:

- `transaction_count`
- `total_amount`
- `average_amount`
- `approved_count`
- `declined_count`

## Command

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-21
```

Validate the Gold output:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-21
```

## Production Thinking

Gold datasets should be business-ready and easy for analysts or dashboards to consume.

They should avoid forcing every downstream consumer to rewrite the same grouping, filtering, and business metric logic.

## Fraud Investigation Dataset

The fraud investigation Gold dataset combines:

- Silver card authorization events
- Silver login events
- Bronze fraud risk events

Inputs:

```text
s3://banking-silver/transaction/card-authorizations/process_date=YYYY-MM-DD/
s3://banking-silver/digital-activity/login-events/process_date=YYYY-MM-DD/
s3://banking-bronze/fraud-risk/risk-events/ingest_date=YYYY-MM-DD/
```

Output:

```text
s3://banking-gold/fraud-investigation/cases/process_date=YYYY-MM-DD/part-00000.jsonl
```

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_fraud_investigation.ps1 -ProcessDate 2026-05-21 -RiskIngestDate 2026-05-20
```

Validate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_fraud_investigation.ps1 -ProcessDate 2026-05-21
```

## Partition Contract

Gold jobs accept a `ProcessDate` parameter and read the matching Silver `process_date` partition.

Fraud investigation also accepts `RiskIngestDate` because fraud risk events may arrive on a different physical ingest date than the business transaction date. This models a common enterprise issue: late-arriving and cross-domain data rarely lands in perfect lockstep.

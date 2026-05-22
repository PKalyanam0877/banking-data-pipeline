# Silver Processing

Silver processing converts validated Bronze records into cleaned, deduplicated records.

The first Silver job processes card authorization events.

Input:

```text
s3://banking-bronze/transaction/card-authorizations/ingest_date=YYYY-MM-DD/
```

Output:

```text
s3://banking-silver/transaction/card-authorizations/process_date=YYYY-MM-DD/part-00000.jsonl
```

## Current Silver Rules

The card authorization Silver job:

- Reads Bronze JSON objects
- Extracts `parsed_value`
- Validates required card authorization fields
- Standardizes `amount` to two decimal places
- Requires `currency = USD`
- Allows `authorization_status` values `approved` and `declined`
- Deduplicates by `event_id`
- Preserves Bronze Kafka lineage fields

## Command

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_silver_card_authorizations.ps1 -ProcessDate 2026-05-21
```

Validate the Silver output:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_silver_card_authorizations.ps1 -ProcessDate 2026-05-21
```

## Production Thinking

Silver is not just a copy of Bronze.

Silver should represent technically reliable data that downstream systems can reuse. Invalid records should eventually be written to quarantine with rejection reasons instead of only being printed.

## Login Events

Login event Silver processing reads:

```text
s3://banking-bronze/digital-activity/login-events/ingest_date=YYYY-MM-DD/
```

and writes:

```text
s3://banking-silver/digital-activity/login-events/process_date=YYYY-MM-DD/part-00000.jsonl
```

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_silver_login_events.ps1 -ProcessDate 2026-05-21 -BronzeIngestDate 2026-05-21
```

Validate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_silver_login_events.ps1 -ProcessDate 2026-05-21
```

## Partition Contract

Silver jobs accept a `ProcessDate` parameter.

For card authorizations, the same date is used to read the Bronze `ingest_date` partition and write the Silver `process_date` partition.

Login Silver also accepts `BronzeIngestDate` because login events may physically land in Bronze on a different date than the business process date.

In production this date should come from an orchestrator logical date, not from the server clock. This prevents timezone drift and makes reruns auditable.

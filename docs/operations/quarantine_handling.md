# Quarantine Handling

Quarantine handling preserves rejected records instead of losing them in terminal logs.

Rejected records are stored in:

```text
s3://banking-quarantine/
```

Current quarantine paths:

```text
s3://banking-quarantine/silver-login-events/process_date=YYYY-MM-DD/
s3://banking-quarantine/silver-card-authorizations/process_date=YYYY-MM-DD/
```

Each quarantine record includes:

- `job_name`
- `process_date`
- `failed_at`
- `source_bucket`
- `source_object_key`
- `rejection_reason`
- `raw_record`

## List Quarantine Records

Login rejects:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/list_quarantine_records.ps1 -QuarantinePrefix silver-login-events -ProcessDate 2026-05-21
```

Card authorization rejects:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/list_quarantine_records.ps1 -QuarantinePrefix silver-card-authorizations -ProcessDate 2026-05-21
```

## Controlled Failure Tests

Inject one bad login Bronze record:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/inject_bad_bronze_login_event.ps1 -IngestDate 2026-05-21
```

Inject one bad card authorization Bronze record:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/inject_bad_bronze_card_authorization.ps1 -IngestDate 2026-05-21
```

Then rerun the matching Silver job.

## Production Thinking

Banks should not silently drop bad records.

Quarantine records help engineers and auditors answer:

- Which source object failed?
- Why did it fail?
- Which job rejected it?
- Which business process date was affected?
- Can the record be repaired and replayed?

This supports incident response, regulatory traceability, and data quality operations.

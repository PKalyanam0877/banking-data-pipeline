# Pipeline Audit

Pipeline audit records are operational metadata written to MinIO after pipeline jobs run.

Audit records are stored under:

```text
s3://banking-bronze/platform-audit/pipeline-runs/process_date=YYYY-MM-DD/
```

Each audit record captures:

- `run_id`
- `job_name`
- `status`
- `process_date`
- input and output prefixes
- records read, written, and rejected
- start and finish timestamps
- optional error message

## List Audit Runs

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/list_pipeline_audit_runs.ps1 -ProcessDate 2026-05-21
```

## Build Gold Pipeline Health Dataset

Audit records can be promoted into a Gold operational dataset:

```text
s3://banking-gold/platform-observability/pipeline-health/process_date=YYYY-MM-DD/part-00000.jsonl
s3://banking-gold/platform-observability/pipeline-health-latest/process_date=YYYY-MM-DD/part-00000.jsonl
```

The first path stores run history. The second path stores the latest run per job for dashboard status views.

Run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_pipeline_health.ps1 -ProcessDate 2026-05-21
```

Validate:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_pipeline_health.ps1 -ProcessDate 2026-05-21
```

## Production Thinking

Audit logs help platform teams answer operational questions:

- Did the job run?
- Which partition did it process?
- How many records moved between layers?
- Did any records fail validation?
- Which source prefix produced a downstream dataset?

This is the foundation for pipeline health dashboards, SLA checks, incident review, and compliance evidence.

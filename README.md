# Banking-Grade Real-Time Risk, Fraud & Compliance Data Platform

This project simulates an enterprise banking data platform using synthetic data.

## Phase 1 Architecture

The platform follows a hybrid streaming, CDC, and batch lakehouse architecture:

- PostgreSQL simulates core banking system-of-record data.
- Debezium captures database changes into Kafka.
- Python producers simulate real-time card, ACH, and digital banking events.
- MinIO simulates S3-style object storage.
- Spark will process Bronze, Silver, and Gold data layers.
- Governance includes PII classification, masking/tokenization, auditability, data quality, and lineage.

## Current State

The local platform currently supports:

- PostgreSQL core banking tables with Debezium CDC into Kafka
- Synthetic Kafka producers for card authorization, login, and fraud risk events
- Bronze landing from Kafka into MinIO
- Silver cleansing and deduplication for card authorization and login events
- Gold transaction monitoring dashboard dataset
- Gold fraud investigation case dataset
- Bronze, Silver, and Gold validation scripts
- Quarantine handling for rejected Silver records
- Pipeline audit and Gold pipeline health observability
- Spark standalone services for distributed processing practice

The project now follows a medallion-style local lakehouse flow:

```text
Kafka / CDC sources
  -> MinIO Bronze
  -> Silver cleaned datasets
  -> Gold business-ready datasets
```

Current engineering focus:

- Partition-aware processing with explicit `ProcessDate`
- Rerun safety and idempotency
- Data quality checks per partition
- Auditability through Kafka topic, partition, offset, and ingest metadata
- Rejected-record traceability through quarantine records

## Recommended Run Pattern

Use an explicit business date instead of relying on the server clock:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_silver_card_authorizations.ps1 -ProcessDate 2026-05-21
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_silver_card_authorizations.ps1 -ProcessDate 2026-05-21
```

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-21
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-21
```

## Fresh Partition Demo Path

This path proves the pipeline can replay a new partition from producers through Gold
outputs and observability. Replace `2026-05-25` with the business date being tested.

Run the full Phase 2 orchestration wrapper:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_fresh_partition_pipeline.ps1 -ProcessDate 2026-05-25
```

For replaying already-landed Bronze data without producing new Kafka events:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_fresh_partition_pipeline.ps1 -ProcessDate 2026-05-25 -SkipProducers
```

The manual command sequence below is kept as an expanded reference.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/check_platform_health.ps1
```

Generate and land Bronze events:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bulk_card_authorization_producer.ps1
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bulk_bronze_card_authorization_writer.ps1 -IngestDate 2026-05-25

powershell -ExecutionPolicy Bypass -File scripts/setup/run_login_event_producer.ps1
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_login_events_writer.ps1 -IngestDate 2026-05-25

powershell -ExecutionPolicy Bypass -File scripts/setup/run_risk_event_producer.ps1
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_risk_events_writer.ps1 -IngestDate 2026-05-25
```

Run and validate Silver and Gold:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_silver_card_authorizations.ps1 -ProcessDate 2026-05-25
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_silver_card_authorizations.ps1 -ProcessDate 2026-05-25

powershell -ExecutionPolicy Bypass -File scripts/setup/run_silver_login_events.ps1 -ProcessDate 2026-05-25 -BronzeIngestDate 2026-05-25
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_silver_login_events.ps1 -ProcessDate 2026-05-25

powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-25
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_transaction_monitoring.ps1 -ProcessDate 2026-05-25

powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_fraud_investigation.ps1 -ProcessDate 2026-05-25 -RiskIngestDate 2026-05-25
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_gold_fraud_investigation.ps1 -ProcessDate 2026-05-25
```

Refresh and inspect pipeline health:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_gold_pipeline_health.ps1 -ProcessDate 2026-05-25
powershell -ExecutionPolicy Bypass -File scripts/setup/show_latest_pipeline_health.ps1 -ProcessDate 2026-05-25
```

Expected healthy signals:

- Silver card authorizations writes 1,500 records with 0 rejected records.
- Silver login events writes valid login records with 0 rejected records.
- Gold transaction monitoring writes 10 aggregate rows.
- Gold fraud investigation writes 3 case rows.
- Latest pipeline health shows success for Silver and Gold jobs.

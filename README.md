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

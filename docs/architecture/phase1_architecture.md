# Phase 1 Architecture

## Architecture Pattern

This platform uses a hybrid streaming, CDC, and batch lakehouse architecture.

## Simulated Source Systems

- PostgreSQL core banking database for customers, accounts, balances, and customer risk profiles.
- Python Kafka producers for card authorization, ACH initiation, and digital activity events.
- Synthetic batch files for ACH settlement, returns, reversals, and reconciliation.

## Data Flow

- PostgreSQL changes flow through Debezium into Kafka.
- Real-time synthetic events flow from Python producers into Kafka.
- Batch files land directly in MinIO Bronze storage.
- Spark processes Bronze data into Silver and Gold layers.

## Medallion Layers

- Bronze: raw source-aligned data with synthetic PII, restricted and append-only.
- Silver: cleaned, deduplicated, validated, masked/tokenized data.
- Gold: business-ready data products for fraud risk, monitoring, features, and investigations.

## Governance

V1 governance focuses on:

- PII classification
- Masking and tokenization
- Auditability
- Data quality controls
- Lineage

## Monitoring

V1 monitoring focuses on:

- Kafka consumer lag
- Pipeline job failures
- Data freshness
- Data quality failure counts
- Transaction volume anomalies


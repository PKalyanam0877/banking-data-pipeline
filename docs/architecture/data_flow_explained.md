# Data Flow Explained

This document explains how data moves through the local banking data platform.

The platform currently demonstrates two important enterprise ingestion patterns:

- Business event streaming with Kafka producers
- Database change data capture with Debezium

## 1. PostgreSQL As Core Banking State

PostgreSQL simulates the bank's core system of record.

Current core banking tables:

- `customers`
- `accounts`
- `account_balances`
- `customer_risk_profiles`

These tables represent operational state. For example, PostgreSQL can answer:

- What is the current status of a customer?
- Which accounts belong to a customer?
- Is an account open, restricted, frozen, or closed?
- What is the customer's KYC risk rating?

Example:

```sql
SELECT customer_id, first_name, last_name, customer_status
FROM customers
WHERE customer_id = 'cust_100002';
```

If the result shows `customer_status = 'restricted'`, that is the current source-of-truth state for the customer.

Useful command:

```powershell
docker exec -it banking_postgres psql -U banking_user -d banking
```

Useful SQL checks:

```sql
\dt

SELECT customer_id, first_name, last_name, customer_status
FROM customers;

SELECT account_id, customer_id, account_type, account_status
FROM accounts;

SELECT customer_id, kyc_status, kyc_risk_rating, fraud_watchlist_flag, risk_score
FROM customer_risk_profiles;
```

## 2. Debezium CDC Flow

Debezium captures row-level changes from PostgreSQL and publishes them to Kafka.

Flow:

```text
PostgreSQL table update
  -> PostgreSQL write-ahead log
  -> Debezium PostgreSQL connector
  -> Kafka CDC topic
  -> Kafka consumer
```

Example CDC topic:

```text
banking.cdc.core-banking.public.customers
```

When `cust_100002` was updated to `restricted`, Debezium produced a CDC event with:

```json
"op": "u",
"snapshot": "false",
"after": {
  "customer_id": "cust_100002",
  "customer_status": "restricted"
}
```

Important fields:

- `op = r`: initial snapshot read
- `op = c`: insert/create
- `op = u`: update
- `op = d`: delete
- `snapshot = false`: live CDC event from the PostgreSQL WAL
- `after`: row state after the change
- `source.lsn`: PostgreSQL log sequence number for traceability

For now, update events may show:

```json
"before": null
```

That is expected with the current PostgreSQL replica identity setting. If full old-row values are needed later, the table can be configured with:

```sql
ALTER TABLE customers REPLICA IDENTITY FULL;
```

That improves audit detail but increases CDC payload size.

Useful CDC consumer command:

```powershell
docker exec -it banking_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic banking.cdc.core-banking.public.customers --from-beginning
```

Useful PostgreSQL update command:

```powershell
docker exec banking_postgres psql -U banking_user -d banking -c "UPDATE customers SET customer_status = 'restricted', updated_at = CURRENT_TIMESTAMP WHERE customer_id = 'cust_100002';"
```

Expected CDC signal:

```json
"op": "u"
```

This proves the change was captured as a live update event.

## 3. Python Producers For Business Events

Python producers simulate real banking source systems.

Current producers:

- Card authorization producer
- Digital login event producer
- Fraud risk event producer

Example card topic:

```text
banking.transaction.card-authorizations.v1
```

The card producer sends messages where:

- Kafka key = `card_id`
- Kafka value = card authorization JSON event

Example consumer output:

```text
card_300003 | {"event_id": "evt_card_auth_000008", ...}
```

The left side is the Kafka message key. The right side is the message value.

Useful consumer command:

```powershell
docker exec -it banking_kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic banking.transaction.card-authorizations.v1 --from-beginning --property print.key=true --property key.separator=" | "
```

Useful producer command:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_card_authorization_producer.ps1
```

Example interpretation:

```text
card_300003 | {"event_id": "evt_card_auth_000008", ...}
```

Means:

- `card_300003` is the Kafka message key
- JSON is the Kafka message value
- Events with the same key are routed to the same partition
- Ordering is preserved within that partition

## 4. Kafka Broker, Topics, Keys, And Partitions

Kafka stores messages in topics split into partitions.

Topic example:

```text
banking.transaction.card-authorizations.v1
```

Message key example:

```text
card_300003
```

Why the key matters:

- Kafka sends the same key to the same partition.
- Kafka preserves ordering within a partition.
- Events for the same card can be processed in order.

This matters for fraud patterns such as:

- Multiple attempts from the same card
- Decline followed by approval
- Velocity rules
- Same card used across risky merchants

Kafka is not just a queue. It is an ordered distributed event log.

## 5. MinIO Bronze Landing

MinIO simulates S3-style object storage.

Kafka messages are landed into the Bronze bucket as raw JSON files.

Example bucket:

```text
banking-bronze
```

Example Bronze path:

```text
transaction/card-authorizations/ingest_date=YYYY-MM-DD/
```

Each Bronze object contains:

- Kafka topic
- Kafka partition
- Kafka offset
- Kafka key
- Raw message value
- Parsed JSON value
- Bronze ingestion timestamp

This makes the raw event replayable and auditable.

Useful Bronze writer commands:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_card_authorization_writer.ps1 -IngestDate 2026-05-21
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_login_events_writer.ps1 -IngestDate 2026-05-21
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_risk_events_writer.ps1 -IngestDate 2026-05-21
```

Expected MinIO paths:

```text
banking-bronze/
  transaction/card-authorizations/ingest_date=YYYY-MM-DD/
  digital-activity/login-events/ingest_date=YYYY-MM-DD/
  fraud-risk/risk-events/ingest_date=YYYY-MM-DD/
```

Bronze writers accept an explicit `IngestDate` parameter. In production this would normally come from an orchestrator or replay request, not from the laptop clock.

## 6. Current End-To-End Platform

Current flows:

```text
PostgreSQL
  -> Debezium
  -> Kafka CDC topics
```

```text
Python card producer
  -> Kafka card authorization topic
  -> MinIO Bronze
```

```text
Python login producer
  -> Kafka digital activity topic
  -> MinIO Bronze
```

```text
Python fraud risk producer
  -> Kafka fraud risk topic
  -> MinIO Bronze
```

## 7. Banking Interpretation

PostgreSQL answers current-state questions:

```text
What is the current status of this customer?
```

Kafka answers event-history questions:

```text
What happened over time for this card, customer, or account?
```

MinIO Bronze answers audit and replay questions:

```text
What raw data did the platform receive?
Can we trace a fraud alert back to source events?
Can we replay the pipeline from raw records?
```

Together, these patterns create a realistic banking data platform foundation.

## 8. Quick Health Check

After restarting the laptop or Docker Desktop, run:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/check_platform_health.ps1
```

This checks:

- Docker Compose services
- Kafka topics
- Debezium connector status
- PostgreSQL table counts

Expected healthy state:

```text
banking_postgres   Up
banking_kafka      Up
banking_debezium   Up
postgres-core-banking-connector RUNNING
```

# Spark Setup

Spark is added in standalone mode for local distributed processing practice.

Services:

```text
spark-master
spark-worker
```

Containers:

```text
banking_spark_master
banking_spark_worker
```

## Start Spark

```powershell
docker compose up -d spark-master spark-worker
```

## Check Status

```powershell
docker compose ps spark-master spark-worker
```

## Spark UIs

Spark master UI:

```text
http://localhost:8080
```

Spark worker UI:

```text
http://localhost:8081
```

## Read Bronze Card Authorizations From MinIO

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_spark_read_bronze_card_authorizations.ps1 -IngestDate 2026-05-21
```

The wrapper submits:

```text
src/processing/spark/read_bronze_card_authorizations.py
```

against:

```text
s3a://banking-bronze/transaction/card-authorizations/ingest_date=YYYY-MM-DD/
```

It includes the Hadoop AWS packages required for Spark to read MinIO through `s3a://`.

## Network

Spark uses the same Docker network as the rest of the platform:

```text
banking_network
```

This allows future Spark jobs to communicate with:

```text
minio:9000
kafka:9092
postgres:5432
```

## Production Thinking

Standalone Spark is useful for learning Spark internals locally:

- master and worker roles
- executors
- task distribution
- Spark UI debugging
- partitioned file reads and writes

Later, this can evolve toward Kubernetes-based Spark submission.

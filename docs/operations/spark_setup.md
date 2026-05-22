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

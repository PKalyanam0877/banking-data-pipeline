param(
    [string]$IngestDate = "",
    [string]$InputPath = ""
)

$ErrorActionPreference = "Stop"

if ($InputPath -eq "") {
    if ($IngestDate -eq "") {
        Write-Error "Either IngestDate or InputPath is required. Example: -IngestDate 2026-05-21"
    }

    $InputPath = "s3a://banking-bronze/transaction/card-authorizations/ingest_date=$IngestDate/"
}

docker exec -it banking_spark_master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 `
    /opt/spark/work-dir/src/processing/spark/read_bronze_card_authorizations.py `
    --input-path $InputPath `
    --s3-endpoint http://minio:9000 `
    --s3-access-key minioadmin `
    --s3-secret-key minioadmin

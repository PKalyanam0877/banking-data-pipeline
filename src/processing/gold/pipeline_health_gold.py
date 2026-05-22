import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[2]))
from common.pipeline_audit import AUDIT_PREFIX, write_pipeline_audit_event


GOLD_PREFIX = "platform-observability/pipeline-health/"
LATEST_GOLD_PREFIX = "platform-observability/pipeline-health-latest/"
JOB_NAME = "gold_pipeline_health"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Audit process date partition to summarize, format YYYY-MM-DD",
    )
    return parser.parse_args()


def build_minio_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_json_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".json"):
                yield item["Key"]


def read_json_object(s3_client, bucket, object_key):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def calculate_duration_seconds(started_at, finished_at):
    start_time = datetime.fromisoformat(started_at)
    finish_time = datetime.fromisoformat(finished_at)
    return round((finish_time - start_time).total_seconds(), 3)


def build_health_record(audit_record):
    return {
        "run_id": audit_record["run_id"],
        "job_name": audit_record["job_name"],
        "status": audit_record["status"],
        "process_date": audit_record["process_date"],
        "records_read": audit_record["records_read"],
        "records_written": audit_record["records_written"],
        "records_rejected": audit_record["records_rejected"],
        "started_at": audit_record["started_at"],
        "finished_at": audit_record["finished_at"],
        "duration_seconds": calculate_duration_seconds(
            audit_record["started_at"],
            audit_record["finished_at"],
        ),
        "input_prefix": audit_record["input_prefix"],
        "output_prefix": audit_record["output_prefix"],
        "error_message": audit_record.get("error_message"),
        "gold_processed_time": datetime.now(UTC).isoformat(),
    }


def select_latest_run_per_job(records):
    latest_by_job = {}

    for record in records:
        job_name = record["job_name"]
        current_latest = latest_by_job.get(job_name)
        if current_latest is None or record["finished_at"] > current_latest["finished_at"]:
            latest_by_job[job_name] = record

    return sorted(latest_by_job.values(), key=lambda record: record["job_name"])


def write_gold_records(s3_client, bucket, prefix, records, process_date):
    object_key = f"{prefix}process_date={process_date}/part-00000.jsonl"
    body = "\n".join(json.dumps(record) for record in records).encode("utf-8")

    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/x-ndjson",
    )
    return object_key


def main():
    started_at = datetime.now(UTC).isoformat()
    args = parse_args()
    process_date = args.process_date
    audit_input_prefix = f"{AUDIT_PREFIX}/process_date={process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    audit_object_keys = list(list_json_objects(s3_client, bronze_bucket, audit_input_prefix))

    if not audit_object_keys:
        audit_key = write_pipeline_audit_event(
            s3_client=s3_client,
            bucket=bronze_bucket,
            job_name=JOB_NAME,
            status="failed",
            process_date=process_date,
            input_prefix=f"s3://{bronze_bucket}/{audit_input_prefix}",
            output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
            records_read=0,
            records_written=0,
            records_rejected=0,
            started_at=started_at,
            error_message=f"No pipeline audit records found under {audit_input_prefix}",
        )
        print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")
        raise SystemExit(f"No pipeline audit records found under {audit_input_prefix}")

    audit_records = [
        read_json_object(s3_client, bronze_bucket, object_key)
        for object_key in audit_object_keys
    ]
    health_records = sorted(
        (build_health_record(record) for record in audit_records),
        key=lambda record: (record["finished_at"], record["job_name"]),
    )
    latest_health_records = select_latest_run_per_job(health_records)

    gold_key = write_gold_records(s3_client, gold_bucket, GOLD_PREFIX, health_records, process_date)
    latest_gold_key = write_gold_records(
        s3_client,
        gold_bucket,
        LATEST_GOLD_PREFIX,
        latest_health_records,
        process_date,
    )
    audit_key = write_pipeline_audit_event(
        s3_client=s3_client,
        bucket=bronze_bucket,
        job_name=JOB_NAME,
        status="success",
        process_date=process_date,
        input_prefix=f"s3://{bronze_bucket}/{audit_input_prefix}",
        output_prefix=(
            f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/;"
            f"s3://{gold_bucket}/{LATEST_GOLD_PREFIX}process_date={process_date}/"
        ),
        records_read=len(audit_records),
        records_written=len(health_records) + len(latest_health_records),
        records_rejected=0,
        started_at=started_at,
    )

    print("Gold pipeline health summary")
    print(f"Process date: {process_date}")
    print(f"Audit records read: {len(audit_records)}")
    print(f"Gold health records written: {len(health_records)}")
    print(f"Gold latest health records written: {len(latest_health_records)}")
    print(f"Wrote s3://{gold_bucket}/{gold_key}")
    print(f"Wrote s3://{gold_bucket}/{latest_gold_key}")
    print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")


if __name__ == "__main__":
    main()

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
from common.pipeline_audit import write_pipeline_audit_event


BRONZE_PREFIX = "digital-activity/login-events/"
SILVER_PREFIX = "digital-activity/login-events/"
JOB_NAME = "silver_login_events"

REQUIRED_FIELDS = {
    "event_id",
    "login_event_id",
    "customer_id",
    "event_time",
    "event_type",
    "login_status",
    "device_id",
    "device_type",
    "ip_address",
    "country",
    "city",
    "channel",
    "source_system",
    "schema_version",
}

ALLOWED_EVENT_TYPES = {
    "login_success",
    "login_failed",
    "password_reset",
    "new_device_registered",
}

ALLOWED_LOGIN_STATUSES = {"success", "failed"}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Date partition to process, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--bronze-ingest-date",
        default=None,
        help="Bronze login ingest date to read. Defaults to --process-date.",
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


def list_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            yield item["Key"]


def read_json_object(s3_client, bucket, object_key):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def validate_event(event):
    missing_fields = REQUIRED_FIELDS - set(event)
    if missing_fields:
        raise ValueError(f"missing fields: {sorted(missing_fields)}")

    for field in REQUIRED_FIELDS:
        if event.get(field) in (None, ""):
            raise ValueError(f"field is null or empty: {field}")

    if event["event_type"] not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event['event_type']}")

    if event["login_status"] not in ALLOWED_LOGIN_STATUSES:
        raise ValueError(f"unsupported login_status: {event['login_status']}")


def transform_event(bronze_record):
    event = bronze_record.get("parsed_value")
    if not isinstance(event, dict):
        raise ValueError("parsed_value must be a JSON object")

    validate_event(event)

    return {
        "event_id": event["event_id"],
        "login_event_id": event["login_event_id"],
        "customer_id": event["customer_id"],
        "event_time": event["event_time"],
        "event_type": event["event_type"],
        "login_status": event["login_status"],
        "failure_reason": event.get("failure_reason"),
        "device_id": event["device_id"],
        "device_type": event["device_type"],
        "ip_address": event["ip_address"],
        "country": event["country"],
        "city": event["city"],
        "channel": event["channel"],
        "user_agent": event.get("user_agent"),
        "source_system": event["source_system"],
        "schema_version": event["schema_version"],
        "bronze_kafka_topic": bronze_record["kafka_topic"],
        "bronze_kafka_partition": bronze_record["kafka_partition"],
        "bronze_kafka_offset": bronze_record["kafka_offset"],
        "bronze_ingest_time": bronze_record["bronze_ingest_time"],
        "silver_processed_time": datetime.now(UTC).isoformat(),
    }


def write_silver_records(s3_client, bucket, records, process_date):
    object_key = f"{SILVER_PREFIX}process_date={process_date}/part-00000.jsonl"
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
    bronze_ingest_date = args.bronze_ingest_date or process_date
    bronze_input_prefix = f"{BRONZE_PREFIX}ingest_date={bronze_ingest_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "banking-silver")

    s3_client = build_minio_client(endpoint, access_key, secret_key)

    records_by_event_id = {}
    read_count = 0
    failed_count = 0

    for object_key in list_objects(s3_client, bronze_bucket, bronze_input_prefix):
        read_count += 1
        try:
            bronze_record = read_json_object(s3_client, bronze_bucket, object_key)
            silver_record = transform_event(bronze_record)
            records_by_event_id[silver_record["event_id"]] = silver_record
        except Exception as exc:
            failed_count += 1
            print(f"Rejected s3://{bronze_bucket}/{object_key}: {exc}")

    silver_records = sorted(records_by_event_id.values(), key=lambda item: item["event_id"])

    if not silver_records:
        audit_key = write_pipeline_audit_event(
            s3_client=s3_client,
            bucket=bronze_bucket,
            job_name=JOB_NAME,
            status="failed",
            process_date=process_date,
            input_prefix=f"s3://{bronze_bucket}/{bronze_input_prefix}",
            output_prefix=f"s3://{silver_bucket}/{SILVER_PREFIX}process_date={process_date}/",
            records_read=read_count,
            records_written=0,
            records_rejected=failed_count,
            started_at=started_at,
            error_message="No valid Silver login records produced.",
        )
        print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")
        raise SystemExit("No valid Silver login records produced.")

    silver_key = write_silver_records(s3_client, silver_bucket, silver_records, process_date)
    audit_key = write_pipeline_audit_event(
        s3_client=s3_client,
        bucket=bronze_bucket,
        job_name=JOB_NAME,
        status="success",
        process_date=process_date,
        input_prefix=f"s3://{bronze_bucket}/{bronze_input_prefix}",
        output_prefix=f"s3://{silver_bucket}/{SILVER_PREFIX}process_date={process_date}/",
        records_read=read_count,
        records_written=len(silver_records),
        records_rejected=failed_count,
        started_at=started_at,
    )

    print("Silver login processing summary")
    print(f"Process date: {process_date}")
    print(f"Bronze ingest date: {bronze_ingest_date}")
    print(f"Bronze input prefix: s3://{bronze_bucket}/{bronze_input_prefix}")
    print(f"Bronze objects read: {read_count}")
    print(f"Rejected records: {failed_count}")
    print(f"Deduplicated Silver records: {len(silver_records)}")
    print(f"Wrote s3://{silver_bucket}/{silver_key}")
    print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")


if __name__ == "__main__":
    main()

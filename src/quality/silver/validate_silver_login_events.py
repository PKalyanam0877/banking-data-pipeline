import argparse
import json
import os
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv


SILVER_PREFIX = "digital-activity/login-events/"

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
    "bronze_kafka_topic",
    "bronze_kafka_partition",
    "bronze_kafka_offset",
    "bronze_ingest_time",
    "silver_processed_time",
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
        help="Silver process date partition to validate, format YYYY-MM-DD",
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


def list_jsonl_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".jsonl"):
                yield item["Key"]


def read_jsonl_object(s3_client, bucket, object_key):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read().decode("utf-8")
    for line_number, line in enumerate(body.splitlines(), start=1):
        if line.strip():
            yield line_number, json.loads(line)


def validate_record(record):
    errors = []

    missing_fields = REQUIRED_FIELDS - set(record)
    if missing_fields:
        errors.append(f"missing fields: {sorted(missing_fields)}")

    for field in REQUIRED_FIELDS:
        if record.get(field) in (None, ""):
            errors.append(f"field is null or empty: {field}")

    if record.get("event_type") not in ALLOWED_EVENT_TYPES:
        errors.append(f"invalid event_type: {record.get('event_type')}")

    if record.get("login_status") not in ALLOWED_LOGIN_STATUSES:
        errors.append(f"invalid login_status: {record.get('login_status')}")

    return errors


def main():
    args = parse_args()
    silver_input_prefix = f"{SILVER_PREFIX}process_date={args.process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    silver_bucket = os.getenv("SILVER_BUCKET", "banking-silver")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    object_keys = list(list_jsonl_objects(s3_client, silver_bucket, silver_input_prefix))

    if not object_keys:
        raise SystemExit(f"No Silver login JSONL files found under {silver_input_prefix}")

    seen_event_ids = set()
    duplicate_event_ids = set()
    checked_count = 0
    failed_count = 0

    for object_key in object_keys:
        for line_number, record in read_jsonl_object(s3_client, silver_bucket, object_key):
            checked_count += 1
            event_id = record.get("event_id")

            if event_id in seen_event_ids:
                duplicate_event_ids.add(event_id)
            else:
                seen_event_ids.add(event_id)

            errors = validate_record(record)
            if errors:
                failed_count += 1
                print(f"FAILED: s3://{silver_bucket}/{object_key} line {line_number}")
                for error in errors:
                    print(f"  - {error}")

    print("\nSilver login validation summary")
    print(f"Bucket: {silver_bucket}")
    print(f"Process date: {args.process_date}")
    print(f"Prefix: {silver_input_prefix}")
    print(f"Files checked: {len(object_keys)}")
    print(f"Records checked: {checked_count}")
    print(f"Duplicate event_ids: {len(duplicate_event_ids)}")
    print(f"Failed records: {failed_count}")

    if duplicate_event_ids:
        print(f"Duplicate IDs: {sorted(duplicate_event_ids)}")
        raise SystemExit(1)

    if failed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

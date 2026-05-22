import argparse
import json
import os
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv


GOLD_PREFIX = "platform-observability/pipeline-health/"
LATEST_GOLD_PREFIX = "platform-observability/pipeline-health-latest/"

REQUIRED_FIELDS = {
    "run_id",
    "job_name",
    "status",
    "process_date",
    "records_read",
    "records_written",
    "records_rejected",
    "started_at",
    "finished_at",
    "duration_seconds",
    "input_prefix",
    "output_prefix",
    "gold_processed_time",
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Gold process date partition to validate, format YYYY-MM-DD",
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


def validate_record(record, expected_process_date):
    errors = []

    missing_fields = REQUIRED_FIELDS - set(record)
    if missing_fields:
        errors.append(f"missing fields: {sorted(missing_fields)}")

    for field in REQUIRED_FIELDS:
        if record.get(field) in (None, ""):
            errors.append(f"field is null or empty: {field}")

    if record.get("status") not in {"success", "failed"}:
        errors.append(f"invalid status: {record.get('status')}")

    if record.get("process_date") != expected_process_date:
        errors.append(f"unexpected process_date: {record.get('process_date')}")

    for field in ["records_read", "records_written", "records_rejected"]:
        if not isinstance(record.get(field), int) or record[field] < 0:
            errors.append(f"invalid count field {field}: {record.get(field)}")

    duration_seconds = record.get("duration_seconds")
    if not isinstance(duration_seconds, (int, float)) or duration_seconds < 0:
        errors.append(f"invalid duration_seconds: {duration_seconds}")

    return errors


def validate_prefix(s3_client, gold_bucket, gold_input_prefix, process_date):
    object_keys = list(list_jsonl_objects(s3_client, gold_bucket, gold_input_prefix))
    if not object_keys:
        raise SystemExit(f"No Gold pipeline health JSONL files found under {gold_input_prefix}")

    checked_count = 0
    failed_count = 0

    for object_key in object_keys:
        for line_number, record in read_jsonl_object(s3_client, gold_bucket, object_key):
            checked_count += 1
            errors = validate_record(record, process_date)
            if errors:
                failed_count += 1
                print(f"FAILED: s3://{gold_bucket}/{object_key} line {line_number}")
                for error in errors:
                    print(f"  - {error}")

    return len(object_keys), checked_count, failed_count


def main():
    args = parse_args()
    history_input_prefix = f"{GOLD_PREFIX}process_date={args.process_date}/"
    latest_input_prefix = f"{LATEST_GOLD_PREFIX}process_date={args.process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    history_files, history_checked, history_failed = validate_prefix(
        s3_client,
        gold_bucket,
        history_input_prefix,
        args.process_date,
    )
    latest_files, latest_checked, latest_failed = validate_prefix(
        s3_client,
        gold_bucket,
        latest_input_prefix,
        args.process_date,
    )

    total_failed_count = history_failed + latest_failed

    print("\nGold pipeline health validation summary")
    print(f"Bucket: {gold_bucket}")
    print(f"Process date: {args.process_date}")
    print(f"History prefix: {history_input_prefix}")
    print(f"Latest prefix: {latest_input_prefix}")
    print(f"History files checked: {history_files}")
    print(f"Latest files checked: {latest_files}")
    print(f"History records checked: {history_checked}")
    print(f"Latest records checked: {latest_checked}")
    print(f"Failed records: {total_failed_count}")

    if total_failed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

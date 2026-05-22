import argparse
import json
import os
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv


GOLD_PREFIX = "fraud-investigation/cases/"

REQUIRED_FIELDS = {
    "risk_event_id",
    "customer_id",
    "account_id",
    "card_id",
    "source_transaction_id",
    "risk_score",
    "risk_level",
    "recommended_action",
    "rule_flags",
    "explanation",
    "login_event_count",
    "failed_login_count",
    "new_device_count",
    "password_reset_count",
    "source_events",
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


def validate_record(record):
    errors = []

    missing_fields = REQUIRED_FIELDS - set(record)
    if missing_fields:
        errors.append(f"missing fields: {sorted(missing_fields)}")

    for field in REQUIRED_FIELDS:
        if record.get(field) in (None, ""):
            errors.append(f"field is null or empty: {field}")

    if not isinstance(record.get("risk_score"), int):
        errors.append(f"risk_score must be an integer: {record.get('risk_score')}")

    if record.get("risk_level") not in {"low", "medium", "medium_high", "high"}:
        errors.append(f"invalid risk_level: {record.get('risk_level')}")

    for field in ["login_event_count", "failed_login_count", "new_device_count", "password_reset_count"]:
        if not isinstance(record.get(field), int) or record[field] < 0:
            errors.append(f"invalid count field {field}: {record.get(field)}")

    if not isinstance(record.get("rule_flags"), list) or not record["rule_flags"]:
        errors.append("rule_flags must be a non-empty list")

    if not isinstance(record.get("source_events"), list) or not record["source_events"]:
        errors.append("source_events must be a non-empty list")

    return errors


def main():
    args = parse_args()
    gold_input_prefix = f"{GOLD_PREFIX}process_date={args.process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    object_keys = list(list_jsonl_objects(s3_client, gold_bucket, gold_input_prefix))

    if not object_keys:
        raise SystemExit(f"No Gold fraud investigation JSONL files found under {gold_input_prefix}")

    checked_count = 0
    failed_count = 0

    for object_key in object_keys:
        for line_number, record in read_jsonl_object(s3_client, gold_bucket, object_key):
            checked_count += 1
            errors = validate_record(record)
            if errors:
                failed_count += 1
                print(f"FAILED: s3://{gold_bucket}/{object_key} line {line_number}")
                for error in errors:
                    print(f"  - {error}")

    print("\nGold fraud investigation validation summary")
    print(f"Bucket: {gold_bucket}")
    print(f"Process date: {args.process_date}")
    print(f"Prefix: {gold_input_prefix}")
    print(f"Files checked: {len(object_keys)}")
    print(f"Records checked: {checked_count}")
    print(f"Failed records: {failed_count}")

    if failed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import argparse
import json
import os
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import boto3
from botocore.client import Config
from dotenv import load_dotenv


GOLD_PREFIX = "transaction-monitoring/dashboard/"

REQUIRED_FIELDS = {
    "authorization_status",
    "channel",
    "merchant_country",
    "merchant_category_code",
    "transaction_count",
    "total_amount",
    "average_amount",
    "approved_count",
    "declined_count",
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


def is_non_negative_decimal(value):
    try:
        return Decimal(str(value)) >= Decimal("0.00")
    except (InvalidOperation, TypeError, ValueError):
        return False


def validate_record(record):
    errors = []

    missing_fields = REQUIRED_FIELDS - set(record)
    if missing_fields:
        errors.append(f"missing fields: {sorted(missing_fields)}")

    for field in REQUIRED_FIELDS:
        if record.get(field) in (None, ""):
            errors.append(f"field is null or empty: {field}")

    if record.get("authorization_status") not in {"approved", "declined"}:
        errors.append(
            f"invalid authorization_status: {record.get('authorization_status')}"
        )

    transaction_count = record.get("transaction_count")
    approved_count = record.get("approved_count")
    declined_count = record.get("declined_count")

    if not isinstance(transaction_count, int) or transaction_count <= 0:
        errors.append(f"invalid transaction_count: {transaction_count}")

    if not isinstance(approved_count, int) or approved_count < 0:
        errors.append(f"invalid approved_count: {approved_count}")

    if not isinstance(declined_count, int) or declined_count < 0:
        errors.append(f"invalid declined_count: {declined_count}")

    if all(isinstance(value, int) for value in [transaction_count, approved_count, declined_count]):
        if approved_count + declined_count != transaction_count:
            errors.append(
                "approved_count + declined_count must equal transaction_count"
            )

    if not is_non_negative_decimal(record.get("total_amount")):
        errors.append(f"invalid total_amount: {record.get('total_amount')}")

    if not is_non_negative_decimal(record.get("average_amount")):
        errors.append(f"invalid average_amount: {record.get('average_amount')}")

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
        raise SystemExit(f"No Gold JSONL files found under {gold_input_prefix}")

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

    print("\nGold validation summary")
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

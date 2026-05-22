import argparse
import json
import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv


REQUIRED_BRONZE_FIELDS = {
    "bronze_ingest_time",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "raw_value",
    "parsed_value",
}

REQUIRED_EVENT_FIELDS = {
    "event_time",
    "customer_id",
    "source_system",
    "schema_version",
}

EVENT_ID_FIELDS = {"event_id", "risk_event_id"}


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


def validate_record(record):
    errors = []

    missing_bronze_fields = REQUIRED_BRONZE_FIELDS - set(record)
    if missing_bronze_fields:
        errors.append(f"missing Bronze fields: {sorted(missing_bronze_fields)}")

    parsed_value = record.get("parsed_value")
    if not isinstance(parsed_value, dict):
        errors.append("parsed_value must be a JSON object")
        return errors

    missing_event_fields = REQUIRED_EVENT_FIELDS - set(parsed_value)
    if missing_event_fields:
        errors.append(f"missing event fields: {sorted(missing_event_fields)}")

    for field in REQUIRED_EVENT_FIELDS:
        if parsed_value.get(field) in (None, ""):
            errors.append(f"event field is null or empty: {field}")

    if not any(parsed_value.get(field) not in (None, "") for field in EVENT_ID_FIELDS):
        errors.append(
            "missing event identifier: expected one of "
            f"{sorted(EVENT_ID_FIELDS)}"
        )

    if record.get("raw_value") in (None, ""):
        errors.append("raw_value is null or empty")

    if record.get("kafka_topic") in (None, ""):
        errors.append("kafka_topic is null or empty")

    if record.get("kafka_partition") is None:
        errors.append("kafka_partition is null")

    if record.get("kafka_offset") is None:
        errors.append("kafka_offset is null")

    return errors


def parse_args():
    parser = argparse.ArgumentParser(description="Validate Bronze objects in MinIO.")
    parser.add_argument(
        "--prefix",
        required=True,
        help="Bronze prefix to validate, such as transaction/card-authorizations/",
    )
    parser.add_argument(
        "--max-objects",
        type=int,
        default=100,
        help="Maximum number of objects to validate.",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")

    s3_client = build_minio_client(endpoint, access_key, secret_key)

    checked_count = 0
    failed_count = 0

    for object_key in list_objects(s3_client, bucket, args.prefix):
        if checked_count >= args.max_objects:
            break

        checked_count += 1

        try:
            record = read_json_object(s3_client, bucket, object_key)
            errors = validate_record(record)
        except Exception as exc:
            errors = [f"failed to read or parse object: {exc}"]

        if errors:
            failed_count += 1
            print(f"FAILED: s3://{bucket}/{object_key}")
            for error in errors:
                print(f"  - {error}")

    passed_count = checked_count - failed_count

    print("\nBronze validation summary")
    print(f"Bucket: {bucket}")
    print(f"Prefix: {args.prefix}")
    print(f"Checked: {checked_count}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {failed_count}")

    if checked_count == 0:
        raise SystemExit("No Bronze objects found for the provided prefix.")

    if failed_count > 0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

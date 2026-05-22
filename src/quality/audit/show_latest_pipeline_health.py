import argparse
import json
import os
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv


LATEST_GOLD_PREFIX = "platform-observability/pipeline-health-latest/"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Gold latest pipeline health process date, format YYYY-MM-DD",
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


def read_jsonl_object(s3_client, bucket, object_key):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read().decode("utf-8")
    for line in body.splitlines():
        if line.strip():
            yield json.loads(line)


def format_row(values, widths):
    return " | ".join(str(value).ljust(width) for value, width in zip(values, widths))


def main():
    args = parse_args()
    object_key = (
        f"{LATEST_GOLD_PREFIX}"
        f"process_date={args.process_date}/"
        "part-00000.jsonl"
    )

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)

    try:
        records = list(read_jsonl_object(s3_client, gold_bucket, object_key))
    except s3_client.exceptions.NoSuchKey as exc:
        raise SystemExit(
            f"No latest pipeline health file found at s3://{gold_bucket}/{object_key}"
        ) from exc

    if not records:
        raise SystemExit(f"Latest pipeline health file is empty: s3://{gold_bucket}/{object_key}")

    records = sorted(records, key=lambda record: record["job_name"])

    headers = [
        "job_name",
        "status",
        "read",
        "written",
        "rejected",
        "seconds",
        "finished_at",
    ]
    rows = [
        [
            record["job_name"],
            record["status"],
            record["records_read"],
            record["records_written"],
            record["records_rejected"],
            record["duration_seconds"],
            record["finished_at"],
        ]
        for record in records
    ]
    widths = [
        max(len(str(row[index])) for row in [headers, *rows])
        for index in range(len(headers))
    ]

    print("Latest pipeline health")
    print(f"Process date: {args.process_date}")
    print(f"Source: s3://{gold_bucket}/{object_key}")
    print("")
    print(format_row(headers, widths))
    print(format_row(["-" * width for width in widths], widths))
    for row in rows:
        print(format_row(row, widths))

    failed_records = [record for record in records if record["status"] != "success"]
    if failed_records:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import argparse
import json
import os
from datetime import UTC, datetime

import boto3
from botocore.client import Config
from dotenv import load_dotenv


AUDIT_PREFIX = "platform-audit/pipeline-runs"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Audit process date partition to inspect, format YYYY-MM-DD",
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


def list_audit_objects(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".json"):
                yield item["Key"]


def read_json_object(s3_client, bucket, object_key):
    response = s3_client.get_object(Bucket=bucket, Key=object_key)
    return json.loads(response["Body"].read().decode("utf-8"))


def main():
    args = parse_args()
    audit_input_prefix = f"{AUDIT_PREFIX}/process_date={args.process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    object_keys = sorted(list_audit_objects(s3_client, bronze_bucket, audit_input_prefix))

    if not object_keys:
        raise SystemExit(f"No pipeline audit records found under {audit_input_prefix}")

    audit_records = [
        read_json_object(s3_client, bronze_bucket, object_key)
        for object_key in object_keys
    ]

    failed_runs = [
        record for record in audit_records if record.get("status") != "success"
    ]

    print("Pipeline audit summary")
    print(f"Bucket: {bronze_bucket}")
    print(f"Process date: {args.process_date}")
    print(f"Prefix: {audit_input_prefix}")
    print(f"Audit records found: {len(audit_records)}")
    print("")
    print("job_name | status | records_read | records_written | records_rejected | finished_at")

    for record in sorted(audit_records, key=lambda item: item["finished_at"]):
        print(
            f"{record['job_name']} | "
            f"{record['status']} | "
            f"{record['records_read']} | "
            f"{record['records_written']} | "
            f"{record['records_rejected']} | "
            f"{record['finished_at']}"
        )

    if failed_runs:
        print("")
        print(f"Failed audit records: {len(failed_runs)}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()

import argparse
import json
import os

import boto3
from botocore.client import Config
from dotenv import load_dotenv


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--quarantine-prefix",
        required=True,
        help="Quarantine prefix to inspect, such as silver-login-events",
    )
    parser.add_argument(
        "--process-date",
        required=True,
        help="Quarantine process date partition to inspect, format YYYY-MM-DD",
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


def main():
    args = parse_args()
    input_prefix = f"{args.quarantine_prefix}/process_date={args.process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    quarantine_bucket = os.getenv("QUARANTINE_BUCKET", "banking-quarantine")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    object_keys = sorted(list_json_objects(s3_client, quarantine_bucket, input_prefix))

    print("Quarantine records")
    print(f"Bucket: {quarantine_bucket}")
    print(f"Prefix: {input_prefix}")
    print(f"Records found: {len(object_keys)}")

    for object_key in object_keys:
        record = read_json_object(s3_client, quarantine_bucket, object_key)
        print(
            f"{record['job_name']} | "
            f"{record['process_date']} | "
            f"{record['rejection_reason']} | "
            f"s3://{record['source_bucket']}/{record['source_object_key']}"
        )


if __name__ == "__main__":
    main()

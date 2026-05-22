import argparse
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from botocore.client import Config
from dotenv import load_dotenv


BRONZE_PREFIX = "digital-activity/login-events"
TOPIC = "banking.digital-activity.login-events.v1"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ingest-date",
        required=True,
        help="Bronze ingest date partition to write the bad test record, format YYYY-MM-DD",
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


def build_bad_bronze_record():
    parsed_value = {
        "event_id": f"evt_bad_login_{uuid4().hex[:8]}",
        "login_event_id": f"login_bad_{uuid4().hex[:8]}",
        # customer_id is intentionally missing to test quarantine handling.
        "event_time": datetime.now(UTC).isoformat(),
        "event_type": "login_success",
        "login_status": "success",
        "device_id": "device_bad_test",
        "device_type": "mobile",
        "ip_address": "198.51.100.10",
        "country": "US",
        "city": "Chicago",
        "channel": "mobile_app",
        "source_system": "synthetic-bad-test",
        "schema_version": "1.0.0",
    }

    return {
        "bronze_ingest_time": datetime.now(UTC).isoformat(),
        "kafka_topic": TOPIC,
        "kafka_partition": 999,
        "kafka_offset": -1,
        "kafka_key": "quarantine_test",
        "raw_value": json.dumps(parsed_value),
        "parsed_value": parsed_value,
    }


def main():
    args = parse_args()
    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    bronze_record = build_bad_bronze_record()
    object_key = (
        f"{BRONZE_PREFIX}/"
        f"ingest_date={args.ingest_date}/"
        f"bad-login-event-{uuid4().hex}.json"
    )

    s3_client.put_object(
        Bucket=bronze_bucket,
        Key=object_key,
        Body=json.dumps(bronze_record, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    print(f"Wrote bad Bronze test record: s3://{bronze_bucket}/{object_key}")


if __name__ == "__main__":
    main()

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[2]))
from common.pipeline_audit import write_pipeline_audit_event


SILVER_PREFIX = "transaction/card-authorizations/"
GOLD_PREFIX = "transaction-monitoring/dashboard/"
JOB_NAME = "gold_transaction_monitoring"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Silver process date partition to read and Gold process date to write, format YYYY-MM-DD",
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
    for line in body.splitlines():
        if line.strip():
            yield json.loads(line)


def build_group_key(record):
    return (
        record["authorization_status"],
        record["channel"],
        record["merchant_country"],
        record["merchant_category_code"],
    )


def aggregate_records(records):
    groups = defaultdict(
        lambda: {
            "transaction_count": 0,
            "total_amount": Decimal("0.00"),
            "approved_count": 0,
            "declined_count": 0,
        }
    )

    for record in records:
        key = build_group_key(record)
        amount = Decimal(str(record["amount"]))
        group = groups[key]

        group["transaction_count"] += 1
        group["total_amount"] += amount

        if record["authorization_status"] == "approved":
            group["approved_count"] += 1
        elif record["authorization_status"] == "declined":
            group["declined_count"] += 1

    processed_time = datetime.now(UTC).isoformat()
    output_records = []

    for key, metrics in groups.items():
        authorization_status, channel, merchant_country, merchant_category_code = key
        transaction_count = metrics["transaction_count"]
        total_amount = metrics["total_amount"]
        average_amount = total_amount / Decimal(transaction_count)

        output_records.append(
            {
                "authorization_status": authorization_status,
                "channel": channel,
                "merchant_country": merchant_country,
                "merchant_category_code": merchant_category_code,
                "transaction_count": transaction_count,
                "total_amount": str(total_amount.quantize(Decimal("0.01"))),
                "average_amount": str(average_amount.quantize(Decimal("0.01"))),
                "approved_count": metrics["approved_count"],
                "declined_count": metrics["declined_count"],
                "gold_processed_time": processed_time,
            }
        )

    return sorted(
        output_records,
        key=lambda item: (
            item["authorization_status"],
            item["channel"],
            item["merchant_country"],
            item["merchant_category_code"],
        ),
    )


def write_gold_records(s3_client, bucket, records, process_date):
    object_key = f"{GOLD_PREFIX}process_date={process_date}/part-00000.jsonl"
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
    silver_input_prefix = f"{SILVER_PREFIX}process_date={process_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "banking-silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)

    silver_records = []
    silver_files = list(list_jsonl_objects(s3_client, silver_bucket, silver_input_prefix))

    if not silver_files:
        audit_key = write_pipeline_audit_event(
            s3_client=s3_client,
            bucket=bronze_bucket,
            job_name=JOB_NAME,
            status="failed",
            process_date=process_date,
            input_prefix=f"s3://{silver_bucket}/{silver_input_prefix}",
            output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
            records_read=0,
            records_written=0,
            records_rejected=0,
            started_at=started_at,
            error_message=f"No Silver files found under {silver_input_prefix}",
        )
        print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")
        raise SystemExit(f"No Silver files found under {silver_input_prefix}")

    for object_key in silver_files:
        silver_records.extend(read_jsonl_object(s3_client, silver_bucket, object_key))

    gold_records = aggregate_records(silver_records)

    if not gold_records:
        audit_key = write_pipeline_audit_event(
            s3_client=s3_client,
            bucket=bronze_bucket,
            job_name=JOB_NAME,
            status="failed",
            process_date=process_date,
            input_prefix=f"s3://{silver_bucket}/{silver_input_prefix}",
            output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
            records_read=len(silver_records),
            records_written=0,
            records_rejected=0,
            started_at=started_at,
            error_message="No Gold records produced.",
        )
        print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")
        raise SystemExit("No Gold records produced.")

    gold_key = write_gold_records(s3_client, gold_bucket, gold_records, process_date)
    audit_key = write_pipeline_audit_event(
        s3_client=s3_client,
        bucket=bronze_bucket,
        job_name=JOB_NAME,
        status="success",
        process_date=process_date,
        input_prefix=f"s3://{silver_bucket}/{silver_input_prefix}",
        output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
        records_read=len(silver_records),
        records_written=len(gold_records),
        records_rejected=0,
        started_at=started_at,
    )

    print("Gold transaction monitoring summary")
    print(f"Process date: {process_date}")
    print(f"Silver input prefix: s3://{silver_bucket}/{silver_input_prefix}")
    print(f"Silver files read: {len(silver_files)}")
    print(f"Silver records read: {len(silver_records)}")
    print(f"Gold aggregate records: {len(gold_records)}")
    print(f"Wrote s3://{gold_bucket}/{gold_key}")
    print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")


if __name__ == "__main__":
    main()

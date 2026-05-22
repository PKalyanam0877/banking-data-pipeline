import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parents[2]))
from common.pipeline_audit import write_pipeline_audit_event


SILVER_CARD_PREFIX = "transaction/card-authorizations/"
SILVER_LOGIN_PREFIX = "digital-activity/login-events/"
BRONZE_RISK_PREFIX = "fraud-risk/risk-events/"
GOLD_PREFIX = "fraud-investigation/cases/"
JOB_NAME = "gold_fraud_investigation"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Business date partition to read and write, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--risk-ingest-date",
        default=None,
        help="Bronze risk event ingest date to read. Defaults to --process-date.",
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


def list_objects(s3_client, bucket, prefix, suffix=None):
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if suffix is None or key.endswith(suffix):
                yield key


def read_jsonl_objects(s3_client, bucket, prefix):
    for object_key in list_objects(s3_client, bucket, prefix, suffix=".jsonl"):
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        body = response["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if line.strip():
                yield json.loads(line)


def read_bronze_risk_events(s3_client, bucket, prefix):
    for object_key in list_objects(s3_client, bucket, prefix, suffix=".json"):
        response = s3_client.get_object(Bucket=bucket, Key=object_key)
        bronze_record = json.loads(response["Body"].read().decode("utf-8"))
        parsed_value = bronze_record.get("parsed_value")
        if isinstance(parsed_value, dict):
            yield parsed_value


def summarize_login_events(login_events):
    return {
        "login_event_count": len(login_events),
        "failed_login_count": sum(
            1 for event in login_events if event["event_type"] == "login_failed"
        ),
        "new_device_count": sum(
            1 for event in login_events if event["event_type"] == "new_device_registered"
        ),
        "password_reset_count": sum(
            1 for event in login_events if event["event_type"] == "password_reset"
        ),
    }


def build_investigation_record(risk_event, card_by_transaction_id, logins_by_customer_id):
    transaction_id = risk_event["source_transaction_id"]
    customer_id = risk_event["customer_id"]
    card_event = card_by_transaction_id.get(transaction_id, {})
    login_events = logins_by_customer_id.get(customer_id, [])
    login_summary = summarize_login_events(login_events)

    return {
        "risk_event_id": risk_event["risk_event_id"],
        "customer_id": customer_id,
        "account_id": risk_event["account_id"],
        "card_id": risk_event["card_id"],
        "source_transaction_id": transaction_id,
        "risk_score": risk_event["risk_score"],
        "risk_level": risk_event["risk_level"],
        "recommended_action": risk_event["recommended_action"],
        "rule_flags": risk_event["rule_flags"],
        "explanation": risk_event["explanation"],
        "transaction_amount": card_event.get("amount"),
        "merchant_name": card_event.get("merchant_name"),
        "merchant_country": card_event.get("merchant_country"),
        "merchant_category_code": card_event.get("merchant_category_code"),
        "authorization_status": card_event.get("authorization_status"),
        "login_event_count": login_summary["login_event_count"],
        "failed_login_count": login_summary["failed_login_count"],
        "new_device_count": login_summary["new_device_count"],
        "password_reset_count": login_summary["password_reset_count"],
        "source_events": risk_event.get("source_events", []),
        "gold_processed_time": datetime.now(UTC).isoformat(),
    }


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
    risk_ingest_date = args.risk_ingest_date or process_date
    silver_card_input_prefix = f"{SILVER_CARD_PREFIX}process_date={process_date}/"
    silver_login_input_prefix = f"{SILVER_LOGIN_PREFIX}process_date={process_date}/"
    bronze_risk_input_prefix = f"{BRONZE_RISK_PREFIX}ingest_date={risk_ingest_date}/"

    load_dotenv()

    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bronze_bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")
    silver_bucket = os.getenv("SILVER_BUCKET", "banking-silver")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)

    card_events = list(read_jsonl_objects(s3_client, silver_bucket, silver_card_input_prefix))
    login_events = list(read_jsonl_objects(s3_client, silver_bucket, silver_login_input_prefix))
    risk_events = list(read_bronze_risk_events(s3_client, bronze_bucket, bronze_risk_input_prefix))

    card_by_transaction_id = {
        event["transaction_id"]: event for event in card_events
    }

    logins_by_customer_id = {}
    for event in login_events:
        logins_by_customer_id.setdefault(event["customer_id"], []).append(event)

    investigation_records = [
        build_investigation_record(
            risk_event,
            card_by_transaction_id,
            logins_by_customer_id,
        )
        for risk_event in risk_events
    ]

    print("Gold fraud investigation input summary")
    print(f"Process date: {process_date}")
    print(f"Risk ingest date: {risk_ingest_date}")
    print(f"Silver card input prefix: s3://{silver_bucket}/{silver_card_input_prefix}")
    print(f"Silver login input prefix: s3://{silver_bucket}/{silver_login_input_prefix}")
    print(f"Bronze risk input prefix: s3://{bronze_bucket}/{bronze_risk_input_prefix}")
    print(f"Silver card records read: {len(card_events)}")
    print(f"Silver login records read: {len(login_events)}")
    print(f"Risk events read: {len(risk_events)}")

    if not investigation_records:
        audit_key = write_pipeline_audit_event(
            s3_client=s3_client,
            bucket=bronze_bucket,
            job_name=JOB_NAME,
            status="failed",
            process_date=process_date,
            input_prefix=(
                f"s3://{silver_bucket}/{silver_card_input_prefix};"
                f"s3://{silver_bucket}/{silver_login_input_prefix};"
                f"s3://{bronze_bucket}/{bronze_risk_input_prefix}"
            ),
            output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
            records_read=len(card_events) + len(login_events) + len(risk_events),
            records_written=0,
            records_rejected=0,
            started_at=started_at,
            error_message="No fraud investigation records produced.",
        )
        print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")
        raise SystemExit("No fraud investigation records produced.")

    gold_key = write_gold_records(s3_client, gold_bucket, investigation_records, process_date)
    audit_key = write_pipeline_audit_event(
        s3_client=s3_client,
        bucket=bronze_bucket,
        job_name=JOB_NAME,
        status="success",
        process_date=process_date,
        input_prefix=(
            f"s3://{silver_bucket}/{silver_card_input_prefix};"
            f"s3://{silver_bucket}/{silver_login_input_prefix};"
            f"s3://{bronze_bucket}/{bronze_risk_input_prefix}"
        ),
        output_prefix=f"s3://{gold_bucket}/{GOLD_PREFIX}process_date={process_date}/",
        records_read=len(card_events) + len(login_events) + len(risk_events),
        records_written=len(investigation_records),
        records_rejected=0,
        started_at=started_at,
    )

    print("Gold fraud investigation summary")
    print(f"Gold investigation records: {len(investigation_records)}")
    print(f"Wrote s3://{gold_bucket}/{gold_key}")
    print(f"Wrote audit event: s3://{bronze_bucket}/{audit_key}")


if __name__ == "__main__":
    main()

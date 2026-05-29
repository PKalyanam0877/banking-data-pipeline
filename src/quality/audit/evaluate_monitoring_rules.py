import argparse
import json
import os
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv


PIPELINE_HEALTH_PREFIX = "platform-observability/pipeline-health-latest/"
TRANSACTION_MONITORING_PREFIX = "transaction-monitoring/dashboard/"
FRAUD_CASES_PREFIX = "fraud-investigation/cases/"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--process-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Gold process date partition to evaluate, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Monitoring JSON output path. Defaults to data/monitoring/monitoring_report_<date>.json",
    )
    parser.add_argument(
        "--fail-on-critical",
        action="store_true",
        help="Return a non-zero exit code when a critical finding is detected.",
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


def read_jsonl_partition(s3_client, bucket, prefix, process_date):
    partition_prefix = f"{prefix}process_date={process_date}/"
    object_keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=partition_prefix):
        for item in page.get("Contents", []):
            if item["Key"].endswith(".jsonl"):
                object_keys.append(item["Key"])

    records = []
    for object_key in object_keys:
        try:
            response = s3_client.get_object(Bucket=bucket, Key=object_key)
        except ClientError:
            continue
        body = response["Body"].read().decode("utf-8")
        for line in body.splitlines():
            if line.strip():
                records.append(json.loads(line))

    return records, object_keys


def fraud_case_key(record):
    return (
        record.get("risk_event_id"),
        record.get("customer_id"),
        record.get("source_transaction_id"),
    )


def dedupe_fraud_records(records):
    unique_records = {}
    for record in sorted(
        records,
        key=lambda item: (
            int(item.get("risk_score") or 0),
            item.get("gold_processed_time") or "",
        ),
        reverse=True,
    ):
        unique_records.setdefault(fraud_case_key(record), record)
    return list(unique_records.values())


def percent(part, whole):
    if not whole:
        return Decimal("0")
    return (Decimal(part) / Decimal(whole) * Decimal("100")).quantize(
        Decimal("0.1")
    )


def summarize_health(records):
    status_counts = Counter(record.get("status", "unknown") for record in records)
    return {
        "jobs": len(records),
        "success": status_counts.get("success", 0),
        "failed": sum(
            count for status, count in status_counts.items() if status != "success"
        ),
        "read": sum(int(record.get("records_read") or 0) for record in records),
        "written": sum(int(record.get("records_written") or 0) for record in records),
        "rejected": sum(int(record.get("records_rejected") or 0) for record in records),
    }


def summarize_transactions(records):
    transactions = sum(int(record.get("transaction_count") or 0) for record in records)
    declined = sum(int(record.get("declined_count") or 0) for record in records)
    approved = sum(int(record.get("approved_count") or 0) for record in records)
    return {
        "groups": len(records),
        "transactions": transactions,
        "approved": approved,
        "declined": declined,
        "decline_rate": str(percent(declined, transactions)),
        "amount": str(
            sum(Decimal(str(record.get("total_amount") or "0")) for record in records)
        ),
    }


def summarize_fraud(records):
    unique_records = dedupe_fraud_records(records)
    unmatched_merchants = sum(
        1 for record in unique_records if record.get("merchant_name") in (None, "")
    )
    return {
        "cases": len(unique_records),
        "raw_cases": len(records),
        "duplicates": max(len(records) - len(unique_records), 0),
        "max_score": max(
            (int(record.get("risk_score") or 0) for record in unique_records),
            default=0,
        ),
        "unmatched_merchants": unmatched_merchants,
    }


def add_finding(findings, severity, rule_id, message, value=None, threshold=None):
    findings.append(
        {
            "severity": severity,
            "rule_id": rule_id,
            "message": message,
            "value": value,
            "threshold": threshold,
        }
    )


def evaluate_rules(health_records, transaction_records, fraud_records):
    health = summarize_health(health_records)
    transactions = summarize_transactions(transaction_records)
    fraud = summarize_fraud(fraud_records)
    findings = []

    if not health_records:
        add_finding(
            findings,
            "critical",
            "missing_pipeline_health",
            "No latest pipeline health records were found.",
        )
    if not transaction_records:
        add_finding(
            findings,
            "critical",
            "missing_transaction_monitoring",
            "No Gold transaction monitoring records were found.",
        )
    if not fraud_records:
        add_finding(findings, "warning", "missing_fraud_cases", "No Gold fraud case records were found.")

    for record in health_records:
        status = record.get("status", "unknown")
        if status != "success":
            add_finding(
                findings,
                "critical",
                "pipeline_job_not_successful",
                f"{record.get('job_name', 'unknown job')} finished with status {status}.",
                status,
                "success",
            )

    if health["rejected"] > 0:
        add_finding(
            findings,
            "warning",
            "rejected_records_present",
            f"{health['rejected']:,} records were rejected in latest pipeline health.",
            health["rejected"],
            0,
        )

    decline_rate = Decimal(transactions["decline_rate"])
    if decline_rate > Decimal("20.0"):
        add_finding(
            findings,
            "warning",
            "decline_rate_high",
            f"Decline rate is {decline_rate}% for this process date.",
            str(decline_rate),
            "20.0",
        )

    if fraud["max_score"] >= 90:
        add_finding(
            findings,
            "warning",
            "high_fraud_risk_score",
            f"Maximum fraud risk score is {fraud['max_score']}.",
            fraud["max_score"],
            90,
        )

    if fraud["duplicates"] > 0:
        add_finding(
            findings,
            "warning",
            "duplicate_fraud_cases",
            f"{fraud['duplicates']:,} duplicate fraud case rows were detected across repeated runs.",
            fraud["duplicates"],
            0,
        )

    if fraud["unmatched_merchants"] > 0:
        add_finding(
            findings,
            "warning",
            "unmatched_fraud_merchants",
            f"{fraud['unmatched_merchants']:,} fraud cases have no matched merchant.",
            fraud["unmatched_merchants"],
            0,
        )

    severity_order = {"healthy": 0, "warning": 1, "critical": 2}
    status = "healthy"
    for finding in findings:
        if severity_order[finding["severity"]] > severity_order[status]:
            status = finding["severity"]

    return {
        "status": status,
        "metrics": {
            "pipeline_health": health,
            "transactions": transactions,
            "fraud": fraud,
        },
        "findings": findings,
    }


def main():
    args = parse_args()
    process_date = args.process_date
    output_path = Path(
        args.output_path or f"data/monitoring/monitoring_report_{process_date}.json"
    )

    load_dotenv()
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    gold_bucket = os.getenv("GOLD_BUCKET", "banking-gold")

    s3_client = build_minio_client(endpoint, access_key, secret_key)
    try:
        health_records, health_keys = read_jsonl_partition(
            s3_client,
            gold_bucket,
            PIPELINE_HEALTH_PREFIX,
            process_date,
        )
        transaction_records, transaction_keys = read_jsonl_partition(
            s3_client,
            gold_bucket,
            TRANSACTION_MONITORING_PREFIX,
            process_date,
        )
        fraud_records, fraud_keys = read_jsonl_partition(
            s3_client,
            gold_bucket,
            FRAUD_CASES_PREFIX,
            process_date,
        )
    except EndpointConnectionError as exc:
        raise SystemExit(
            f"Could not connect to MinIO at {endpoint}. Start the platform with: docker compose up -d"
        ) from exc

    report = {
        "process_date": process_date,
        "generated_at": datetime.now(UTC).isoformat(),
        "sources": {
            "health": f"s3://{gold_bucket}/{health_keys[0]}" if health_keys else None,
            "transactions": f"s3://{gold_bucket}/{transaction_keys[0]}" if transaction_keys else None,
            "fraud": f"s3://{gold_bucket}/{fraud_keys[0]}" if fraud_keys else None,
        },
    }
    report.update(evaluate_rules(health_records, transaction_records, fraud_records))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    critical_count = sum(1 for finding in report["findings"] if finding["severity"] == "critical")
    warning_count = sum(1 for finding in report["findings"] if finding["severity"] == "warning")

    print("Monitoring rules evaluated")
    print(f"Process date: {process_date}")
    print(f"Status: {report['status']}")
    print(f"Critical findings: {critical_count}")
    print(f"Warning findings: {warning_count}")
    print(f"Output: {output_path.resolve()}")

    if args.fail_on_critical and critical_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

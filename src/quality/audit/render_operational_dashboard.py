import argparse
import html
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
        help="Gold process date partition to render, format YYYY-MM-DD",
    )
    parser.add_argument(
        "--output-path",
        default=None,
        help="Dashboard HTML output path. Defaults to data/dashboards/operational_dashboard_<date>.html",
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


def money(value):
    return f"${Decimal(str(value or '0')).quantize(Decimal('0.01')):,.2f}"


def number(value):
    return f"{int(value or 0):,}"


def text(value):
    return html.escape("" if value is None else str(value))


def summarize_health(records):
    status_counts = Counter(record.get("status", "unknown") for record in records)
    return {
        "jobs": len(records),
        "success": status_counts.get("success", 0),
        "failed": sum(count for status, count in status_counts.items() if status != "success"),
        "read": sum(int(record.get("records_read") or 0) for record in records),
        "written": sum(int(record.get("records_written") or 0) for record in records),
        "rejected": sum(int(record.get("records_rejected") or 0) for record in records),
    }


def summarize_transactions(records):
    return {
        "groups": len(records),
        "transactions": sum(int(record.get("transaction_count") or 0) for record in records),
        "amount": sum(Decimal(str(record.get("total_amount") or "0")) for record in records),
        "approved": sum(int(record.get("approved_count") or 0) for record in records),
        "declined": sum(int(record.get("declined_count") or 0) for record in records),
    }


def summarize_fraud(records):
    high_risk_levels = {"medium_high", "high"}
    return {
        "cases": len(records),
        "high_risk": sum(1 for record in records if record.get("risk_level") in high_risk_levels),
        "max_score": max((int(record.get("risk_score") or 0) for record in records), default=0),
        "failed_logins": sum(int(record.get("failed_login_count") or 0) for record in records),
    }


def status_class(status):
    return "ok" if status == "success" else "bad"


def render_kpi(label, value, note="", tone="neutral"):
    return (
        f'<section class="metric {tone}">'
        f"<span>{text(label)}</span>"
        f"<strong>{text(value)}</strong>"
        f"<small>{text(note)}</small>"
        "</section>"
    )


def render_health_table(records):
    if not records:
        return '<p class="empty">No pipeline health records found for this process date.</p>'

    rows = []
    for record in sorted(records, key=lambda item: item.get("job_name", "")):
        rows.append(
            "<tr>"
            f"<td>{text(record.get('job_name'))}</td>"
            f"<td><span class=\"pill {status_class(record.get('status'))}\">{text(record.get('status'))}</span></td>"
            f"<td class=\"num\">{number(record.get('records_read'))}</td>"
            f"<td class=\"num\">{number(record.get('records_written'))}</td>"
            f"<td class=\"num\">{number(record.get('records_rejected'))}</td>"
            f"<td class=\"num\">{text(record.get('duration_seconds'))}</td>"
            f"<td>{text(record.get('finished_at'))}</td>"
            "</tr>"
        )

    return (
        "<table><thead><tr>"
        "<th>Job</th><th>Status</th><th>Read</th><th>Written</th><th>Rejected</th><th>Seconds</th><th>Finished</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_transaction_table(records):
    if not records:
        return '<p class="empty">No transaction monitoring records found for this process date.</p>'

    top_records = sorted(
        records,
        key=lambda item: int(item.get("transaction_count") or 0),
        reverse=True,
    )[:10]
    rows = []
    for record in top_records:
        rows.append(
            "<tr>"
            f"<td>{text(record.get('authorization_status'))}</td>"
            f"<td>{text(record.get('channel'))}</td>"
            f"<td>{text(record.get('merchant_country'))}</td>"
            f"<td>{text(record.get('merchant_category_code'))}</td>"
            f"<td class=\"num\">{number(record.get('transaction_count'))}</td>"
            f"<td class=\"num\">{money(record.get('total_amount'))}</td>"
            f"<td class=\"num\">{money(record.get('average_amount'))}</td>"
            "</tr>"
        )

    return (
        "<table><thead><tr>"
        "<th>Status</th><th>Channel</th><th>Country</th><th>MCC</th><th>Count</th><th>Total</th><th>Average</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_fraud_table(records):
    if not records:
        return '<p class="empty">No fraud investigation records found for this process date.</p>'

    top_records = sorted(
        records,
        key=lambda item: int(item.get("risk_score") or 0),
        reverse=True,
    )[:10]
    rows = []
    for record in top_records:
        rows.append(
            "<tr>"
            f"<td>{text(record.get('risk_event_id'))}</td>"
            f"<td>{text(record.get('customer_id'))}</td>"
            f"<td class=\"num\">{number(record.get('risk_score'))}</td>"
            f"<td>{text(record.get('risk_level'))}</td>"
            f"<td>{text(record.get('recommended_action'))}</td>"
            f"<td class=\"num\">{number(record.get('failed_login_count'))}</td>"
            f"<td>{text(record.get('merchant_name'))}</td>"
            "</tr>"
        )

    return (
        "<table><thead><tr>"
        "<th>Risk Event</th><th>Customer</th><th>Score</th><th>Level</th><th>Action</th><th>Failed Logins</th><th>Merchant</th>"
        "</tr></thead><tbody>"
        + "".join(rows)
        + "</tbody></table>"
    )


def render_dashboard(process_date, health_records, transaction_records, fraud_records, sources):
    generated_at = datetime.now(UTC).isoformat()
    health = summarize_health(health_records)
    transactions = summarize_transactions(transaction_records)
    fraud = summarize_fraud(fraud_records)
    health_tone = "good" if health["failed"] == 0 and health["jobs"] > 0 else "warn"

    html_body = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Banking Operations Dashboard - {text(process_date)}</title>
  <style>
    :root {{
      --bg: #f5f7fa;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #5b6776;
      --line: #d8dee8;
      --good: #116b4f;
      --bad: #b42318;
      --warn: #9a6700;
      --accent: #22577a;
      --violet: #5b4b8a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: Arial, Helvetica, sans-serif;
      font-size: 14px;
      letter-spacing: 0;
    }}
    header {{
      background: #13293d;
      color: white;
      padding: 22px 28px;
      border-bottom: 4px solid #2a9d8f;
    }}
    h1 {{ margin: 0 0 6px; font-size: 26px; font-weight: 700; }}
    header p {{ margin: 0; color: #d8e6ef; }}
    main {{ padding: 24px 28px 32px; max-width: 1440px; margin: 0 auto; }}
    .metrics {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 12px;
      margin-bottom: 22px;
    }}
    .metric {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 5px solid var(--accent);
      border-radius: 6px;
      padding: 14px 16px;
      min-height: 96px;
    }}
    .metric.good {{ border-left-color: var(--good); }}
    .metric.warn {{ border-left-color: var(--warn); }}
    .metric.bad {{ border-left-color: var(--bad); }}
    .metric.violet {{ border-left-color: var(--violet); }}
    .metric span {{ display: block; color: var(--muted); font-size: 12px; text-transform: uppercase; }}
    .metric strong {{ display: block; margin-top: 8px; font-size: 28px; }}
    .metric small {{ display: block; margin-top: 6px; color: var(--muted); }}
    section.band {{
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 6px;
      margin-top: 16px;
      overflow: hidden;
    }}
    .band h2 {{
      margin: 0;
      padding: 14px 16px;
      font-size: 17px;
      border-bottom: 1px solid var(--line);
      background: #eef3f8;
    }}
    .table-wrap {{ overflow-x: auto; }}
    table {{ width: 100%; border-collapse: collapse; min-width: 860px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-size: 12px; text-transform: uppercase; background: #fafbfc; }}
    td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
    .pill {{ display: inline-block; min-width: 66px; border-radius: 999px; padding: 3px 9px; text-align: center; font-size: 12px; }}
    .pill.ok {{ color: white; background: var(--good); }}
    .pill.bad {{ color: white; background: var(--bad); }}
    .empty {{ margin: 0; padding: 16px; color: var(--muted); }}
    footer {{ margin-top: 18px; color: var(--muted); font-size: 12px; line-height: 1.5; }}
    code {{ background: #e8edf3; padding: 2px 5px; border-radius: 4px; }}
  </style>
</head>
<body>
  <header>
    <h1>Banking Operations Dashboard</h1>
    <p>Process date {text(process_date)} | Generated {text(generated_at)}</p>
  </header>
  <main>
    <div class="metrics">
      {render_kpi("Pipeline Jobs", health["jobs"], f"{health['success']} success / {health['failed']} failing", health_tone)}
      {render_kpi("Records Moved", number(health["written"]), f"{number(health['read'])} read", "good")}
      {render_kpi("Rejected Records", number(health["rejected"]), "latest job health", "bad" if health["rejected"] else "good")}
      {render_kpi("Transactions", number(transactions["transactions"]), f"{money(transactions['amount'])} total amount", "violet")}
      {render_kpi("Declines", number(transactions["declined"]), f"{number(transactions['approved'])} approved", "warn" if transactions["declined"] else "good")}
      {render_kpi("Fraud Cases", number(fraud["cases"]), f"{fraud['high_risk']} medium-high/high", "bad" if fraud["high_risk"] else "good")}
      {render_kpi("Max Risk Score", number(fraud["max_score"]), f"{number(fraud['failed_logins'])} failed logins", "bad" if fraud["max_score"] >= 90 else "warn")}
    </div>

    <section class="band">
      <h2>Pipeline Health</h2>
      <div class="table-wrap">{render_health_table(health_records)}</div>
    </section>

    <section class="band">
      <h2>Transaction Monitoring</h2>
      <div class="table-wrap">{render_transaction_table(transaction_records)}</div>
    </section>

    <section class="band">
      <h2>Fraud Investigation</h2>
      <div class="table-wrap">{render_fraud_table(fraud_records)}</div>
    </section>

    <footer>
      Sources:
      <code>{text(sources["health"])}</code>
      <code>{text(sources["transactions"])}</code>
      <code>{text(sources["fraud"])}</code>
    </footer>
  </main>
</body>
</html>
"""
    return html_body


def main():
    args = parse_args()
    process_date = args.process_date
    output_path = Path(args.output_path or f"data/dashboards/operational_dashboard_{process_date}.html")

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

    sources = {
        "health": f"s3://{gold_bucket}/{health_keys[0]}" if health_keys else "missing pipeline health",
        "transactions": (
            f"s3://{gold_bucket}/{transaction_keys[0]}"
            if transaction_keys
            else "missing transaction monitoring"
        ),
        "fraud": f"s3://{gold_bucket}/{fraud_keys[0]}" if fraud_keys else "missing fraud cases",
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        render_dashboard(process_date, health_records, transaction_records, fraud_records, sources),
        encoding="utf-8",
    )

    print("Operational dashboard rendered")
    print(f"Process date: {process_date}")
    print(f"Pipeline health records: {len(health_records)}")
    print(f"Transaction monitoring records: {len(transaction_records)}")
    print(f"Fraud investigation records: {len(fraud_records)}")
    print(f"Output: {output_path.resolve()}")


if __name__ == "__main__":
    main()

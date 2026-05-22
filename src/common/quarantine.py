import json
from datetime import UTC, datetime
from uuid import uuid4


def write_quarantine_record(
    s3_client,
    bucket,
    quarantine_prefix,
    job_name,
    process_date,
    source_bucket,
    source_object_key,
    rejection_reason,
    raw_record,
):
    failed_at = datetime.now(UTC).isoformat()
    object_id = uuid4().hex
    object_key = (
        f"{quarantine_prefix}/"
        f"process_date={process_date}/"
        f"{job_name}-{object_id}.json"
    )

    quarantine_record = {
        "job_name": job_name,
        "process_date": process_date,
        "failed_at": failed_at,
        "source_bucket": source_bucket,
        "source_object_key": source_object_key,
        "rejection_reason": rejection_reason,
        "raw_record": raw_record,
    }

    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(quarantine_record, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return object_key

import json
from datetime import UTC, datetime
from uuid import uuid4


AUDIT_PREFIX = "platform-audit/pipeline-runs"


def write_pipeline_audit_event(
    s3_client,
    bucket,
    job_name,
    status,
    process_date,
    input_prefix,
    output_prefix,
    records_read,
    records_written,
    records_rejected,
    started_at,
    finished_at=None,
    error_message=None,
):
    finished_at = finished_at or datetime.now(UTC).isoformat()
    run_id = f"{job_name}-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}-{uuid4().hex[:8]}"

    audit_record = {
        "run_id": run_id,
        "job_name": job_name,
        "status": status,
        "process_date": process_date,
        "input_prefix": input_prefix,
        "output_prefix": output_prefix,
        "records_read": records_read,
        "records_written": records_written,
        "records_rejected": records_rejected,
        "started_at": started_at,
        "finished_at": finished_at,
        "error_message": error_message,
    }

    object_key = (
        f"{AUDIT_PREFIX}/"
        f"process_date={process_date}/"
        f"{run_id}.json"
    )

    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=json.dumps(audit_record, indent=2).encode("utf-8"),
        ContentType="application/json",
    )

    return object_key

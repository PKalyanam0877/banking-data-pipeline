from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

PROCESS_DATE = '{{ dag_run.conf.get("process_date", ds) }}'
INGEST_DATE = '{{ dag_run.conf.get("ingest_date", dag_run.conf.get("process_date", ds)) }}'
RISK_INGEST_DATE = (
    '{{ dag_run.conf.get("risk_ingest_date", '
    'dag_run.conf.get("ingest_date", dag_run.conf.get("process_date", ds))) }}'
)
SKIP_PRODUCERS = '{{ dag_run.conf.get("skip_producers", false) | lower }}'


def python_task(task_id: str, command: str, retries: int = 0) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"python {command}",
        cwd=PROJECT_DIR,
        retries=retries,
        retry_delay=timedelta(seconds=15),
    )


def skip_for_replay_task(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f'if [ "{SKIP_PRODUCERS}" = "true" ]; then '
            f'echo "Skipping {task_id} for existing Bronze replay"; '
            f"else python {command}; fi"
        ),
        cwd=PROJECT_DIR,
    )


with DAG(
    dag_id="banking_fresh_partition_pipeline",
    description="Produce, land, process, validate, and observe a banking pipeline partition.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["banking", "phase-2", "medallion"],
) as dag:
    ensure_kafka_topics = python_task(
        "ensure_kafka_topics",
        "src/processing/bronze/ensure_kafka_topics.py",
        retries=3,
    )

    produce_card_authorizations = skip_for_replay_task(
        "produce_card_authorizations",
        "src/producers/card_transactions/card_authorization_producer.py --count 1500",
    )

    land_card_authorizations_bronze = skip_for_replay_task(
        "land_card_authorizations_bronze",
        (
            "src/processing/bronze/kafka_to_minio_bronze.py "
            "--topic banking.transaction.card-authorizations.v1 "
            "--bronze-prefix transaction/card-authorizations "
            "--consumer-group bronze-card-authorizations-airflow "
            "--max-messages 1500 "
            f"--ingest-date {INGEST_DATE}"
        ),
    )

    produce_login_events = skip_for_replay_task(
        "produce_login_events",
        "src/producers/digital_activity/login_event_producer.py",
    )

    land_login_events_bronze = skip_for_replay_task(
        "land_login_events_bronze",
        (
            "src/processing/bronze/kafka_to_minio_bronze.py "
            "--topic banking.digital-activity.login-events.v1 "
            "--bronze-prefix digital-activity/login-events "
            "--consumer-group bronze-login-events-airflow "
            "--max-messages 10 "
            f"--ingest-date {INGEST_DATE}"
        ),
    )

    produce_risk_events = skip_for_replay_task(
        "produce_risk_events",
        "src/producers/fraud_risk/risk_event_producer.py",
    )

    land_risk_events_bronze = skip_for_replay_task(
        "land_risk_events_bronze",
        (
            "src/processing/bronze/kafka_to_minio_bronze.py "
            "--topic banking.fraud-risk.risk-events.v1 "
            "--bronze-prefix fraud-risk/risk-events "
            "--consumer-group bronze-risk-events-airflow "
            "--max-messages 3 "
            f"--ingest-date {RISK_INGEST_DATE}"
        ),
    )

    run_silver_card_authorizations = python_task(
        "run_silver_card_authorizations",
        f"src/processing/silver/card_authorizations_bronze_to_silver.py --process-date {PROCESS_DATE}",
    )

    validate_silver_card_authorizations = python_task(
        "validate_silver_card_authorizations",
        f"src/quality/silver/validate_silver_card_authorizations.py --process-date {PROCESS_DATE}",
    )

    run_silver_login_events = python_task(
        "run_silver_login_events",
        (
            "src/processing/silver/login_events_bronze_to_silver.py "
            f"--process-date {PROCESS_DATE} "
            f"--bronze-ingest-date {INGEST_DATE}"
        ),
    )

    validate_silver_login_events = python_task(
        "validate_silver_login_events",
        f"src/quality/silver/validate_silver_login_events.py --process-date {PROCESS_DATE}",
    )

    run_gold_transaction_monitoring = python_task(
        "run_gold_transaction_monitoring",
        f"src/processing/gold/transaction_monitoring_gold.py --process-date {PROCESS_DATE}",
    )

    validate_gold_transaction_monitoring = python_task(
        "validate_gold_transaction_monitoring",
        f"src/quality/gold/validate_gold_transaction_monitoring.py --process-date {PROCESS_DATE}",
    )

    run_gold_fraud_investigation = python_task(
        "run_gold_fraud_investigation",
        (
            "src/processing/gold/fraud_investigation_gold.py "
            f"--process-date {PROCESS_DATE} "
            f"--risk-ingest-date {RISK_INGEST_DATE}"
        ),
    )

    validate_gold_fraud_investigation = python_task(
        "validate_gold_fraud_investigation",
        f"src/quality/gold/validate_gold_fraud_investigation.py --process-date {PROCESS_DATE}",
    )

    run_gold_pipeline_health = python_task(
        "run_gold_pipeline_health",
        f"src/processing/gold/pipeline_health_gold.py --process-date {PROCESS_DATE}",
    )

    show_latest_pipeline_health = python_task(
        "show_latest_pipeline_health",
        f"src/quality/audit/show_latest_pipeline_health.py --process-date {PROCESS_DATE}",
    )

    (
        ensure_kafka_topics
        >> produce_card_authorizations
        >> land_card_authorizations_bronze
        >> run_silver_card_authorizations
        >> validate_silver_card_authorizations
        >> run_gold_transaction_monitoring
        >> validate_gold_transaction_monitoring
    )

    (
        ensure_kafka_topics
        >> produce_login_events
        >> land_login_events_bronze
        >> run_silver_login_events
        >> validate_silver_login_events
    )

    ensure_kafka_topics >> produce_risk_events >> land_risk_events_bronze

    [
        validate_silver_card_authorizations,
        validate_silver_login_events,
        land_risk_events_bronze,
    ] >> run_gold_fraud_investigation >> validate_gold_fraud_investigation

    [
        validate_gold_transaction_monitoring,
        validate_gold_fraud_investigation,
    ] >> run_gold_pipeline_health >> show_latest_pipeline_health

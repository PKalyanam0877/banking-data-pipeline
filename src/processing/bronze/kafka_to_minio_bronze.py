import argparse
import json
import os
from datetime import UTC, datetime
from uuid import uuid4

import boto3
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv


def build_consumer(bootstrap_servers, topic, consumer_group):
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def build_minio_client(endpoint, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def build_bronze_object_key(bronze_prefix, ingest_date, topic, partition, offset):
    object_id = uuid4().hex
    return (
        f"{bronze_prefix}/"
        f"ingest_date={ingest_date}/"
        f"{topic}-partition={partition}-offset={offset}-{object_id}.json"
    )


def build_bronze_record(message):
    raw_value = message.value().decode("utf-8")

    try:
        parsed_value = json.loads(raw_value)
    except json.JSONDecodeError:
        parsed_value = None

    return {
        "bronze_ingest_time": datetime.now(UTC).isoformat(),
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
        "kafka_key": message.key().decode("utf-8") if message.key() else None,
        "raw_value": raw_value,
        "parsed_value": parsed_value,
    }


def write_record_to_minio(s3_client, bucket, object_key, record):
    body = json.dumps(record, indent=2).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=body,
        ContentType="application/json",
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Land raw Kafka messages into a MinIO Bronze bucket."
    )
    parser.add_argument("--topic", required=True, help="Kafka topic to consume.")
    parser.add_argument(
        "--bronze-prefix",
        required=True,
        help="Bronze object prefix, such as transaction/card-authorizations.",
    )
    parser.add_argument(
        "--consumer-group",
        required=True,
        help="Kafka consumer group for this Bronze writer.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum messages to write before exiting.",
    )
    parser.add_argument(
        "--ingest-date",
        default=datetime.now(UTC).date().isoformat(),
        help="Bronze ingest date partition to write, format YYYY-MM-DD.",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.getenv("BRONZE_BUCKET", "banking-bronze")
    max_messages = args.max_messages or int(os.getenv("BRONZE_MAX_MESSAGES", "10"))

    consumer = build_consumer(bootstrap_servers, args.topic, args.consumer_group)
    s3_client = build_minio_client(endpoint, access_key, secret_key)

    written_count = 0

    try:
        while written_count < max_messages:
            message = consumer.poll(timeout=10.0)

            if message is None:
                print("No more Kafka messages available before timeout.")
                break

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())

            record = build_bronze_record(message)
            object_key = build_bronze_object_key(
                args.bronze_prefix,
                args.ingest_date,
                message.topic(),
                message.partition(),
                message.offset(),
            )
            write_record_to_minio(s3_client, bucket, object_key, record)

            written_count += 1
            if written_count <= 10 or written_count % 100 == 0:
                print(f"Wrote {written_count}: s3://{bucket}/{object_key}")

        consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    print(f"Ingest date: {args.ingest_date}")
    print(f"Wrote {written_count} Bronze records to bucket {bucket}")


if __name__ == "__main__":
    main()

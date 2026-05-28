import os
import socket
import time

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv


TOPICS = [
    ("banking.debezium.connect-configs.v1", 1),
    ("banking.debezium.connect-offsets.v1", 1),
    ("banking.debezium.connect-status.v1", 1),
    ("banking.cdc.core-banking.public.customers", 3),
    ("banking.cdc.core-banking.public.accounts", 3),
    ("banking.cdc.core-banking.public.account_balances", 3),
    ("banking.cdc.core-banking.public.customer_risk_profiles", 3),
    ("banking.transaction.card-authorizations.v1", 6),
    ("banking.transaction.card-declines.v1", 3),
    ("banking.payment.ach-events.v1", 6),
    ("banking.digital-activity.login-events.v1", 6),
    ("banking.digital-activity.device-events.v1", 3),
    ("banking.fraud-risk.risk-events.v1", 6),
    ("banking.platform-observability.pipeline-events.v1", 3),
]


def parse_bootstrap_hosts(bootstrap_servers: str) -> list[tuple[str, int]]:
    hosts = []
    for server in bootstrap_servers.split(","):
        server = server.strip()
        if not server:
            continue
        host, _, port = server.rpartition(":")
        hosts.append((host or server, int(port or "9092")))
    return hosts


def wait_for_dns(bootstrap_servers: str, attempts: int = 12, delay_seconds: int = 5) -> None:
    hosts = parse_bootstrap_hosts(bootstrap_servers)
    for attempt in range(1, attempts + 1):
        unresolved = []
        for host, port in hosts:
            try:
                socket.getaddrinfo(host, port)
            except socket.gaierror:
                unresolved.append(f"{host}:{port}")

        if not unresolved:
            return

        print(
            "Waiting for Kafka DNS resolution "
            f"({attempt}/{attempts}): {', '.join(unresolved)}"
        )
        time.sleep(delay_seconds)

    raise RuntimeError(f"Unable to resolve Kafka bootstrap servers: {bootstrap_servers}")


def list_topics_with_retry(admin_client: AdminClient, attempts: int = 6, delay_seconds: int = 10):
    for attempt in range(1, attempts + 1):
        try:
            return admin_client.list_topics(timeout=20)
        except KafkaException as exc:
            if attempt == attempts:
                raise
            print(f"Kafka metadata unavailable ({attempt}/{attempts}): {exc}")
            time.sleep(delay_seconds)


def main():
    load_dotenv()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"Ensuring Kafka topics on {bootstrap_servers}")
    wait_for_dns(bootstrap_servers)

    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    existing_topics = set(list_topics_with_retry(admin_client).topics)
    topics_to_create = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name, partitions in TOPICS
        if name not in existing_topics
    ]

    if not topics_to_create:
        print("All Kafka topics already exist.")
        return

    futures = admin_client.create_topics(topics_to_create)
    for topic_name, future in futures.items():
        try:
            future.result()
            print(f"Created Kafka topic: {topic_name}")
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                print(f"Kafka topic already exists: {topic_name}")
                continue
            raise


if __name__ == "__main__":
    main()

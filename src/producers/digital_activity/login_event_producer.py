import json
import os
from datetime import UTC, datetime

from confluent_kafka import Producer
from dotenv import load_dotenv


DELIVERY_ERRORS = []


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
        DELIVERY_ERRORS.append(str(error))
        return

    print(
        "Delivered event to "
        f"{message.topic()} [partition {message.partition()}] "
        f"offset {message.offset()}"
    )


def build_login_event(
    sequence_number,
    customer_id,
    event_type,
    login_status,
    failure_reason,
    device_id,
    device_type,
    ip_address,
    country,
    city,
    channel,
    user_agent,
):
    event_time = datetime.now(UTC).isoformat()
    sequence_text = f"{sequence_number:06d}"

    return {
        "event_id": f"evt_login_{sequence_text}",
        "login_event_id": f"login_{sequence_text}",
        "customer_id": customer_id,
        "event_time": event_time,
        "event_type": event_type,
        "login_status": login_status,
        "failure_reason": failure_reason,
        "device_id": device_id,
        "device_type": device_type,
        "ip_address": ip_address,
        "country": country,
        "city": city,
        "channel": channel,
        "user_agent": user_agent,
        "source_system": "synthetic-digital-banking-platform",
        "schema_version": "1.0.0",
    }


def build_login_events():
    return [
        build_login_event(1, "cust_100001", "login_success", "success", None, "dev_100001", "ios", "73.22.14.101", "US", "Chicago", "mobile", "BankingApp/5.2 iOS"),
        build_login_event(2, "cust_100002", "login_success", "success", None, "dev_100002", "android", "98.45.20.18", "US", "Chicago", "mobile", "BankingApp/5.2 Android"),
        build_login_event(3, "cust_100003", "login_failed", "failed", "invalid_password", "dev_unknown_001", "web", "185.220.101.42", "NL", "Amsterdam", "web", "Mozilla/5.0"),
        build_login_event(4, "cust_100003", "login_failed", "failed", "invalid_password", "dev_unknown_001", "web", "185.220.101.42", "NL", "Amsterdam", "web", "Mozilla/5.0"),
        build_login_event(5, "cust_100003", "login_failed", "failed", "invalid_password", "dev_unknown_001", "web", "185.220.101.42", "NL", "Amsterdam", "web", "Mozilla/5.0"),
        build_login_event(6, "cust_100003", "password_reset", "success", None, "dev_unknown_001", "web", "185.220.101.42", "NL", "Amsterdam", "web", "Mozilla/5.0"),
        build_login_event(7, "cust_100003", "new_device_registered", "success", None, "dev_900003", "web", "185.220.101.42", "NL", "Amsterdam", "web", "Mozilla/5.0"),
        build_login_event(8, "cust_100005", "login_success", "success", None, "dev_900005", "android", "45.33.12.88", "US", "Miami", "mobile", "BankingApp/5.2 Android"),
        build_login_event(9, "cust_100004", "login_success", "success", None, "dev_100004", "ios", "104.21.10.7", "US", "Evanston", "mobile", "BankingApp/5.2 iOS"),
        build_login_event(10, "cust_100005", "new_device_registered", "success", None, "dev_900006", "web", "45.33.12.88", "US", "Miami", "web", "Mozilla/5.0"),
    ]


def main():
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("LOGIN_EVENTS_TOPIC", "banking.digital-activity.login-events.v1")

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "login-event-producer",
            "acks": "all",
            "message.timeout.ms": 10000,
        }
    )

    events = build_login_events()
    for event in events:
        producer.produce(
            topic=topic,
            key=event["customer_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

    remaining_count = producer.flush()

    if remaining_count > 0:
        raise RuntimeError(f"{remaining_count} login events were not delivered")

    if DELIVERY_ERRORS:
        sample_errors = "; ".join(DELIVERY_ERRORS[:5])
        raise RuntimeError(
            f"{len(DELIVERY_ERRORS)} login events failed delivery: {sample_errors}"
        )

    print(f"Produced {len(events)} login events")


if __name__ == "__main__":
    main()

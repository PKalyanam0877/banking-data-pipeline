import argparse
import json
import os
from datetime import UTC, datetime

from confluent_kafka import Producer
from dotenv import load_dotenv


SCENARIOS = [
    ("cust_100001", "acct_200001", "card_300001", "42.50", "mrc_100001", "Lakeview Grocery", "5411", "US", "Chicago", "approved", "card_present", True, "chip"),
    ("cust_100002", "acct_200003", "card_300002", "18.25", "mrc_100002", "Loop Coffee", "5814", "US", "Chicago", "approved", "card_present", True, "contactless"),
    ("cust_100004", "acct_200005", "card_300004", "126.90", "mrc_100003", "North Shore Fuel", "5541", "US", "Evanston", "approved", "card_present", True, "swipe"),
    ("cust_100001", "acct_200001", "card_300001", "76.13", "mrc_100004", "Chicago Pharmacy", "5912", "US", "Chicago", "approved", "card_present", True, "chip"),
    ("cust_100005", "acct_200006", "card_300005", "9.99", "mrc_100005", "StreamBox Digital", "4899", "US", "Seattle", "approved", "ecommerce", False, "card_on_file"),
    ("cust_100002", "acct_200003", "card_300002", "215.40", "mrc_100006", "Midwest Electronics", "5732", "US", "Chicago", "approved", "card_present", True, "chip"),
    ("cust_100004", "acct_200005", "card_300004", "64.20", "mrc_100007", "Union Station Books", "5942", "US", "Chicago", "approved", "card_present", True, "contactless"),
    ("cust_100003", "acct_200004", "card_300003", "2450.00", "mrc_900001", "Global Luxury Watches", "5944", "AE", "Dubai", "approved", "ecommerce", False, "manual_keyed"),
    ("cust_100005", "acct_200006", "card_300005", "1800.00", "mrc_900002", "Quick Crypto Exchange", "6051", "US", "Miami", "approved", "ecommerce", False, "manual_keyed"),
    ("cust_100003", "acct_200004", "card_300003", "799.99", "mrc_100008", "Online Electronics Outlet", "5732", "US", "New York", "declined", "ecommerce", False, "manual_keyed"),
]

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


def build_card_authorization_event(
    sequence_number,
    run_id,
    customer_id,
    account_id,
    card_id,
    amount,
    merchant_id,
    merchant_name,
    merchant_category_code,
    merchant_country,
    merchant_city,
    authorization_status,
    channel,
    card_present,
    entry_mode,
):
    event_time = datetime.now(UTC).isoformat()
    sequence_text = f"{sequence_number:06d}"
    event_suffix = f"{run_id}_{sequence_text}"

    return {
        "event_id": f"evt_card_auth_{event_suffix}",
        "transaction_id": f"txn_{event_suffix}",
        "authorization_id": f"auth_{event_suffix}",
        "customer_id": customer_id,
        "account_id": account_id,
        "card_id": card_id,
        "event_time": event_time,
        "amount": amount,
        "currency": "USD",
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "merchant_category_code": merchant_category_code,
        "merchant_country": merchant_country,
        "merchant_city": merchant_city,
        "authorization_status": authorization_status,
        "channel": channel,
        "card_present": card_present,
        "entry_mode": entry_mode,
        "source_system": "synthetic-card-processor",
        "schema_version": "1.0.0",
    }


def build_card_authorization_events(count):
    run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    events = []

    for index in range(count):
        scenario = SCENARIOS[index % len(SCENARIOS)]
        events.append(build_card_authorization_event(index + 1, run_id, *scenario))

    return events


def parse_args():
    parser = argparse.ArgumentParser(description="Produce synthetic card authorization events.")
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of card authorization events to produce.",
    )
    return parser.parse_args()


def main():
    load_dotenv()
    args = parse_args()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv(
        "CARD_AUTH_TOPIC",
        "banking.transaction.card-authorizations.v1",
    )

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "card-authorization-producer",
            "acks": "all",
            "message.timeout.ms": 10000,
        }
    )

    events = build_card_authorization_events(args.count)
    for event in events:
        producer.produce(
            topic=topic,
            key=event["card_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

    remaining_count = producer.flush()

    if remaining_count > 0:
        raise RuntimeError(f"{remaining_count} card authorization events were not delivered")

    if DELIVERY_ERRORS:
        sample_errors = "; ".join(DELIVERY_ERRORS[:5])
        raise RuntimeError(
            f"{len(DELIVERY_ERRORS)} card authorization events failed delivery: "
            f"{sample_errors}"
        )

    print(f"Produced {len(events)} card authorization events")


if __name__ == "__main__":
    main()

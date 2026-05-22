import json
import os
from datetime import UTC, datetime

from confluent_kafka import Producer
from dotenv import load_dotenv


def delivery_report(error, message):
    if error is not None:
        print(f"Delivery failed: {error}")
        return

    print(
        "Delivered event to "
        f"{message.topic()} [partition {message.partition()}] "
        f"offset {message.offset()}"
    )


def build_risk_event(
    risk_event_id,
    source_transaction_id,
    customer_id,
    account_id,
    card_id,
    risk_score,
    risk_level,
    recommended_action,
    rule_flags,
    explanation,
    source_events,
):
    return {
        "risk_event_id": risk_event_id,
        "event_time": datetime.now(UTC).isoformat(),
        "source_transaction_id": source_transaction_id,
        "customer_id": customer_id,
        "account_id": account_id,
        "card_id": card_id,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "recommended_action": recommended_action,
        "rule_flags": rule_flags,
        "explanation": explanation,
        "source_events": source_events,
        "source_system": "synthetic-fraud-risk-engine",
        "schema_version": "1.0.0",
    }


def build_risk_events():
    return [
        build_risk_event(
            risk_event_id="risk_000001",
            source_transaction_id="txn_000008",
            customer_id="cust_100003",
            account_id="acct_200004",
            card_id="card_300003",
            risk_score=98,
            risk_level="high",
            recommended_action="block_and_review",
            rule_flags=[
                "multiple_failed_logins",
                "password_reset",
                "new_device_registered",
                "restricted_account",
                "high_value_transaction",
                "cross_border",
                "manual_keyed",
            ],
            explanation="Account takeover indicators preceded a high-value cross-border ecommerce transaction.",
            source_events=["evt_login_000003", "evt_login_000006", "evt_login_000007", "evt_card_auth_000008"],
        ),
        build_risk_event(
            risk_event_id="risk_000002",
            source_transaction_id="txn_000009",
            customer_id="cust_100005",
            account_id="acct_200006",
            card_id="card_300005",
            risk_score=82,
            risk_level="medium_high",
            recommended_action="step_up_authentication",
            rule_flags=[
                "new_account",
                "high_value_transaction",
                "high_risk_merchant_category",
                "card_not_present",
                "manual_keyed",
                "new_device_registered",
            ],
            explanation="New customer account attempted a high-value crypto/quasi-cash ecommerce transaction.",
            source_events=["evt_login_000010", "evt_card_auth_000009"],
        ),
        build_risk_event(
            risk_event_id="risk_000003",
            source_transaction_id="txn_000008",
            customer_id="cust_100003",
            account_id="acct_200004",
            card_id="card_300003",
            risk_score=95,
            risk_level="high",
            recommended_action="block_and_review",
            rule_flags=[
                "restricted_customer",
                "restricted_account",
                "high_kyc_risk_rating",
                "fraud_watchlist",
                "high_value_transaction",
                "cross_border",
                "luxury_merchant",
            ],
            explanation="Restricted high-risk customer attempted a high-value cross-border luxury merchant purchase.",
            source_events=["evt_card_auth_000008"],
        ),
    ]


def main():
    load_dotenv()

    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("RISK_EVENTS_TOPIC", "banking.fraud-risk.risk-events.v1")

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "risk-event-producer",
            "acks": "all",
            "message.timeout.ms": 10000,
        }
    )

    events = build_risk_events()
    for event in events:
        producer.produce(
            topic=topic,
            key=event["customer_id"],
            value=json.dumps(event),
            callback=delivery_report,
        )

    producer.flush()

    print(f"Produced {len(events)} fraud risk events")


if __name__ == "__main__":
    main()

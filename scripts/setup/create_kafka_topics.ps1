$ErrorActionPreference = "Stop"

$topics = @(
    @{ Name = "banking.debezium.connect-configs.v1"; Partitions = 1; ReplicationFactor = 1 },
    @{ Name = "banking.debezium.connect-offsets.v1"; Partitions = 1; ReplicationFactor = 1 },
    @{ Name = "banking.debezium.connect-status.v1"; Partitions = 1; ReplicationFactor = 1 },
    @{ Name = "banking.cdc.core-banking.public.customers"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.cdc.core-banking.public.accounts"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.cdc.core-banking.public.account_balances"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.cdc.core-banking.public.customer_risk_profiles"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.transaction.card-authorizations.v1"; Partitions = 6; ReplicationFactor = 1 },
    @{ Name = "banking.transaction.card-declines.v1"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.payment.ach-events.v1"; Partitions = 6; ReplicationFactor = 1 },
    @{ Name = "banking.digital-activity.login-events.v1"; Partitions = 6; ReplicationFactor = 1 },
    @{ Name = "banking.digital-activity.device-events.v1"; Partitions = 3; ReplicationFactor = 1 },
    @{ Name = "banking.fraud-risk.risk-events.v1"; Partitions = 6; ReplicationFactor = 1 },
    @{ Name = "banking.platform-observability.pipeline-events.v1"; Partitions = 3; ReplicationFactor = 1 }
)

foreach ($topic in $topics) {
    docker exec banking_kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server kafka:9092 `
        --create `
        --if-not-exists `
        --topic $topic.Name `
        --partitions $topic.Partitions `
        --replication-factor $topic.ReplicationFactor
}

docker exec banking_kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:9092 `
    --list

$ErrorActionPreference = "Stop"

Write-Host "`n== Docker Compose Services =="
docker compose ps

Write-Host "`n== Kafka Topics =="
docker exec banking_kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:9092 `
    --list

Write-Host "`n== Debezium Connector Status =="
curl.exe http://localhost:8083/connectors/postgres-core-banking-connector/status

Write-Host "`n`n== PostgreSQL Core Banking Table Counts =="
docker exec banking_postgres psql -U banking_user -d banking -c "
SELECT 'customers' AS table_name, COUNT(*) FROM customers
UNION ALL SELECT 'accounts', COUNT(*) FROM accounts
UNION ALL SELECT 'account_balances', COUNT(*) FROM account_balances
UNION ALL SELECT 'customer_risk_profiles', COUNT(*) FROM customer_risk_profiles;
"

# Architecture Decisions

## ADR-001: Use Hybrid Streaming, CDC, and Batch

Decision: Use Kafka for real-time events, Debezium CDC for PostgreSQL source-of-truth changes, and batch files for settlement/reconciliation style workloads.

Reason: Banking systems need both low-latency fraud detection and correctness-oriented batch reconciliation.

## ADR-002: Use Medallion Architecture

Decision: Organize data into Bronze, Silver, and Gold layers.

Reason: This separates raw auditability, technical quality, and business-ready consumption.

## ADR-003: Store Synthetic Raw PII In Bronze

Decision: Bronze will contain fake raw PII, while Silver applies masking and tokenization.

Reason: This teaches realistic privacy controls while keeping the project safe because all data is synthetic.

## ADR-004: Use Dotted Kafka Namespaces With Hyphenated Words

Decision: Kafka topics use dotted namespace segments and hyphens inside multi-word segments.

Example: `banking.transaction.card-authorizations.v1`

Reason: Dots make ownership and domain boundaries readable, while avoiding underscores prevents Kafka/JMX metric-name collision risks when topic metrics are exported.

Exception: Debezium CDC topics preserve source table names exactly, such as `banking.cdc.core-banking.public.account_balances`. This keeps CDC topics source-aligned with PostgreSQL and avoids hiding upstream schema names behind custom aliases.

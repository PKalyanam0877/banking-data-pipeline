# Bronze Validation

Bronze validation checks raw landed objects before they are promoted into Silver.

The first validation script reads JSON objects from MinIO and verifies:

- Required Bronze metadata fields exist
- `raw_value` is present
- `parsed_value` is valid JSON
- Required event fields exist
- Required event fields are not null or empty

Required Bronze metadata fields:

- `bronze_ingest_time`
- `kafka_topic`
- `kafka_partition`
- `kafka_offset`
- `kafka_key`
- `raw_value`
- `parsed_value`

Required event fields:

- `event_time`
- `customer_id`
- `source_system`
- `schema_version`

Event identifier requirement:

- Source events use `event_id`
- Fraud risk events use `risk_event_id`
- At least one of those identifiers must be present

## Commands

Write Bronze card authorization objects for a controlled ingest date:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_card_authorization_writer.ps1 -IngestDate 2026-05-21
```

Write Bronze login event objects for a controlled ingest date:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_login_events_writer.ps1 -IngestDate 2026-05-21
```

Write Bronze risk event objects for a controlled ingest date:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/run_bronze_risk_events_writer.ps1 -IngestDate 2026-05-21
```

Validate card authorization Bronze objects:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_bronze_card_authorizations.ps1
```

Validate login event Bronze objects:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_bronze_login_events.ps1
```

Validate risk event Bronze objects:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/setup/validate_bronze_risk_events.ps1
```

## Production Thinking

Bronze validation should not silently drop bad records.

In later phases, invalid records should be written to a quarantine path with failure reasons so engineers can debug data issues and auditors can understand what happened.

Bronze writers should use an explicit ingest date during controlled testing or replay. This avoids timezone surprises where new records land under the next UTC date while the business date is still the prior local date.

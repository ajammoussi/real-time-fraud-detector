# Ingestion

```
Binance WebSocket ──► ingestion/producer.py ──► Kafka: transactions_raw
                                                   │
                        ┌──────────────────────────┴─────────────────────────┐
                        ▼                                                    ▼
        ingestion/lake_consumer.py                            ingestion/predict_consumer.py
        (group lake-writers) → SeaweedFS                      (group prediction-workers) → API path
```

One topic, two independent consumer groups: the lake writes every trade, the
prediction consumer scores it. Offsets are tracked separately, so a slow lake
flush never blocks serving.

## Producer — what the trade becomes

`ingestion/producer.py` subscribes to the public `aggTrade` streams
(`wss://stream.binance.com:9443/stream`, no API key) and maps each message:

| Binance field | Transaction field | Notes |
|---|---|---|
| `a` (aggregate trade id) | `transaction_id` | `txn_<symbol>_<id>` |
| `T` (trade time, ms) | `timestamp` | ISO-8601 UTC |
| `p × q` (price × qty) | `amount_usd` | notional, capped at 49 999.99 to stay inside the lake pre-check bounds |
| `s` (symbol) | `currency` | quote asset (`USDT`, `BTC`, …) |
| `m` (maker side) | `device_type` | `exchange_maker` / `exchange_taker` |

`user_id`, `merchant_id`, `merchant_cat`, `ip_hash` and `card_last4` are derived
deterministically from the trade id: the feed has no parties, and the platform
needs stable entities for the velocity features. `country` comes from the quote
asset (`ZZ` for crypto quotes) and is **not** a model feature.

Records carry **no `label`** — a public trade feed has none. See
[Dataset & labels](training.md#dataset--labels).

```bash
# dry-run one message, print it without publishing
python ingestion/producer.py --dry-run --max-messages 1
```

## Lake consumer — batches, quarantine, commits

`ingestion/lake_consumer.py` accumulates messages and flushes when either
trigger fires:

| Setting | Default | Meaning |
|---|---|---|
| `LAKE_BATCH_SIZE` | 1000 | flush after N messages |
| `LAKE_BATCH_TIMEOUT_SECS` | 300 | or after N seconds |

Each batch is coerced to a frame, stripped of `_source/_symbol/...` provenance
columns, and passed through cheap pre-checks: required columns present, no nulls
in `transaction_id`/`amount_usd`/`timestamp`, amount in `[0.01, 50 000]`, ≥99 %
unique ids, `hour_of_day ∈ [0,23]`, `device_type` in the allowed set.

- pass → `s3://datalake/raw/realtime_txn_<UTC>.parquet`
- fail → `s3://datalake/quarantine/realtime_txn_<UTC>.parquet`

Offsets are committed only after the object is written, so a crash replays the
batch rather than dropping it.

Metrics (own registry, `:8002`): `lake_rows_written_total`,
`lake_batches_written_total`, `kafka_consumer_lag`.

## Validation

`gx/expectations/fraud_transactions_suite.json` is the **training dataset**
contract (it requires a `label` column and at least 500 rows). It runs:

- inside `make data`, where a failure blocks training;
- in CI via `scripts/pipeline_gate.sh`, against the newest **labelled** batch —
  live batches are unlabelled by design, so they are skipped.

```bash
# lake readiness + validation of the newest labelled batch (what CI runs)
bash scripts/pipeline_gate.sh

# validate one parquet file — local path or s3:// URI
python gx/validate.py s3://datalake/raw/seed_txn_<timestamp>.parquet
```

## Inspecting the lake

```bash
python scripts/ingestion_ctl.py status          # counts + last write, as JSON
python scripts/ingestion_ctl.py latest          # newest raw batch (s3:// URI)
python scripts/ingestion_ctl.py latest --requires label   # newest labelled batch
```

`GET /ingest/status` on the API returns the same counts, read straight from the
lake (503 if the object store is unreachable).

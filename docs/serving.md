# Serving

Two entry points, one scoring path:

```
HTTP POST /predict/ ──┐
                      ├──► api/inference.score(tx) ──► features.store (Redis) ──► model ──► decision
Kafka consumer ───────┘                                │
                                                       └──► db.writer.record_prediction (PostgreSQL)
```

`api/inference.score` is the single implementation of feature engineering,
feature-store lookup, thresholding and version stamping. An audit row therefore
describes exactly what the model saw, whichever way the transaction arrived.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/predict/` | score one transaction |
| `GET` | `/predict/model/info` | registry metadata for the served stage |
| `GET` | `/ingest/status` | lake counts, read straight from SeaweedFS |
| `GET` | `/health` | liveness |
| `GET` | `/metrics` | Prometheus scrape (no redirect, single process) |

```bash
curl -X POST http://localhost:8000/predict/ \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "txn_123",
    "timestamp": "2024-06-15T14:32:07Z",
    "user_id": "usr_001",
    "merchant_id": "mrch_001",
    "merchant_cat": "electronics",
    "amount_usd": 49.99,
    "currency": "USD",
    "country": "US",
    "device_type": "mobile",
    "ip_hash": "aabb1122",
    "card_last4": "1234",
    "is_international": false,
    "hour_of_day": 14,
    "day_of_week": 2
  }'
```

```json
{
  "transaction_id": "txn_123",
  "fraud_probability": 0.12,
  "decision": "APPROVE",
  "model_version": "2",
  "shadow_probability": 0.15,
  "latency_ms": 45.3
}
```

`label` is accepted and ignored: serving never needs a label, and the feed has
none.

## Model loading

`api/model_loader.py` caches the artifact **and** its registry metadata
(`version`, `run_id`, `decision_threshold`) as one unit, and re-reads the
registry after `MODEL_CACHE_TTL_SECS` (default 60). Consequences:

- a promotion reaches the API without a restart, within one TTL;
- the version and threshold stamped on a prediction always describe the model
  that produced it — a freshly promoted version cannot be stamped on a score
  computed by the previous artifact;
- when the version is unchanged, the artifact is not re-downloaded.

The API runs a **single** Uvicorn worker: each worker owns its own Prometheus
registry and model cache, so multiple workers make every counter flap between
processes. Requests are handled in a thread pool, so a single process still
serves concurrently.

## Shadow routing

`api/shadow.py` scores a configurable fraction (`SHADOW_TRAFFIC_FRACTION`) of
traffic against `CHALLENGER_STAGE` (default `Staging`) and records the
challenger probability alongside the champion's. When the challenger stage has
no servable version, the router stops retrying the registry for one TTL instead
of hitting MLflow on every request.

## Prediction consumer

```bash
docker compose -f docker/docker-compose.yml up -d kafka-consumer
docker compose -f docker/docker-compose.yml logs -f kafka-consumer

docker compose -f docker/docker-compose.yml exec kafka \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group prediction-workers --describe
```

It scores `transactions_raw` on its own consumer group, waits for a Production
model at startup, and counts duplicates separately: a replayed message hits the
`transaction_id` unique index, increments `consumer_duplicates_total`, and adds
no row.

## Metrics

| Metric | Type | Set by |
|---|---|---|
| `predictions_total{decision,model_version}` | counter | API |
| `prediction_latency_seconds` | histogram | API |
| `fraud_probability_last` | gauge | API |
| `feature_store_errors_total` | counter | API |
| `consumer_messages_processed_total` | counter | prediction consumer |
| `consumer_duplicates_total` | counter | prediction consumer |
| `consumer_errors_total` | counter | prediction consumer |
| `kafka_consumer_lag` | gauge | both consumers |

```bash
curl http://localhost:8000/metrics | grep predictions_total
```

When the online feature store is unreachable the request still succeeds: the
lookup falls back to zero velocity features and increments
`feature_store_errors_total`.

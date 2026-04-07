# Monitoring

```
predictions (PostgreSQL) ──► run_drift.py ──► Evidently report ──► s3://datalake/drift_reports/
                                    │
                                    └──► Pushgateway ──► Prometheus ──► Alertmanager ──► alert-webhook ──► GitHub Actions
                                                              │
                                                           Grafana
```

## Drift

`monitoring/run_drift.py` compares **served inputs**, not raw transactions: it
reads `features_json` — the exact vector the model scored — from the audit
table.

| Window | Rows | Default |
|---|---|---|
| reference | 14 → 1 days ago | `REFERENCE_DAYS = 14` |
| current | last 24 hours | `CURRENT_DAYS = 1` |

Fewer than 200 rows in either window is an error rather than a meaningless
report. Evidently's `DataDriftPreset` runs over the declared numeric/categorical
columns and the drifted-column **share** is extracted and published as the
gauge `evidently_drift_share`.

Outputs:

- `report.html` and `report.json` in `/tmp/drift_reports/<date>/` (container),
- the same two files uploaded to `s3://datalake/drift_reports/<date>/`,
- the gauge pushed to Pushgateway (`job="drift"`).

```bash
make drift
curl -s localhost:9091/metrics | grep evidently_drift_share
curl -s "localhost:9090/api/v1/query?query=evidently_drift_share"
curl -s localhost:9090/api/v1/rules | jq '.data.groups[] | select(.name=="drift_alerts")'
```

The gauge persists in Pushgateway until the next push, so a stale value keeps
evaluating in Prometheus — expected, and the reason the alert carries a `for:`
window.

A drift share above 0.15 is normal when the reference window is synthetic seed
data and the current window is live Binance traffic: it is a real distribution
difference, not a detector fault.

## Alert rules

`monitoring/prometheus/rules/drift.yml`:

| Alert | Expression | For |
|---|---|---|
| `FraudDriftHigh` | `evidently_drift_share > 0.15` | 1h |
| `HighPredictionLatency` | `histogram_quantile(0.99, sum(rate(prediction_latency_seconds_bucket[5m])) by (le)) > 0.5` | 5m |
| `NoPredictions` | `sum(rate(predictions_total[15m])) == 0 or absent(predictions_total)` | 30m |
| `LakeConsumerLagHigh` | `kafka_consumer_lag{job="lake-consumer"} > 5000` | 15m |

Scrape jobs: `fraud-api`, `kafka-consumer:9101`, `lake-consumer:8002`,
`pushgateway` (with `honor_labels: true`, so the pushed `job="drift"` survives),
and Prometheus itself.

## Alert delivery

Alertmanager routes everything to the webhook receiver. Alertmanager **does not
expand `${VAR}`** in its config and has no `--config.expand-env` flag, so the
compose entrypoint writes `ALERT_WEBHOOK_TOKEN` into `/tmp/alert_token` and the
config reads it via `credentials_file`.

`monitoring/alert_webhook.py` then:

| Situation | Response |
|---|---|
| missing/wrong bearer token (when a token is configured) | 401 |
| `GITHUB_OWNER`/`GITHUB_REPO`/`GITHUB_TOKEN` incomplete | 503 for **any** alert, no external call |
| firing `FraudDriftHigh` with complete config | 200 `{"status":"ok","dispatched":[run_url]}` |
| any other alert with complete config | 200 `{"status":"ok","dispatched":[]}` |

`ALERT_WEBHOOK_TOKEN` is shared by Alertmanager and the webhook; leave
`GITHUB_TOKEN` empty locally so alerts never dispatch a real workflow.

## Grafana

`monitoring/grafana/dashboards/overview.json` is provisioned at start with the
`prometheus` datasource: throughput, p99 latency, fraud rate (rejected share),
drift share, lake rows written, consumer lag.

Prometheus also needs no manual step — `docker compose up -d prometheus` picks
up the config and rules from `monitoring/prometheus/`.

## Checking the stack

```bash
curl -s localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health}'
curl -s localhost:9093/api/v2/alerts
curl -s localhost:9093/-/ready
```

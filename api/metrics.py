"""Prometheus metrics definitions."""

from prometheus_client import Counter, Gauge, Histogram

predictions_total = Counter(
    "predictions_total",
    "Total prediction requests",
    ["decision", "model_version"],
)
prediction_latency = Histogram(
    "prediction_latency_seconds",
    "Prediction latency in seconds",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)
fraud_prob_gauge = Gauge(
    "fraud_probability_last",
    "Last observed fraud probability",
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag",
    "Kafka consumer lag (messages behind)",
)

"""Feast feature definitions for fraud detection."""
from datetime import timedelta
from feast import Entity, Feature, FeatureView, FileSource, ValueType

transactions_source = FileSource(
    path="s3://datalake/raw/",
    timestamp_field="timestamp",
    s3_endpoint_override="http://localhost:8333",
)

user = Entity(name="user_id", value_type=ValueType.STRING,
              description="E-commerce user")

merchant = Entity(name="merchant_id", value_type=ValueType.STRING,
                  description="Merchant")

user_features = FeatureView(
    name="user_features",
    entities=["user_id"],
    ttl=timedelta(days=7),
    features=[
        Feature(name="amount_usd", dtype=ValueType.FLOAT),
        Feature(name="hour_of_day", dtype=ValueType.INT64),
        Feature(name="day_of_week", dtype=ValueType.INT64),
        Feature(name="is_international", dtype=ValueType.BOOL),
    ],
    online=True,
    source=transactions_source,
)

merchant_features = FeatureView(
    name="merchant_features",
    entities=["merchant_id"],
    ttl=timedelta(days=7),
    features=[
        Feature(name="amount_usd", dtype=ValueType.FLOAT),
        Feature(name="merchant_cat", dtype=ValueType.STRING),
    ],
    online=True,
    source=transactions_source,
)

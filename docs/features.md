# Features

Velocity is the signal: how much a user or merchant has moved in the last seven
days. Those aggregates are computed in one job, stored as Parquet in the lake,
and served two ways by Feast — historical point-in-time joins for training, and
Redis lookups for serving.

```
s3://datalake/raw/ ──► features/compute.py ──► s3://datalake/features/{user,merchant}_stats.parquet
                                                        │
                                     ┌──────────────────┴──────────────────┐
                                     ▼                                     ▼
                        Feast historical join                  Feast online store (Redis)
                        (training/build_dataset.py)            (api/inference.py)
```

## Computation

`features/compute.py` reads the raw lake, groups by entity into hourly buckets,
and writes two fixed keys:

```
s3://<bucket>/features/user_stats.parquet
s3://<bucket>/features/merchant_stats.parquet
```

Each row is stamped with the **end** of its hour bucket as `event_timestamp`, so
a point-in-time join cannot leak the future into a training row. Fewer than
1 000 rows is treated as an error rather than producing meaningless aggregates.

## Feast repository

`feature_repo/definitions.py` declares:

| Object | Detail |
|---|---|
| `user` / `merchant` entities | `ValueType.STRING` |
| `user_stats` / `merchant_stats` FeatureViews | TTL 7 days, `online=True`, `Float32` fields |
| Sources | `FileSource` over the two `s3://` Parquet keys, `s3_endpoint_override` from `SEAWEED_ENDPOINT` |

`feature_repo/feature_store.yaml` pins the local provider, a local `registry.db`,
Redis as the online store (`connection_string: redis:6379` — Feast's format is
`host:port`, not a URL) and the file offline store.

> The definitions module is **not** named `features.py` on purpose: `feast apply`
> imports repo modules by name, and the images put `/app` on `PYTHONPATH`, so a
> local `features.py` would be shadowed by the project's `features/` package and
> apply would silently register nothing.

## Running it

```bash
make features
```

That is three steps in one job:

```bash
python features/compute.py                 # aggregates → lake Parquet
cd feature_repo && feast apply             # register the views
feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"   # lake → Redis
```

The Makefile target fails loudly if `feast apply` registers no feature view, so
a shadowed module cannot pass as success.

## Access layer

`features/store.py` is the only module that imports Feast:

- `get_store()` — cached `FeatureStore` for `settings.feature_repo_path`
- `historical_features(entity_df)` — point-in-time join for training
- `online_features(user_ids, merchant_ids)` — paired lookups for serving
- `with_defaults(df)` — fills missing velocity columns with 0 for cold entities

Serving treats the feature store as optional: a failed lookup increments
`feature_store_errors_total` and scores with defaults rather than failing the
request. The online store only knows entities that were materialised, so a fresh
entity scores with zeros until the next `make features`.

## Feature names

`features/schema.py` holds `USER_FEATURES`, `MERCHANT_FEATURES` and
`VELOCITY_FEATURES` as plain constants (no Feast import), so monitoring and
training can name the columns without pulling the feature-store stack.

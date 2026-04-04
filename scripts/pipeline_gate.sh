#!/usr/bin/env bash
# Ingestion health-check and lake readiness gate.
#
# ROLE CHANGE
# -----------
# This script ensures that the data lake has enough validated Parquet files 
# before allowing training to proceed, and it can also seed the lake with synthetic data 
# if it's empty (e.g., on first run).
#
#   Binance WebSocket → kafka/producer.py → Kafka (transactions_raw)
#                    → kafka/lake_consumer.py → SeaweedFS raw/*.parquet
#
# This script is now a READINESS GATE used by CI/CD workflows:
#   1. Check that SeaweedFS has enough validated Parquet files to train on.
#   2. Optionally trigger a one-time seed if the lake is empty (first run).
#   3. Kick off Feast feature materialisation over whatever is in raw/.
#
# It exits 0 if training can proceed, non-zero otherwise.
set -euo pipefail

MIN_RAW_FILES="${MIN_RAW_FILES:-1}"   # minimum parquet files before training
SEED_IF_EMPTY="${SEED_IF_EMPTY:-true}"  # write seed data if lake is empty
PYTHON_BIN="${PYTHON_BIN:-python}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "ERROR: neither 'python' nor 'python3' is available in PATH." >&2
    exit 1
  fi
fi

echo "=== [1/3] Checking lake readiness ==="
STATUS=$($PYTHON_BIN scripts/ingestion_ctl.py status 2>&1)
echo "$STATUS"

RAW_FILES=$(echo "$STATUS" | $PYTHON_BIN -c "import sys,json; d=json.loads(sys.stdin.read()); print(d['lake_raw_files'])" 2>/dev/null || echo "0")

if [ "$RAW_FILES" -lt "$MIN_RAW_FILES" ]; then
  if [ "$SEED_IF_EMPTY" = "true" ]; then
    echo "=== Lake has $RAW_FILES files (< $MIN_RAW_FILES required) — writing seed data ==="
    $PYTHON_BIN scripts/ingestion_ctl.py seed --n-rows 50000 --fraud-rate 0.02
  else
    echo "ERROR: Lake has $RAW_FILES raw files, need >= $MIN_RAW_FILES. " \
         "Start kafka/producer.py + kafka/lake_consumer.py to populate the lake." >&2
    exit 1
  fi
else
  echo "=== Lake has $RAW_FILES raw parquet files — ready to train ==="
fi

echo "=== [2/3] Running Great Expectations on latest raw batch ==="
# Find most recent raw parquet and validate it
LATEST=$($PYTHON_BIN -c "
import boto3, os
cfg_ep  = os.environ.get('SEAWEED_ENDPOINT', 'http://localhost:8333')
cfg_ak  = os.environ.get('SEAWEED_ACCESS_KEY', 'minioadmin')
cfg_sk  = os.environ.get('SEAWEED_SECRET_KEY', 'minioadmin')
bucket  = os.environ.get('DATALAKE_BUCKET', 'datalake')
s3 = boto3.client('s3', endpoint_url=cfg_ep,
                  aws_access_key_id=cfg_ak, aws_secret_access_key=cfg_sk)
objs = s3.list_objects_v2(Bucket=bucket, Prefix='raw/')
files = sorted(
    [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')],
    key=lambda x: x['LastModified'], reverse=True
)
print(f\"s3://{bucket}/{files[0]['Key']}\" if files else '')
" 2>/dev/null)

if [ -n "$LATEST" ]; then
  $PYTHON_BIN gx/validate.py "$LATEST" && echo "GX validation passed: $LATEST"
else
  echo "WARNING: No parquet files found for GX validation — skipping."
fi

echo "=== [3/3] Materialising Feast features ==="
cd feast && feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"

echo "=== Ingestion pipeline ready — training can proceed ==="

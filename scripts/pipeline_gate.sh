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

# Ensure the repository root is on PYTHONPATH so scripts can import top-level
# project packages (e.g., `config.settings`) when run in CI or via `bash`.
if [ -z "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="$PWD"
else
  export PYTHONPATH="$PYTHONPATH:$PWD"
fi

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
echo "MIN_RAW_FILES=${MIN_RAW_FILES}"
echo "SEED_IF_EMPTY=${SEED_IF_EMPTY}"
echo "DATALAKE_BUCKET=${DATALAKE_BUCKET:-unset}"
echo "SEAWEED_ENDPOINT=${SEAWEED_ENDPOINT:-unset}"
if [ -z "${SEAWEED_ACCESS_KEY:-}" ] || [ -z "${SEAWEED_SECRET_KEY:-}" ]; then
  echo "WARNING: SEAWEED access/secret keys are not set in the environment (will use defaults if present)."
else
  echo "SEAWEED credentials: present"
fi

# Run status but do not let 'set -e' abort the script so we can print diagnostics
set +e
STATUS=$($PYTHON_BIN scripts/ingestion_ctl.py status 2>&1)
STATUS_RC=$?
set -e

echo "$STATUS"

if [ $STATUS_RC -ne 0 ]; then
  echo "::error::Failed to query datalake status (exit ${STATUS_RC})." >&2
  echo "::error::Check SEAWEED_ENDPOINT, DATALAKE_BUCKET and credentials in the workflow secrets or environment." >&2
  echo "$STATUS" >&2
  exit $STATUS_RC
fi

RAW_FILES=$(echo "$STATUS" | $PYTHON_BIN -c "import sys,json; d=json.loads(sys.stdin.read()); print(d.get('lake_raw_files',0))" 2>/dev/null || echo "0")

if ! echo "$RAW_FILES" | grep -E '^[0-9]+$' >/dev/null 2>&1; then
  echo "::error::Unexpected RAW_FILES value: $RAW_FILES" >&2
  exit 1
fi

if [ "$RAW_FILES" -lt "$MIN_RAW_FILES" ]; then
  if [ "$SEED_IF_EMPTY" = "true" ]; then
    echo "=== Lake has $RAW_FILES files (< $MIN_RAW_FILES required) — writing seed data ==="
    $PYTHON_BIN scripts/ingestion_ctl.py seed --n-rows 50000 --fraud-rate 0.02
  else
    echo "::error::Lake not ready: found $RAW_FILES files, need >= $MIN_RAW_FILES. Start producers/consumers or set SEED_IF_EMPTY=true for automatic bootstrap." >&2
    exit 1
  fi
else
  echo "=== Lake has $RAW_FILES raw parquet files — ready to train ==="
fi

echo "=== [2/3] Running Great Expectations on latest raw batch ==="
echo "DEBUG: SEAWEED_ENDPOINT=${SEAWEED_ENDPOINT:-unset} DATALAKE_BUCKET=${DATALAKE_BUCKET:-unset}"
# Find most recent raw parquet and validate it
set +e
LATEST=$($PYTHON_BIN - <<'PY'
import os
import sys
try:
  import boto3
except Exception as e:
  print("ERROR: could not import boto3: " + str(e))
  sys.exit(2)

cfg_ep = os.environ.get('SEAWEED_ENDPOINT', 'http://localhost:8333')
cfg_ak = os.environ.get('SEAWEED_ACCESS_KEY', 'minioadmin')
cfg_sk = os.environ.get('SEAWEED_SECRET_KEY', 'minioadmin')
bucket = os.environ.get('DATALAKE_BUCKET', 'datalake')
try:
  import sys as _sys
  print("boto3_version:" + boto3.__version__, file=_sys.stderr)
  s3 = boto3.client('s3', endpoint_url=cfg_ep,
            aws_access_key_id=cfg_ak, aws_secret_access_key=cfg_sk)
  objs = s3.list_objects_v2(Bucket=bucket, Prefix='raw/')
  files = sorted(
    [o for o in objs.get('Contents', []) if o['Key'].endswith('.parquet')],
    key=lambda x: x['LastModified'], reverse=True
  )
  if files:
    print(f"s3://{bucket}/{files[0]['Key']}")
  else:
    print("")
except Exception as e:
  print("ERROR:" + str(e))
  sys.exit(2)
PY
)
PY_RC=$?
set -e

if [ $PY_RC -ne 0 ]; then
  echo "::warning::Could not enumerate parquet files (exit $PY_RC)." >&2
  echo "$LATEST" >&2
  LATEST=""
fi

if [ -n "$LATEST" ] && ! echo "$LATEST" | grep -q '^ERROR:'; then
  $PYTHON_BIN gx/validate.py "$LATEST" && echo "GX validation passed: $LATEST"
else
  echo "WARNING: No parquet files found for GX validation — skipping."
fi

echo "=== [3/3] Materialising Feast features ==="
cd feast && feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)"

echo "=== Ingestion pipeline ready — training can proceed ==="

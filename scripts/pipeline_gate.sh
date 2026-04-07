#!/usr/bin/env bash
# Lake readiness gate for the training pipeline.
#
#   Binance WebSocket → ingestion/producer.py → Kafka (transactions_raw)
#                    → ingestion/lake_consumer.py → SeaweedFS raw/*.parquet
#
# Steps:
#   1. Ask the lake how many validated parquet files it holds.
#   2. Seed synthetic labelled data — only when the lake holds no labelled
#      batch yet, so repeated retrains never pile up duplicate seed files.
#   3. Fail unless at least MIN_RAW_FILES files are present.
#   4. Validate the newest *labelled* raw batch against the Great Expectations
#      suite (live feed batches are unlabelled, so they are skipped).
#
# Exits 0 if training can proceed, non-zero otherwise.
set -euo pipefail

# Ensure the repository root is on PYTHONPATH so scripts can import top-level
# project packages (e.g., `config.settings`) when run in CI or via `bash`.
if [ -z "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="$PWD"
else
  export PYTHONPATH="$PYTHONPATH:$PWD"
fi

MIN_RAW_FILES="${MIN_RAW_FILES:-1}"    # minimum parquet files before training
SEED_IF_EMPTY="${SEED_IF_EMPTY:-true}" # write seed data only if lake is empty
PYTHON_BIN="${PYTHON_BIN:-python}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
  else
    echo "ERROR: neither 'python' nor 'python3' is available in PATH." >&2
    exit 1
  fi
fi

raw_file_count() {
  $PYTHON_BIN scripts/ingestion_ctl.py status \
    | $PYTHON_BIN -c "import sys,json; print(json.loads(sys.stdin.read())['lake_raw_files'])"
}

echo "=== [1/2] Checking lake readiness ==="
echo "MIN_RAW_FILES=${MIN_RAW_FILES}"
echo "SEED_IF_EMPTY=${SEED_IF_EMPTY}"
echo "DATALAKE_BUCKET=${DATALAKE_BUCKET:-unset}"
echo "SEAWEED_ENDPOINT=${SEAWEED_ENDPOINT:-unset}"

# Run status but do not let 'set -e' abort the script so we can print diagnostics
set +e
STATUS=$($PYTHON_BIN scripts/ingestion_ctl.py status 2>&1)
STATUS_RC=$?
set -e

echo "$STATUS"

if [ $STATUS_RC -ne 0 ]; then
  echo "::error::Failed to query datalake status (exit ${STATUS_RC})." >&2
  echo "::error::Check SEAWEED_ENDPOINT, DATALAKE_BUCKET and credentials in the workflow secrets or environment." >&2
  exit $STATUS_RC
fi

RAW_FILES=$(echo "$STATUS" | $PYTHON_BIN -c "import sys,json; print(json.load(sys.stdin)['lake_raw_files'])" 2>/dev/null || echo "0")
RAW_FILES=$(echo "$RAW_FILES" | tr -d '[:space:]')

# Live batches never carry a label, so "the lake is empty" is the wrong
# condition: once the producer has written anything, training still has no
# labelled rows. Seed when there is no *labelled* batch yet.
latest_labelled() {
  $PYTHON_BIN scripts/ingestion_ctl.py latest --requires label | tr -d '\r'
}

LATEST=$(latest_labelled)

if { [ "$RAW_FILES" -eq 0 ] || [ -z "$LATEST" ]; } && [ "$SEED_IF_EMPTY" = "true" ]; then
  echo "=== No labelled training batch — writing synthetic labelled seed data ==="
  $PYTHON_BIN scripts/ingestion_ctl.py seed --n-rows 50000 --fraud-rate 0.02
  RAW_FILES=$(raw_file_count)
  LATEST=$(latest_labelled)
fi

if ! echo "$RAW_FILES" | grep -E '^[0-9]+$' >/dev/null 2>&1; then
  echo "::error::Unexpected RAW_FILES value: $RAW_FILES" >&2
  exit 1
fi

if [ "$RAW_FILES" -lt "$MIN_RAW_FILES" ]; then
  echo "::error::Lake not ready: found $RAW_FILES files, need >= $MIN_RAW_FILES. Start producers/consumers or set SEED_IF_EMPTY=true for automatic bootstrap." >&2
  exit 1
fi

echo "=== Lake has $RAW_FILES raw parquet files — ready to train ==="

echo "=== [2/2] Validating the newest labelled raw batch with Great Expectations ==="
# Live batches from the Binance feed carry no `label`; the suite is the
# training-dataset contract, so only labelled batches can satisfy it.
if [ -n "$LATEST" ]; then
  $PYTHON_BIN gx/validate.py "$LATEST"
  echo "GX validation passed: $LATEST"
else
  echo "::error::No labelled raw batch in the lake — live feed batches are unlabelled by design." >&2
  echo "::error::Run 'make seed' (or SEED_IF_EMPTY=true) to write a labelled bootstrap batch." >&2
  exit 1
fi

echo "=== Ingestion pipeline ready — training can proceed ==="

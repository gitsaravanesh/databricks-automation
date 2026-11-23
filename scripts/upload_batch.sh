#!/usr/bin/env bash
# Upload one sample batch to a landing volume (macOS / Linux / Git Bash).
#
# Usage, from the repo root:
#   ./scripts/upload_batch.sh 1 <catalog>                 # dev  (dev_bronze)
#   ./scripts/upload_batch.sh 1 <catalog> test_bronze     # test
#   ./scripts/upload_batch.sh 1 <catalog> bronze          # prod
#   ./scripts/upload_batch.sh 1 <catalog> dev_bronze myprofile   # with a CLI profile
set -euo pipefail

BATCH="${1:?usage: upload_batch.sh <1|2|3> <catalog> [schema] [profile]}"
CATALOG="${2:?usage: upload_batch.sh <1|2|3> <catalog> [schema] [profile]}"
SCHEMA="${3:-dev_bronze}"
PROFILE="${4:-}"
DEST="dbfs:/Volumes/${CATALOG}/${SCHEMA}/landing/incoming"

SRC=$(ls -d sample_data/batch_0"${BATCH}"_* 2>/dev/null | head -n 1 || true)
if [ -z "$SRC" ]; then
  echo "No sample_data folder for batch $BATCH. Run from the repo root." >&2
  exit 1
fi

for table_dir in "$SRC"/*/; do
  table=$(basename "$table_dir")
  echo "Uploading $SRC/$table -> $DEST/$table"
  if [ -n "$PROFILE" ]; then
    databricks fs cp -r --overwrite "$table_dir" "$DEST/$table" --profile "$PROFILE"
  else
    databricks fs cp -r --overwrite "$table_dir" "$DEST/$table"
  fi
done
echo "Batch $BATCH uploaded to $DEST"

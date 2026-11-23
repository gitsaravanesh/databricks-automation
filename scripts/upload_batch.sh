#!/usr/bin/env bash
# Upload one sample batch to the landing volume (macOS / Linux / Git Bash).
#
# Usage, from the repo root:
#   ./scripts/upload_batch.sh 1
#   ./scripts/upload_batch.sh 2 dbfs:/Volumes/workspace/dev_bronze/landing/incoming free
#                              ^ batch  ^ destination (optional)               ^ CLI profile (optional)
set -euo pipefail

BATCH="${1:?usage: upload_batch.sh <1|2|3> [destination] [profile]}"
DEST="${2:-dbfs:/Volumes/workspace/dev_bronze/landing/incoming}"
PROFILE="${3:-}"

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
echo "Batch $BATCH uploaded. Now run: databricks bundle run -t dev bronze_silver_job"

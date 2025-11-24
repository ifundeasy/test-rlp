#!/usr/bin/env bash
set -euo pipefail

LOGFILE="benchmark/1-prepare.log"
ENVFILE="benchmark/bench.env"

if [[ -f benchmark/bench.env ]]; then
  # shellcheck disable=SC1091
  source $ENVFILE
else
  echo "$ENVFILE not found." >&2
  exit 1
fi

echo "❯ go run ./cmd/main.go csv load-data" | tee -a "$LOGFILE"

# Run csv load-data:
# - stdout+stderr displayed in terminal
# - also appended to LOGFILE
go run ./cmd/main.go csv load-data 2>&1 | tee -a "$LOGFILE"

# Get the last 2 lines containing BENCH_* from LOGFILE (must be from the latest run)
# Example output:
#   BENCH_LOOKUPRES_MANAGE_USER=1243
#   BENCH_LOOKUPRES_VIEW_USER=542
last_two="$(
  grep 'BENCH_LOOKUPRES_' "$LOGFILE" \
    | tail -n 2 \
    | sed 's/^.*\[csv\] *//'
)"

# Ensure ENVFILE exists; do not overwrite, instead remove the last 2 lines
if [[ -f "$ENVFILE" ]]; then
  # Remove the last 2 lines from the existing env file (if it has ≥2 lines)
  line_count="$(wc -l < "$ENVFILE" | tr -d ' ')"
  if (( line_count > 2 )); then
    head -n $((line_count - 2)) "$ENVFILE" > "$ENVFILE.tmp"
    mv "$ENVFILE.tmp" "$ENVFILE"
  else
    # If file has 0, 1, or 2 lines, just reset it to empty
    : > "$ENVFILE"
  fi
else
  # Create an empty env file if it does not exist yet
  : > "$ENVFILE"
fi

# Append new BENCH_* exports to ENVFILE
while IFS= read -r line; do
  [ -z "$line" ] && continue
  # Write as "export ..." and also display in terminal
  echo "export $line" | tee -a "$ENVFILE"
done <<< "$last_two"

echo
echo "Env vars updated in $ENVFILE"

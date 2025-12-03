#!/bin/bash


set -euo pipefail

CONTAINER_NAME="${1:-}"
# Optional: target PID to follow; monitor exits when PID is gone
TARGET_PID="${2:-}"
# Sampling interval in milliseconds (used to derive sleep seconds)
INTERVAL_MS=200

LIST_ALL=false
if [[ -z "${CONTAINER_NAME}" ]]; then
    LIST_ALL=true
fi

if [[ "${LIST_ALL}" == "false" ]]; then
    # Verify the container exists
    if ! docker inspect "${CONTAINER_NAME}" >/dev/null 2>&1; then
        echo "Container '${CONTAINER_NAME}' not found. Use 'docker ps' to list running containers." >&2
        exit 1
    fi
fi

# Derive sleep seconds from INTERVAL_MS with millisecond precision
if command -v bc >/dev/null 2>&1; then
    SLEEP_SECONDS="$(printf '%.3f' "$(echo "${INTERVAL_MS}/1000" | bc -l)")"
else
    # Fallback if bc is unavailable: approximate using awk
    SLEEP_SECONDS="$(awk -v ms="${INTERVAL_MS}" 'BEGIN { printf "%.3f", ms/1000 }')"
fi
LOGFILE_DIR="$(dirname "$0")"
LOGFILE="$LOGFILE_DIR/2-monitor.log"
# Ensure logfile exists but do not truncate if already present
if [[ ! -f "$LOGFILE" ]]; then
    : > "$LOGFILE"
fi

STOPPED=false
trap 'STOPPED=true' INT TERM

while [[ "$STOPPED" != "true" ]]; do
    # If a target PID is provided, exit when it is gone
    if [[ -n "$TARGET_PID" ]]; then
        if ! kill -0 "$TARGET_PID" 2>/dev/null; then
            # macOS 'date' doesn't support nanosecond (%N); use Python for microseconds
            if command -v python3 >/dev/null 2>&1; then
                ts=$(python3 -c 'import datetime; print(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f"))')
            else
                # Fallback: seconds + milliseconds using perl
                if command -v perl >/dev/null 2>&1; then
                    ts=$(perl -MTime::HiRes=time -e 'printf("%s", scalar localtime(time));')
                else
                    ts=$(date +"%Y/%m/%d %H:%M:%S")
                fi
            fi
            echo "${ts} [monitor] Target PID ${TARGET_PID} exited; stopping monitor." >> "$LOGFILE"
            break
        fi
    fi
    # macOS 'date' doesn't support nanosecond (%N); use Python for microseconds
    if command -v python3 >/dev/null 2>&1; then
        ts=$(python3 -c 'import datetime; print(datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S.%f"))')
    else
        # Fallback: seconds + milliseconds using perl
        if command -v perl >/dev/null 2>&1; then
            ts=$(perl -MTime::HiRes=time -e 'printf("%s", scalar localtime(time));')
        else
            ts=$(date +"%Y/%m/%d %H:%M:%S")
        fi
    fi

    if [[ "${LIST_ALL}" == "true" ]]; then
        # Snapshot stats for all running containers
        # Output lines: name cpu% mem_usage mem%
        stats_output=$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}' 2>/dev/null || true)
        if [[ -n "$stats_output" ]]; then
            while IFS='|' read -r name cpu_perc mem_usage mem_perc; do
                [[ -z "$name" ]] && continue
                usage=$(echo "$mem_usage" | awk -F'/' '{gsub(/^\s+|\s+$/,"",$1); print $1}')
                limit=$(echo "$mem_usage" | awk -F'/' '{gsub(/^\s+|\s+$/,"",$2); print $2}')
                echo "${ts} [${name}] CPU: ${cpu_perc} | Memory: ${mem_perc} (${usage}/${limit})" >> "$LOGFILE"
            done <<< "$stats_output"
        else
            echo "${ts} [monitor] CPU: N/A | Memory: N/A (no containers?)" >> "$LOGFILE"
        fi
    else
        # Single container mode
        if stats_line=$(docker stats --no-stream --format '{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}' "${CONTAINER_NAME}" 2>/dev/null); then
            IFS='|' read -r name cpu_perc mem_usage mem_perc <<< "${stats_line}"
            usage=$(echo "$mem_usage" | awk -F'/' '{gsub(/^\s+|\s+$/,"",$1); print $1}')
            limit=$(echo "$mem_usage" | awk -F'/' '{gsub(/^\s+|\s+$/,"",$2); print $2}')
            echo "${ts} [${name}] CPU: ${cpu_perc} | Memory: ${mem_perc} (${usage}/${limit})" >> "$LOGFILE"
        else
            echo "${ts} [${CONTAINER_NAME}] CPU: N/A | Memory: N/A (container stopped?)" >> "$LOGFILE"
        fi
    fi

    sleep "${SLEEP_SECONDS}"
done
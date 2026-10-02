#!/bin/bash
# EC2 user-data (on-demand recommended; a spot interruption restarts the replay): rebuild the 2025 snapshots next to the source buckets (us-west-2) and store the
# result in a staging bucket. The instance shuts itself down when done (launch it with
# --instance-initiated-shutdown-behavior terminate), and after MAX_MINUTES at the latest.
#
# Expects an instance role that can read s3://spotlake/rawdata/aws/*, s3://sps-query-data/aws/*
# and write s3://$STAGING/*.
set -euxo pipefail

STAGING=spotlake-open-data-staging
REPO=https://github.com/ddps-lab/spotlake-open-data
COMMIT=__COMMIT__
MAX_MINUTES=300
WORK=/data

shutdown -h +"$MAX_MINUTES"
exec > >(tee -a /var/log/reprocess.log) 2>&1
finish() {
    status=$?
    aws s3 cp /var/log/reprocess.log "s3://$STAGING/logs/reprocess-$(date -u +%Y%m%dT%H%M%SZ)-exit$status.log" || true
    shutdown -h now
}
trap finish EXIT

export HOME=/root
mkdir -p "$WORK" && cd "$WORK"
# Every 10 minutes upload the log and the snapshots written so far, so an interruption or the
# deadline shutdown does not lose them.
( while sleep 600; do
    aws s3 cp --only-show-errors /var/log/reprocess.log "s3://$STAGING/logs/reprocess-running.log" || true
    [ -d "$WORK/build/data" ] && aws s3 sync --only-show-errors --exclude "*.tmp" "$WORK/build/data/" "s3://$STAGING/data/" || true
done ) &

dnf install -y git python3.11 python3.11-pip
git clone "$REPO" repo && git -C repo checkout "$COMMIT"
python3.11 -m venv venv && venv/bin/pip install -q -r repo/requirements.txt

# Inputs: warm-up month + 2025 snapshots, legacy rotating capacities, per-target files.
IN="$WORK/input"
pids=()
for m in 2024/12 2025/01 2025/02 2025/03 2025/04 2025/05 2025/06 2025/07 2025/08 2025/09 2025/10 2025/11 2025/12; do
    aws s3 sync --only-show-errors "s3://spotlake/rawdata/aws/$m/" "$IN/spotlake/rawdata/aws/$m/" & pids+=($!)
done
for m in 2025/02 2025/03 2025/04 2025/05 2025/06 2025/07 2025/08 2025/09 2025/10 2025/11 2025/12; do
    aws s3 sync --only-show-errors "s3://spotlake/rawdata/aws/sps/$m/" "$IN/spotlake/rawdata/aws/sps/$m/" & pids+=($!)
done
for m in 2024/12 2025/01 2025/02; do
    aws s3 sync --only-show-errors "s3://sps-query-data/aws/$m/" "$IN/sps-query-data/aws/$m/" & pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid"; done   # set -e aborts on any failed download
du -sh "$IN"/*

cd repo
# Leave time for the exit handler before the MAX_MINUTES shutdown.
timeout $((MAX_MINUTES - 20))m ../venv/bin/python -W ignore \
    scripts/reprocess_2025.py --out "$WORK/build" --local-root "$IN" --workers 8

aws s3 sync --only-show-errors "$WORK/build/data/" "s3://$STAGING/data/"
aws s3 cp "$WORK/build/validation.csv" "s3://$STAGING/validation.csv"
echo "reprocess finished"

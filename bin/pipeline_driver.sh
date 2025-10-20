#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# run_pipeline.sh
#
#   download      – bash src/update_samples.sh
#   stitch        – python3 src/stitching_job_v2.py …
#   find_anomaly  – bash src/find_anomaly.sh
#   notify        – python3 src/generate_slack_notification.py
#
# Usage examples
#   ./run_pipeline.sh download
#   ./run_pipeline.sh stitch
#   ./run_pipeline.sh find_anomaly
#   ./run_pipeline.sh notify
#   ./run_pipeline.sh download stitch find_anomaly notify   # full chain
# ---------------------------------------------------------------------------
set -euo pipefail

# ────────────────────────────────────────────────────────────────────────────
# Helper: pretty banner
# ────────────────────────────────────────────────────────────────────────────
banner () {
  echo "──────────────────────────────────────────────────────────────"
  echo "▶ $*"
  echo "──────────────────────────────────────────────────────────────"
}

# ────────────────────────────────────────────────────────────────────────────
# Function: download  (update 45 samples)
# ────────────────────────────────────────────────────────────────────────────
download () {
  banner "Update samples"
  bash ./src/update_samples.sh
}

# ────────────────────────────────────────────────────────────────────────────
# Function: stitch  (combine & stitch today's batch)
# ────────────────────────────────────────────────────────────────────────────
stitch () {
  banner "Stitch data"
  BASE_ROOT="data"
  WORKERS=$(command -v nproc >/dev/null && nproc || python3 - <<<'import os; print(os.cpu_count())')

  for topic_dir in "$BASE_ROOT"/*; do
    topic=$(basename "$topic_dir")
    echo "▶ Stitching topic: $topic"

    # Find latest daily_samples/output_YYYY-MM-DD directory
    latest_sample_dir=$(ls -d "$topic_dir/daily_samples/output_"*/ 2>/dev/null | sort -V | tail -n 1)

    if [[ -z "$latest_sample_dir" ]]; then
      echo "❌ No output_* directory found in $topic_dir/daily_samples/, skipping."
      continue
    fi

    SAMPLES_ROOT="$latest_sample_dir"
    STITCHED_ROOT="$topic_dir/stitched_output"

    python3 src/stitching_job_v2.py \
        --samples-root  "$SAMPLES_ROOT" \
        --stitched-root "$STITCHED_ROOT" \
        --out-root      "$STITCHED_ROOT" \
        --workers       "$WORKERS"
  done
}

# ────────────────────────────────────────────────────────────────────────────
# Function: rescale_pickles  (scale residuals using stitched data)
# ────────────────────────────────────────────────────────────────────────────
rescale_pickles () {
  banner "Rescale Chebyshev residuals"
  BASE_ROOT="data"

  for topic_dir in "$BASE_ROOT"/*; do
    if [[ ! -d "$topic_dir/stitched_output" ]]; then
      echo "❌ No stitched_output in $topic_dir, skipping."
      continue
    fi

    echo "▶ Rescaling: $(basename "$topic_dir")"
    python3 src/scale_min_residual.py "$topic_dir"
  done
}




# ────────────────────────────────────────────────────────────────────────────
# Function: find_anomaly  (run your anomaly-detection pipeline)
# ────────────────────────────────────────────────────────────────────────────
find_anomaly () {
  banner "Find anomalies"
  bash ./src/find_anomaly.sh
}

# ────────────────────────────────────────────────────────────────────────────
# Function: notify  (send Slack messages)
# ────────────────────────────────────────────────────────────────────────────
notify () {
  banner "Slack notifications"
  python3 src/generate_slack_notification.py
}

# ────────────────────────────────────────────────────────────────────────────
# Main 
# ────────────────────────────────────────────────────────────────────────────

#download
stitch
rescale_pickles
find_anomaly
notify

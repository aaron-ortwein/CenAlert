#!/bin/bash
set -euo pipefail

# Dummy events file (since it's not used)
DUMMY_EVENTS="dummy_events.csv"
touch "$DUMMY_EVENTS"

# Algorithm constant
ALGORITHM="chebyshev"

# Path to your Python script
SCRIPT_PATH="src/run_one2.py"

# Top-level where your term subdirectories live
TERMS_ROOT="data"

# For every term directory under TERMS_ROOT...
for term_dir in "$TERMS_ROOT"/*; do
  # Strip trailing slash and leading path
  term=$(basename "${term_dir%/}")
  echo "=== Processing term: $term ==="

  # Within each term, these must exist:
PARAMS_DIR="${term_dir}/ChebyshevPreferredFinal_Scaled"
TIME_SERIES_DIR="${term_dir}/stitched_output"
OUTPUT_BASE="${term_dir}/final_output"

# Skip anything that doesn’t look like a proper term folder
missing=0

if [[ ! -d "$PARAMS_DIR" ]]; then
  echo "  › Missing: $PARAMS_DIR" >&2
  missing=1
fi

if [[ ! -d "$TIME_SERIES_DIR" ]]; then
  echo "  › Missing: $TIME_SERIES_DIR" >&2
  missing=1
fi

if [[ $missing -eq 1 ]]; then
  echo "  › Skipping ${term_dir}, due to missing required subdir(s)." >&2
  continue
fi


  # Make sure the per-term output directory exists
  mkdir -p "$OUTPUT_BASE"

  # Loop over each “parameter” file (one per country)…
  for param_file in "$PARAMS_DIR"/*; do
    country_code=$(basename "$param_file")
    time_series_file="${TIME_SERIES_DIR}/${country_code}_stitched.csv"

    if [[ ! -f "$time_series_file" ]]; then
      echo "  ⚠️  No time series for ${country_code}, skipping."
      continue
    fi

    # Run Python worker
    python "$SCRIPT_PATH" \
      --path "$time_series_file" \
      --events "$DUMMY_EVENTS" \
      --algorithm "$ALGORITHM" \
      --parameters "$param_file"

    # Move results into term-and-country specific dirs
    country_out="${OUTPUT_BASE}/${country_code}"
    mkdir -p "$country_out"

    if [[ -f "annotated.csv" ]]; then
      mv annotated.csv "${country_out}/annotated.csv"
      echo "    • Moved annotated.csv → ${country_out}/"
    fi

    if [[ -f "anomalies.csv" ]]; then
      mv anomalies.csv "${country_out}/anomalies.csv"
      echo "    • Moved anomalies.csv → ${country_out}/"
    fi
  done

  echo "=== Done term: $term ==="$'\n'
done

echo "All terms processed!"

# 🧵 Time Series Trend Aggregation Pipeline

This repository manages the collection, stitching, and storage of time series trend data (e.g., Google Trends) for multiple topics and countries. It supports repeated sampling and fusion of noisy snapshots into consistent, analyzable time series.

---

## 📁 Directory Structure

Your working `data/` directory should be organized as follows:

```
data/
  topic_1/
    daily_samples/
      output_2025-07-01/
      output_2025-07-02/
    stitched_output/
    final_output/   # optional
  topic_2/
    ...
```

Each topic folder contains:

- `daily_samples/`: Time-stamped output directories from repeated sampling
- `stitched_output/`: Combined time series output
- `final_output/`: Optional post-processed results (e.g., anomalies)

---

## 🚀 Bootstrapping the System (Manual Step)

Before the pipeline can run automatically, you **must manually bootstrap the first set of samples and stitched output**.

### 1. Sample the initial time series

Run the `update_samples` script with a `--start-month` argument to define when your time series should begin:

```bash
python3 src/update_samples.py --start-month YYYY-MM
```

This populates the `daily_samples/` directory for each topic with samples starting from the given month.

### 2. Stitch the initial samples

Once the samples are created, stitch them into the first complete time series using:

```bash
python3 src/firsttime_stitch.py --samples-root <latest_output_dir> --stitched-root <stitched_output_dir>
```

This creates the initial files like `US_stitched.csv` inside `stitched_output/`.

---

## 🔁 Running the Automatic Pipeline

After bootstrapping, you can use the `pipeline_driver.sh` script to update and stitch new data automatically. It:

1. Identifies the most recent `daily_samples/output_YYYY-MM-DD/`
2. Calls the stitching pipeline for each topic
3. Saves the updated stitched files to `stitched_output/`

### Example:

```bash
bash bin/pipeline_driver.sh
```

This script includes logic to determine the current date and automatically stitch all topics in `data/`.

---

## 📊 Database Integration

All database-related operations are handled through:

```
/bin/cenalert_db
```

Use this tool to:

- Load anomalies
- Start database
- Update anomalie and explanations
- Manage explanations

Make sure your database credentials and environment variables are set appropriately before using this tool.

---

## 🔧 Scripts Overview

| Script                       | Description                                               |
| ---------------------------- | --------------------------------------------------------- |
| `src/update_samples.py`      | Collects time series samples per topic                    |
| `src/firsttime_stitch.py`    | One-time initial stitching of bootstrapped samples        |
| `src/stitching_job_v2.py`    | Regular stitching logic used by the automated pipeline    |
| `bin/pipeline_driver.sh`     | Automates stitching across all topics using latest sample |
| `bin/cenalert_db`            | Database CLI for loading/storing data                     |

---

## ✅ Requirements

- Python 3.8+
- `pandas`, `numpy`, `polars`, `tqdm`
- PostgreSQL (for database integration)

Install Python dependencies:

```bash
pip install -r requirements.txt
```

---


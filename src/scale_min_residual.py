import os
import pickle
import polars as pl
import numpy as np
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description="Scale pickled residuals using stitched CSV values.")
parser.add_argument("base_dir", help="Base directory containing ChebyshevPreferredFinal, stitched_output, etc.")
args = parser.parse_args()

# Directories relative to base_dir
input_pickle_dir = os.path.join(args.base_dir, "ChebyshevPreferredFinal")
stitched_csv_dir = os.path.join(args.base_dir, "stitched_output")
output_pickle_dir = os.path.join(args.base_dir, "ChebyshevPreferredFinal_Scaled")

# Create output dir if not exists
os.makedirs(output_pickle_dir, exist_ok=True)

# Process each pickle file
for country_code in os.listdir(input_pickle_dir):
    pickle_path = os.path.join(input_pickle_dir, country_code)
    csv_path = os.path.join(stitched_csv_dir, f"{country_code}_stitched.csv")

    if not os.path.isfile(csv_path):
        print(f"[WARNING] No stitched output for {country_code}, skipping.")
        continue

    # Load pickle
    try:
        with open(pickle_path, "rb") as f:
            data = pickle.load(f)
    except Exception as e:
        print(f"[ERROR] Could not load {country_code}: {e}")
        continue

    window, z, k, min_residual, efficiency = data

    # Load CSV and compute scaling factor
    try:
        df = pl.read_csv(csv_path, schema_overrides={"value": pl.Float64})
        max_val = df["value"].max()
        scaling_factor = max_val / 100 if max_val > 100 else 1
    except Exception as e:
        print(f"[ERROR] Could not process CSV for {country_code}: {e}")
        continue

    # Scale residual and save
    new_min_residual = min_residual * scaling_factor
    new_data = (window, z, k, new_min_residual, efficiency)
    out_path = os.path.join(output_pickle_dir, country_code)

    with open(out_path, "wb") as f:
        pickle.dump(new_data, f)

    print(f"[OK] Scaled and saved for {country_code} with scale {scaling_factor:.4f}")

import os
import sys
import argparse
import warnings
import pickle

import polars as pl

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from common.detection import ChebyshevInequality, MedianMethod, IsolationForest, LocalOutlierFactor
# from lib.event_match import match_all

def main():
    warnings.filterwarnings('ignore')

    parser = argparse.ArgumentParser(description="Run anomaly detection on a single time series")
    parser.add_argument("--path", required=True, help="path to time series")
    parser.add_argument("--events", required=False, help="events to match against")
    parser.add_argument("--algorithm", required=True, help="anomaly detection algorithm to use", choices=["chebyshev", "median", "iforest", "lof"])
    parser.add_argument("--parameters", required=True, help="path to algorithm parameters")
    
    args = parser.parse_args()

    try:
        df = pl.read_csv(
            args.path,
            try_parse_dates=True,
            schema_overrides={"value": pl.Float64}
        )
        # events = pl.read_csv(args.events, try_parse_dates=True)
        with open(args.parameters, "rb") as file: parameters = list(pickle.load(file))
    except FileNotFoundError as e:
        print(e)
        exit(1)

    print(parameters)
    parameters[0] = round(parameters[0])

    if args.algorithm == "chebyshev":
        detector = ChebyshevInequality(*parameters)
    elif args.algorithm == "median":
        detector = MedianMethod(*parameters)
    elif args.algorithm == "iforest":
        detector = IsolationForest(*parameters)
    elif args.algorithm == "lof":
        detector = LocalOutlierFactor(*parameters)
    
    annotated = detector.run(df)
    anomalies = detector.anomalies()
    # matches = match_all(anomalies, events)
    # censorship_events = matches.filter((-3 <= pl.col("proximity")) & (pl.col("proximity") <= 3))
    
    

    print(anomalies)
    print(anomalies["impact"].sum())

    # === add quartile column based on impact ===
    # 0 being the lowest impact and 3 being the highest
    anomalies = anomalies.sort("impact")
    n = anomalies.height

    if n == 0:
        anomalies = anomalies.with_columns(pl.lit(None).alias("quartile"))
    else:
        if n < 4:
            quartiles = list(range(n))  # first always 0
        else:
            quartiles = (
                        anomalies.select(pl.col("impact").rank("ordinal").alias("rank"))["rank"] - 1
            ) * 4 // n
            quartiles = quartiles.cast(pl.Int64).to_list()

        anomalies = anomalies.with_columns(pl.Series("quartile", quartiles))

    annotated.write_csv("annotated.csv")
    anomalies.write_csv("anomalies.csv")

    # matches.sort("impact").write_csv("anomalies.csv")
    # censorship_events.sort("impact").write_csv("censorship.csv")

if __name__ == "__main__":
    main()

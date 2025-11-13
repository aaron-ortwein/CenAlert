import os
import sys
import argparse
import warnings
import pickle

import polars as pl

sys.path.append(os.path.dirname(os.path.realpath(__file__)))
from lib.detection import ChebyshevInequality, MedianMethod, IsolationForest, LocalOutlierFactor

def main():
    warnings.filterwarnings('ignore')

    parser = argparse.ArgumentParser(description="Run anomaly detection on a single time series")
    parser.add_argument("--path", required=True, help="path to time series")
    parser.add_argument("--events", required=False, help="events to match against")
    parser.add_argument("--algorithm", required=True, help="anomaly detection algorithm to use", choices=["chebyshev", "median", "iforest", "lof"])
    parser.add_argument("--parameters", required=True, help="path to algorithm parameters")
    parser.add_argument("--output", required=False, default=".", help="path to output directory")
    
    args = parser.parse_args()

    try:
        df = pl.read_csv(args.path, try_parse_dates=True)
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
    
    print(anomalies)
    print(anomalies["impact"].sum())
    
    os.makedirs(args.output, exist_ok=True)

    annotated.write_csv(os.path.join(args.output, "annotated.csv"))
    anomalies.sort("impact").write_csv(os.path.join(args.output, "anomalies.csv"))
    

if __name__ == "__main__":
    main()
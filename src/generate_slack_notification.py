import subprocess
import os
import logging
import sys
import csv
import requests
from math import inf
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timedelta, timezone
import pycountry

# Load Slack webhook URL from env; fail fast if missing
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
if not SLACK_WEBHOOK_URL:
    logging.error("Environment variable SLACK_WEBHOOK_URL is not set.")
    sys.exit(1)

ANOMALY_DIR = Path("final_output")
TOPIC = "VPN"

def scan_recent_anomalies():
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    a_day_before_yesterday = (datetime.now(timezone.utc) - timedelta(days=2)).date()

    ongoing_events = defaultdict(list)
    started_yesterday_events = []
    ended_before_yesterday = []

    for country_dir in ANOMALY_DIR.iterdir():
        csv_path = country_dir / "anomalies.csv"
        if not csv_path.exists():
            continue

        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                try:
                    start_d = datetime.strptime(row["start"], "%Y-%m-%d").date()
                    end_d = datetime.strptime(row["end"], "%Y-%m-%d").date()
                    impact = float(row["impact"])
                    score = float(row["score"])
                    residual = float(row.get("residual", 0))

                    if score != 0:
                        metric_name, metric_value = "score", score
                    else:
                        metric_name, metric_value = "residual", residual

                    entry_txt = (
                        f"{row['start']}→{row['end']} "
                        f"(impact {impact:.2f}, {metric_name} {metric_value:.2f})"
                    )

                    if end_d >= yesterday:
                        ongoing_events[country_dir.name].append(entry_txt)
                    if start_d == yesterday:
                        started_yesterday_events.append({
                            "country": country_dir.name,
                            "start": row["start"],
                            "end": row["end"],
                            "impact": impact,
                            metric_name: metric_value,
                        })
                    if end_d == a_day_before_yesterday:
                        ended_before_yesterday.append({
                            "country": country_dir.name,
                            "start": row["start"],
                            "end": row["end"],
                            "peak": row.get("peak", "N/A"),
                            "impact": impact,
                            metric_name: metric_value,
                        })
                except Exception as e:
                    logging.warning(f"Malformed row in {csv_path}: {e}")

    return ongoing_events, started_yesterday_events, ended_before_yesterday

def _post(text: str):
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=10)
        resp.raise_for_status()
        logging.info("Slack message sent.")
    except Exception as e:
        logging.error(f"Failed Slack message: {e}")

def country_name(code: str) -> str:
    country = pycountry.countries.get(alpha_2=code.upper())
    return country.name if country else code.upper()

def _country_line(country: str, events: list[str]) -> str:
    return f"*{country_name(country)}*:  " + "  |  ".join(events)

def send_slack_notification(ongoing_events, started_yesterday, ended_before_yesterday):
    def _metric(ev):
        if ev.get("score", 0):
            return "score", float(ev["score"])
        return "residual", float(ev.get("residual", 0))

    def _metric_line(label, value):
        if label == "score" and value:
            bound = 1 / value**2
            return (
                f"> Score: `{value:.2f}`  "
                f"_(No more than a {bound*100:.4f}% probability to deviate this much)_"
            )
        elif label == "score":
            return f"> Score: `{value:.2f}`"
        else:
            return f"> Residual: `{value:.2f}`"

    # New spikes
    for ev in started_yesterday:
        label, val = _metric(ev)
        msg = (
            f"*New spike detected in {TOPIC} searches*\n"
            f"> Country: *{country_name(ev['country'])}*\n"
            f"> Date: `{ev['start']}`\n"
            f"{_metric_line(label, val)}\n"
            f"> Impact: `{ev['impact']:.2f}`"
        )
        _post(msg)

    # Ended spikes
    for ev in ended_before_yesterday:
        label, val = _metric(ev)
        peak = ev.get("peak", "N/A")
        msg = (
            f"*Spike just ended in {TOPIC} searches*\n"
            f"> Country: *{country_name(ev['country'])}*\n"
            f"> Period: `{ev['start']} → {ev['end']}`\n"
            f"> Peak: `{peak}`\n"
            f"{_metric_line(label, val)}\n"
            f"> Impact: `{ev['impact']:.2f}`"
        )
        _post(msg)

    # Ongoing anomalies
    if ongoing_events:
        header = "*Ongoing Anomalies*"
        body = [f"> {_country_line(c, evts)}" for c, evts in sorted(ongoing_events.items())]
        _post("\n".join([header, *body]))

def main():
    ongoing, started, ended = scan_recent_anomalies()
    send_slack_notification(ongoing, started, ended)

if __name__ == "__main__":
    main()

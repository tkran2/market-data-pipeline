#!/usr/bin/env python3
"""Summarize native-only benchmark trials without conflating them with capture."""
import argparse
import csv
import json
import statistics

parser = argparse.ArgumentParser()
parser.add_argument("csv")
args = parser.parse_args()
with open(args.csv, newline="") as stream:
    rows = list(csv.DictReader(stream))
summary = {}
for profile in sorted({row["profile"] for row in rows}):
    trials = [row for row in rows if row["profile"] == profile]
    summary[profile] = {
        "trials": len(trials), "levels": int(trials[0]["levels"]),
        "events_per_trial": int(trials[0]["events"]),
        "median_events_per_second": statistics.median(float(row["events_per_second"]) for row in trials),
        "median_p50_ns": statistics.median(int(row["p50_ns"]) for row in trials),
        "median_p99_ns": statistics.median(int(row["p99_ns"]) for row in trials),
    }
print(json.dumps({"scope": "C++ book only; excludes network, JSON, IPC and journal commits", "profiles": summary}, indent=2))

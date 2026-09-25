#!/usr/bin/env python3
"""Summarize line coverage from a JaCoCo CSV report.

Prints a Markdown summary line, and with --badge also writes a shields.io endpoint badge JSON file.
"""
import argparse
import csv
import json

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("csv", help="JaCoCo CSV report, e.g. target/site/jacoco/jacoco.csv")
parser.add_argument("--badge", help="write shields.io endpoint badge JSON to this file")
args = parser.parse_args()

missed = covered = 0
with open(args.csv, newline="") as f:
    for row in csv.DictReader(f):
        missed += int(row["LINE_MISSED"])
        covered += int(row["LINE_COVERED"])
total = missed + covered
pct = 100 * covered / total

print(f"Line coverage: **{pct:.1f}%** ({covered} of {total} lines)")

if args.badge:
    color = "brightgreen" if pct >= 80 else "yellow" if pct >= 60 else "red"
    with open(args.badge, "w") as f:
        json.dump({"schemaVersion": 1, "label": "coverage", "message": f"{pct:.1f}%", "color": color}, f)

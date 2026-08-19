#!/usr/bin/env bash
# Award the local average to Cal Club drivers who ran the Vegas tour instead of
# the 04-03-2022 local event, per their online requests.
#
# Usage: -a <car number> -n <class> -d <MM-DD-YYYY>
set -euo pipefail

date="04-03-2022"

while read -r car class; do
    ./clubpoints.py -a "$car" -n "$class" -d "$date"
done <<'ENTRIES'
0   PAX
3   PAX
187 PAX
87  PAXL
19  PAX
97  CS
80  STS
170 PAX
500 PAX
70  PAXL
ENTRIES

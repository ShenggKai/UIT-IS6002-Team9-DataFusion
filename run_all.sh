#!/usr/bin/env bash
# Sinh du lieu, chay thi nghiem, ve bieu do.
# Script nay duoc giu de may Linux/macOS. Phien kiem chung Windows khong chay lai file nay.
set -euo pipefail
cd "$(dirname "$0")"
python src/gen_data.py --rows 5000000 --seed 42 --out data
python src/run_experiments.py --data data --out results-rerun --runs 7 --parallelism 1 2 --seed 42
python src/make_charts.py results-rerun

#!/usr/bin/env bash
# Chạy toàn bộ cài đặt minh họa: sinh dữ liệu -> thí nghiệm -> biểu đồ
set -euo pipefail
python src/gen_data.py --rows 5000000 --out data
python src/run_experiments.py --data data --out results --runs 7
python src/make_charts.py results

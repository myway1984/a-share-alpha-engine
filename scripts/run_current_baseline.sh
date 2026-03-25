#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

END_DATE="${1:-2026-03-24}"

PYTHONPATH=src python3 -m qstrategy_v2.cli \
  --provider tushare \
  --start-date 2024-09-01 \
  --end-date "$END_DATE" \
  --top-n 15 \
  --buffer-rank 25 \
  --rebalance-interval-trade-days 5 \
  --min-holding-trade-days 10 \
  --max-new-positions-per-rebalance 2 \
  --exclude-factors price_volume_corr,sue \
  --output-dir reports/current_baseline

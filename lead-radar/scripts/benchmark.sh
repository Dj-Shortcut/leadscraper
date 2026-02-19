#!/usr/bin/env bash
set -euo pipefail

INPUT_DIR="${1:-data/raw}"
POSTCODES="${2:-9400}"
MONTHS="${3:-18}"
CHUNKSIZE="${4:-200000}"
OUT_DIR="${5:-data/processed}"

mkdir -p "$OUT_DIR"
NORMAL_OUT="$OUT_DIR/benchmark_normal.csv"
FAST_OUT="$OUT_DIR/benchmark_fast.csv"

run_cmd() {
  local mode="$1"
  shift
  local start end elapsed
  start=$(python - <<'PY'
import time
print(time.perf_counter())
PY
)
  "$@"
  end=$(python - <<'PY'
import time
print(time.perf_counter())
PY
)
  elapsed=$(python - <<PY
start=float("$start")
end=float("$end")
print(f"{end-start:.2f}")
PY
)
  echo "$mode runtime: ${elapsed}s"
}

echo "Benchmark input=$INPUT_DIR postcodes=$POSTCODES months=$MONTHS limit=0"

if ! python - <<'PY'
import importlib.util
import sys
sys.exit(0 if importlib.util.find_spec("pandas") else 1)
PY
then
  echo "WARNING: pandas is not installed; skipping fast benchmark run."
  exit 0
fi

run_cmd "normal" python -m src.cli --input "$INPUT_DIR" --output "$NORMAL_OUT" --postcodes "$POSTCODES" --months "$MONTHS" --limit 0 --verbose
run_cmd "fast" python -m src.cli --input "$INPUT_DIR" --output "$FAST_OUT" --postcodes "$POSTCODES" --months "$MONTHS" --limit 0 --fast --chunksize "$CHUNKSIZE" --verbose

normal_count=$(python - <<PY
import csv
from pathlib import Path
p=Path("$NORMAL_OUT")
with p.open("r",encoding="utf-8",newline="") as h:
    print(sum(1 for _ in csv.DictReader(h)))
PY
)
fast_count=$(python - <<PY
import csv
from pathlib import Path
p=Path("$FAST_OUT")
with p.open("r",encoding="utf-8",newline="") as h:
    print(sum(1 for _ in csv.DictReader(h)))
PY
)

echo "normal count: $normal_count"
echo "fast count:   $fast_count"

echo "Outputs: $NORMAL_OUT and $FAST_OUT"

#!/usr/bin/env bash
# 一键启动：生成基准数据 -> 来源服务 (:8081) -> 工作台 (:8080)
set -euo pipefail
cd "$(dirname "$0")/.."

DATA_DIR="${DATA_DIR:-./.runtime/ground_truth}"
DB_PATH="${DB_PATH:-./.runtime/workbench.db}"
SOURCE_PORT="${SOURCE_PORT:-8081}"
BENCH_PORT="${BENCH_PORT:-8080}"

python3 -m collect_workbench.init_data --out "$DATA_DIR"
echo "来源服务: http://127.0.0.1:$SOURCE_PORT"
python3 -m collect_workbench.source_server \
  --data-dir "$DATA_DIR" --port "$SOURCE_PORT" &
SRC_PID=$!
echo "工作台:   http://127.0.0.1:$BENCH_PORT  (Ctrl-C 退出)"
python3 -m collect_workbench.webapp \
  --db "$DB_PATH" \
  --source "http://127.0.0.1:$SOURCE_PORT" \
  --ground-truth "$DATA_DIR" \
  --port "$BENCH_PORT" &
WB_PID=$!
trap 'kill $SRC_PID $WB_PID 2>/dev/null || true' EXIT
wait

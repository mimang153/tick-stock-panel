#!/usr/bin/env bash
# 手动重建后端测试环境并运行 pytest（供本地/受限环境使用）。
#
# 背景：仓库自带的 backend/.venv 属 root 且为空壳，当前用户无法写入；系统 python
# 也没有 polars 等依赖，直接 python -m pytest 会卡在 import。本脚本用 uv 在一个
# 可写目录重建完整依赖环境（含 dev extras），再用该环境跑测试。
#
# 用法：
#   bash run_tests.sh                      # 全量测试
#   bash run_tests.sh -q tests/test_xxx.py # 透传任意 pytest 参数
#   PYTEST_VENV=/my/writable/venv bash run_tests.sh -q tests/  # 自定义 venv 路径
#
# 环境变量：
#   PYTEST_VENV   venv 目标目录（默认 backend/.venv-pytest，需可写）
#   UV_BIN        uv 可执行程序（默认 uv）
#   PYTEST_BIN    pytest 可执行程序（默认 <venv>/bin/python -m pytest）

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$REPO_ROOT/backend"
VENV_DIR="${PYTEST_VENV:-$BACKEND_DIR/.venv-pytest}"
UV_BIN="${UV_BIN:-uv}"

if ! command -v "$UV_BIN" >/dev/null 2>&1; then
  echo "错误: 找不到 uv（尝试: export UV_BIN=/path/to/uv 或安装 uv）。" >&2
  exit 2
fi

echo "[1/3] 用 uv 在可写目录同步依赖（含 dev extras）: $VENV_DIR"
(
  cd "$BACKEND_DIR"
  UV_PROJECT_ENVIRONMENT="$VENV_DIR" "$UV_BIN" sync --frozen --extra dev
)

VENV_PY="$VENV_DIR/bin/python"
if [ ! -x "$VENV_PY" ]; then
  echo "错误: venv python 不存在: $VENV_PY" >&2
  exit 2
fi

echo "[2/3] 校验关键依赖可导入"
"$VENV_PY" -c "import polars, pytest, pytest_asyncio, fastapi; print('deps OK: polars', polars.__version__, '/ pytest', pytest.__version__)"

echo "[3/3] 运行 pytest（禁用 cache 写入，规避只读 .pytest_cache）"
cd "$BACKEND_DIR"
if [ -n "${PYTEST_BIN:-}" ]; then
  # 允许覆盖为外部 pytest；否则用 venv 内 python -m pytest
  exec "$PYTEST_BIN" -p no:cacheprovider "$@"
fi
exec "$VENV_PY" -m pytest -p no:cacheprovider "$@"

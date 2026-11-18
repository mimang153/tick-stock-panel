#!/usr/bin/env bash
# 后端 .py 文件物理行数门禁（pre-commit 增量钩子后端脚本）。
#
# 用法：pre-commit 把本次改动的 backend/**/*.py 以绝对/相对路径 $@ 传入。
# 对每个文件统计物理行数（awk 数行，等价 wc -l），超过阈值即失败。
# 阈值默认 1000，可用环境变量 FILE_LINE_LIMIT 覆盖。
#
# 与 .pre-commit-config.yaml 的设计保持一致：
#   - 增量执行：只检查本次改动触碰的文件，存量历史文件不连坐；
#   - 阶段 4 已完成超长文件拆分，目录级/批量文件豁免已收敛；
#     仅未拆分的存量巨型文件在 config 的 exclude 中按单文件显式登记，
#     并同步在整改计划中记录为待重构技术债；
#   - 新代码不受豁免影响，从一开始就应该控制在合理行数内。
set -u

THRESHOLD="${FILE_LINE_LIMIT:-1000}"
fail=0

for f in "$@"; do
  # 忽略非普通文件/目录
  [ -f "$f" ] || continue

  # 仅统计 Python 文件
  case "$f" in
    *.py) ;;
    *) continue ;;
  esac

  n="$(awk 'END{print NR}' "$f" 2>/dev/null || echo 0)"

  if [ -n "$n" ] && [ "$n" -gt "$THRESHOLD" ]; then
    echo "文件行数超标: $f ($n 行 > $THRESHOLD 行)。"
    echo "  - 新代码建议拆分到多模块；确属业务必要的巨型文件，"
    echo "    请在 .pre-commit-config.yaml 的 backend-file-length/exclude 及"
    echo "    backend/pyproject.toml 的 per-file-ignores 中显式豁免（勿用于日常小改动）。"
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "文件行数门禁未通过。"
  exit 1
fi

exit 0

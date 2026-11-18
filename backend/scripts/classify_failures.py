"""把全量失败清单按"单独跑是否仍失败"分类（诊断用，只读）。

用法：容器内 `python scripts/classify_failures.py /tmp/fail_after_pilot.txt`
逐文件起独立 pytest 进程跑该文件的失败用例，输出：
- STABLE 单独跑仍失败（真实迁移债务）
- FLAKY  单独跑通过（全量并跑时的状态污染/顺序依赖）
"""

from __future__ import annotations

import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

PY = sys.executable


def main() -> None:
    lines = Path(sys.argv[1]).read_text(encoding="utf-8").splitlines()
    node_ids = [
        re.sub(r"^FAILED\s+", "", line).split(" - ")[0].strip()
        for line in lines
        if line.startswith("FAILED")
    ]
    by_file: dict[str, list[str]] = defaultdict(list)
    for node in node_ids:
        file, _, _ = node.partition("::")
        by_file[file].append(node)

    stable, flaky = [], []  # type: ignore[var-annotated]
    for file, nodes in sorted(by_file.items()):
        # 每个文件独立进程，只跑该文件的失败用例
        proc = subprocess.run(
            [PY, "-m", "pytest", "-q", "--tb=no", *nodes],
            cwd="/app/backend",
            capture_output=True,
            text=True,
        )
        # 从输出解析哪些 node 仍失败
        still_failed = set()
        for line in proc.stdout.splitlines():
            if line.startswith("FAILED "):
                still_failed.add(line.split(" - ")[0].replace("FAILED ", "").strip())
        for node in nodes:
            (stable if node in still_failed else flaky).append(node)
        tag = "STABLE" if still_failed else "FLAKY "
        print(f"{tag} {file}: {len(still_failed)}/{len(nodes)} stable", flush=True)

    print("\n=== 汇总 ===")
    print(f"STABLE {len(stable)} / FLAKY {len(flaky)} / TOTAL {len(node_ids)}")
    Path("/tmp/failures_stable.txt").write_text("\n".join(stable), encoding="utf-8")
    Path("/tmp/failures_flaky.txt").write_text("\n".join(flaky), encoding="utf-8")


if __name__ == "__main__":
    main()
